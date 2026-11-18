import os

import boto3
"""TPC-H 验证作业 (Daft on Ray): Q1 / Q3 / Q6, 核对 SF1 的标准答案并把 Q3 结果写回输出桶。

用法 (在 Ray head 上):
  ray job submit --address http://127.0.0.1:8265 --working-dir /tmp/job -- python tpch.py --sf 1

环境变量 DAFT_RUNNER=ray 已在镜像/RayCluster 里设置; 这里也显式调用 set_runner_ray, 保证走分布式。
"""
import argparse
import datetime as dt
import time

import daft
from daft import DataType, col
from daft.io import IOConfig, S3Config

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
ACCOUNT = os.environ.get("AWS_ACCOUNT_ID") or boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
INPUT = f"s3://daft-ray-input-{ACCOUNT}-{REGION}/tpch"
OUTPUT = f"s3://daft-ray-output-{ACCOUNT}-{REGION}/tpch"

# DuckDB 生成的 DECIMAL(15,2) 列统一转 float64 再算 (和 Daft 官方 TPC-H 实现的做法类似), 避免 decimal 运算差异
DECIMAL_COLS = ["l_quantity", "l_extendedprice", "l_discount", "l_tax", "o_totalprice", "c_acctbal"]


def d(y, m, day):
    return dt.date(y, m, day)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", default="1")
    ap.add_argument("--repeat", type=int, default=1, help="重复跑 Q1/Q3/Q6 的轮数, 方便在 Dashboard/Grafana 里持续观察资源")
    args = ap.parse_args()

    daft.set_runner_ray()
    io = IOConfig(s3=S3Config(region_name=REGION))

    def read(table):
        df = daft.read_parquet(f"{INPUT}/sf{args.sf}/{table}/*.parquet", io_config=io)
        for c in DECIMAL_COLS:
            if c in df.column_names:
                df = df.with_column(c, col(c).cast(DataType.float64()))
        return df

    results = {}

    def timed(name, fn):
        t0 = time.time()
        out = fn()
        dt_s = time.time() - t0
        print(f"TIMING {name} {dt_s:.2f}s", flush=True)
        results[name] = out
        return out

    # Q6
    def q6():
        li = read("lineitem")
        df = li.where(
            (col("l_shipdate") >= d(1994, 1, 1))
            & (col("l_shipdate") < d(1995, 1, 1))
            & (col("l_discount") >= 0.05)
            & (col("l_discount") <= 0.07)
            & (col("l_quantity") < 24)
        ).agg((col("l_extendedprice") * col("l_discount")).sum().alias("revenue"))
        return df.to_pydict()

    # Q1
    def q1():
        li = read("lineitem")
        df = (
            li.where(col("l_shipdate") <= d(1998, 9, 2))
            .groupby("l_returnflag", "l_linestatus")
            .agg(
                col("l_quantity").sum().alias("sum_qty"),
                col("l_extendedprice").sum().alias("sum_base_price"),
                (col("l_extendedprice") * (1 - col("l_discount"))).sum().alias("sum_disc_price"),
                (col("l_extendedprice") * (1 - col("l_discount")) * (1 + col("l_tax"))).sum().alias("sum_charge"),
                col("l_quantity").mean().alias("avg_qty"),
                col("l_extendedprice").mean().alias("avg_price"),
                col("l_discount").mean().alias("avg_disc"),
                col("l_orderkey").count().alias("count_order"),
            )
            .sort(["l_returnflag", "l_linestatus"])
        )
        return df.to_pydict()

    # Q3 (两次 join + 聚合 + 排序, 会产生 shuffle); 结果写回输出桶验证写权限
    def q3():
        cust = read("customer").where(col("c_mktsegment") == "BUILDING")
        orders = read("orders").where(col("o_orderdate") < d(1995, 3, 15))
        li = read("lineitem").where(col("l_shipdate") > d(1995, 3, 15))
        df = (
            cust.join(orders, left_on="c_custkey", right_on="o_custkey")
            .join(li, left_on="o_orderkey", right_on="l_orderkey")
            .groupby("o_orderkey", "o_orderdate", "o_shippriority")
            .agg((col("l_extendedprice") * (1 - col("l_discount"))).sum().alias("revenue"))
            .sort(["revenue", "o_orderdate"], desc=[True, False])
            .limit(10)
        )
        df.write_parquet(f"{OUTPUT}/sf{args.sf}/q3/", io_config=io)
        return df.to_pydict()

    for i in range(args.repeat):
        print(f"ROUND {i + 1}/{args.repeat}", flush=True)
        timed("q6", q6)
        timed("q1", q1)
        timed("q3", q3)

    print("RESULT_Q6", results["q6"], flush=True)
    print("RESULT_Q1", {k: [round(x, 4) if isinstance(x, float) else x for x in v] for k, v in results["q1"].items()}, flush=True)
    q3r = results["q3"]
    print("RESULT_Q3_TOP", q3r["o_orderkey"][0], round(q3r["revenue"][0], 4), q3r["o_orderdate"][0], q3r["o_shippriority"][0], flush=True)

    if args.sf == "1":
        ok = True
        ok &= abs(results["q6"]["revenue"][0] - 123141078.2283) < 0.01
        ok &= results["q1"]["count_order"] == [1478493, 38854, 2920374, 1478870]
        ok &= q3r["o_orderkey"][0] == 2456423
        print("VALIDATION", "PASS" if ok else "FAIL", flush=True)
        if not ok:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
