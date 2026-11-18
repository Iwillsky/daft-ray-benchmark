"""用 DuckDB 在同一份 S3 数据集上独立计算 TPC-H Q1-Q22 的参考结果, 存到 <ref-root>/sf<SF>/q<N>.json
(默认放输出桶: 工作负载的 IAM 角色对输入桶只读, 不写回输入桶)。

为什么需要: TPC-H 官方标准答案只有 SF1 (DuckDB 的 tpch_answers() 也只有 SF0.01/0.1/1)。SF100 以上没有公开标准答案,
所以用另一个引擎 (DuckDB) 在同一份数据上算一遍作为参照, 和 Daft 分布式执行的结果交叉验证。
注意它验证的是"Daft 的查询实现与分布式执行是否正确", 不能证明数据本身符合 TPC-H 规范
(数据规范性靠两点: 生成器行数与规范逐表核对; 同一条流水线生成的 SF1 与官方答案一致)。

对 DuckDB 自带的 SQL 做了两处与 Daft 侧一致的调整:
  - Q11: 阈值系数 0.0001000000 (SF1 的值) 换成规范的 0.0001/SF
  - Q15: 与最大值的浮点相等比较改成相对误差 1e-9 (DuckDB 里 CTE 可能被计算两次, 并行求和顺序不同会让浮点相等失败)

两种运行方式 (都需要镜像里预装的 duckdb + httpfs 扩展, 节点离线可用):
  --local  直接在当前进程里跑 (用于 K8s Job 跑在一台大内存节点上; 通过 boto3 取 IRSA 临时凭证读 S3)
  默认     提交成一个 Ray remote task 跑在某个 worker 上 (需要 worker 足够大)
"""
import argparse
import json
import os
import time

import boto3

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
ACCOUNT = os.environ.get("AWS_ACCOUNT_ID") or boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
DEFAULT_ROOT = f"s3://daft-ray-input-{ACCOUNT}-{REGION}/tpch-v2"
DEFAULT_REF_ROOT = f"s3://daft-ray-output-{ACCOUNT}-{REGION}/tpch-v2/reference"
TABLES = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]


def cell(v):
    import datetime
    from decimal import Decimal

    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()[:10]
    if isinstance(v, Decimal):
        return float(v)
    return v


def patch_sql(q, sql, sf):
    if q == 11:
        assert "0.0001000000" in sql
        sql = sql.replace("0.0001000000", repr(0.0001 / sf))
    if q == 15:
        assert "AND total_revenue = (" in sql
        sql = sql.replace("AND total_revenue = (", "AND total_revenue >= (1 - 1e-9) * (")
    return sql


def run_reference(sf, root, ref_root, questions, queries, threads, mem_gb, temp_dir, skip_existing=False):
    import duckdb

    ref_bucket, ref_prefix = ref_root[len("s3://"):].split("/", 1)
    s3 = boto3.client("s3", region_name=REGION)
    sess = boto3.Session()

    con = duckdb.connect()
    con.execute("LOAD httpfs")
    con.execute(f"SET threads={threads}")
    con.execute(f"SET memory_limit='{mem_gb}GB'")
    os.makedirs(temp_dir, exist_ok=True)
    con.execute(f"SET temp_directory='{temp_dir}'")

    def refresh_secret():
        c = sess.get_credentials().get_frozen_credentials()
        con.execute(
            f"CREATE OR REPLACE SECRET s3sec (TYPE s3, KEY_ID '{c.access_key}', SECRET '{c.secret_key}', "
            f"SESSION_TOKEN '{c.token}', REGION '{REGION}')"
        )

    refresh_secret()
    for t in TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{root}/sf{sf}/{t}/*.parquet')")  # DuckDB 列名大小写不敏感

    out = []
    for q in questions:
        if skip_existing:  # 进程被 OOM 杀掉后重跑时, 跳过已经算好的题
            try:
                s3.head_object(Bucket=ref_bucket, Key=f"{ref_prefix}/sf{sf}/q{q}.json")
                print(f"REFERENCE q{q} exists, skipped", flush=True)
                continue
            except Exception:
                pass
        refresh_secret()  # 临时凭证约 1 小时过期, 每题前刷新 (单题超过 1 小时会失败)
        t0 = time.time()
        try:
            cur = con.execute(patch_sql(q, queries[str(q)], sf))
            cols = [d[0] for d in cur.description]
            rows = [[cell(v) for v in r] for r in cur.fetchall()]
            secs = round(time.time() - t0, 2)
            body = json.dumps({"engine": f"duckdb {duckdb.__version__}", "seconds": secs, "columns": cols, "rows": rows})
            s3.put_object(Bucket=ref_bucket, Key=f"{ref_prefix}/sf{sf}/q{q}.json", Body=body.encode())
            print(f"REFERENCE q{q} rows={len(rows)} seconds={secs}", flush=True)
            out.append((q, len(rows), secs))
        except Exception as e:
            print(f"REFERENCE q{q} FAILED after {time.time() - t0:.0f}s: {type(e).__name__}: {str(e)[:300]}", flush=True)
            out.append((q, "FAILED", str(e)[:100]))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--root", default=DEFAULT_ROOT)
    ap.add_argument("--ref-root", default=DEFAULT_REF_ROOT)
    ap.add_argument("--questions", default=",".join(str(i) for i in range(1, 23)))
    ap.add_argument("--local", action="store_true", help="不用 Ray, 直接在当前进程里跑 DuckDB")
    ap.add_argument("--skip-existing", action="store_true", help="跳过 ref-root 里已存在的 q<N>.json (用于被 OOM 杀掉后续跑)")
    ap.add_argument("--threads", type=int, default=88)
    ap.add_argument("--mem-gb", type=int, default=520)
    ap.add_argument("--temp-dir", default="/tmp/ray/duckdb_tmp")
    ap.add_argument("--queries-file", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "tpch_duckdb_queries.json"))
    args = ap.parse_args()
    with open(args.queries_file) as f:
        queries = json.load(f)["queries"]
    qs = [int(x) for x in args.questions.split(",")]
    if args.local:
        print(run_reference(args.sf, args.root, args.ref_root, qs, queries, args.threads, args.mem_gb, args.temp_dir, args.skip_existing), flush=True)
    else:
        import ray

        ray.init(address="auto")
        remote = ray.remote(num_cpus=args.threads)(run_reference)
        print(ray.get(remote.remote(args.sf, args.root, args.ref_root, qs, queries, args.threads, args.mem_gb, args.temp_dir)), flush=True)


if __name__ == "__main__":
    main()
