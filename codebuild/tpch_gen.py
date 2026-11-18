"""生成 TPC-H Parquet 数据集 (布局与类型对齐 Daft 官方 TPC-H benchmark), 并上传到 S3。

参考: https://docs.getdaft.io/en/stable/benchmarks/  (Detailed Benchmarking Setup -> Data)
  - Parquet on S3; 每张表拆成 N 个文件 (SF100=32 个, SF1000=512 个), 同一张表的文件平铺在 <表名>/ 前缀下
  - 列名为大写 (Daft benchmarking/tpch/data_generation.py 的 SCHEMA), 类型为 int64 / float64 / date / string
    (Daft 原流程是 dbgen CSV -> daft.read_csv 推断类型 -> write_parquet; 这里用 DuckDB 的 dbgen 直接生成同样内容,
     DECIMAL 列转 DOUBLE 以对齐 Daft 推断出的 float64)
  - nation / region 是静态小表, 单文件 (Daft 原流程也不拆分这两张表)

环境变量:
  SF         规模因子 (整数, 如 1 / 100 / 1000)
  CHILDREN   每张大表拆成多少个文件 (dbgen 的 children 参数)。SF1000 用 512, SF100 用 32, SF1 用 8
  PROCS      并行进程数 (内存 ~ 每进程 SF/CHILDREN * 2GB; XLARGE ARM=64GB 内存可用 12)
  DATA_BUCKET, DATA_ROOT (默认 tpch-v2)
输出: s3://$DATA_BUCKET/$DATA_ROOT/sf$SF/<table>/<table>-<NNNN>.parquet 和 sf$SF/_manifest.json
"""
import json
import os
import shutil
import subprocess
import sys
import time
from multiprocessing import Pool

import duckdb

SF = int(os.environ["SF"])
CHILDREN = int(os.environ["CHILDREN"])
PROCS = int(os.environ.get("PROCS", "4"))
BUCKET = os.environ["DATA_BUCKET"]
ROOT = os.environ.get("DATA_ROOT", "tpch-v2")
PREFIX = f"s3://{BUCKET}/{ROOT}/sf{SF}"
WORK = "/tmp/tpch_work"

PARTITIONED = ["lineitem", "orders", "customer", "part", "partsupp", "supplier"]
STATIC = ["nation", "region"]

# 精确行数: 表规模按 TPC-H 规范线性于 SF; lineitem 每个 SF 不是精确整数, 取已知值 (SF1 / SF100 来自本集群的实测)
EXPECTED_ROWS = {
    "orders": 1_500_000 * SF,
    "customer": 150_000 * SF,
    "part": 200_000 * SF,
    "partsupp": 800_000 * SF,
    "supplier": 10_000 * SF,
    "nation": 25,
    "region": 5,
}
KNOWN_LINEITEM = {1: 6_001_215, 100: 600_037_902}


def select_sql(con, table):
    """DECIMAL -> DOUBLE, 列名转大写, 其余类型原样 (BIGINT/DATE/VARCHAR)。"""
    cols = con.execute(f"DESCRIBE {table}").fetchall()
    parts = []
    for name, typ, *_ in cols:
        if typ.startswith("DECIMAL"):
            parts.append(f"CAST({name} AS DOUBLE) AS {name.upper()}")
        else:
            parts.append(f"{name} AS {name.upper()}")
    return ", ".join(parts)


def run(cmd):
    subprocess.run(cmd, check=True)


def work(step):
    for attempt in range(3):
        try:
            return do_chunk(step)
        except Exception as e:  # 临时性失败 (S3/网络) 重试
            print(f"chunk {step} attempt {attempt + 1} failed: {e!r}", flush=True)
            shutil.rmtree(f"{WORK}/{step}", ignore_errors=True)
            time.sleep(5)
    raise RuntimeError(f"chunk {step} failed 3 times")


def do_chunk(step):
    out = f"{WORK}/{step}"
    shutil.rmtree(out, ignore_errors=True)
    con = duckdb.connect()
    con.execute("LOAD tpch")
    con.execute("SET threads=2")
    con.execute(f"CALL dbgen(sf={SF}, children={CHILDREN}, step={step})")
    counts, static_rows = {}, {}
    for t in PARTITIONED:
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        counts[t] = n
        if n:
            os.makedirs(f"{out}/{t}", exist_ok=True)
            con.execute(f"COPY (SELECT {select_sql(con, t)} FROM {t}) TO '{out}/{t}/{t}-{step:04d}.parquet' (FORMAT parquet)")
    for t in STATIC:  # nation/region 在各 chunk 里是切片, 返回给父进程合并成单文件
        static_rows[t] = con.execute(f"SELECT * FROM {t}").fetchall()
    con.close()
    if os.path.isdir(out):
        run(["aws", "s3", "cp", "--recursive", out, PREFIX + "/", "--only-show-errors"])
    shutil.rmtree(out, ignore_errors=True)
    return step, counts, static_rows


def s3_summary(table):
    r = subprocess.run(["aws", "s3", "ls", f"{PREFIX}/{table}/", "--recursive", "--summarize"], capture_output=True, text=True, check=True)
    files = size = 0
    for line in r.stdout.splitlines():
        if line.strip().startswith("Total Objects:"):
            files = int(line.split(":")[1])
        if line.strip().startswith("Total Size:"):
            size = int(line.split(":")[1])
    return files, size


def main():
    t0 = time.time()
    os.makedirs(WORK, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL tpch")  # 只在父进程安装一次, 子进程 LOAD
    con.close()

    totals = {t: 0 for t in PARTITIONED}
    static = {t: [] for t in STATIC}
    done = 0
    with Pool(PROCS) as pool:
        for step, counts, static_rows in pool.imap_unordered(work, range(CHILDREN)):
            for t, n in counts.items():
                totals[t] += n
            for t, rows in static_rows.items():
                static[t].extend(rows)
            done += 1
            if done % 8 == 0 or done == CHILDREN:
                print(f"chunks done {done}/{CHILDREN}  elapsed {time.time() - t0:.0f}s", flush=True)

    # nation / region: 合并所有切片, 去重后写成单文件
    con = duckdb.connect()
    con.execute("LOAD tpch")
    con.execute("CALL dbgen(sf=0.01)")  # 仅为拿表结构; 数据用各 chunk 汇总的行
    for t in STATIC:
        rows = sorted(set(static[t]))
        con.execute(f"DELETE FROM {t}")
        for r in rows:
            con.execute(f"INSERT INTO {t} VALUES ({','.join('?' * len(r))})", list(r))
        totals[t] = len(rows)
        os.makedirs(f"{WORK}/static/{t}", exist_ok=True)
        con.execute(f"COPY (SELECT {select_sql(con, t)} FROM {t} ORDER BY 1) TO '{WORK}/static/{t}/{t}-0000.parquet' (FORMAT parquet)")
    run(["aws", "s3", "cp", "--recursive", f"{WORK}/static", PREFIX + "/", "--only-show-errors"])

    # ---- 校验 ----
    problems = []
    for t, exp in EXPECTED_ROWS.items():
        if totals[t] != exp:
            problems.append(f"{t}: rows {totals[t]} != expected {exp}")
    li = totals["lineitem"]
    if SF in KNOWN_LINEITEM and li != KNOWN_LINEITEM[SF]:
        problems.append(f"lineitem: rows {li} != known {KNOWN_LINEITEM[SF]}")
    if abs(li - 6_000_000 * SF) > 0.001 * 6_000_000 * SF:
        problems.append(f"lineitem: rows {li} not within 0.1% of 6M*SF")

    files = {}
    for t in PARTITIONED + STATIC:
        n_files, size = s3_summary(t)
        files[t] = {"files": n_files, "bytes": size}
        want = CHILDREN if t in PARTITIONED else 1
        if n_files != want:
            problems.append(f"{t}: S3 has {n_files} files, expected {want}")

    manifest = {
        "scale_factor": SF,
        "children": CHILDREN,
        "generator": f"duckdb {duckdb.__version__} tpch dbgen(children={CHILDREN}); DECIMAL->DOUBLE, upper-case column names",
        "layout": "flat: <table>/<table>-NNNN.parquet (aligned with Daft TPC-H benchmark: SF100=32 files/table, SF1000=512)",
        "rows": totals,
        "s3": files,
        "problems": problems,
        "elapsed_seconds": round(time.time() - t0),
    }
    with open(f"{WORK}/_manifest.json", "w") as f:
        json.dump(manifest, f, indent=1)
    run(["aws", "s3", "cp", f"{WORK}/_manifest.json", PREFIX + "/_manifest.json", "--only-show-errors"])
    print("MANIFEST", json.dumps(manifest), flush=True)
    if problems:
        print("DATA CHECK FAILED:", problems, flush=True)
        sys.exit(1)
    print("DATA CHECK PASSED", flush=True)


if __name__ == "__main__":
    main()
