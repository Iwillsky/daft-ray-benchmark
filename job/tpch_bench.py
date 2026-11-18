"""TPC-H Q1-Q22 benchmark (Daft on Ray), 流程对齐 Daft 官方 TPC-H benchmark, 并带结果校验与自动生成 RESULTS.md。

对齐点 (https://docs.getdaft.io/en/stable/benchmarks/):
  - 查询: 官方 q1-q22 实现 (tpch_queries.py; q11 的阈值按规范取 0.0001/SF)
  - 数据: S3 上的分区 Parquet (<root>/sf<SF>/<table>/*.parquet), 每次都直接从 S3 读, 不做任何缓存
  - 每个问题先 warm-up 跑一遍, 再正式计时跑一遍 (只对 collect() 计时), 逐题报告耗时, 汇总总耗时
额外加的:
  - 结果校验: SF1 对照 TPC-H 官方标准答案; 其它 SF 对照 DuckDB 在同一份数据上算出的参考结果 (tpch_reference.py)
  - 单题失败 (例如内存不足) 不会中断整个作业, 记为 ERROR 继续跑后面的题
  - 作业结束后自动生成 RESULTS.md (总耗时、逐题耗时与校验、运行期间集群的 CPU/内存/网络/本地盘吞吐), 写到输出桶

用法 (在 Ray head 上):
  ray job submit --address http://127.0.0.1:8265 --working-dir /tmp/job -- \
      python tpch_bench.py --sf 3000 --cluster daft-ray-graviton
退出码: 0 全部通过; 1 有查询结果不一致或出错; 2 有查询没有可用的期望答案 (默认视为失败, 可用 --allow-unchecked 放行)
"""
import argparse
import datetime as dt
import json
import os
import sys
import time

import boto3
import daft
import ray

import tpch_check
import tpch_queries
import tpch_report

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
ACCOUNT = os.environ.get("AWS_ACCOUNT_ID") or boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
DEFAULT_ROOT = f"s3://daft-ray-input-{ACCOUNT}-{REGION}/tpch-v2"
DEFAULT_OUT = f"s3://daft-ray-output-{ACCOUNT}-{REGION}/tpch-v2/results"
DEFAULT_REF_ROOT = f"s3://daft-ray-output-{ACCOUNT}-{REGION}/tpch-v2/reference"

# ORDER BY 键可能并列、并列行先后顺序不确定的题: {题号: ([排序键所在列序号...], 唯一标识列序号)}, 详见 tpch_check.py 顶部
TIE_SPEC = {3: ([1, 2], 0), 10: ([2], 0), 11: ([1], 0), 18: ([4, 3], 2)}

# 集群 -> (AMP workspace, worker 机型, 展示名); 与 Prometheus 的 externalLabels.cluster 一致
# 集群名 -> (AMP workspace, worker 机型, 展示名); 本仓库不内置任何账号专属值, 运行时用 --amp-workspace 传入 workspace ID
CLUSTERS = {}


def parse_questions(s):
    out = []
    for part in s.split(","):
        if "-" in part:
            a, b = part.split("-")
            out += list(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return sorted(set(out))


def s3_json(uri):
    bucket, key = uri[len("s3://"):].split("/", 1)
    try:
        body = boto3.client("s3", region_name=REGION).get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception:
        return None
    return json.loads(body)


def s3_put(uri, text):
    bucket, key = uri[len("s3://"):].split("/", 1)
    boto3.client("s3", region_name=REGION).put_object(Bucket=bucket, Key=key, Body=text.encode())


PLAN_MAX_CHARS = 60000
METRICS_MAX_ROWS = 600


def capture_plan(df, rec):
    """记录 Daft 的物理计划文本 (含 shuffle 节点, 以及 Daft 在大 shuffle 时打印的 flight_shuffle 提示)。只做计划, 不执行。出错只记错误, 不影响查询。"""
    try:
        import io
        buf = io.StringIO()
        df.explain(show_all=True, file=buf)
        txt = buf.getvalue()
        rec["plan"] = txt[:PLAN_MAX_CHARS]
        rec["plan_truncated"] = len(txt) > PLAN_MAX_CHARS
    except Exception as e:
        rec["plan_error"] = f"{type(e).__name__}: {str(e)[:300]}"


def capture_metrics(df, rec):
    """collect() 之后记录 df.metrics (Daft 每个算子的行数/字节/耗时等统计, RecordBatch)。没有就记原因。"""
    try:
        m = df.metrics
        if m is None:
            rec["op_metrics_note"] = "df.metrics 为 None"
            return
        try:
            rows = m.to_pylist()
        except Exception:
            d = m.to_pydict()
            n = len(next(iter(d.values()))) if d else 0
            rows = [{k: d[k][i] for k in d} for i in range(n)]
        rec["op_metrics_columns"] = list(rows[0].keys()) if rows else []
        rec["op_metrics"] = rows[:METRICS_MAX_ROWS]
        rec["op_metrics_rows"] = len(rows)
    except Exception as e:
        rec["op_metrics_error"] = f"{type(e).__name__}: {str(e)[:300]}"


def make_cred_provider():
    """给 Daft 的 S3Config 用的凭证函数: 每次调用都经 boto3 默认凭证链 (IRSA web identity) 取凭证, boto3 会在到期前自动续期。
    返回的 S3Credentials 带 expiry, Daft 在到期前 buffer_time 秒重新调用本函数。
    函数在 __main__ 里定义, cloudpickle 按值序列化 (worker 上不需要 import 本文件); 所有 import 放在函数体内, 每个 worker 进程各自建 boto3 session。"""
    def provider():
        # 不要在闭包里缓存 boto3 Session: Session 含线程锁, driver 上调用过一次后再被 cloudpickle 序列化会报 "cannot pickle '_thread.lock'"。
        # 每次调用新建 Session 即可 (约每 50 分钟才调用一次), 新 Session 会重新做 AssumeRoleWithWebIdentity 取到全新的 1 小时凭证。
        import datetime as _dt

        import boto3
        from daft.io import S3Credentials
        creds = boto3.Session().get_credentials()
        fz = creds.get_frozen_credentials()
        exp = getattr(creds, "_expiry_time", None)
        # Daft 要求 expiry 是 tz 为 datetime.timezone.utc 的 datetime; 拿不到过期时间就当 30 分钟后过期 (到时重新取)
        exp = exp.astimezone(_dt.timezone.utc) if exp is not None else _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(minutes=30)
        return S3Credentials(key_id=fz.access_key, access_key=fz.secret_key, session_token=fz.token, expiry=exp)
    return provider


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--root", default=DEFAULT_ROOT)
    ap.add_argument("--questions", default="1-22")
    ap.add_argument("--no-warmup", action="store_true")
    ap.add_argument("--order", choices=["asc", "desc"], default="asc", help="题目执行顺序: asc 从小到大 (默认), desc 从大到小 (例如 --questions 1-10 --order desc 即 Q10->Q1)")
    ap.add_argument("--sequence", default=None, help="显式指定题目执行顺序 (逗号分隔, 必须是 --questions 的一个排列), 例如 9,18,4,...; 给了就忽略 --order")
    ap.add_argument("--shuffle-seed", type=int, default=None, help="按这个随机种子把题目顺序打乱 (SF500/SF1000 每次投递用不同种子); 种子和实际顺序会打印并写进 summary.json。给了就忽略 --order, 但 --sequence 优先")
    ap.add_argument("--expected", choices=["auto", "official", "reference", "none"], default="auto")
    ap.add_argument("--allow-unchecked", action="store_true")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--ref-root", default=DEFAULT_REF_ROOT, help="DuckDB 参考结果所在位置 (tpch_reference.py 的输出)")
    ap.add_argument("--cluster", default=None, help="Prometheus 里的 cluster 标签 (daft-ray-graviton / daft-ray-x86); 不给则不生成集群指标")
    ap.add_argument("--amp-workspace", default=None)
    ap.add_argument("--report-wait", type=int, default=90, help="作业结束后等多少秒再查 AMP (抓取 + 远程写入有延迟)")
    ap.add_argument("--s3-max-connections", type=int, default=None, help="Daft S3Config.max_connections (每个 IO 线程的最大连接数, Daft 默认 8); 不给则用默认值")
    ap.add_argument("--s3-retry-mode", choices=["standard", "adaptive"], default=None, help="Daft S3Config.retry_mode (默认 adaptive); 不给则用默认值")
    ap.add_argument("--shuffle-stats", action="store_true", help="跟踪每题的 shuffle 数据量: 执行前记录 Daft 的物理计划 (含 shuffle/repartition 节点和 Daft 对大 shuffle 的提示), 执行后记录 df.metrics (各算子的行数/字节等统计); 都写进 summary.json 的 plan / op_metrics 字段")
    ap.add_argument("--s3-refresh-creds", action="store_true", help="用 boto3 提供 S3 凭证并在到期前自动刷新 (IRSA 的 STS 凭证只有 1 小时有效期); 同时加 custom_retry_msgs=['ExpiredToken'] 兜底")
    ap.add_argument("--s3-buffer-time", type=int, default=600, help="--s3-refresh-creds 时, 凭证到期前多少秒就重新取 (Daft 默认 10 秒)")
    args = ap.parse_args()

    daft.set_runner_ray()
    s3_kwargs = {"region_name": REGION}
    if args.s3_max_connections is not None:
        s3_kwargs["max_connections"] = args.s3_max_connections
    if args.s3_retry_mode is not None:
        s3_kwargs["retry_mode"] = args.s3_retry_mode
    if args.s3_refresh_creds:
        s3_kwargs["credentials_provider"] = make_cred_provider()
        s3_kwargs["buffer_time"] = args.s3_buffer_time
        s3_kwargs["custom_retry_msgs"] = ["ExpiredToken"]
    io = daft.io.IOConfig(s3=daft.io.S3Config(**s3_kwargs))
    base = f"{args.root}/sf{args.sf}"

    def get_df(table):
        return daft.read_parquet(f"{base}/{table}/*.parquet", io_config=io)

    def build(q):
        fn = getattr(tpch_queries, f"q{q}")
        return fn(get_df, sf=args.sf) if q == 11 else fn(get_df)

    mode = args.expected
    if mode == "auto":
        mode = "official" if args.sf == 1 else "reference"
    job_start = time.time()
    print(f"CONFIG sf={args.sf} root={base} expected={mode} questions={args.questions} order={args.order} sequence={args.sequence} warmup={not args.no_warmup} cluster={args.cluster} s3={s3_kwargs}", flush=True)
    print(f"CONFIG daft={daft.__version__} ray={ray.__version__} cluster_resources={ray.cluster_resources()}", flush=True)

    rows = []
    qlist = parse_questions(args.questions)
    if args.sequence:
        seq = [int(x) for x in args.sequence.split(",")]
        assert sorted(seq) == qlist, f"--sequence 必须是 --questions 的排列: {seq} vs {qlist}"
        qlist = seq
    elif args.shuffle_seed is not None:
        import random
        random.Random(args.shuffle_seed).shuffle(qlist)
    elif args.order == "desc":
        qlist = qlist[::-1]
    print(f"ORDER shuffle_seed={args.shuffle_seed} sequence={','.join(map(str, qlist))}", flush=True)
    for q in qlist:
        rec = {"q": q}
        try:
            if not args.no_warmup:
                t0 = time.time()
                build(q).collect()
                rec["warmup_s"] = round(time.time() - t0, 3)
            df = build(q)  # 新建计划, 避免复用 warm-up 的结果缓存
            if args.shuffle_stats:
                capture_plan(df, rec)  # 不执行查询, 只做优化和物理计划; 在 t_start 之前, 不计入计时
            rec["t_start"] = time.time()
            df = df.collect()
            rec["t_end"] = time.time()
            rec["run_s"] = round(rec["t_end"] - rec["t_start"], 3)
            if args.shuffle_stats:
                capture_metrics(df, rec)
            actual = df.to_pydict()
            rec["rows"] = len(next(iter(actual.values()))) if actual else 0

            expected, rel, ab = None, 1e-9, 1e-6
            if mode == "official":
                expected = tpch_check.load_official(os.path.join(os.path.dirname(os.path.abspath(__file__)), "tpch_answers_sf1.json"), q)
                rel, ab = 1e-9, 0.01  # 官方答案按 2~4 位小数舍入
            elif mode == "reference":
                ref = s3_json(f"{args.ref_root}/sf{args.sf}/q{q}.json")
                expected = ref["rows"] if ref else None
            if expected is None:
                rec["check"] = "UNCHECKED"
            else:
                problems = tpch_check.compare(actual, expected, rel, ab, tie_spec=TIE_SPEC.get(q))
                rec["check"] = "PASS" if not problems else "FAIL"
                if problems:
                    rec["problems"] = problems
        except Exception as e:  # 单题出错 (如内存不足) 不中断整个作业
            rec["check"] = "ERROR"
            rec["error"] = f"{type(e).__name__}: {str(e)[:2500]}"
            rec.setdefault("run_s", None)
            rec.setdefault("rows", None)
        rows.append(rec)
        print(f"Q{q:<2} warmup={rec.get('warmup_s', '-')}s run={rec.get('run_s')}s rows={rec.get('rows')} check={rec['check']}", flush=True)
        for p in rec.get("problems", []):
            print(f"    MISMATCH {p}", flush=True)
        if rec.get("error"):
            print(f"    ERROR {rec['error']}", flush=True)

    job_end = time.time()
    total = round(sum(r["run_s"] or 0 for r in rows), 3)
    cnt = {k: sum(r["check"] == k for r in rows) for k in ("PASS", "FAIL", "ERROR", "UNCHECKED")}
    print(f"SUMMARY sf={args.sf} questions={len(rows)} total_run_seconds={total} pass={cnt['PASS']} fail={cnt['FAIL']} error={cnt['ERROR']} unchecked={cnt['UNCHECKED']}", flush=True)

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # 同一秒结束的并发作业会撞同一个目录 (实测 2 个作业的 summary.json 互相覆盖), 所以加上随机种子/进程号
    run_dir = f"{args.out}/sf{args.sf}/{args.cluster or 'run'}-{stamp}-{args.shuffle_seed if args.shuffle_seed is not None else os.getpid()}"
    summary = {
        "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "sf": args.sf, "data": base, "expected_mode": mode, "cluster": args.cluster,
        "daft": daft.__version__, "ray": ray.__version__, "cluster_resources": ray.cluster_resources(),
        "shuffle_seed": args.shuffle_seed, "question_order": qlist, "job_start": job_start, "job_end": job_end, "total_run_seconds": total, "results": rows,
    }
    s3_put(f"{run_dir}/summary.json", json.dumps(summary, indent=1, default=str))
    print(f"RESULT_JSON {run_dir}/summary.json", flush=True)

    # ---- 自动生成 RESULTS.md (含集群指标); 失败只打印, 不影响作业退出码 ----
    if args.cluster:
        try:
            ws, itype, label = CLUSTERS.get(args.cluster, (None, "?", args.cluster))
            ws = args.amp_workspace or ws
            print(f"等待 {args.report_wait} 秒让最后的指标写入 AMP ...", flush=True)
            time.sleep(args.report_wait)
            ips, series = tpch_report.collect(ws, args.cluster, job_start, job_end + 30)
            iso = lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            md = tpch_report.build_markdown(
                summary, ips, series,
                {"cluster": args.cluster, "label": label, "instance_type": itype, "start": job_start, "end": job_end, "start_iso": iso(job_start), "end_iso": iso(job_end)},
            )
            s3_put(f"{run_dir}/RESULTS.md", md)
            print(f"RESULT_MD {run_dir}/RESULTS.md", flush=True)
        except Exception as e:
            print(f"REPORT_FAILED {type(e).__name__}: {str(e)[:300]} (可以稍后用 scripts/make_report.py 补生成)", flush=True)

    if cnt["FAIL"] or cnt["ERROR"]:
        sys.exit(1)
    if cnt["UNCHECKED"] and not args.allow_unchecked:
        sys.exit(2)


if __name__ == "__main__":
    main()
