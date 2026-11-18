"""CPU 计算对比用的 TPC-H 基准 (Daft on Ray): 数据先全部载入内存, 再只对"计算"计时。

和 tpch_bench.py 的区别 (那个脚本每道题都从 S3 重新读 Parquet):
  1. 载入阶段: 每张表 daft.read_parquet(...).collect(), 结果以分区形式放进 Ray object store (worker 上 /dev/shm, 即 RAM), 单独计时 (这一段是 I/O + Parquet 解码)。
  2. 计时阶段: 22 道题全部基于已物化的表, 不再读 S3、不再解码 Parquet。先丢弃 --warmup-passes 遍, 再正式跑 --passes 遍, 每遍每题单独计时 (只对 collect() 计时)。
  3. 每次计时前后读 worker Pod 的 cgroup cpu.stat, 得到这条查询消耗的 CPU 核秒 (user+system) 和是否被限流, 比墙钟更能反映"算了多少"。
  4. 有效性: 每次计时同时读 worker 本地 spill 目录; 只要出现 spill 文件, 该次测量标记为 invalid (spill 会把磁盘 I/O 混进计算耗时)。
  5. 正确性: 第一遍正式测量后对结果做校验 (SF1 对官方答案, 其它 SF 对 DuckDB 参考结果), 校验不计入耗时。
输出: 每次测量一行 "M ..." 打到日志, 最后把全部原始数据写成 JSON 到 S3 (RESULT_JSON ...)。
用法 (在 Ray head 上): python tpch_cpu_bench.py --sf 100 --tag r8gd.16xlarge
"""
import argparse
import datetime as dt
import json
import os
import statistics
import subprocess
import sys
import time

import boto3
import daft
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

import tpch_check
import tpch_queries

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
ACCOUNT = os.environ.get("AWS_ACCOUNT_ID") or boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
DEFAULT_ROOT = f"s3://daft-ray-input-{ACCOUNT}-{REGION}/tpch-v2"
DEFAULT_REF_ROOT = f"s3://daft-ray-output-{ACCOUNT}-{REGION}/tpch-v2/reference"
DEFAULT_OUT = f"s3://daft-ray-output-{ACCOUNT}-{REGION}/tpch-v2/results/cpu"
TIE_SPEC = {3: ([1, 2], 0), 10: ([2], 0), 11: ([1], 0), 18: ([4, 3], 2)}   # 与 tpch_bench.py 一致
TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]


@ray.remote(num_cpus=0)
class Probe:
    """跑在 worker 节点上的探针: 读这个 Pod 的 cgroup CPU 计数、spill 目录、CPU 拓扑。"""

    def read(self):
        d = {}
        try:
            for line in open("/sys/fs/cgroup/cpu.stat"):
                k, v = line.split()
                d[k] = int(v)
        except Exception:  # cgroup v1 兜底
            try:
                d["usage_usec"] = int(open("/sys/fs/cgroup/cpuacct/cpuacct.usage").read()) // 1000
            except Exception:
                pass
        spill = 0
        try:
            import glob
            for p in glob.glob("/tmp/ray/session_*/ray_spilled_objects*"):
                for _r, _d, fs in os.walk(p):
                    spill += len(fs)
        except Exception:
            pass
        d["spill_files"] = spill
        return d

    def topology(self):
        out = {"nproc": os.cpu_count()}
        try:
            out["lscpu"] = subprocess.run(["lscpu"], capture_output=True, text=True, timeout=10).stdout
        except Exception as e:  # noqa: BLE001
            out["lscpu_error"] = str(e)
        try:
            out["numa_nodes"] = len([x for x in os.listdir("/sys/devices/system/node") if x.startswith("node")])
        except Exception:
            pass
        try:
            out["thread_siblings_cpu0"] = open("/sys/devices/system/cpu/cpu0/topology/thread_siblings_list").read().strip()
        except Exception:
            pass
        try:
            out["mem_total_kb"] = int([l for l in open("/proc/meminfo") if l.startswith("MemTotal")][0].split()[1])
        except Exception:
            pass
        return out


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
        return json.loads(boto3.client("s3", region_name=REGION).get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:
        return None


def s3_put(uri, text):
    bucket, key = uri[len("s3://"):].split("/", 1)
    boto3.client("s3", region_name=REGION).put_object(Bucket=bucket, Key=key, Body=text.encode())


def store_used_gib():
    tot = ray.cluster_resources().get("object_store_memory", 0)
    avail = ray.available_resources().get("object_store_memory", 0)
    return (tot - avail) / 2 ** 30, tot / 2 ** 30


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--root", default=DEFAULT_ROOT)
    ap.add_argument("--questions", default="1-22")
    ap.add_argument("--passes", type=int, default=5, help="正式测量的遍数")
    ap.add_argument("--warmup-passes", type=int, default=1, help="丢弃的预热遍数")
    ap.add_argument("--tag", required=True, help="结果标签, 例如 r8gd.16xlarge")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--ref-root", default=DEFAULT_REF_ROOT)
    ap.add_argument("--s3-max-connections", type=int, default=32)
    ap.add_argument("--source", choices=["memory", "files"], default="memory",
                    help="memory: 先把表物化进 Ray object store 再计时 (默认); files: 不物化, 每条查询都从 --files-root 下的本地 Parquet 懒读取")
    ap.add_argument("--files-root", default="/data/tpch", help="--source files 时本地 Parquet 的根目录 (<root>/sf<SF>/<table>/*.parquet)")
    args = ap.parse_args()
    daft.set_runner_ray()
    io = daft.io.IOConfig(s3=daft.io.S3Config(region_name=REGION, max_connections=args.s3_max_connections))
    base = f"{args.root}/sf{args.sf}"
    qlist = parse_questions(args.questions)
    job_start = time.time()
    print(f"CONFIG sf={args.sf} root={base} tag={args.tag} questions={args.questions} passes={args.passes} warmup_passes={args.warmup_passes}", flush=True)
    print(f"CONFIG daft={daft.__version__} ray={ray.__version__} cluster_resources={ray.cluster_resources()}", flush=True)

    # ---- 找到全部 worker 节点 (一台机器上可以有多个 Pod, 每个 Pod 是一个 Ray 节点), 每个节点起一个探针 ----
    workers = [n for n in ray.nodes() if n.get("Alive") and n.get("Resources", {}).get("CPU", 0) > 0]
    probes = [Probe.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=w["NodeID"], soft=False)).remote() for w in workers]
    print(f"CONFIG workers={len(workers)} cpus_per_worker={[w['Resources'].get('CPU') for w in workers]}", flush=True)
    topo = ray.get(probes[0].topology.remote())
    print("TOPOLOGY " + json.dumps({k: v for k, v in topo.items() if k != "lscpu"}), flush=True)
    for line in (topo.get("lscpu") or "").splitlines():
        print("LSCPU " + line, flush=True)

    def read_all():
        """所有 worker Pod 的 cgroup 计数求和 (各 Pod 的 cpu.stat 是独立的)。"""
        tot = {}
        for d in ray.get([pr.read.remote() for pr in probes]):
            for k, v in d.items():
                tot[k] = tot.get(k, 0) + v
        return tot

    # ---- 载入阶段 (只有 --source memory) ----
    tables, load = {}, {}
    t_load0 = time.time()
    if args.source == "memory":
        for name in TABLES:
            t0 = time.time()
            tables[name] = daft.read_parquet(f"{base}/{name}/*.parquet", io_config=io).collect()
            load[name] = {"seconds": round(time.time() - t0, 3), "rows": len(tables[name])}
            print(f"LOAD {name} {load[name]['seconds']}s rows={load[name]['rows']}", flush=True)
        used, total = store_used_gib()
        print(f"LOAD_DONE seconds={time.time() - t_load0:.1f} object_store_used_gib={used:.1f}/{total:.1f}", flush=True)
    t_load1 = time.time()

    def get_df(t):
        if args.source == "files":      # 每次都新建惰性读取, 不缓存; 数据在本地盘 (页缓存预热后基本不碰网络)
            return daft.read_parquet(f"{args.files_root}/sf{args.sf}/{t}/*.parquet")
        return tables[t]

    def build(q):
        fn = getattr(tpch_queries, f"q{q}")
        return fn(get_df, sf=args.sf) if q == 11 else fn(get_df)

    mode = "official" if args.sf == 1 else "reference"

    def check(q, df):
        actual = df.to_pydict()
        if mode == "official":
            expected = tpch_check.load_official(os.path.join(os.path.dirname(os.path.abspath(__file__)), "tpch_answers_sf1.json"), q)
            rel, ab = 1e-9, 0.01
        else:
            ref = s3_json(f"{args.ref_root}/sf{args.sf}/q{q}.json")
            expected, rel, ab = (ref["rows"] if ref else None), 1e-9, 1e-6
        if expected is None:
            return "UNCHECKED", []
        problems = tpch_check.compare(actual, expected, rel, ab, tie_spec=TIE_SPEC.get(q))
        return ("PASS" if not problems else "FAIL"), problems

    # ---- 计时阶段 ----
    meas = []
    checks = {}
    for p in range(-args.warmup_passes + 1, args.passes + 1):       # 预热遍用 0, -1, ...; 正式遍 1..N
        kind = "warmup" if p <= 0 else "measured"
        for q in qlist:
            plan = build(q)                                          # 建计划不计时
            c0 = read_all()
            t0 = time.time()
            res = plan.collect()
            t1 = time.time()
            c1 = read_all()
            rec = {"pass": p, "kind": kind, "q": q, "t_start": t0, "t_end": t1, "wall_s": round(t1 - t0, 4)}
            if "usage_usec" in c0 and "usage_usec" in c1:
                rec["cpu_s"] = round((c1["usage_usec"] - c0["usage_usec"]) / 1e6, 4)
                rec["user_s"] = round((c1.get("user_usec", 0) - c0.get("user_usec", 0)) / 1e6, 4)
                rec["sys_s"] = round((c1.get("system_usec", 0) - c0.get("system_usec", 0)) / 1e6, 4)
                rec["throttled_s"] = round((c1.get("throttled_usec", 0) - c0.get("throttled_usec", 0)) / 1e6, 4)
                rec["cores_busy"] = round(rec["cpu_s"] / max(rec["wall_s"], 1e-9), 2)
            rec["spill_files"] = c1.get("spill_files", 0)
            rec["valid"] = c1.get("spill_files", 0) == 0
            rec["rows"] = len(res)
            if p == 1:                                                # 第一遍正式测量后校验正确性 (不计时)
                try:
                    chk, problems = check(q, res)
                except Exception as e:  # noqa: BLE001
                    chk, problems = "ERROR", [f"{type(e).__name__}: {str(e)[:300]}"]
                checks[q] = {"check": chk, "problems": problems}
                rec["check"] = chk
            meas.append(rec)
            print(f"M pass={p} {kind} q={q} wall={rec['wall_s']} cpu_s={rec.get('cpu_s')} cores_busy={rec.get('cores_busy')} sys_s={rec.get('sys_s')} "
                  f"throttled_s={rec.get('throttled_s')} rows={rec['rows']} spill_files={rec['spill_files']}{' check=' + rec['check'] if 'check' in rec else ''}", flush=True)
            del res
        used, total = store_used_gib()
        wall = sum(r["wall_s"] for r in meas if r["pass"] == p)
        cpu = sum(r.get("cpu_s", 0) for r in meas if r["pass"] == p)
        print(f"PASS_DONE pass={p} {kind} wall_total={wall:.1f}s cpu_total={cpu:.0f}s object_store_used_gib={used:.1f}/{total:.1f}", flush=True)

    # ---- 汇总 ----
    good = [r for r in meas if r["kind"] == "measured" and r["valid"]]
    summary = {}
    for q in qlist:
        w = [r["wall_s"] for r in good if r["q"] == q]
        c = [r["cpu_s"] for r in good if r["q"] == q and "cpu_s" in r]
        if w:
            summary[q] = {"n": len(w), "wall_median": statistics.median(w), "wall_min": min(w), "wall_max": max(w), "cpu_median": statistics.median(c) if c else None}
    n_pass = sum(1 for v in checks.values() if v["check"] == "PASS")
    n_fail = sum(1 for v in checks.values() if v["check"] == "FAIL")
    n_err = sum(1 for v in checks.values() if v["check"] == "ERROR")
    inval = sum(1 for r in meas if r["kind"] == "measured" and not r["valid"])
    tot_med = sum(v["wall_median"] for v in summary.values())
    print(f"SUMMARY sf={args.sf} tag={args.tag} queries={len(qlist)} measured_passes={args.passes} wall_median_total={tot_med:.1f}s "
          f"check_pass={n_pass} fail={n_fail} error={n_err} invalid_measurements={inval}", flush=True)
    doc = {"tag": args.tag, "sf": args.sf, "daft": daft.__version__, "ray": ray.__version__, "job_start": job_start, "job_end": time.time(),
           "source": args.source, "n_workers": len(workers), "load": load, "load_total_s": round(t_load1 - t_load0, 3), "load_window": [t_load0, t_load1], "topology": topo,
           "cluster_resources": ray.cluster_resources(), "checks": {str(k): v for k, v in checks.items()}, "measurements": meas,
           "passes": args.passes, "warmup_passes": args.warmup_passes}
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    uri = f"{args.out}/sf{args.sf}/{args.tag}-{stamp}/cpu_bench.json"
    s3_put(uri, json.dumps(doc, indent=1, default=str))
    print(f"RESULT_JSON {uri}", flush=True)
    if n_fail or n_err:
        sys.exit(1)


if __name__ == "__main__":
    main()
