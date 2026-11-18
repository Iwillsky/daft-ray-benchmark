"""作业结束后自动生成 RESULTS.md: 总耗时、逐题耗时、校验结果, 加上运行期间 worker 集群的 CPU / 内存 / 网络吞吐 / 本地盘吞吐。

指标来自 AMP (Prometheus 远程写入), 口径:
  - worker 节点 = 窗口内 kube_pod_info 里名字含 "-worker-" 的 Ray worker Pod 所在节点 (用 host_ip 对应 node-exporter 的 instance=IP:9100)
  - "整个集群" = 全部 worker 节点之和 (head 与系统节点不计)
  - CPU 利用率 = Σ繁忙核秒 / Σ总核秒;  内存 = 1 − ΣMemAvailable / ΣMemTotal
  - 网络 = 主网卡 (排除 lo / veth / eni / cali) 的收发字节速率;  本地盘 = 实例存储 RAID0 (md*) 的读写字节速率
  - 采样步长 15s, 逐题的数值是该题"计时运行"时间段内采样点的平均; Prometheus 抓取间隔 30s, 所以只有几秒的小题数值粗糙
凭证: 用 Pod 的 IRSA 凭证对 AMP 做 SigV4 签名 (角色需要 AmazonPrometheusQueryAccess)。
"""
from __future__ import annotations

import json
import statistics
import urllib.parse
import urllib.request

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

REGION = "us-west-2"
STEP = 15
NIC = 'device!~"lo|veth.*|eni.*|cali.*"'


def _query_range(session, workspace, promql, start, end):
    url = f"https://aps-workspaces.{REGION}.amazonaws.com/workspaces/{workspace}/api/v1/query_range"
    body = urllib.parse.urlencode({"query": promql, "start": start, "end": end, "step": STEP})
    req = AWSRequest(method="POST", url=url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"})
    SigV4Auth(session.get_credentials().get_frozen_credentials(), "aps", REGION).add_auth(req)
    r = urllib.request.Request(url, data=body.encode(), headers=dict(req.headers), method="POST")
    with urllib.request.urlopen(r, timeout=120) as resp:
        data = json.load(resp)
    if data.get("status") != "success":
        raise RuntimeError(str(data)[:300])
    return data["data"]["result"]


def _series(session, workspace, promql, start, end):
    """聚合成单条序列: 返回 [(ts, value), ...]; 查不到返回 []。"""
    res = _query_range(session, workspace, promql, start, end)
    if not res:
        return []
    return [(float(t), float(v)) for t, v in res[0]["values"] if v not in ("NaN", "+Inf", "-Inf")]


def _mean(points, a=None, b=None):
    vals = [v for t, v in points if (a is None or t >= a) and (b is None or t <= b)]
    return statistics.fmean(vals) if vals else None


def _max(points, a=None, b=None):
    vals = [v for t, v in points if (a is None or t >= a) and (b is None or t <= b)]
    return max(vals) if vals else None


def worker_node_ips(session, workspace, cluster, start, end):
    res = _query_range(session, workspace, f'max by (host_ip)(kube_pod_info{{cluster="{cluster}",namespace="ray",pod=~".*-worker-.*"}})', start, end)
    return sorted({r["metric"]["host_ip"] for r in res if r["metric"].get("host_ip")})


def collect(workspace, cluster, start, end):
    s = boto3.Session()
    ips = worker_node_ips(s, workspace, cluster, start, end)
    if not ips:
        raise RuntimeError("找不到 worker 节点 (kube_pod_info 里没有窗口内的 worker Pod)")
    ipre = "|".join(f"{i}:9100" for i in ips)
    sel = f'cluster="{cluster}",instance=~"{ipre}"'
    q = {
        "cpu": f'1 - sum(rate(node_cpu_seconds_total{{mode="idle",{sel}}}[1m])) / sum(rate(node_cpu_seconds_total{{{sel}}}[1m]))',
        "mem": f"1 - sum(node_memory_MemAvailable_bytes{{{sel}}}) / sum(node_memory_MemTotal_bytes{{{sel}}})",
        "rx_gbps": f"sum(rate(node_network_receive_bytes_total{{{sel},{NIC}}}[1m])) * 8 / 1e9",
        "tx_gbps": f"sum(rate(node_network_transmit_bytes_total{{{sel},{NIC}}}[1m])) * 8 / 1e9",
        "rx_gbps_node_peak": f"max(sum by (instance)(rate(node_network_receive_bytes_total{{{sel},{NIC}}}[1m])) * 8 / 1e9)",
        "disk_read_mbps": f'sum(rate(node_disk_read_bytes_total{{{sel},device=~"md.*"}}[1m])) / 1e6',
        "disk_write_mbps": f'sum(rate(node_disk_written_bytes_total{{{sel},device=~"md.*"}}[1m])) / 1e6',
    }
    series = {}
    for k, promql in q.items():
        try:
            series[k] = _series(s, workspace, promql, start, end)
        except Exception as e:  # 单项失败不影响其它
            series[k] = []
            series[f"{k}_error"] = str(e)[:200]
    return ips, series


def _pct(x):
    return "-" if x is None else f"{x * 100:.1f}%"


def _num(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


def build_markdown(summary, ips, series, meta):
    a, b = meta["start"], meta["end"]
    dur = b - a
    rows = summary["results"]
    L = []
    L.append(f"# TPC-H SF{summary['sf']} (Q1-Q22) 运行结果: {meta['label']}")
    L.append("")
    L.append(f"- 集群: `{meta['cluster']}`;  worker: {len(ips)} 台 {meta['instance_type']};  架构标签/镜像以集群为准")
    L.append(f"- Daft {summary['daft']} / Ray {summary['ray']};  数据: `{summary['data']}`;  校验口径: {summary['expected_mode']}")
    L.append(f"- 作业时间窗 (driver 视角): {meta['start_iso']} → {meta['end_iso']}  (共 {dur:.0f} 秒 = {dur / 60:.1f} 分钟)")
    L.append(f"- worker 节点 IP: {', '.join(ips)}")
    L.append("")
    n_pass = sum(r["check"] == "PASS" for r in rows)
    n_fail = sum(r["check"] == "FAIL" for r in rows)
    n_err = sum(r["check"] == "ERROR" for r in rows)
    n_unc = sum(r["check"] == "UNCHECKED" for r in rows)
    L.append("## 汇总")
    L.append("")
    L.append("| 项 | 值 |")
    L.append("|---|---|")
    L.append(f"| 作业总耗时 (含 warm-up) | {dur:.0f} 秒 ({dur / 60:.1f} 分钟) |")
    L.append(f"| 各题计时部分合计 | {summary['total_run_seconds']:.1f} 秒 |")
    L.append(f"| 校验 | PASS {n_pass} / FAIL {n_fail} / ERROR {n_err} / 无参考 {n_unc} |")
    L.append(f"| 集群 CPU 平均 (峰值) | {_pct(_mean(series['cpu']))} ({_pct(_max(series['cpu']))}) |")
    L.append(f"| 集群内存平均 (峰值) | {_pct(_mean(series['mem']))} ({_pct(_max(series['mem']))}) |")
    L.append(f"| 网络接收 平均 / 峰值 (全部 worker 合计) | {_num(_mean(series['rx_gbps']))} / {_num(_max(series['rx_gbps']))} Gbps |")
    L.append(f"| 网络接收 单台峰值 | {_num(_max(series['rx_gbps_node_peak']))} Gbps |")
    L.append(f"| 网络发送 平均 / 峰值 (合计) | {_num(_mean(series['tx_gbps']))} / {_num(_max(series['tx_gbps']))} Gbps |")
    L.append(f"| 本地 NVMe 读 平均 / 峰值 (合计) | {_num(_mean(series['disk_read_mbps']), 0)} / {_num(_max(series['disk_read_mbps']), 0)} MB/s |")
    L.append(f"| 本地 NVMe 写 平均 / 峰值 (合计) | {_num(_mean(series['disk_write_mbps']), 0)} / {_num(_max(series['disk_write_mbps']), 0)} MB/s |")
    wmean = _mean(series["disk_write_mbps"])
    L.append(f"| 本地 NVMe 写入总量 (平均速率 × 窗口) | {'-' if wmean is None else f'{wmean * dur / 1e6:.2f} TB'} |")
    L.append("")
    L.append("## 逐题 (计时运行期间的集群平均值)")
    L.append("")
    L.append("| 题 | warm-up(s) | 计时(s) | 行数 | 校验 | CPU | 内存 | 网络收 Gbps | 盘写 MB/s | 盘读 MB/s |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        qs, qe = r.get("t_start"), r.get("t_end")
        if qs and qe:
            cpu, mem = _mean(series["cpu"], qs, qe), _mean(series["mem"], qs, qe)
            rx, dw, dr = _mean(series["rx_gbps"], qs, qe), _mean(series["disk_write_mbps"], qs, qe), _mean(series["disk_read_mbps"], qs, qe)
        else:
            cpu = mem = rx = dw = dr = None
        L.append(
            f"| Q{r['q']} | {_num(r.get('warmup_s'), 1)} | {_num(r.get('run_s'), 1)} | {r.get('rows', '-')} | {r['check']} | "
            f"{_pct(cpu)} | {_pct(mem)} | {_num(rx)} | {_num(dw, 0)} | {_num(dr, 0)} |"
        )
    bad = [r for r in rows if r["check"] in ("FAIL", "ERROR")]
    if bad:
        L.append("")
        L.append("## 失败 / 错误详情")
        for r in bad:
            L.append(f"- Q{r['q']} {r['check']}: " + "; ".join(r.get("problems", [r.get("error", "")]))[:600])
    L.append("")
    L.append("## 口径与限制")
    L.append("")
    L.append("- 指标来自 AMP: node-exporter (节点级) 与 kube-state-metrics; 15 秒采样步长, Prometheus 抓取间隔 30 秒, 运行只有几秒的题目数值粗糙。")
    L.append("- 作业总耗时是 driver 进程视角 (含读取参考结果等少量开销), 与 `ray job list` 的起止时间可能差几秒。")
    L.append("- 逐题数值是该题计时运行时间段内的平均, 不含 warm-up; 单次运行, 没有重复, 无法给出误差范围。")
    L.append("- 校验: SF1 用 TPC-H 官方标准答案; 其它规模用 DuckDB 在同一份数据上算的参考结果 (对 Q11 阈值用规范的 0.0001/SF)。")
    for k in series:
        if k.endswith("_error"):
            L.append(f"- 指标 `{k[:-6]}` 查询失败: {series[k]}")
    return "\n".join(L) + "\n"
