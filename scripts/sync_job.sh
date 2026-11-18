#!/usr/bin/env bash
# 把 job/ 下的作业脚本拷到 Ray head 的 /tmp/job (head 重建后 /tmp/job 会丢, 需要重新同步)。
# 用法: scripts/sync_job.sh [kube-context]      默认 context = 集群名 (config.env 的 CLUSTER_NAME)
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config.env}"
CTX="${1:-$(. "$CONFIG_FILE"; echo "${CLUSTER_NAME:-daft-ray-static}")}"
K="kubectl --context $CTX"
H=$($K -n ray get pod -l ray.io/node-type=head -o name | head -1)
[ -n "$H" ] || { echo "没有找到 Ray head Pod (context $CTX)"; exit 1; }
$K -n ray exec "$H" -c ray-head -- sh -c 'rm -rf /tmp/job && mkdir -p /tmp/job'
for f in tpch_bench.py tpch_cpu_bench.py tpch_check.py tpch_queries.py tpch_report.py tpch_reference.py tpch_answers_sf1.json tpch_duckdb_queries.json; do
  $K -n ray exec -i "$H" -c ray-head -- sh -c "cat > /tmp/job/$f" < "$ROOT_DIR/job/$f"
done
$K -n ray exec "$H" -c ray-head -- sh -c 'ls /tmp/job | tr "\n" " "; echo'
