#!/usr/bin/env bash
# 向集群提交一个 TPC-H 作业 (tpch_bench.py), 等它结束并打印逐题结果。
# 用法: scripts/run_benchmark.sh [--sf 1] [--questions 1-22] [--id <作业 id>] [--no-wait] [-- 传给 tpch_bench.py 的其它参数]
#   例: scripts/run_benchmark.sh --sf 100 --questions 1-22 -- --s3-max-connections 32 --s3-refresh-creds
# 说明: 作业 id 默认 bench-<SF>-<时间>; 作业超过 1 小时务必带 --s3-refresh-creds (IRSA 的 STS 凭证 1 小时过期)。
#       指标报告需要 AMP workspace ID, 从 terraform output 自动取。
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config
SF=1; Q="1-22"; ID=""; WAIT=1
while [ $# -gt 0 ]; do
  case "$1" in
    --sf) SF="$2"; shift 2 ;;
    --questions) Q="$2"; shift 2 ;;
    --id) ID="$2"; shift 2 ;;
    --no-wait) WAIT=0; shift ;;
    --) shift; break ;;
    *) echo "未知参数 $1"; exit 1 ;;
  esac
done
ID="${ID:-bench-sf$SF-$(date +%H%M%S)}"
CTX="$CLUSTER_NAME"
K="kubectl --context $CTX"
AMP="$(terraform -chdir="$ROOT_DIR/terraform" output -json grafana_dashboard 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['amp_workspace_id'])" 2>/dev/null || true)"
H=$($K -n ray get pod -l ray.io/node-type=head -o name | head -1)
[ -n "$H" ] || { echo "没有找到 Ray head Pod"; exit 1; }
"$LIB_DIR/sync_job.sh" "$CTX" >/dev/null
EXTRA=()
[ -n "$AMP" ] && EXTRA+=(--amp-workspace "$AMP")
echo "提交作业 $ID: SF$SF Q$Q (cluster 标签 $CLUSTER_NAME)"
$K -n ray exec "$H" -c ray-head -- ray job submit --address http://127.0.0.1:8265 --working-dir /tmp/job \
  --submission-id "$ID" --no-wait -- python tpch_bench.py --sf "$SF" --cluster "$CLUSTER_NAME" --questions "$Q" --allow-unchecked ${EXTRA[@]+"${EXTRA[@]}"} "$@"
[ "$WAIT" = 1 ] || { echo "已提交 (不等待)。查看: kubectl --context $CTX -n ray exec $H -c ray-head -- ray job status $ID"; exit 0; }
while :; do
  sleep 20
  OUT=$($K -n ray exec "$H" -c ray-head -- ray job status "$ID" 2>&1 || true)
  case "$OUT" in
    *SUCCEEDED*|*"' succeeded"*) ST=SUCCEEDED ;; *FAILED*|*"' failed"*) ST=FAILED ;; *STOPPED*|*stopped*) ST=STOPPED ;; *) ST=RUNNING ;;
  esac
  DONE=$($K -n ray exec "$H" -c ray-head -- sh -c "tr '\\r' '\\n' < /tmp/ray/session_latest/logs/job-driver-$ID.log | grep -a -c -E '^Q[0-9]+ +warmup'" 2>/dev/null || echo 0)
  echo "  $(date +%T) 状态 $ST, 已完成题数 ${DONE:-0}"
  [ "$ST" = RUNNING ] || break
done
$K -n ray exec "$H" -c ray-head -- sh -c "tr '\\r' '\\n' < /tmp/ray/session_latest/logs/job-driver-$ID.log | grep -a -E '^Q[0-9]+ +warmup|^SUMMARY|^RESULT_(JSON|MD)|MISMATCH'"
[ "$ST" = SUCCEEDED ]
