#!/usr/bin/env bash
# 向集群提交 TPC-H 作业 (tpch_bench.py): 支持多路并发 (多流)、数据来自 S3 或 FSx, 等作业结束并打印逐题结果。
#
# 用法: scripts/run_benchmark.sh [选项] [-- 传给 tpch_bench.py 的其它参数]
#   --sf N            规模 (默认 1)
#   --questions Q     题目范围, 如 1-10 / 1-22 (默认 1-22)
#   --streams N       并发作业数 (默认 1)。N>1 时每条流用不同的随机种子打乱 Q 序 (--shuffle-seed), 所有流同时提交
#   --seed S          起始种子 (默认随机); 流 i 使用 S+i, 这样不同机型可以用同一组种子做对比
#   --source s3|fsx   数据来源 (默认 s3); fsx 时读 /fsx/<前缀>/sf<N> (仅 elastic 模式的集群)
#   --id ID           作业 id 前缀 (默认 bench-sf<N>-<时间>); 多流时依次为 ID-j1, ID-j2, ...
#   --no-wait         提交后不等待
#   --no-refresh      不要自动加 --s3-refresh-creds
#
# S3 凭证自动刷新: IRSA 的 STS 凭证只有 1 小时有效期, 读 S3 的作业超过 1 小时会在 Daft 里报 ExpiredToken。
# 因此 数据来自 S3 且 (多流 或 SF>=1000) 时, 自动给 tpch_bench.py 加 --s3-refresh-creds (到期前 10 分钟重新取凭证, 并对 ExpiredToken 兜底重试)。
# 说明: 多流的 driver 默认放在 worker 上 (--entrypoint-num-cpus 1), 避免多个 driver 把 head 的内存占满 (测试二里 10 路并发曾把 8C/48Gi 的 head OOM)。
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config
SF=1; Q="1-22"; ID=""; WAIT=1; STREAMS=1; SEED=""; SOURCE=s3; AUTO_REFRESH=1
while [ $# -gt 0 ]; do
  case "$1" in
    --sf) SF="$2"; shift 2 ;;
    --questions) Q="$2"; shift 2 ;;
    --streams) STREAMS="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --source) SOURCE="$2"; shift 2 ;;
    --id) ID="$2"; shift 2 ;;
    --no-wait) WAIT=0; shift ;;
    --no-refresh) AUTO_REFRESH=0; shift ;;
    --) shift; break ;;
    *) echo "未知参数 $1"; exit 1 ;;
  esac
done
ID="${ID:-bench-sf$SF-$(date +%H%M%S)}"
CTX="$CLUSTER_NAME"
K="kubectl --context $CTX"
H=$($K -n ray get pod -l ray.io/node-type=head -o name | head -1)
[ -n "$H" ] || { echo "没有找到 Ray head Pod (context $CTX)"; exit 1; }
AMP="$(terraform -chdir="$ROOT_DIR/terraform" output -json grafana_dashboard 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['amp_workspace_id'])" 2>/dev/null || true)"
"$LIB_DIR/sync_job.sh" "$CTX" >/dev/null

EXTRA=(--allow-unchecked)
[ -n "$AMP" ] && EXTRA+=(--amp-workspace "$AMP")
if [ "$SOURCE" = fsx ]; then
  EXTRA+=(--root "/fsx/$DATA_PREFIX")
elif [ "$AUTO_REFRESH" = 1 ] && { [ "$STREAMS" -gt 1 ] || [ "$SF" -ge 1000 ]; }; then
  EXTRA+=(--s3-refresh-creds)
  echo "数据来自 S3 且 (多流或 SF>=1000): 自动加 --s3-refresh-creds"
fi
ENTRY=()
if [ "$STREAMS" -gt 1 ] || [ "${CLUSTER_MODE:-static}" = elastic ]; then ENTRY=(--entrypoint-num-cpus 1 --entrypoint-memory 32212254720); fi
[ -n "$SEED" ] || SEED=$(python3 -c "import random; print(random.SystemRandom().randrange(1000, 10**8))")

IDS=()
for i in $(seq 1 "$STREAMS"); do
  JID="$ID"; SHUF=()
  if [ "$STREAMS" -gt 1 ]; then JID="$ID-j$i"; SHUF=(--shuffle-seed $((SEED + i))); fi
  echo "提交 $JID: SF$SF Q$Q source=$SOURCE ${SHUF[*]:-}"
  $K -n ray exec "$H" -c ray-head -- ray job submit --address http://127.0.0.1:8265 --working-dir /tmp/job \
    --submission-id "$JID" --no-wait ${ENTRY[@]+"${ENTRY[@]}"} -- python tpch_bench.py --sf "$SF" --cluster "$CLUSTER_NAME" \
    --questions "$Q" ${EXTRA[@]+"${EXTRA[@]}"} ${SHUF[@]+"${SHUF[@]}"} "$@" >/dev/null &
  IDS+=("$JID")
done
wait
[ "$WAIT" = 1 ] || { echo "已提交: ${IDS[*]}"; exit 0; }

RC=0
while :; do
  sleep 20; RUNNING=0; LINE="  $(date +%T)"
  for JID in "${IDS[@]}"; do
    OUT=$($K -n ray exec "$H" -c ray-head -- ray job status "$JID" 2>&1 || true)
    case "$OUT" in
      *SUCCEEDED*|*"' succeeded"*) ST=SUCCEEDED ;; *FAILED*|*"' failed"*) ST=FAILED ;; *STOPPED*|*stopped*) ST=STOPPED ;; *) ST=RUNNING ;;
    esac
    DONE=$($K -n ray exec "$H" -c ray-head -- sh -c "tr '\\r' '\\n' < /tmp/ray/session_latest/logs/job-driver-$JID.log | grep -a -c -E '^Q[0-9]+ +warmup'" 2>/dev/null || echo 0)
    LINE="$LINE | $JID $ST ${DONE:-0}题"
    [ "$ST" = RUNNING ] && RUNNING=1
    [ "$ST" = SUCCEEDED ] || [ "$ST" = RUNNING ] || RC=1
  done
  echo "$LINE"
  [ "$RUNNING" = 1 ] || break
done
# Daft 的 request_resources 不会随作业结束撤销, 不清掉 autoscaler 就不会缩容 (elastic 集群会一直开着 worker 节点)
$K -n ray exec -i "$H" -c ray-head -- python - < "$LIB_DIR/clear_request_resources.py" >/dev/null 2>&1 || true
for JID in "${IDS[@]}"; do
  echo "== $JID"
  $K -n ray exec "$H" -c ray-head -- sh -c "tr '\\r' '\\n' < /tmp/ray/session_latest/logs/job-driver-$JID.log | grep -a -E '^Q[0-9]+ +warmup|^SUMMARY|^RESULT_(JSON|MD)|MISMATCH'"
done
exit $RC
