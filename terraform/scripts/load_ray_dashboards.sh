#!/usr/bin/env bash
# Ray head 启动后会在 /tmp/ray/session_latest/metrics/grafana/dashboards/ 生成官方 Grafana 面板 JSON。
# 这里把 default / data 两个面板读出来, 放进 monitoring 命名空间的 ConfigMap (标签 grafana_dashboard=1), Grafana 的 sidecar 会自动加载。
# 用法: load_ray_dashboards.sh <集群名> <region> [aws profile]
set -euo pipefail
CLUSTER="$1"; REGION="$2"; PROFILE="${3:-}"
export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION"
[ -n "$PROFILE" ] && export AWS_PROFILE="$PROFILE"
KUBECONFIG_TMP="$(mktemp)"; export KUBECONFIG="$KUBECONFIG_TMP"
trap 'rm -f "$KUBECONFIG_TMP"' EXIT
aws eks update-kubeconfig --name "$CLUSTER" --region "$REGION" --alias "$CLUSTER" >/dev/null
K="kubectl --context $CLUSTER"

echo "等待 Ray head Pod Ready (最多 20 分钟) ..."
for _ in $(seq 1 120); do
  if $K -n ray get pod -l ray.io/node-type=head -o name 2>/dev/null | grep -q .; then break; fi
  sleep 10
done
$K -n ray wait --for=condition=Ready pod -l ray.io/node-type=head --timeout=1200s
HEAD=$($K -n ray get pod -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
D=/tmp/ray/session_latest/metrics/grafana/dashboards
for _ in $(seq 1 30); do
  $K -n ray exec "$HEAD" -c ray-head -- test -s "$D/default_grafana_dashboard.json" && break
  sleep 5
done
for kind in default data; do
  F="$(mktemp)"
  $K -n ray exec "$HEAD" -c ray-head -- cat "$D/${kind}_grafana_dashboard.json" > "$F"
  $K -n monitoring create configmap "ray-dashboard-$kind" --from-file="${kind}_grafana_dashboard.json=$F" \
    --dry-run=client -o yaml | $K label --local -f - grafana_dashboard=1 -o yaml | $K apply --server-side --force-conflicts -f -
  rm -f "$F"
done
echo "Ray 面板已载入 (ConfigMap ray-dashboard-default / ray-dashboard-data)"
