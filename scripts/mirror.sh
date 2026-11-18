#!/usr/bin/env bash
# 步骤: 把集群需要、但节点 (和 ECR Pull-through Cache) 取不到的资源中转进私有 ECR:
#   1) Helm chart (OCI 方式推到 ECR charts/*): kuberay-operator / kube-prometheus-stack / grafana
#   2) Grafana 镜像 (Docker Hub 的 grafana/grafana 没有凭证做不了 Pull-through Cache) -> mirror/grafana/grafana:<版本>[-amd64]
# 其余镜像 (quay.io 的 KubeRay operator / Prometheus 全家, registry.k8s.io 的 kube-state-metrics, kiwigrid sidecar)
# 由 bootstrap.sh 建的 Pull-through Cache 规则在节点第一次拉取时自动缓存, 不需要手工中转。
# 用法: scripts/mirror.sh <arm64|x86_64>   (参数 = worker 架构, 决定中转哪个 Grafana 镜像)
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config
ARCH="${1:?用法: mirror.sh <arm64|x86_64>}"

# ---- charts
need=""
chk() { # 名称 chart 版本 仓库URL
  if ecr_tag_exists "charts/$2" "$3"; then info "chart 已在 ECR: charts/$2:$3"; else need="$need $1|$2|$3|$4"; fi
}
chk kuberay kuberay-operator "$KUBERAY_CHART_VERSION" https://ray-project.github.io/kuberay-helm/
chk prom    kube-prometheus-stack "$KPS_CHART_VERSION" https://prometheus-community.github.io/helm-charts
chk grafana grafana "$GRAFANA_CHART_VERSION" https://grafana.github.io/helm-charts
if [ -n "$need" ]; then
  info "中转 Helm chart:$need"
  package_source
  cb_run daft-ray-build codebuild/buildspec-charts.yml "$(cb_env "CHARTS=${need# }")"
  info "chart 已推送到 oci://$ECR_REGISTRY/charts"
fi

# ---- Grafana 镜像
if [ "$ARCH" = arm64 ]; then PLATFORM=linux/arm64; TAG="$GRAFANA_IMAGE_VERSION"; else PLATFORM=linux/amd64; TAG="$GRAFANA_IMAGE_VERSION-amd64"; fi
if ecr_tag_exists mirror/grafana/grafana "$TAG"; then
  info "Grafana 镜像已在 ECR: mirror/grafana/grafana:$TAG"
else
  info "中转 Grafana 镜像 grafana/grafana:$GRAFANA_IMAGE_VERSION ($PLATFORM) -> mirror/grafana/grafana:$TAG"
  package_source
  cb_run daft-ray-build codebuild/buildspec-mirror-image.yml "$(cb_env "SRC_IMAGE=grafana/grafana:$GRAFANA_IMAGE_VERSION" "DST_REPO=mirror/grafana/grafana" "DST_TAG=$TAG" "PLATFORM=$PLATFORM")"
fi
