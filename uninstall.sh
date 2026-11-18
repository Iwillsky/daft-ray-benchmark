#!/usr/bin/env bash
# 销毁 Terraform 创建的全部资源 (EKS 集群、节点、VPC / NAT / Endpoint、IAM 角色、AMP workspace)。
# 不会删除: S3 桶和里面的数据、ECR 里的镜像和 chart、CodeBuild 项目 (它们是 install.sh 第 2 步建的, 可复用, 费用很低)。
#   ./uninstall.sh            销毁集群 (需要确认)
#   ./uninstall.sh --yes      不确认
#   ./uninstall.sh --workers-only   只把 worker 缩到 0 (保留系统节点和监控, 约 $1/小时), 不销毁
# 想连前置资源一起清理, 手工删除: S3 桶 (daft-ray-input-* / daft-ray-output-*)、ECR 仓库 (daft-ray, charts/*, mirror/*)、
# CodeBuild 项目 daft-ray-build / daft-ray-build-x86、IAM 角色 daft-ray-codebuild、ECR Pull-through Cache 规则 (quay / k8s / ecr-public)。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$ROOT/scripts/lib.sh"
YES=0; WORKERS_ONLY=0
for a in "$@"; do case "$a" in --yes|-y) YES=1 ;; --workers-only) WORKERS_ONLY=1 ;; -h|--help) sed -n '2,10p' "$0"; exit 0 ;; *) echo "未知参数 $a"; exit 1 ;; esac; done
load_config
VARS="$STATE_DIR/terraform.tfvars"
[ -f "$VARS" ] || { echo "找不到 $VARS (还没执行过 install.sh 第 6 步?)"; exit 1; }
if [ "$WORKERS_ONLY" = 1 ]; then
  step_begin "1/1" "worker 缩到 0 (保留系统节点和监控)"
  terraform -chdir="$ROOT/terraform" apply -auto-approve -input=false -no-color -var-file="$VARS" -var worker_count=0 2>&1 | tail -5 | sed 's/^/    /'
  step_ok "恢复: ./install.sh --from 7 (worker 数以 config.env 的 WORKER_COUNT 为准)"
  exit 0
fi
step_begin "1/1" "销毁集群 $CLUSTER_NAME"
if [ "$YES" != 1 ]; then printf '  将销毁 EKS 集群 %s 及 VPC / 节点 / AMP workspace (S3 数据和 ECR 镜像保留)。继续? [y/N] ' "$CLUSTER_NAME"; read -r ans; case "$ans" in y|Y|yes) ;; *) echo "已取消"; exit 1 ;; esac; fi
terraform -chdir="$ROOT/terraform" destroy -auto-approve -input=false -no-color -var-file="$VARS" 2>&1 | tee -a "$LOG_FILE" | grep --line-buffered -E 'Destroying\.\.\.|Destruction complete|Error|Destroy complete' | sed 's/^/    /' \
  || step_fail "销毁失败" "常见原因: 节点上的 ENI 尚未释放导致子网 / 安全组删除超时, 等几分钟后重跑 ./uninstall.sh" "详细日志: $LOG_FILE"
step_ok
