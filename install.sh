#!/usr/bin/env bash
# 一键部署: 生成数据 -> 上传 ECR 镜像 -> 创建 EKS 集群 -> 安装 Helm chart (KubeRay / Prometheus / Grafana) -> 起 Ray 集群并验证。
# 每一步都幂等 (已存在就跳过), 中途失败修好问题后直接重跑即可, 也可以 --from <步骤号> 从某一步继续。
#
#   ./install.sh                    全流程 (先显示计划并确认)
#   ./install.sh --yes             不确认, 直接执行
#   ./install.sh --dry-run         只显示每一步要做什么, 不调用 AWS
#   ./install.sh --from 6          从第 6 步开始 (前面步骤视为已完成)
#   ./install.sh --only 5          只做第 5 步
#   ./install.sh --skip 5          跳过某些步骤 (逗号分隔, 例: --skip 5 跳过数据生成)
#   ./install.sh --config my.env   使用其它配置文件 (默认 config.env)
#
# 步骤:
#   1 预检         工具 / 凭证 / 机型和可用区
#   2 AWS 前置资源 S3 桶、ECR 仓库、Pull-through Cache、CodeBuild 角色和项目 (scripts/bootstrap.sh)
#   3 Ray+Daft 镜像 CodeBuild 构建并推到 ECR (scripts/build_image.sh)
#   4 Helm chart 与镜像中转  chart -> ECR (OCI), Grafana 镜像 -> ECR (scripts/mirror.sh)
#   5 TPC-H 数据与参考结果   CodeBuild 生成到 S3 (scripts/gen_data.sh, gen_reference.sh)
#   6 EKS 集群与节点  Terraform: VPC / Endpoint / EKS / 系统节点 / worker 节点
#   7 集群内组件      Terraform: KubeRay、Prometheus->AMP、Grafana、RayCluster
#   8 验证           等 Ray 就绪, 同步作业脚本, 跑冒烟作业, 打印访问方式
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$ROOT/scripts/lib.sh"
TOTAL=8
YES=0; DRY=0; FROM=1; ONLY=""; SKIP=","
while [ $# -gt 0 ]; do
  case "$1" in
    -y|--yes) YES=1; shift ;;
    --dry-run) DRY=1; shift ;;
    --from) FROM="$2"; shift 2 ;;
    --only) ONLY="$2"; shift 2 ;;
    --skip) SKIP=",$2,"; shift 2 ;;
    --config) export CONFIG_FILE="$2"; shift 2 ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "未知参数: $1 (见 --help)"; exit 1 ;;
  esac
done
: >"$LOG_FILE"
should_run() { # 步骤号
  [ -n "$ONLY" ] && { [ "$ONLY" = "$1" ]; return; }
  case "$SKIP" in *",$1,"*) return 1 ;; esac
  [ "$1" -ge "$FROM" ]
}
# 运行子脚本, 输出缩进显示并写日志; 返回子脚本的退出码
run_script() {
  local rc
  set +e
  "$@" 2>&1 | tee -a "$LOG_FILE" | sed 's/^/    /'
  rc=${PIPESTATUS[0]}
  set -e
  return "$rc"
}

# ---------------------------------------------------------------- 配置 (dry-run 不访问 AWS)
if [ "$DRY" = 1 ]; then
  CF="${CONFIG_FILE:-$ROOT/config.env}"; [ -f "$CF" ] || CF="$ROOT/config.env.example"
  set -a; . "$CF"; set +a
  ACCOUNT_ID="<账号ID>"; AWS_REGION="${AWS_REGION:-us-west-2}"
  INPUT_BUCKET="${INPUT_BUCKET:-daft-ray-input-$ACCOUNT_ID-$AWS_REGION}"; OUTPUT_BUCKET="${OUTPUT_BUCKET:-daft-ray-output-$ACCOUNT_ID-$AWS_REGION}"
  ECR_REGISTRY="$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
else
  load_config
fi
WORKER_INSTANCE_TYPE="${WORKER_INSTANCE_TYPE:-r8g.4xlarge}"; CLUSTER_NAME="${CLUSTER_NAME:-daft-ray-static}"; WORKER_COUNT="${WORKER_COUNT:-3}"
DATA_SCALE_FACTORS="${DATA_SCALE_FACTORS:-1 100}"; GENERATE_REFERENCE="${GENERATE_REFERENCE:-true}"
TFDIR="$ROOT/terraform"; TFVARS="$STATE_DIR/terraform.tfvars"

printf '%s\n' "${C_BLD}Daft on Ray / EKS 一键部署${C_RST}"
cat <<EOF
  账号 / 区域   : $ACCOUNT_ID / $AWS_REGION   (AZ: $AZ1 [节点], $AZ2 [仅控制面])
  集群          : $CLUSTER_NAME   机型 ${WORKER_INSTANCE_TYPE} x $WORKER_COUNT
  S3            : s3://$INPUT_BUCKET/$DATA_PREFIX  (数据)   s3://$OUTPUT_BUCKET/$DATA_PREFIX  (结果)
  数据规模      : SF $DATA_SCALE_FACTORS   参考结果: $GENERATE_REFERENCE
  ECR           : $ECR_REGISTRY
EOF

if [ "$DRY" = 1 ]; then
  cat <<EOF

${C_YEL}--dry-run: 只列出各步骤要做的事, 不调用 AWS${C_RST}
  [1/8] 预检         检查 aws / terraform / kubectl / python3 / zip, 凭证, 机型 $WORKER_INSTANCE_TYPE 在 $AZ1 的供货
  [2/8] 前置资源     bootstrap.sh: S3 桶 x2, ECR 仓库 x5, Pull-through Cache x3, IAM 角色 daft-ray-codebuild, CodeBuild 项目 x2
  [3/8] 镜像         build_image.sh <由机型决定的架构>: CodeBuild 构建 daft-ray:$IMAGE_TAG[-x86_64]
  [4/8] 中转         mirror.sh: charts/{kuberay-operator $KUBERAY_CHART_VERSION, kube-prometheus-stack $KPS_CHART_VERSION, grafana $GRAFANA_CHART_VERSION}, mirror/grafana/grafana:$GRAFANA_IMAGE_VERSION
  [5/8] 数据         gen_data.sh / gen_reference.sh: SF $DATA_SCALE_FACTORS
  [6/8] 集群         terraform apply (目标: VPC / Endpoint / EKS / 系统节点 / worker 节点 / coredns)
  [7/8] 组件         terraform apply (KubeRay operator / kube-prometheus-stack / Grafana / PodMonitor / RayCluster / Ray 面板)
  [8/8] 验证         等 Ray head 和 $WORKER_COUNT 个 worker Ready, 同步 job/, 跑 SF1 冒烟作业, 打印 terraform output
EOF
  exit 0
fi

# ---------------------------------------------------------------- 1 预检
if should_run 1; then
  step_begin "1/$TOTAL" "预检: 工具、凭证、机型"
  miss=""
  for c in aws terraform kubectl python3 zip; do command -v "$c" >/dev/null 2>&1 || miss="$miss $c"; done
  [ -z "$miss" ] || step_fail "缺少工具:$miss" "macOS: brew install awscli terraform kubernetes-cli (zip / python3 系统自带)" "Terraform >= 1.6, AWS CLI v2"
  info "工具齐全: aws $(aws --version 2>&1 | awk '{print $1}' | cut -d/ -f2), terraform $(terraform version | head -1 | awk '{print $2}'), kubectl $(kubectl version --client -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["clientVersion"]["gitVersion"])' 2>/dev/null || echo ?)"
  info "AWS 身份: $(aws sts get-caller-identity --query Arn --output text)"
  ARCH="$(worker_arch)"
  [ -n "$ARCH" ] && [ "$ARCH" != None ] || step_fail "无法识别机型 $WORKER_INSTANCE_TYPE" "检查 config.env 里的 WORKER_INSTANCE_TYPE 拼写" "aws ec2 describe-instance-types --instance-types $WORKER_INSTANCE_TYPE"
  info "机型 $WORKER_INSTANCE_TYPE 架构 $ARCH"
  off="$(aws ec2 describe-instance-type-offerings --location-type availability-zone --filters "Name=instance-type,Values=$WORKER_INSTANCE_TYPE" "Name=location,Values=$AZ1" --query 'InstanceTypeOfferings[0].InstanceType' --output text 2>/dev/null || true)"
  [ "$off" = "$WORKER_INSTANCE_TYPE" ] || step_fail "$WORKER_INSTANCE_TYPE 在 $AZ1 没有供货" "换 AZ1 (config.env) 或换机型" "aws ec2 describe-instance-type-offerings --location-type availability-zone --filters Name=instance-type,Values=$WORKER_INSTANCE_TYPE"
  info "$AZ1 有 $WORKER_INSTANCE_TYPE 供货"
  step_ok
  if [ "$YES" != 1 ]; then
    printf '\n  将创建的主要计费资源: %s 台 %s + 2 台系统节点 + EKS 控制面 + NAT + VPC Endpoint + AMP, 约 $4~5/小时; CodeBuild 和数据生成另计。\n  继续? [y/N] ' "$WORKER_COUNT" "$WORKER_INSTANCE_TYPE"
    read -r ans; case "$ans" in y|Y|yes) ;; *) echo "已取消"; exit 1 ;; esac
  fi
fi
ARCH="${ARCH:-$(worker_arch)}"

# ---------------------------------------------------------------- 2 前置资源
if should_run 2; then
  step_begin "2/$TOTAL" "AWS 前置资源: S3 桶 / ECR 仓库 / Pull-through Cache / CodeBuild"
  run_script "$ROOT/scripts/bootstrap.sh" || step_fail "bootstrap 失败" "看上面的 AWS 报错; 常见原因: 权限不足 (需要 IAM / S3 / ECR / CodeBuild 管理权限)、桶名已被别的账号占用"
  step_ok
fi

# ---------------------------------------------------------------- 3 镜像
if should_run 3; then
  step_begin "3/$TOTAL" "构建并推送 Ray+Daft 镜像到 ECR ($ARCH)"
  run_script "$ROOT/scripts/build_image.sh" "$ARCH" || step_fail "镜像构建失败" "查看 CodeBuild 日志: aws logs tail /aws/codebuild/daft-ray-build$( [ "$ARCH" = x86_64 ] && echo -x86) --since 1h" "Docker Hub 匿名拉取被限流时重跑即可"
  step_ok
fi

# ---------------------------------------------------------------- 4 中转
if should_run 4; then
  step_begin "4/$TOTAL" "Helm chart 与 Grafana 镜像中转到 ECR"
  run_script "$ROOT/scripts/mirror.sh" "$ARCH" || step_fail "中转失败" "查看 CodeBuild 日志: aws logs tail /aws/codebuild/daft-ray-build --since 1h"
  step_ok
fi

# ---------------------------------------------------------------- 5 数据
if should_run 5; then
  step_begin "5/$TOTAL" "TPC-H 数据与参考结果: SF $DATA_SCALE_FACTORS"
  for sf in $DATA_SCALE_FACTORS; do
    info "--- SF$sf 数据"
    run_script "$ROOT/scripts/gen_data.sh" "$sf" || step_fail "SF$sf 数据生成失败" "查看 CodeBuild 日志: aws logs tail /aws/codebuild/daft-ray-build --since 3h" "重跑 ./install.sh --only 5 会跳过已完成的规模"
    if [ "$GENERATE_REFERENCE" = true ]; then
      info "--- SF$sf 参考结果"
      run_script "$ROOT/scripts/gen_reference.sh" "$sf" || step_fail "SF$sf 参考结果生成失败" "DuckDB 偶发被 OOM 杀掉 (退出码 137), buildspec 已自动重试 8 次; 仍失败请换更大的 CodeBuild 规格 (scripts/gen_reference.sh)"
    fi
  done
  step_ok
fi

# ---------------------------------------------------------------- Terraform 变量
write_tfvars() {
  local sfs; sfs="$(echo "$DATA_SCALE_FACTORS" | sed 's/ \+/, /g')"
  {
    echo "region               = \"$AWS_REGION\""
    echo "name                 = \"$CLUSTER_NAME\""
    echo "worker_instance_type = \"$WORKER_INSTANCE_TYPE\""
    echo "worker_count         = $WORKER_COUNT"
    echo "object_store_bytes   = $OBJECT_STORE_BYTES"
    echo "availability_zones   = [\"$AZ1\", \"$AZ2\"]"
    echo "ray_image_tag        = \"$IMAGE_TAG\""
    echo "input_bucket_name    = \"$INPUT_BUCKET\""
    echo "output_bucket_name   = \"$OUTPUT_BUCKET\""
    echo "data_prefix          = \"$DATA_PREFIX\""
    echo "tpch_scale_factors   = [$sfs]"
    echo "kuberay_operator_version      = \"$KUBERAY_CHART_VERSION\""
    echo "kube_prometheus_stack_version = \"$KPS_CHART_VERSION\""
    echo "grafana_chart_version         = \"$GRAFANA_CHART_VERSION\""
    echo "grafana_image_tag_arm64       = \"$GRAFANA_IMAGE_VERSION\""
    echo "grafana_image_tag_amd64       = \"$GRAFANA_IMAGE_VERSION-amd64\""
    [ -z "${AWS_PROFILE:-}" ] || echo "aws_profile          = \"$AWS_PROFILE\""
  } >"$TFVARS"
}
# terraform 输出只显示关键行, 完整输出在日志里
tf_apply() { # 额外参数 (-target=...)
  local rc
  set +e
  terraform -chdir="$TFDIR" apply -auto-approve -input=false -no-color -var-file="$TFVARS" "$@" 2>&1 | tee -a "$LOG_FILE" \
    | grep --line-buffered -E 'Plan:|Creating\.\.\.|Creation complete|Modifications complete|Error|Apply complete|Warning: .*deprecated' | sed 's/^/    /'
  rc=${PIPESTATUS[0]}
  set -e
  return "$rc"
}

# ---------------------------------------------------------------- 6 集群
if should_run 6; then
  step_begin "6/$TOTAL" "Terraform: VPC / EKS 集群 / 系统节点 / worker 节点 (约 15~20 分钟)"
  write_tfvars
  run_script terraform -chdir="$TFDIR" init -input=false -no-color -upgrade=false >/dev/null || step_fail "terraform init 失败" "检查网络能否访问 registry.terraform.io"
  info "分阶段创建: 先只建网络、EKS、节点和 coredns, 节点就绪后第 7 步再装集群内组件 (这样 kubernetes / helm provider 一定拿得到集群)"
  tf_apply -target=aws_eks_node_group.system -target=aws_eks_node_group.worker -target=aws_eks_addon.coredns -target=aws_eks_addon.kube_proxy \
    || step_fail "创建 EKS / 节点失败" "节点组一直 CREATING: 检查 VPC Endpoint 是否建成 (create_vpc_endpoints)、机型在 $AZ1 是否有容量" "修复后重跑 ./install.sh --from 6 (terraform 会接着做)"
  step_ok
fi

# ---------------------------------------------------------------- 7 组件
if should_run 7; then
  step_begin "7/$TOTAL" "Terraform: KubeRay / Prometheus / Grafana / RayCluster (约 10 分钟)"
  [ -f "$TFVARS" ] || write_tfvars
  tf_apply || step_fail "安装集群内组件失败" "helm 拉不到 chart: 确认第 4 步已完成 (ECR 里有 charts/*), 节点能经 Pull-through Cache 拉镜像" "查看详细错误: $LOG_FILE" "修复后重跑 ./install.sh --from 7"
  step_ok
fi

# ---------------------------------------------------------------- 8 验证
if should_run 8; then
  step_begin "8/$TOTAL" "验证: Ray 集群就绪、同步作业脚本、冒烟作业"
  LOGIN="$(terraform -chdir="$TFDIR" output -raw eks_login_command 2>/dev/null)" || step_fail "读不到 terraform 输出" "先完成第 6、7 步"
  eval "$LOGIN" >/dev/null
  info "kubeconfig 已写入 (context $CLUSTER_NAME)"
  K="kubectl --context $CLUSTER_NAME"
  info "节点:"; $K get nodes -L node.kubernetes.io/instance-type,role --no-headers | sed 's/^/        /'
  $K -n ray wait --for=condition=Ready pod -l ray.io/node-type=head --timeout=900s >/dev/null 2>&1 || step_fail "Ray head 15 分钟内没有 Ready" "kubectl --context $CLUSTER_NAME -n ray describe pod -l ray.io/node-type=head" "镜像拉取失败: 确认 ECR 里有 daft-ray:$IMAGE_TAG"
  info "Ray head Ready"
  n=0
  for _ in $(seq 1 90); do
    n=$($K -n ray get pods -l ray.io/node-type=worker --no-headers 2>/dev/null | awk '$2=="1/1" && $3=="Running"' | wc -l | tr -d ' ')
    [ "$n" -ge "$WORKER_COUNT" ] && break; sleep 10
  done
  [ "$n" -ge "$WORKER_COUNT" ] || step_fail "只有 $n/$WORKER_COUNT 个 worker Ready" "kubectl --context $CLUSTER_NAME -n ray get pods -o wide" "worker Pod Pending: 资源不足时降低 object store 或 worker_memory_gib"
  info "$n/$WORKER_COUNT 个 worker Ready"
  "$ROOT/scripts/sync_job.sh" "$CLUSTER_NAME" >/dev/null && info "job/ 已同步到 head:/tmp/job"
  case " $DATA_SCALE_FACTORS " in
    *" 1 "*) info "冒烟作业: SF1 Q1-Q3 (对照官方标准答案)"
             run_script "$ROOT/scripts/run_benchmark.sh" --sf 1 --questions 1-3 -- --no-warmup || step_fail "冒烟作业失败" "kubectl --context $CLUSTER_NAME -n ray logs -l ray.io/node-type=head -c ray-head --tail=100" ;;
    *) info "DATA_SCALE_FACTORS 里没有 1, 跳过冒烟作业 (手动: scripts/run_benchmark.sh --sf <SF>)" ;;
  esac
  step_ok
  printf '\n%s部署完成%s。访问方式 (Dashboard 没有对外暴露, 先开 port-forward):\n' "$C_GRN$C_BLD" "$C_RST"
  terraform -chdir="$TFDIR" output -no-color ray_dashboard grafana_dashboard data_sample_s3 eks_login_command | sed 's/^/  /'
  printf '\n  日志位置: terraform -chdir=terraform output logs\n  提交作业: scripts/run_benchmark.sh --sf 100 --questions 1-22 -- --s3-refresh-creds\n  销毁集群: ./uninstall.sh\n'
fi
