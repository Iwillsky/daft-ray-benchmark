#!/usr/bin/env bash
# 步骤: 创建 Terraform 之外必须先存在的 AWS 资源 (全部幂等, 已存在就跳过):
#   - S3 输入桶 / 输出桶
#   - ECR 仓库: daft-ray, charts/{kuberay-operator,kube-prometheus-stack,grafana}, mirror/grafana/grafana
#   - ECR Pull-through Cache 规则: quay / k8s / ecr-public (节点访问不了公网镜像仓库时由 ECR 代拉)
#   - CodeBuild 服务角色 daft-ray-codebuild 和两个项目 daft-ray-build (ARM) / daft-ray-build-x86
# 用法: scripts/bootstrap.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

render() { # 模板 -> 标准输出
  sed -e "s#@ACCOUNT@#$ACCOUNT_ID#g" -e "s#@REGION@#$AWS_REGION#g" -e "s#@INPUT_BUCKET@#$INPUT_BUCKET#g" \
      -e "s#@OUTPUT_BUCKET@#$OUTPUT_BUCKET#g" -e "s#@ECR_REGISTRY@#$ECR_REGISTRY#g" -e "s#@DATA_PREFIX@#$DATA_PREFIX#g" "$1"
}

ensure_bucket() {
  local b="$1"
  if aws s3api head-bucket --bucket "$b" 2>/dev/null; then info "S3 桶已存在: $b"; return; fi
  if [ "$AWS_REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$b" >/dev/null
  else
    aws s3api create-bucket --bucket "$b" --create-bucket-configuration "LocationConstraint=$AWS_REGION" >/dev/null
  fi
  aws s3api put-public-access-block --bucket "$b" \
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  info "已创建 S3 桶: $b"
}

ensure_repo() {
  if aws ecr describe-repositories --repository-names "$1" >/dev/null 2>&1; then info "ECR 仓库已存在: $1"; return; fi
  aws ecr create-repository --repository-name "$1" --image-tag-mutability MUTABLE >/dev/null
  info "已创建 ECR 仓库: $1"
}

ensure_ptc() { # 前缀 上游
  if aws ecr describe-pull-through-cache-rules --ecr-repository-prefixes "$1" >/dev/null 2>&1; then info "Pull-through cache 规则已存在: $1 -> $2"; return; fi
  aws ecr create-pull-through-cache-rule --ecr-repository-prefix "$1" --upstream-registry-url "$2" >/dev/null
  info "已创建 Pull-through cache 规则: $1 -> $2"
}

ensure_role() {
  local created=0
  if ! aws iam get-role --role-name daft-ray-codebuild >/dev/null 2>&1; then
    aws iam create-role --role-name daft-ray-codebuild --assume-role-policy-document "file://$ROOT_DIR/codebuild/templates/trust.json" >/dev/null
    created=1; info "已创建 IAM 角色 daft-ray-codebuild"
  else
    info "IAM 角色已存在: daft-ray-codebuild (策略会刷新)"
  fi
  render "$ROOT_DIR/codebuild/templates/role-policy.json.tpl" >"$STATE_DIR/role-policy.json"
  aws iam put-role-policy --role-name daft-ray-codebuild --policy-name build --policy-document "file://$STATE_DIR/role-policy.json"
  if [ "$created" = 1 ]; then info "等待 IAM 角色生效 (15 秒)"; sleep 15; fi
}

ensure_project() { # 名称 模板
  render "$ROOT_DIR/codebuild/templates/$2" >"$STATE_DIR/$1.json"
  if aws codebuild batch-get-projects --names "$1" --query 'projects[0].name' --output text 2>/dev/null | grep -q "^$1$"; then
    aws codebuild update-project --cli-input-json "file://$STATE_DIR/$1.json" >/dev/null; info "CodeBuild 项目已更新: $1"
  else
    aws codebuild create-project --cli-input-json "file://$STATE_DIR/$1.json" >/dev/null; info "已创建 CodeBuild 项目: $1"
  fi
}

ensure_bucket "$INPUT_BUCKET"
ensure_bucket "$OUTPUT_BUCKET"
for r in daft-ray charts/kuberay-operator charts/kube-prometheus-stack charts/grafana mirror/grafana/grafana; do ensure_repo "$r"; done
ensure_ptc quay quay.io
ensure_ptc k8s registry.k8s.io
ensure_ptc ecr-public public.ecr.aws
ensure_role
ensure_project daft-ray-build project-arm.json.tpl
ensure_project daft-ray-build-x86 project-x86.json.tpl
