#!/usr/bin/env bash
# 步骤: 用 CodeBuild 的原生构建环境构建 Ray + Daft 镜像并推到 ECR (本机不需要 docker)。
#   arm64  -> 项目 daft-ray-build      (ARM_CONTAINER),  基础镜像 rayproject/ray:2.59.0-py311-aarch64, 标签 <IMAGE_TAG>
#   x86_64 -> 项目 daft-ray-build-x86  (LINUX_CONTAINER), 基础镜像 rayproject/ray:2.59.0-py311,         标签 <IMAGE_TAG>-x86_64
# 用法: scripts/build_image.sh <arm64|x86_64> [--force]
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config
ARCH="${1:?用法: build_image.sh <arm64|x86_64> [--force]}"; FORCE="${2:-}"
case "$ARCH" in
  arm64)  PROJECT=daft-ray-build;     BASE="$RAY_BASE_IMAGE_ARM"; TAG="$IMAGE_TAG";          EXPECT=aarch64 ;;
  x86_64) PROJECT=daft-ray-build-x86; BASE="$RAY_BASE_IMAGE_X86"; TAG="$IMAGE_TAG-x86_64";   EXPECT=x86_64 ;;
  *) echo "架构只能是 arm64 或 x86_64"; exit 1 ;;
esac
if [ "$FORCE" != "--force" ] && ecr_tag_exists daft-ray "$TAG"; then
  info "镜像已存在: $ECR_REGISTRY/daft-ray:$TAG (加 --force 重新构建)"; exit 0
fi
info "打包源码并上传到 s3://$INPUT_BUCKET/build/src.zip"
package_source
info "构建 $ARCH 镜像 $ECR_REGISTRY/daft-ray:$TAG (基础镜像 $BASE, 约 2~5 分钟)"
cb_run "$PROJECT" codebuild/buildspec-image.yml "$(cb_env "IMAGE_URI=$ECR_REGISTRY/daft-ray:$TAG" "BASE_IMAGE=$BASE" "EXPECT_ARCH=$EXPECT")"
ecr_tag_exists daft-ray "$TAG" || { echo "CodeBuild 成功但 ECR 里没有 $TAG"; exit 1; }
info "已推送 $ECR_REGISTRY/daft-ray:$TAG"
