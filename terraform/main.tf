data "aws_caller_identity" "current" {}

# 机型信息: 架构 (arm64 / x86_64) 和是否带本地 NVMe, 都从 AWS API 读, 不靠机型名猜
data "aws_ec2_instance_type" "worker" {
  instance_type = var.worker_instance_type
}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # ---- 由机型推导
  is_arm          = contains(data.aws_ec2_instance_type.worker.supported_architectures, "arm64")
  has_nvme        = data.aws_ec2_instance_type.worker.instance_storage_supported
  arch_label      = local.is_arm ? "arm64" : "amd64"
  worker_ami_type = local.is_arm ? "AL2023_ARM_64_STANDARD" : "AL2023_x86_64_STANDARD"
  system_type     = coalesce(var.system_instance_type, local.is_arm ? (local.elastic ? "m8g.4xlarge" : "m8g.2xlarge") : (local.elastic ? "m8i.4xlarge" : "m8i.2xlarge"))

  # ---- 镜像
  ecr_registry = coalesce(var.ecr_registry, "${local.account_id}.dkr.ecr.${var.region}.amazonaws.com")
  ray_image    = coalesce(var.ray_image, "${local.ecr_registry}/daft-ray:${var.ray_image_tag}${local.is_arm ? "" : "-x86_64"}")
  grafana_tag  = local.is_arm ? var.grafana_image_tag_arm64 : var.grafana_image_tag_amd64

  # ---- Ray 规格 (同测试一: shm 比 object store 大 2 GiB)
  shm_gib = ceil(var.object_store_bytes / 1073741824) + 2

  # ---- S3
  input_bucket  = coalesce(var.input_bucket_name, "daft-ray-input-${local.account_id}-${var.region}")
  output_bucket = coalesce(var.output_bucket_name, "daft-ray-output-${local.account_id}-${var.region}")

  # ---- AMP
  amp_workspace_id = var.create_amp_workspace ? aws_prometheus_workspace.this[0].id : var.amp_workspace_id

  # ---- 命令里的 profile 参数
  profile_arg         = var.aws_profile == null ? "" : " --profile ${var.aws_profile}"
  kube_context        = var.name
  oidc_issuer         = replace(aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")
  node_azs            = var.availability_zones
  private_node_subnet = aws_subnet.private[0].id

  tags = merge(var.tags, { cluster = var.name })
}

resource "aws_prometheus_workspace" "this" {
  count = var.create_amp_workspace ? 1 : 0
  alias = var.name
  tags  = local.tags
}

resource "aws_s3_bucket" "input" {
  count  = var.create_buckets ? 1 : 0
  bucket = local.input_bucket
  tags   = local.tags
}

resource "aws_s3_bucket" "output" {
  count  = var.create_buckets ? 1 : 0
  bucket = local.output_bucket
  tags   = local.tags
}

# 账号级的 ECR pull-through cache (节点访问不了 quay.io / registry.k8s.io, 镜像经 ECR 中转)
resource "aws_ecr_pull_through_cache_rule" "this" {
  for_each = var.create_ecr_pull_through_cache ? {
    quay       = "quay.io"
    k8s        = "registry.k8s.io"
    ecr-public = "public.ecr.aws"
  } : {}
  ecr_repository_prefix = each.key
  upstream_registry_url = each.value
}
