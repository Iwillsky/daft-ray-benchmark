# ---------------------------------------------------------------- 控制面
data "aws_iam_policy_document" "eks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${var.name}-cluster"
  assume_role_policy = data.aws_iam_policy_document.eks_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "this" {
  name     = var.name
  version  = var.kubernetes_version
  role_arn = aws_iam_role.cluster.arn

  # 不装默认的 vpc-cni / kube-proxy / coredns, 之后用托管 addon 创建 (同 eksctl 的 disableDefaultAddons)
  bootstrap_self_managed_addons = false

  vpc_config {
    subnet_ids              = aws_subnet.private[*].id
    endpoint_private_access = true # 节点经私网访问 K8s API (需要 VPC Endpoint 才能加入集群)
    endpoint_public_access  = true # 你的 kubectl 经公网访问
    public_access_cidrs     = var.endpoint_public_access_cidrs
  }

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  enabled_cluster_log_types = var.enabled_cluster_log_types
  tags                      = local.tags

  depends_on = [aws_iam_role_policy_attachment.cluster, aws_vpc_endpoint.interface, aws_vpc_endpoint.s3]
}

# IRSA 需要的 OIDC provider
data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "this" {
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
  tags            = local.tags
}

# ---------------------------------------------------------------- 托管 addon
resource "aws_eks_addon" "vpc_cni" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "vpc-cni"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
}

resource "aws_eks_addon" "kube_proxy" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "kube-proxy"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
}

# coredns 需要有节点才会 ACTIVE
resource "aws_eks_addon" "coredns" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "coredns"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  depends_on                  = [aws_eks_node_group.system]
}

# ---------------------------------------------------------------- 节点角色 (系统节点和 worker 共用)
data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${var.name}-node"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])
  role       = aws_iam_role.node.name
  policy_arn = each.value
}

# 镜像经 ECR pull-through cache 中转时, 节点首次拉取要有权限在 ECR 里建仓库
resource "aws_iam_role_policy" "node_ecr_pull_through" {
  name = "EcrPullThroughCache"
  role = aws_iam_role.node.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "EcrPullThroughCache"
      Effect   = "Allow"
      Action   = ["ecr:CreateRepository", "ecr:BatchImportUpstreamImage", "ecr:TagResource"]
      Resource = "arn:aws:ecr:${var.region}:${local.account_id}:repository/*"
    }]
  })
}

# ---------------------------------------------------------------- 系统节点组 (role=system)
resource "aws_eks_node_group" "system" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "ng-system"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = [local.private_node_subnet]
  instance_types  = [local.system_type]
  ami_type        = local.worker_ami_type
  capacity_type   = "ON_DEMAND"
  disk_size       = 100

  scaling_config {
    min_size     = 0
    max_size     = max(var.system_node_count, 1)
    desired_size = var.system_node_count
  }

  labels = { role = "system" }
  tags   = merge(local.tags, { workload = "system" })

  depends_on = [aws_iam_role_policy_attachment.node, aws_eks_addon.vpc_cni, aws_eks_addon.kube_proxy]
}

# ---------------------------------------------------------------- worker 启动模板
# EBS 机型: 根盘 600 GiB gp3 / 8000 IOPS / 500 MiB/s, Ray spill 走根盘 (/tmp/ray)。
# NVMe 机型: 根盘 100 GiB, 用 nodeadm 把本地 NVMe 组成 RAID0 (Karpenter 的 instanceStorePolicy: RAID0 等价), spill 走 NVMe。
locals {
  nvme_userdata = <<-EOT
    MIME-Version: 1.0
    Content-Type: multipart/mixed; boundary="BOUNDARY"

    --BOUNDARY
    Content-Type: application/node.eks.aws

    ---
    apiVersion: node.eks.aws/v1alpha1
    kind: NodeConfig
    spec:
      instance:
        localStorage:
          strategy: RAID0

    --BOUNDARY--
  EOT
}

resource "aws_launch_template" "worker" {
  count       = local.elastic ? 0 : 1
  name_prefix = "${var.name}-worker-"

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = local.has_nvme ? var.nvme_root_volume_gib : var.ebs_root_volume_gib
      volume_type           = "gp3"
      iops                  = local.has_nvme ? 3000 : var.ebs_root_iops
      throughput            = local.has_nvme ? 125 : var.ebs_root_throughput
      encrypted             = true
      delete_on_termination = true
    }
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 2
    http_tokens                 = "required"
  }

  user_data = local.has_nvme ? base64encode(local.nvme_userdata) : null

  tag_specifications {
    resource_type = "instance"
    tags          = merge(local.tags, { workload = "ray-worker", benchmark = "tpch-${var.worker_instance_type}" })
  }
  tag_specifications {
    resource_type = "volume"
    tags          = merge(local.tags, { workload = "ray-worker" })
  }
}

# ---------------------------------------------------------------- worker 节点组 (仅 static 模式: min = max = desired)
resource "aws_eks_node_group" "worker" {
  count           = local.elastic ? 0 : 1
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "ng-worker"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = [local.private_node_subnet]
  instance_types  = [var.worker_instance_type]
  ami_type        = local.worker_ami_type
  capacity_type   = "ON_DEMAND"

  launch_template {
    id      = aws_launch_template.worker[0].id
    version = aws_launch_template.worker[0].latest_version
  }

  scaling_config {
    min_size     = var.worker_count
    max_size     = max(var.worker_count, 1) # EKS 要求 max_size >= 1; worker_count = 0 时节点数缩到 0
    desired_size = var.worker_count
  }

  labels = { role = "ray-worker" }
  taint {
    key    = "workload"
    value  = "ray-worker"
    effect = "NO_SCHEDULE"
  }
  tags = merge(local.tags, { workload = "ray-worker" })

  depends_on = [aws_iam_role_policy_attachment.node, aws_eks_addon.vpc_cni, aws_eks_addon.kube_proxy]
}
