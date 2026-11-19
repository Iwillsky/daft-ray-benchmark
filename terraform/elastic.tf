# ======================================================================================
# 测试二 (cluster_mode = "elastic"): Karpenter 弹性 worker + FSx for Lustre + 独立 head 节点
#   - 系统节点 ng-system (m8g.4xlarge x2): Karpenter / KubeRay operator / Prometheus / Grafana
#   - head 节点 ng-head (r8g.8xlarge x1, role=ray-head, 有污点): Ray head, 16C / 192Gi (多路并发作业时 head 内存峰值约 33 GiB)
#   - worker: Karpenter NodePool (默认 r8g.16xlarge, vCPU 上限 1024, 只放 AZ1), 每台 2 个 Ray worker Pod (31C / 224Gi), 空闲 5 分钟回收
#   - FSx for Lustre PERSISTENT_2 挂在 /fsx: Ray spill 目录 /fsx/spill, TPC-H 数据 /fsx/<前缀>/sf<N>
# Karpenter 的 SQS 中断队列没有创建 (只用 On-Demand)。
# ======================================================================================

variable "elastic_head_cpus" {
  type    = number
  default = 16
}

variable "elastic_head_memory_gib" {
  type    = number
  default = 192
}

locals {
  elastic = var.cluster_mode == "elastic"

  # 每台 worker 节点放 2 个 Ray worker Pod; 由机型规格推导 (r8g.16xlarge: 64 vCPU / 512 GiB -> 31C / 224Gi, object store 112 GiB)
  e_vcpus     = data.aws_ec2_instance_type.worker.default_vcpus
  e_mem_gib   = floor(data.aws_ec2_instance_type.worker.memory_size / 1024)
  e_pod_cpus  = floor(local.e_vcpus / 2) - 1
  e_pod_mem   = floor(local.e_mem_gib * 0.4375)
  e_obj_bytes = floor(local.e_pod_mem / 2) * 1073741824
  e_shm_gib   = floor(local.e_pod_mem / 2) + 2
  e_max_pods  = floor(var.elastic_max_vcpus / local.e_vcpus) * 2
}

# ---------------------------------------------------------------- head 节点组
resource "aws_eks_node_group" "head" {
  count           = local.elastic ? 1 : 0
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "ng-head"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = [local.private_node_subnet]
  instance_types  = [var.head_instance_type]
  ami_type        = local.worker_ami_type
  capacity_type   = "ON_DEMAND"
  disk_size       = 100

  scaling_config {
    min_size     = 1
    max_size     = 1
    desired_size = 1
  }

  labels = { role = "ray-head" }
  taint {
    key    = "workload"
    value  = "ray-head"
    effect = "NO_SCHEDULE"
  }
  tags       = merge(local.tags, { workload = "ray-head" })
  depends_on = [aws_iam_role_policy_attachment.node, aws_eks_addon.vpc_cni, aws_eks_addon.kube_proxy]
}

# ---------------------------------------------------------------- Karpenter: 节点角色 / 控制器角色 (EKS Pod Identity)
resource "aws_eks_addon" "pod_identity_agent" {
  count                       = local.elastic ? 1 : 0
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "eks-pod-identity-agent"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  depends_on                  = [aws_eks_node_group.system]
}

resource "aws_iam_role" "karpenter_node" {
  count              = local.elastic ? 1 : 0
  name               = "KarpenterNodeRole-${var.name}"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "karpenter_node" {
  for_each = local.elastic ? toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ]) : toset([])
  role       = aws_iam_role.karpenter_node[0].name
  policy_arn = each.value
}

resource "aws_iam_role_policy" "karpenter_node_ecr_pull_through" {
  count = local.elastic ? 1 : 0
  name  = "EcrPullThroughCache"
  role  = aws_iam_role.karpenter_node[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ecr:CreateRepository", "ecr:BatchImportUpstreamImage", "ecr:TagResource"]
      Resource = "arn:aws:ecr:${var.region}:${local.account_id}:repository/*"
    }]
  })
}

# 节点没有 IAM API 的 VPC Endpoint, Karpenter 无法自己建 instance profile, 所以预先建好并在 EC2NodeClass 里指定
resource "aws_iam_instance_profile" "karpenter_node" {
  count = local.elastic ? 1 : 0
  name  = "KarpenterNodeInstanceProfile-${var.name}"
  role  = aws_iam_role.karpenter_node[0].name
  tags  = local.tags
}

resource "aws_eks_access_entry" "karpenter_node" {
  count         = local.elastic ? 1 : 0
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = aws_iam_role.karpenter_node[0].arn
  type          = "EC2_LINUX"
}

data "aws_iam_policy_document" "pods_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "karpenter_controller" {
  statement {
    sid = "Ec2Describe"
    actions = [
      "ec2:DescribeAvailabilityZones", "ec2:DescribeImages", "ec2:DescribeInstances", "ec2:DescribeInstanceTypeOfferings",
      "ec2:DescribeInstanceTypes", "ec2:DescribeLaunchTemplates", "ec2:DescribeSecurityGroups", "ec2:DescribeSpotPriceHistory",
      "ec2:DescribeSubnets",
    ]
    resources = ["*"]
  }
  statement {
    sid = "Ec2Provision"
    actions = [
      "ec2:RunInstances", "ec2:CreateFleet", "ec2:CreateLaunchTemplate", "ec2:CreateTags", "ec2:DeleteLaunchTemplate",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.region]
    }
  }
  statement {
    sid       = "Ec2Terminate"
    actions   = ["ec2:TerminateInstances"]
    resources = ["*"]
    condition {
      test     = "StringLike"
      variable = "ec2:ResourceTag/karpenter.sh/nodepool"
      values   = ["*"]
    }
  }
  statement {
    sid       = "PassNodeRole"
    actions   = ["iam:PassRole"]
    resources = [try(aws_iam_role.karpenter_node[0].arn, "*")]
  }
  statement {
    sid       = "InstanceProfileRead"
    actions   = ["iam:GetInstanceProfile"]
    resources = ["*"]
  }
  statement {
    sid       = "AmiAlias"
    actions   = ["ssm:GetParameter"]
    resources = ["arn:aws:ssm:${var.region}::parameter/aws/service/*"]
  }
  statement {
    sid       = "EksDescribe"
    actions   = ["eks:DescribeCluster"]
    resources = [aws_eks_cluster.this.arn]
  }
}

resource "aws_iam_role" "karpenter_controller" {
  count              = local.elastic ? 1 : 0
  name               = "${var.name}-karpenter"
  assume_role_policy = data.aws_iam_policy_document.pods_trust.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "karpenter_controller" {
  count  = local.elastic ? 1 : 0
  name   = "karpenter-controller"
  role   = aws_iam_role.karpenter_controller[0].id
  policy = data.aws_iam_policy_document.karpenter_controller.json
}

resource "aws_eks_pod_identity_association" "karpenter" {
  count           = local.elastic ? 1 : 0
  cluster_name    = aws_eks_cluster.this.name
  namespace       = "kube-system"
  service_account = "karpenter"
  role_arn        = aws_iam_role.karpenter_controller[0].arn
  depends_on      = [aws_eks_addon.pod_identity_agent]
}

# ---------------------------------------------------------------- Karpenter 本体 + NodePool / EC2NodeClass
# 控制器镜像走 ECR Pull-through Cache (ecr-public 前缀); isolatedVPC 让 Karpenter 不去访问没有 VPC Endpoint 的 Pricing API
resource "helm_release" "karpenter" {
  count      = local.elastic ? 1 : 0
  name       = "karpenter"
  namespace  = "kube-system"
  repository = "oci://public.ecr.aws/karpenter"
  chart      = "karpenter"
  version    = var.karpenter_version
  timeout    = 600
  wait       = true

  set {
    name  = "settings.clusterName"
    value = var.name
  }
  set {
    name  = "settings.clusterEndpoint"
    value = aws_eks_cluster.this.endpoint
  }
  set {
    name  = "settings.isolatedVPC"
    value = "true"
  }
  set {
    name  = "controller.image.repository"
    value = "${local.ecr_registry}/ecr-public/karpenter/controller"
  }
  set {
    name  = "controller.image.tag"
    value = var.karpenter_version
  }
  set {
    name  = "controller.image.digest"
    value = ""
  }
  set {
    name  = "controller.resources.requests.cpu"
    value = "1"
  }
  set {
    name  = "controller.resources.requests.memory"
    value = "1Gi"
  }
  set {
    name  = "controller.resources.limits.cpu"
    value = "1"
  }
  set {
    name  = "controller.resources.limits.memory"
    value = "1Gi"
  }
  set {
    name  = "nodeSelector.role"
    value = "system"
  }
  depends_on = [aws_eks_node_group.system, aws_eks_addon.coredns, aws_eks_pod_identity_association.karpenter, aws_eks_access_entry.karpenter_node]
}

resource "kubectl_manifest" "nodepool" {
  count = local.elastic ? 1 : 0
  yaml_body = templatefile("${path.module}/templates/karpenter-nodepool.yaml.tftpl", {
    name             = "ray-${replace(var.worker_instance_type, ".", "")}"
    instance_type    = var.worker_instance_type
    zone             = var.availability_zones[0]
    arch             = local.arch_label
    max_vcpus        = var.elastic_max_vcpus
    max_memory_gib   = var.elastic_max_vcpus * 8
    instance_profile = aws_iam_instance_profile.karpenter_node[0].name
    cluster_name     = var.name
    tags             = jsonencode({ workload = "ray-worker", project = var.name })
  })
  depends_on = [helm_release.karpenter]
}

# ---------------------------------------------------------------- FSx for Lustre
resource "aws_security_group" "fsx" {
  count       = local.elastic ? 1 : 0
  name        = "${var.name}-fsx"
  description = "FSx for Lustre ${var.name}"
  vpc_id      = aws_vpc.this.id
  ingress {
    from_port   = 988
    to_port     = 988
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  ingress {
    from_port   = 1018
    to_port     = 1023
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = merge(local.tags, { Name = "${var.name}-fsx" })
}

resource "aws_fsx_lustre_file_system" "this" {
  count                       = local.elastic ? 1 : 0
  storage_capacity            = var.fsx_storage_gib
  subnet_ids                  = [local.private_node_subnet]
  security_group_ids          = [aws_security_group.fsx[0].id]
  deployment_type             = "PERSISTENT_2"
  per_unit_storage_throughput = var.fsx_throughput_per_tib
  storage_type                = "SSD"
  file_system_type_version    = "2.15"
  data_compression_type       = "NONE"
  skip_final_backup           = true
  tags                        = merge(local.tags, { Name = "${var.name}-fsx" })
}

resource "aws_eks_addon" "fsx_csi" {
  count                       = local.elastic ? 1 : 0
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "aws-fsx-csi-driver"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  depends_on                  = [aws_eks_node_group.system, aws_eks_addon.coredns]
}

# 静态供给: 一个 PV + PVC (ReadWriteMany), head 和所有 worker 挂到 /fsx
resource "kubectl_manifest" "fsx_pv" {
  count = local.elastic ? 1 : 0
  yaml_body = yamlencode({
    apiVersion = "v1"
    kind       = "PersistentVolume"
    metadata   = { name = "fsx-pv" }
    spec = {
      capacity                      = { storage = "${var.fsx_storage_gib}Gi" }
      accessModes                   = ["ReadWriteMany"]
      persistentVolumeReclaimPolicy = "Retain"
      storageClassName              = ""
      mountOptions                  = ["flock"]
      csi = {
        driver       = "fsx.csi.aws.com"
        volumeHandle = aws_fsx_lustre_file_system.this[0].id
        volumeAttributes = {
          dnsname   = aws_fsx_lustre_file_system.this[0].dns_name
          mountname = aws_fsx_lustre_file_system.this[0].mount_name
        }
      }
    }
  })
  depends_on = [aws_eks_addon.fsx_csi]
}

resource "kubectl_manifest" "fsx_pvc" {
  count = local.elastic ? 1 : 0
  yaml_body = yamlencode({
    apiVersion = "v1"
    kind       = "PersistentVolumeClaim"
    metadata   = { name = "fsx-pvc", namespace = "ray" }
    spec = {
      accessModes      = ["ReadWriteMany"]
      storageClassName = ""
      volumeName       = "fsx-pv"
      resources        = { requests = { storage = "${var.fsx_storage_gib}Gi" } }
    }
  })
  depends_on = [kubectl_manifest.fsx_pv, kubernetes_namespace.ray]
}

# 一次性: 以 root 在 FSx 建 spill / 数据目录并把属主交给 Ray 的运行用户 (uid 1000), 否则 Ray 写不进去
resource "kubernetes_job_v1" "fsx_init" {
  count = local.elastic ? 1 : 0
  metadata {
    name      = "fsx-init"
    namespace = "ray"
  }
  spec {
    backoff_limit = 2
    template {
      metadata {}
      spec {
        restart_policy = "Never"
        node_selector  = { role = "system" }
        container {
          name    = "init"
          image   = local.ray_image
          command = ["sh", "-c", "mkdir -p /fsx/spill /fsx/${var.data_prefix} /fsx/bench && chown -R 1000:100 /fsx/spill /fsx/${var.data_prefix} /fsx/bench && chmod 775 /fsx/spill /fsx/${var.data_prefix} /fsx/bench && ls -ld /fsx/*"]
          security_context {
            run_as_user = 0
          }
          volume_mount {
            name       = "fsx"
            mount_path = "/fsx"
          }
        }
        volume {
          name = "fsx"
          persistent_volume_claim {
            claim_name = "fsx-pvc"
          }
        }
      }
    }
  }
  wait_for_completion = true
  timeouts {
    create = "15m"
  }
  depends_on = [kubectl_manifest.fsx_pvc]
}
