provider "aws" {
  region  = var.region
  profile = var.aws_profile
}

# kubernetes / helm / kubectl 都用 `aws eks get-token` 认证, 指向本模板创建的集群
locals {
  eks_exec_args = concat(["eks", "get-token", "--cluster-name", var.name, "--region", var.region],
  var.aws_profile == null ? [] : ["--profile", var.aws_profile])
}

provider "kubernetes" {
  host                   = aws_eks_cluster.this.endpoint
  cluster_ca_certificate = base64decode(aws_eks_cluster.this.certificate_authority[0].data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = local.eks_exec_args
  }
}

data "aws_ecr_authorization_token" "this" {}

provider "helm" {
  kubernetes {
    host                   = aws_eks_cluster.this.endpoint
    cluster_ca_certificate = base64decode(aws_eks_cluster.this.certificate_authority[0].data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = local.eks_exec_args
    }
  }
  # Helm chart 和镜像都存在账号自己的 ECR (OCI) 里
  registry {
    url      = "oci://${local.ecr_registry}"
    username = "AWS"
    password = data.aws_ecr_authorization_token.this.password
  }
}

provider "kubectl" {
  host                   = aws_eks_cluster.this.endpoint
  cluster_ca_certificate = base64decode(aws_eks_cluster.this.certificate_authority[0].data)
  load_config_file       = false
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = local.eks_exec_args
  }
}
