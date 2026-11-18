# IRSA: Pod 用 ServiceAccount 的 web identity 换取 IAM 角色 (1 小时有效的 STS 凭证, 到期需要刷新; Daft 的刷新见 job/tpch_bench.py --s3-refresh-creds)
#   ray-s3-access (ns ray)          Ray head / worker 读写 S3, 读 AMP
#   amp-prometheus (ns monitoring)  Prometheus remote write 到 AMP
#   grafana (ns monitoring)         Grafana 读 AMP

locals {
  irsa = {
    "ray-s3-access"  = { namespace = "ray" }
    "amp-prometheus" = { namespace = "monitoring" }
    "grafana"        = { namespace = "monitoring" }
  }
}

data "aws_iam_policy_document" "irsa_trust" {
  for_each = local.irsa
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.this.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:sub"
      values   = ["system:serviceaccount:${each.value.namespace}:${each.key}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "irsa" {
  for_each           = local.irsa
  name               = "${var.name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.irsa_trust[each.key].json
  tags               = local.tags
}

# ray-s3-access: 输入桶只读, 输出桶读写
data "aws_iam_policy_document" "ray_s3" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${local.input_bucket}", "arn:aws:s3:::${local.output_bucket}"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${local.input_bucket}/*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["arn:aws:s3:::${local.output_bucket}/*"]
  }
}

resource "aws_iam_policy" "ray_s3" {
  name   = "${var.name}-ray-s3-access"
  policy = data.aws_iam_policy_document.ray_s3.json
  tags   = local.tags
}

resource "aws_iam_role_policy_attachment" "ray_s3" {
  role       = aws_iam_role.irsa["ray-s3-access"].name
  policy_arn = aws_iam_policy.ray_s3.arn
}

# Ray 作业结束时会查 AMP 生成指标报告 (tpch_bench.py), 所以 ray-s3-access 也要有 AMP 查询权限
resource "aws_iam_role_policy_attachment" "ray_amp_query" {
  role       = aws_iam_role.irsa["ray-s3-access"].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonPrometheusQueryAccess"
}

resource "aws_iam_role_policy_attachment" "prometheus_remote_write" {
  role       = aws_iam_role.irsa["amp-prometheus"].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonPrometheusRemoteWriteAccess"
}

resource "aws_iam_role_policy_attachment" "grafana_amp_query" {
  role       = aws_iam_role.irsa["grafana"].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonPrometheusQueryAccess"
}
