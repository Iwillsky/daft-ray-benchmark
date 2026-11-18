# 集群内组件: 命名空间 / ServiceAccount (IRSA) / KubeRay operator / Prometheus (remote write 到 AMP) / Grafana / PodMonitor / RayCluster / Ray 面板
# Helm chart 和镜像都从账号自己的 ECR 取 (chart 在 <ecr>/charts/*, 镜像经 pull-through cache 或 mirror 前缀)。

resource "kubernetes_namespace" "ray" {
  metadata { name = "ray" }
  depends_on = [aws_eks_node_group.system]
}

resource "kubernetes_namespace" "monitoring" {
  metadata { name = "monitoring" }
  depends_on = [aws_eks_node_group.system]
}

resource "kubernetes_service_account" "ray_s3_access" {
  metadata {
    name        = "ray-s3-access"
    namespace   = kubernetes_namespace.ray.metadata[0].name
    annotations = { "eks.amazonaws.com/role-arn" = aws_iam_role.irsa["ray-s3-access"].arn }
  }
}

resource "kubernetes_service_account" "grafana" {
  metadata {
    name        = "grafana"
    namespace   = kubernetes_namespace.monitoring.metadata[0].name
    annotations = { "eks.amazonaws.com/role-arn" = aws_iam_role.irsa["grafana"].arn }
  }
}

# ---------------------------------------------------------------- KubeRay operator
resource "helm_release" "kuberay_operator" {
  name       = "kuberay-operator"
  namespace  = kubernetes_namespace.ray.metadata[0].name
  repository = "oci://${local.ecr_registry}/charts"
  chart      = "kuberay-operator"
  version    = var.kuberay_operator_version
  timeout    = 600
  wait       = true

  set {
    name  = "image.repository"
    value = "${local.ecr_registry}/quay/kuberay/operator"
  }
  set {
    name  = "nodeSelector.role"
    value = "system"
  }
  depends_on = [aws_eks_node_group.system, aws_eks_addon.coredns]
}

# ---------------------------------------------------------------- Prometheus (只做采集, 数据 remote write 到 AMP; 本地只留 2 小时)
resource "helm_release" "kube_prometheus_stack" {
  name       = "kps"
  namespace  = kubernetes_namespace.monitoring.metadata[0].name
  repository = "oci://${local.ecr_registry}/charts"
  chart      = "kube-prometheus-stack"
  version    = var.kube_prometheus_stack_version
  timeout    = 900
  wait       = true

  values = [templatefile("${path.module}/templates/kube-prometheus-stack-values.yaml.tftpl", {
    AMP_ROLE_ARN     = aws_iam_role.irsa["amp-prometheus"].arn
    ECR_REGISTRY     = local.ecr_registry
    CLUSTER_NAME     = var.name
    AMP_WORKSPACE_ID = local.amp_workspace_id
  })]
  depends_on = [aws_eks_node_group.system, aws_eks_addon.coredns, aws_iam_role_policy_attachment.prometheus_remote_write]
}

# ---------------------------------------------------------------- Grafana (自建, 数据源 AMP, SigV4 + IRSA; 仅 port-forward 访问)
resource "helm_release" "grafana" {
  name       = "grafana"
  namespace  = kubernetes_namespace.monitoring.metadata[0].name
  repository = "oci://${local.ecr_registry}/charts"
  chart      = "grafana"
  version    = var.grafana_chart_version
  timeout    = 600
  wait       = true

  values = [templatefile("${path.module}/templates/grafana-values.yaml.tftpl", {
    ECR_REGISTRY     = local.ecr_registry
    AMP_WORKSPACE_ID = local.amp_workspace_id
    GRAFANA_TAG      = local.grafana_tag
  })]
  depends_on = [kubernetes_service_account.grafana, aws_iam_role_policy_attachment.grafana_amp_query, aws_eks_addon.coredns]
}

# ---------------------------------------------------------------- PodMonitor: 抓取 Ray head / worker 的 :8080 metrics
resource "kubectl_manifest" "ray_podmonitor" {
  yaml_body  = file("${path.module}/templates/ray-podmonitor.yaml")
  depends_on = [helm_release.kube_prometheus_stack, kubernetes_namespace.ray]
}

# ---------------------------------------------------------------- RayCluster (测试一: 3 个 worker, 每台一个 Pod)
resource "kubectl_manifest" "ray_cluster" {
  yaml_body = templatefile("${path.module}/templates/raycluster.yaml.tftpl", {
    ray_version    = var.ray_version
    image          = local.ray_image
    arch           = local.arch_label
    workers        = var.worker_count
    num_cpus       = var.num_cpus_per_worker
    obj_bytes      = var.object_store_bytes
    shm_gib        = local.shm_gib
    worker_mem_gib = var.worker_memory_gib
    head_mem_gib   = var.head_memory_gib
    head_cpus      = var.head_cpus
  })
  wait_for_rollout = false
  depends_on = [
    helm_release.kuberay_operator,
    kubernetes_service_account.ray_s3_access,
    aws_eks_node_group.worker,
    aws_iam_role_policy_attachment.ray_s3,
  ]
}

# ---------------------------------------------------------------- Ray 自带的 Grafana 面板 -> ConfigMap (Grafana sidecar 自动加载)
resource "null_resource" "ray_dashboards" {
  count = var.load_ray_dashboards ? 1 : 0

  triggers = {
    ray_cluster = kubectl_manifest.ray_cluster.uid
    grafana     = helm_release.grafana.metadata[0].revision
  }

  provisioner "local-exec" {
    command = "bash ${path.module}/scripts/load_ray_dashboards.sh '${var.name}' '${var.region}' '${coalesce(var.aws_profile, "")}'"
  }
}
