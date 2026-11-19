# ======================================================================================
# 输出: Ray / Grafana Dashboard URL、数据样本 S3 位置、EKS 登录命令、日志文件及路径
# ======================================================================================

output "eks_login_command" {
  description = "把集群写进本机 kubeconfig (context 名 = 集群名)。"
  value       = "aws eks update-kubeconfig --name ${var.name} --region ${var.region} --alias ${local.kube_context}${local.profile_arg}"
}

output "ray_dashboard" {
  description = "Ray Dashboard。Dashboard 没有对外暴露 (ClusterIP), 先在本机执行 port_forward_command, 再用 url 访问。"
  value = {
    url                  = "http://localhost:8265"
    port_forward_command = "kubectl --context ${local.kube_context} -n ray port-forward svc/daft-ray-head-svc 8265:8265"
    job_submit_address   = "http://127.0.0.1:8265 (在 head Pod 内: kubectl exec <head> -c ray-head -- ray job submit --address http://127.0.0.1:8265 ...)"
  }
}

output "grafana_dashboard" {
  description = "Grafana。同样仅 port-forward 访问; 数据源 AMP 已预置, Ray 面板 (default / data) 由 sidecar 自动加载。"
  value = {
    url                  = "http://localhost:3000"
    port_forward_command = "kubectl --context ${local.kube_context} -n monitoring port-forward svc/grafana 3000:80"
    user                 = "admin"
    password_command     = "kubectl --context ${local.kube_context} -n monitoring get secret grafana -o jsonpath='{.data.admin-password}' | base64 --decode; echo"
    amp_workspace_id     = local.amp_workspace_id
    amp_query_endpoint   = "https://aps-workspaces.${var.region}.amazonaws.com/workspaces/${local.amp_workspace_id}"
  }
}

output "data_sample_s3" {
  description = "TPC-H 数据样本、参考结果和测试结果在 S3 上的位置。"
  value = {
    input_root = "s3://${local.input_bucket}/${var.data_prefix}"
    datasets   = { for sf in var.tpch_scale_factors : "sf${sf}" => "s3://${local.input_bucket}/${var.data_prefix}/sf${sf}/" }
    reference  = "s3://${local.output_bucket}/${var.data_prefix}/reference"
    results    = "s3://${local.output_bucket}/${var.data_prefix}/results"
    list_cmd   = "aws s3 ls s3://${local.input_bucket}/${var.data_prefix}/ --region ${var.region}${local.profile_arg}"
  }
}

output "logs" {
  description = "日志文件和路径 (全部在 Pod 内 / S3 / CloudWatch, 没有落到本机)。<job-id> 是 ray job submit 的 --submission-id。"
  value = {
    ray_logs_dir_in_pod  = "/tmp/ray/session_latest/logs (head 和 worker Pod 里都有; worker 的 /tmp/ray 是 emptyDir, Pod 删除后丢失)"
    job_driver_log       = "/tmp/ray/session_latest/logs/job-driver-<job-id>.log (在 head Pod 上, 含逐题耗时 / 校验结果 / RESULT_JSON 路径)"
    raylet_log           = "/tmp/ray/session_latest/logs/raylet.out (worker Pod; 内存监视器杀进程等记录在这里)"
    read_job_log_cmd     = "kubectl --context ${local.kube_context} -n ray exec $(kubectl --context ${local.kube_context} -n ray get pod -l ray.io/node-type=head -o name | head -1) -c ray-head -- cat /tmp/ray/session_latest/logs/job-driver-<job-id>.log"
    pod_logs_cmd         = "kubectl --context ${local.kube_context} -n ray logs -l ray.io/node-type=head -c ray-head --tail=200  (worker: -l ray.io/node-type=worker -c ray-worker)"
    operator_log_cmd     = "kubectl --context ${local.kube_context} -n ray logs deploy/kuberay-operator --tail=200"
    benchmark_results_s3 = "s3://${local.output_bucket}/${var.data_prefix}/results/sf<规模>/<集群名>-<时间戳>-<种子或pid>/{summary.json,RESULTS.md}"
    control_plane_logs   = length(var.enabled_cluster_log_types) > 0 ? "CloudWatch Logs: /aws/eks/${var.name}/cluster (${join(", ", var.enabled_cluster_log_types)})" : "未开启 (enabled_cluster_log_types = [])"
    metrics_store        = "AMP workspace ${local.amp_workspace_id} (CPU / 内存 / 网络 / spill 等指标, 15 秒步长)"
  }
}

output "cluster" {
  description = "集群与机型概况。"
  value = {
    name                  = var.name
    region                = var.region
    kubernetes_version    = var.kubernetes_version
    worker_instance_type  = var.worker_instance_type
    worker_count          = local.elastic ? "弹性 (Karpenter, 上限 ${var.elastic_max_vcpus} vCPU = ${local.e_max_pods} 个 worker Pod)" : var.worker_count
    mode                  = var.cluster_mode
    worker_arch           = local.arch_label
    worker_has_local_nvme = local.has_nvme
    worker_spill_target   = local.elastic ? "FSx for Lustre (/fsx/spill)" : (local.has_nvme ? "本地 NVMe RAID0" : "EBS 根盘 (${var.ebs_root_volume_gib} GiB gp3, ${var.ebs_root_iops} IOPS, ${var.ebs_root_throughput} MiB/s)")
    system_instance_type  = local.system_type
    ray_image             = local.ray_image
    object_store_bytes    = local.elastic ? local.e_obj_bytes : var.object_store_bytes
    worker_node_check     = "kubectl --context ${local.kube_context} get nodes -L node.kubernetes.io/instance-type,role"
  }
}

output "fsx" {
  description = "elastic 模式: FSx for Lustre 信息 (static 模式为 null)。作业从 FSx 读数据时用 --root /fsx/<前缀>。"
  value = local.elastic ? {
    file_system_id = aws_fsx_lustre_file_system.this[0].id
    dns_name       = aws_fsx_lustre_file_system.this[0].dns_name
    capacity_gib   = var.fsx_storage_gib
    mount_path     = "/fsx (head 和所有 worker Pod)"
    data_root      = "/fsx/${var.data_prefix}"
    spill_dir      = "/fsx/spill"
    load_data_cmd  = "scripts/fsx_load_data.sh 100 300 500 1000   # 把 S3 数据复制到 FSx"
  } : null
}
