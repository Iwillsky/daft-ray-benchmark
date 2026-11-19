# ======================================================================================
# 输入参数。最常改的是 worker_instance_type (机型); 其余默认值 = 测试一的三节点静态集群配置。
# ======================================================================================

# ---------------------------------------------------------------- 机型 (主要输入)
variable "cluster_mode" {
  description = "static = 测试一 (3 台静态 worker, 无 FSx); elastic = 测试二 (Karpenter 弹性 worker + FSx for Lustre + 独立 head 节点)。"
  type        = string
  default     = "static"

  validation {
    condition     = contains(["static", "elastic"], var.cluster_mode)
    error_message = "cluster_mode 只能是 static 或 elastic。"
  }
}

variable "worker_instance_type" {
  description = "Ray worker 机型 (elastic 模式下是 Karpenter NodePool 的机型, 例如 r8g.16xlarge), 例如 r7i.4xlarge / r8a.4xlarge / r8i.4xlarge / r8g.4xlarge / r9g.4xlarge (EBS 机型) 或 r8gd.4xlarge / r8id.4xlarge (带本地 NVMe)。架构 (arm64 / x86_64) 和是否带 NVMe 由 AWS API 自动识别, 并决定镜像、AMI、根盘和 spill 目录。"
  type        = string
  default     = "r8g.4xlarge"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]*\\.[a-z0-9]+$", var.worker_instance_type))
    error_message = "worker_instance_type 格式应类似 r8g.4xlarge。"
  }
}

variable "worker_count" {
  description = "static 模式: worker 节点数 (静态, 不自动伸缩; 测试一为 3)。elastic 模式不使用 (节点数由 Karpenter 随负载伸缩, 上限见 elastic_max_vcpus)。"
  type        = number
  default     = 3
}

variable "system_instance_type" {
  description = "系统节点机型 (跑 KubeRay operator / Ray head / Prometheus / Grafana)。null = 与 worker 同架构: arm64 用 m8g.2xlarge, x86 用 m8i.2xlarge。"
  type        = string
  default     = null
}

variable "system_node_count" {
  type    = number
  default = 2
}

# ---------------------------------------------------------------- 区域 / 网络 / EKS
variable "region" {
  type    = string
  default = "us-west-2"
}

variable "aws_profile" {
  description = "AWS CLI profile (用于 kubectl 取 token、输出里的登录命令)。null = 默认凭证。"
  type        = string
  default     = null
}

variable "name" {
  description = "EKS 集群名, 同时作为各类资源的名字前缀和 Prometheus 的 cluster 标签。"
  type        = string
  default     = "daft-ray-static"
}

variable "availability_zones" {
  description = "两个 AZ (EKS 控制面要求 2 个); 所有节点只放在第一个 AZ (避免跨 AZ 流量, 与测试一一致)。"
  type        = list(string)
  default     = ["us-west-2b", "us-west-2c"]

  validation {
    condition     = length(var.availability_zones) == 2
    error_message = "需要恰好 2 个 AZ。"
  }
}

variable "kubernetes_version" {
  type    = string
  default = "1.36"
}

variable "vpc_cidr" {
  type    = string
  default = "192.168.0.0/16"
}

variable "create_vpc_endpoints" {
  description = "创建 VPC Endpoint。测试所用账号的 VPC 访问不到 AWS 公网 API 端点 (ec2 / sts / ecr / eks ...), 不建的话节点的 nodeadm 会超时、节点加入不了集群。账号没有这个限制时可以关掉省钱 (每个 Interface Endpoint 约 $0.01/小时)。"
  type        = bool
  default     = true
}

variable "interface_endpoints" {
  description = "要创建的 Interface Endpoint 服务名 (不含 com.amazonaws.<region>. 前缀)。"
  type        = list(string)
  default     = ["ec2", "sts", "ssm", "ecr.api", "ecr.dkr", "eks", "eks-auth", "aps-workspaces"]
}

variable "endpoint_public_access_cidrs" {
  description = "EKS API 公网访问允许的 CIDR (你的 kubectl 所在网络)。"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "enabled_cluster_log_types" {
  description = "EKS 控制面日志 (写 CloudWatch Logs, 会产生费用)。默认不开。例: [\"api\", \"audit\"]"
  type        = list(string)
  default     = []
}

# ---------------------------------------------------------------- worker 存储
variable "ebs_root_volume_gib" {
  description = "EBS 机型的根盘大小 (Ray spill 走根盘: /tmp/ray)。"
  type        = number
  default     = 600
}

variable "ebs_root_iops" {
  type    = number
  default = 8000
}

variable "ebs_root_throughput" {
  description = "MiB/s"
  type        = number
  default     = 500
}

variable "nvme_root_volume_gib" {
  description = "带本地 NVMe 的机型的根盘大小 (spill 走本地 NVMe RAID0)。"
  type        = number
  default     = 100
}

# ---------------------------------------------------------------- Ray / Daft
variable "ecr_registry" {
  description = "存放 Ray 镜像、Helm chart 镜像 (OCI) 和 pull-through cache 的 ECR 域名。null = <账号>.dkr.ecr.<region>.amazonaws.com"
  type        = string
  default     = null
}

variable "ray_image" {
  description = "完整的 Ray 镜像地址。null = <ecr_registry>/daft-ray:<ray_image_tag> (x86 机型自动加 -x86_64 后缀)。"
  type        = string
  default     = null
}

variable "ray_image_tag" {
  type    = string
  default = "2.59.0-daft0.7.26-duckdb1.5.6"
}

variable "ray_version" {
  type    = string
  default = "2.59.0"
}

variable "num_cpus_per_worker" {
  description = "每个 worker 的 Ray CPU 数, 同时是 Pod 的 CPU request/limit。16 vCPU 的 4xlarge 留 1 个给 kubelet / DaemonSet, 所以是 15。"
  type        = number
  default     = 15
}

variable "object_store_bytes" {
  description = "每个 worker 的 object store 字节数。测试一: 64 GiB (68719476736); Q22x3 用例用 80 GB (80000000000)。"
  type        = number
  default     = 68719476736
}

variable "worker_memory_gib" {
  description = "worker Pod 内存 request/limit (GiB)。128 GiB 的 4xlarge 取 112, object store 放 /dev/shm 也计入这部分。"
  type        = number
  default     = 112
}

variable "head_memory_gib" {
  type    = number
  default = 24
}

variable "head_cpus" {
  type    = number
  default = 4
}

# ---------------------------------------------------------------- 数据 (S3)
variable "input_bucket_name" {
  description = "TPC-H 数据样本所在桶 (只读)。null = daft-ray-input-<账号>-<region>"
  type        = string
  default     = null
}

variable "output_bucket_name" {
  description = "参考结果与测试结果所在桶 (读写)。null = daft-ray-output-<账号>-<region>"
  type        = string
  default     = null
}

variable "create_buckets" {
  description = "是否由本模板创建这两个桶。默认 false: 桶里已有 TPC-H 数据, 只引用不创建。"
  type        = bool
  default     = false
}

variable "data_prefix" {
  type    = string
  default = "tpch-v2"
}

variable "tpch_scale_factors" {
  description = "输出里列出的数据规模 (s3://<输入桶>/<前缀>/sf<N>/)。"
  type        = list(number)
  default     = [1, 100, 300, 500, 1000, 3000]
}

# ---------------------------------------------------------------- 监控
variable "create_amp_workspace" {
  type    = bool
  default = true
}

variable "amp_workspace_id" {
  description = "create_amp_workspace = false 时使用已有的 AMP workspace ID。"
  type        = string
  default     = null
}

variable "kuberay_operator_version" {
  type    = string
  default = "1.7.1"
}

variable "kube_prometheus_stack_version" {
  type    = string
  default = "92.10.1"
}

variable "grafana_chart_version" {
  type    = string
  default = "10.5.15"
}

variable "grafana_image_tag_arm64" {
  type    = string
  default = "12.3.1"
}

variable "grafana_image_tag_amd64" {
  type    = string
  default = "12.3.1-amd64"
}

variable "create_ecr_pull_through_cache" {
  description = "创建 ECR pull-through cache 规则 (quay / k8s / ecr-public)。这是账号级资源, 账号里已存在时保持 false。"
  type        = bool
  default     = false
}

variable "load_ray_dashboards" {
  description = "Ray 头节点就绪后, 把 Ray 自动生成的 Grafana 面板 JSON 载入 monitoring 命名空间的 ConfigMap (需要本机有 aws / kubectl)。"
  type        = bool
  default     = true
}

variable "tags" {
  type    = map(string)
  default = { project = "daft-ray-static" }
}

# ---------------------------------------------------------------- 测试二 (elastic) 专用
variable "head_instance_type" {
  description = "elastic 模式: Ray head 独占的节点机型 (测试二用 r8g.8xlarge; 多路并发作业时 head 内存峰值约 33 GiB)。"
  type        = string
  default     = "r8g.8xlarge"
}

variable "elastic_max_vcpus" {
  description = "elastic 模式: Karpenter NodePool 的 vCPU 上限 (测试二 1024 = 16 台 r8g.16xlarge)。EC2 的 On-Demand vCPU 配额要先够。"
  type        = number
  default     = 1024
}

variable "karpenter_version" {
  type    = string
  default = "1.15.0"
}

variable "fsx_storage_gib" {
  description = "elastic 模式: FSx for Lustre 容量 (GiB), PERSISTENT_2 SSD 必须是 2400 的倍数。测试二原始配置是 81600 (约 $38/小时), 这里默认 4800 以免误开大额费用; 复现测试二的吞吐数字请改成 81600。"
  type        = number
  default     = 4800

  validation {
    condition     = var.fsx_storage_gib % 2400 == 0
    error_message = "fsx_storage_gib 必须是 2400 的倍数。"
  }
}

variable "fsx_throughput_per_tib" {
  description = "FSx PERSISTENT_2 每 TiB 吞吐 (MB/s): 125 / 250 / 500 / 1000。测试二用 500。"
  type        = number
  default     = 500
}

variable "grafana_anonymous_viewer" {
  description = "Grafana 开启匿名只读 (Viewer), 让嵌入 iframe 的面板不用再登录。Grafana 只通过 port-forward 访问; 关闭后嵌入页需要用 admin 登录。"
  type        = bool
  default     = true
}
