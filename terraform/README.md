# Terraform 模板 (测试一 static / 测试二 elastic)

一个 Terraform 根, 用 `cluster_mode` 选择两种集群 (EKS + KubeRay + Daft on Ray + Prometheus/AMP + Grafana):

- `static` (测试一): 3 台固定 worker 节点组。
- `elastic` (测试二): 固定 1 台 head 节点, worker 由 Karpenter 按需拉起, 数据和 spill 在 FSx for Lustre (`elastic.tf`)。

**机型是输入参数** (`worker_instance_type`; static 时是节点组机型, elastic 时是 Karpenter NodePool 的机型): 架构 (arm64 / x86_64) 和是否带本地 NVMe 由 AWS API 自动识别, 并据此选择镜像、AMI、根盘、spill 目录和系统节点机型。

## 用法

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars        # 改 worker_instance_type
terraform init
terraform plan  -var worker_instance_type=r9g.4xlarge
terraform apply -var worker_instance_type=r9g.4xlarge
terraform output                                      # 全部输出参数
terraform destroy                                     # 测完务必销毁 (见"成本")
```

同时存在多个集群 (换机型对比, 或测试一 + 测试二) 时, 每个集群用独立的 workspace 和不同的 `name`。`install.sh` / `uninstall.sh` / `scripts/*.sh` 自动用 `export TF_WORKSPACE=<CLUSTER_NAME>`, 手工操作时也这样做:

```bash
export TF_WORKSPACE=daft-ray-r8g   # 不存在时 terraform init 会创建
terraform init && terraform apply -var name=daft-ray-r8g -var worker_instance_type=r8g.4xlarge
```

## 输入参数 (variables.tf)

| 参数 | 默认 | 说明 |
|---|---|---|
| **`worker_instance_type`** | `r8g.4xlarge` | **机型**: r7i / r8i / r8a / r8g / r9g (EBS) 或 r8gd / r8id / r9gd (NVMe) 的 4xlarge |
| `worker_count` | 3 | worker 节点数 (静态, 不伸缩) |
| `system_instance_type` | null | null = arm64 用 m8g.2xlarge, x86 用 m8i.2xlarge (Ray head 要和 worker 同架构) |
| `object_store_bytes` | 68719476736 (64 GiB) | 每个 worker 的 object store; 80 GB 用 `80000000000` |
| `worker_memory_gib` / `num_cpus_per_worker` | 112 / 15 | worker Pod 内存和 Ray CPU 数 (16 vCPU 留 1 个给 kubelet) |
| `ebs_root_volume_gib` / `_iops` / `_throughput` | 600 / 8000 / 500 | EBS 机型根盘 (Ray spill 走根盘); NVMe 机型根盘 100 GiB, spill 走 NVMe RAID0 |
| `input_bucket_name` / `output_bucket_name` | null | null = `daft-ray-input/output-<账号>-<region>` (已有, 只引用) |
| `create_vpc_endpoints` | true | 见"账号网络限制" |
| `region` / `availability_zones` | us-west-2 / [2b, 2c] | 节点只放第一个 AZ |
| `aws_profile` | null | 用于 kubectl 取 token 和输出里的登录命令 |
| **`cluster_mode`** | `static` | `static` 或 `elastic` |
| `head_instance_type` | `r8g.8xlarge` | elastic: Ray head 专用节点组 `ng-head` (固定 1 台) |
| `elastic_max_vcpus` | 1024 | elastic: NodePool 的 vCPU 上限, 也决定 RayCluster 的 `maxReplicas` |
| `elastic_head_cpus` / `elastic_head_memory_gib` | 16 / 192 | elastic: head Pod 的 CPU / 内存 |
| `karpenter_version` | `1.15.0` | elastic: Karpenter chart 版本 (EKS Pod Identity, 无 SQS 中断队列, 仅 On-Demand) |
| `fsx_storage_gib` | 4800 | elastic: FSx 容量, 2400 的倍数 (原测试 81600 ≈ $38/小时) |
| `fsx_throughput_per_tib` | 500 | elastic: PERSISTENT_2 吞吐 (MB/s/TiB) |
| `grafana_anonymous_viewer` | true | Grafana 允许被 iframe 嵌入 (`allow_embedding`, `cookie_samesite=lax`) 并开启匿名 Viewer; 设 false 则只允许登录访问 |

## 输出参数 (outputs.tf)

| 输出 | 内容 |
|---|---|
| `eks_login_command` | `aws eks update-kubeconfig ...` (context 名 = 集群名) |
| `ray_dashboard` | URL `http://localhost:8265` + `kubectl port-forward` 命令 |
| `grafana_dashboard` | URL `http://localhost:3000` + port-forward 命令 + 管理员密码获取命令 + AMP workspace |
| `data_sample_s3` | 输入数据 `s3://<输入桶>/tpch-v2/sf<N>/`、参考结果、测试结果位置 |
| `logs` | Ray 日志目录 (`/tmp/ray/session_latest/logs`)、作业 driver 日志、raylet 日志、读取命令、S3 结果、AMP 指标库 |
| `cluster` | 机型 / 架构 / 模式 / 是否 NVMe / spill 目标 / 镜像 / object store 概况 |
| `fsx` | elastic: FSx 文件系统 ID / DNS / 挂载名 / 容量 (static 为 null) |

> Ray 和 Grafana 的 Dashboard **没有对外暴露** (和测试时一样只开 ClusterIP), 所以 "URL" 是 `localhost` 加一条 port-forward 命令。

## 创建的资源 (对应测试一的配置)

| 文件 | 内容 |
|---|---|
| `network.tf` | VPC 192.168.0.0/16, 2 AZ 的公有 / 私有子网, 单 NAT, S3 Gateway Endpoint, 8 个 Interface Endpoint (ec2 / sts / ssm / ecr.api / ecr.dkr / eks / eks-auth / aps-workspaces) |
| `eks.tf` | EKS 1.36 (私网 + 公网访问), OIDC, 托管 addon (vpc-cni / kube-proxy / coredns), 节点角色, 系统节点组 `ng-system` (2 台, `role=system`), worker 节点组 `ng-worker` (3 台静态, `role=ray-worker` + taint `workload=ray-worker:NoSchedule`), worker 启动模板 (EBS: 600 GiB gp3 8000 IOPS 500 MiB/s; NVMe: nodeadm RAID0) |
| `irsa.tf` | 三个 IRSA 角色: `ray-s3-access` (输入桶只读 / 输出桶读写 + AMP 查询)、`amp-prometheus` (remote write)、`grafana` (AMP 查询) |
| `apps.tf` | 命名空间 `ray` / `monitoring`, KubeRay operator 1.7.1, kube-prometheus-stack 92.10.1 (只采集, remote write 到 AMP), Grafana 10.5.15 (数据源 AMP), Ray PodMonitor, **RayCluster** (`templates/raycluster.yaml.tftpl`: 1 head + N worker, 每台 15 CPU / 112Gi, object store + `/dev/shm`), Ray 官方面板载入 Grafana |
| `main.tf` | AMP workspace, (可选) S3 桶和 ECR pull-through cache 规则, 机型推导 |
| `elastic.tf` | 仅 `cluster_mode=elastic`: head 节点组, Pod Identity agent, Karpenter (helm + 节点角色 / instance profile / access entry + controller 角色), NodePool + EC2NodeClass (`templates/karpenter-nodepool.yaml.tftpl`), FSx for Lustre + 安全组 + CSI 驱动 + PV / PVC, 初始化 `/fsx/spill` 的 Job。弹性 RayCluster 在 `apps.tf` 里按模式选模板 (`templates/raycluster-elastic.yaml.tftpl`) |

## 前置条件 (模板不创建, 由仓库根目录的 install.sh 第 2~5 步准备)

1. **ECR 里的镜像和 chart**: `daft-ray:<tag>` (x86 带 `-x86_64` 后缀)、`charts/kuberay-operator`、`charts/kube-prometheus-stack`、`charts/grafana`、`mirror/grafana/grafana`; 其余镜像 (quay / k8s) 靠 ECR Pull-through Cache 在节点首次拉取时缓存。本账号节点访问不了公网镜像仓库, 所以都经 ECR。
2. **S3 数据**: 输入桶里已有 `tpch-v2/sf<N>/` 的 TPC-H Parquet 数据 (`scripts/gen_data.sh`)。
3. 本机有 `aws`、`kubectl` (Ray 面板载入脚本用)。helm / kubernetes / kubectl provider 都通过 `aws eks get-token` 认证。

单独使用本模板时 (不走 install.sh), 需要自己先完成上面三项, 再 `terraform apply -var-file=...`。推荐直接用 `../install.sh`, 它会生成 tfvars 并分两阶段 apply。

## 账号网络限制 (为什么有 VPC Endpoint)

测试所用账号的 VPC 访问不到 AWS 公网 API 端点, 不建 Interface Endpoint 的话节点的 nodeadm 调 `ec2:DescribeInstances` 超时, 节点组会一直 CREATING。没有这种限制的账号可设 `create_vpc_endpoints = false`。

## 跑测试

```bash
$(terraform output -raw eks_login_command)
../scripts/run_benchmark.sh --sf 1000 --questions 1-22 -- --s3-max-connections 32 --s3-refresh-creds
```

`--s3-refresh-creds` 对超过 1 小时的作业是必需的 (IRSA 的 STS 凭证 1 小时过期, 见 `job/tpch_bench.py`)。

## 成本 (us-west-2 按需, 约值)

3 × worker (r8g.4xlarge ≈ $1.06/h 含 EBS) + 2 × 系统节点 (m8g.2xlarge) + EKS 控制面 $0.10/h + NAT + 8 个 Interface Endpoint (约 $0.08/h) + AMP 按量。三节点集群整体约 $4~5/小时。**不用时 `terraform destroy`**, 或把 `worker_count` 设为 0 再 apply (保留系统节点、NAT、Endpoint 和监控, 约 $1/小时)。

## 验证范围

`terraform fmt` 和 `validate` 通过; `plan` 在测试一 (r8g / r7i / r8gd 三种机型, 58 个资源) 和测试二 (r8g.16xlarge, 76 个资源) 上都能生成计划, 架构、镜像后缀、系统节点机型、NVMe 识别符合预期。
`apply` 会创建真实的 EKS 集群和计费资源, 由 `../install.sh` 分两阶段执行: 先建 VPC / EKS / 节点组 / coredns, 再装集群内组件 (KubeRay、Prometheus、Grafana、RayCluster 等), 以保证 kubernetes / helm provider 认证时集群已存在。
单独使用本模板时如果 provider 认证失败, 先 `terraform apply -target=aws_eks_node_group.system`, 再整体 apply。
