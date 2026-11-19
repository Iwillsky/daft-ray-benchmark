# Daft on Ray on EKS: TPC-H 基准测试一键部署

在 AWS 上用 Terraform + CodeBuild + Helm 复建两套 Daft on Ray (KubeRay + Daft 0.7.26 + Ray 2.59.0) 的 TPC-H 测试环境, 并配套 Prometheus → AMP → Grafana 监控和一个本地 Web 控制台:

| | 集群 | 数据位置 | 典型用途 |
|---|---|---|---|
| **测试一** | 3 节点静态 EKS (`CLUSTER_MODE=static`) | S3 (经 Gateway Endpoint) | 同规格不同机型 (r7i / r8i / r8a / r8g / r9g ...) 的单节点组性能对比 |
| **测试二** | 弹性 EKS: Karpenter 按作业拉起 worker + FSx for Lustre (`CLUSTER_MODE=elastic`) | FSx (从 S3 复制) 或 S3 | 大规模 (SF1000, 上千 vCPU) 多路并发作业 |

两种模式共用同一套脚本和同一个 Terraform 根, 机型是输入参数, 架构 (arm64 / x86_64) 和本地 NVMe 由 AWS API 自动识别。

```
./install.sh        # 生成数据 → 构建并上传 ECR 镜像 → 创建 EKS → 安装组件 → 起 Ray 集群 → 冒烟验证 (9 步)
```

## 前置条件 (两个测试相同)

AWS CLI v2、Terraform ≥ 1.6、kubectl、python3 (≥ 3.9)、zip, 以及一个有 IAM / S3 / ECR / CodeBuild / EKS / EC2 / AMP (测试二另加 FSx) 管理权限的 AWS 凭证。
**本机不需要 docker、helm** (镜像和 chart 都在 CodeBuild 里处理)。

通用选项: `--dry-run` 只打印每一步要做什么 (不调用 AWS) · `--yes` 不确认 · `--from N` 从第 N 步继续 · `--only N` 只做第 N 步 · `--skip N` 跳过第 N 步 · `--config FILE` 用其它配置文件。
每一步显示 `▶ [n/9] 标题` → `✔ 完成 / ↷ 跳过 / ✘ 失败 (耗时)`, 失败时给出排查提示, 完整日志在 `.install/install.log`。所有步骤**幂等**, 修复后重跑同一命令即可。
每个集群名对应一个 Terraform workspace (状态在 `terraform/terraform.tfstate.d/<集群名>/`), 所以测试一和测试二的集群可以同时存在, 只要 `CLUSTER_NAME` 不同。

---

# 测试一: 3 节点静态集群

3 台 4xlarge 静态 worker (每台一个 Ray worker Pod: 15 CPU / 112Gi, object store 64 GiB 放在 `/dev/shm`), 单 AZ; EBS 机型根盘 600 GiB gp3 (8000 IOPS / 500 MiB/s), Ray spill 走根盘; NVMe 机型根盘 100 GiB + 本地 NVMe RAID0; head 4 CPU / 24Gi, 不跑任务; 数据直接从 S3 读; 凭证走 IRSA。

## 快速开始

```bash
cp config.env.example config.env     # 改 WORKER_INSTANCE_TYPE 等
./install.sh --dry-run               # 先看每一步要做什么
./install.sh                         # 全流程, 先确认再执行
```

## 部署步骤

| # | 步骤 | 做什么 | 对应脚本 | 典型耗时 |
|---|---|---|---|---|
| 1 | 预检 | 检查工具、AWS 凭证、机型架构、机型在 AZ1 的供货, 确认费用 | `install.sh` | 数秒 |
| 2 | AWS 前置资源 | S3 输入 / 输出桶, ECR 仓库, ECR Pull-through Cache, IAM 角色 `daft-ray-codebuild`, CodeBuild 项目 (ARM / x86) | `scripts/bootstrap.sh` | 1 分钟 |
| 3 | Ray+Daft 镜像 | CodeBuild 原生构建 (ARM 或 x86) 并推到 ECR; 构建里做架构 / 版本断言和 DuckDB httpfs 预装 | `scripts/build_image.sh` | 3~5 分钟 |
| 4 | chart 与镜像中转 | Helm chart (KubeRay / kube-prometheus-stack / grafana) 以 OCI 推到 ECR; Grafana 镜像中转到 `mirror/grafana/grafana` | `scripts/mirror.sh` | 3 分钟 |
| 5 | TPC-H 数据 | CodeBuild 里用 DuckDB `dbgen` 生成 Parquet 到 S3, 再算 Q1-Q22 参考结果 | `scripts/gen_data.sh` `scripts/gen_reference.sh` | SF100 约 3 分钟, SF1000 约 20 分钟 |
| 6 | EKS 集群 | Terraform: VPC + NAT + VPC Endpoint, EKS 1.36, 托管 addon, 系统节点组, worker 节点组 | `terraform/` | 15~20 分钟 |
| 7 | 集群内组件 | Terraform: KubeRay operator、Prometheus (remote write → AMP)、Grafana、PodMonitor、RayCluster、Ray 面板 | `terraform/` | 5~10 分钟 |
| 8 | FSx 数据 | 测试一不需要, 自动跳过 | | |
| 9 | 验证 | 等 Ray head 和 worker Ready, 同步 `job/` 到 head, 跑 SF1 Q1-Q3 冒烟作业, 打印访问方式 | `install.sh` | 3 分钟 |

## 配置 (config.env)

| 变量 | 默认 | 说明 |
|---|---|---|
| `CLUSTER_MODE` | `static` | 测试一用 `static` |
| `WORKER_INSTANCE_TYPE` | `r8g.4xlarge` | **机型** (主要输入参数) |
| `WORKER_COUNT` | 3 | worker 节点数 |
| `OBJECT_STORE_BYTES` | 68719476736 | 每 worker 的 Ray object store (64 GiB); 80 GB 写 `80000000000` |
| `CLUSTER_NAME` | `daft-ray-static` | 集群名, 也是 kubectl context 名和 Terraform workspace 名 |
| `AWS_REGION` / `AZ1` / `AZ2` | us-west-2 / 2b / 2c | 节点只放 AZ1, AZ2 仅为满足 EKS 控制面 2 个 AZ 的要求 |
| `INPUT_BUCKET` / `OUTPUT_BUCKET` | 留空 | 留空 = `daft-ray-input/output-<账号ID>-<region>` |
| `DATA_SCALE_FACTORS` | `1 100` | 要生成的 TPC-H 规模 (1 100 300 500 1000 3000) |
| `GENERATE_REFERENCE` | `true` | 是否生成 DuckDB 参考结果 |
| `IMAGE_TAG`, `KUBERAY_CHART_VERSION`, ... | 见文件 | 版本号 |

## 跑基准测试

```bash
scripts/run_benchmark.sh --sf 100 --questions 1-22                       # 单流, 数据来自 S3
scripts/run_benchmark.sh --sf 100 --questions 1-22 --streams 3 --seed 7  # 3 路并发, 每路用不同随机种子打乱 Q 序
```
IRSA 的 STS 凭证 1 小时过期, 读 S3 的长作业会报 `ExpiredToken`。`run_benchmark.sh` 在"数据来自 S3 且 (多流 或 SF≥1000)"时自动给 `tpch_bench.py` 加 `--s3-refresh-creds` (到期前 10 分钟重新取凭证, 并对 `ExpiredToken` 兜底重试); 不想要用 `--no-refresh`。
结果写到 `s3://<输出桶>/tpch-v2/results/sf<N>/<集群名>-<时间戳>-<种子或pid>/{summary.json,RESULTS.md}`。

## 费用 (us-west-2 按需, 约值)

3 台 worker + 2 台系统节点 + EKS 控制面 + NAT + 8 个 Interface Endpoint + AMP ≈ **$4~5/小时**。S3 数据: SF100 约 36 GB, SF1000 约 367 GB, SF3000 约 1.1 TB。

---

# 测试二: 弹性集群 + FSx for Lustre

在测试一的基础上改成: worker 不再固定, 由 **Karpenter** 在作业提交后按需拉起 (默认 r8g.16xlarge, On-Demand, 单 AZ, 空闲 5 分钟回收, vCPU 上限 1024); Ray head 独占一个固定节点; 数据放在 **FSx for Lustre** (PERSISTENT_2, 500 MB/s/TiB, 先在 S3 生成, 再复制到 FSx), spill 目录也在 FSx 上。
每台 r8g.16xlarge 跑 2 个 Ray worker Pod (各 31 CPU / 224Gi, object store 112 GiB, shm 114Gi); RayCluster 的 worker 组 `minReplicas=0`, `maxReplicas` 由 `ELASTIC_MAX_VCPUS` 和机型推导 (1024 vCPU / 64 = 16 台 = 32 个 worker Pod)。

## 快速开始

```bash
cp config.test2.env.example config.env     # 机型 / FSx 容量 / vCPU 上限在这里改
./install.sh --dry-run
./install.sh                               # 比测试一多约 15 分钟 (FSx 创建) 和 SF 复制时间
```

## 部署步骤

第 1~5 步与测试一相同 (数据先生成到 S3)。不同的是:

| # | 步骤 | 测试二的内容 |
|---|---|---|
| 6 | EKS 集群 | 同上, 但建的是 **head 节点组** (固定 1 台) 而不是 worker 节点组, 另加 Pod Identity agent |
| 7 | 集群内组件 | 另装 **Karpenter 1.15.0** (EKS Pod Identity) + NodePool / EC2NodeClass, **FSx** 文件系统 + CSI 驱动 + PV `fsx-pv` / PVC `fsx-pvc`, 初始化目录 `/fsx/spill` |
| 8 | FSx 数据 | `scripts/fsx_load_data.sh`: 一个 Indexed Job (16 个 Pod) 把 `DATA_SCALE_FACTORS` 里的规模从 S3 复制到 `/fsx/tpch-v2/sf<N>` (先写 `.part` 再改名, 可重复执行); 这一步会让 Karpenter 拉起 worker 节点 |
| 9 | 验证 | 等 Ray head Ready (worker 此时是 0 个), 跑 SF1 冒烟作业 (数据来自 FSx) |

## 配置 (config.test2.env.example 里的取值)

| 变量 | 值 | 说明 |
|---|---|---|
| `CLUSTER_MODE` | `elastic` | |
| `AWS_REGION` / `AZ1` / `AZ2` | us-east-1 / 1c / 1b | worker、head、FSx 都在 AZ1 (FSx 是单 AZ) |
| `WORKER_INSTANCE_TYPE` | `r8g.16xlarge` | Karpenter NodePool 的机型 (64 vCPU / 512 GiB); Pod 大小由机型自动推导 |
| `HEAD_INSTANCE_TYPE` | `r8g.8xlarge` | Ray head 专用节点 |
| `ELASTIC_MAX_VCPUS` | 1024 | NodePool 的 vCPU 上限 (成本护栏) |
| `FSX_STORAGE_GIB` | 4800 | FSx 容量, 必须是 2400 的倍数。**原测试用 81600 GiB, 约 $38/小时**; 默认取 4800 只够放 SF100~SF300 |
| `KARPENTER_VERSION` | 1.15.0 | |
| `DATA_SCALE_FACTORS` | `100 300 500 1000` | SF1000 约 367 GB, 要放 SF1000 请把 FSx 容量调大 |

## 跑基准测试 (多路并发)

```bash
scripts/run_benchmark.sh --sf 1000 --questions 1-22 --streams 10 --seed 100 --source fsx
```
- 多流时每路作业的 driver 放在 worker 上 (`--entrypoint-num-cpus 1 --entrypoint-memory 32212254720`), 因为 10 路 driver 同时放在 head 上会把 head 内存占满 (原测试里出现过 OOM)。
- 流 i 使用随机种子 `seed + i`, 这样不同机型可以用同一组种子做对比。
- 作业结束后会调用 `scripts/clear_request_resources.py` 清掉 autoscaler 的资源请求, 让 Karpenter 能回收空闲节点。
- `--source s3` 也能用 (不经 FSx); 此时同样会自动加 S3 凭证刷新。

## 费用 (us-east-1 按需, 约值)

固定部分: head + 系统节点 + EKS 控制面 + NAT + Endpoint + AMP ≈ $3~4/小时, **加上 FSx** (PERSISTENT_2 500 MB/s/TiB 约 $0.47/小时/TiB: 4800 GiB ≈ $2.3/小时, 81600 GiB ≈ $38/小时, 只要文件系统存在就一直计费)。
弹性部分: r8g.16xlarge 约 $3.77/小时/台 (AWS Pricing API 查得; 1024 vCPU 全开 16 台约 $60/小时), 作业结束 5 分钟后回收。
**不用时请 `./uninstall.sh`**; 它会先删 RayCluster / NodePool 并等 Karpenter 节点回收, 再销毁 Terraform 资源 (包括 FSx 和里面的数据, 数据仍在 S3)。

---

# 两个测试共用的内容

## 监控: Grafana 和 Ray Dashboard

Ray Dashboard 和 Grafana 都是 ClusterIP, 没有公网入口, 用 `kubectl port-forward` 访问 (命令见 `terraform output ray_dashboard grafana_dashboard`)。
Grafana 默认允许被 iframe 嵌入 (`allow_embedding: true`, `cookie_samesite: lax`) 并开启匿名 Viewer, 以便把它嵌到别的页面 (iframe) 里; 因为只监听 localhost 的 port-forward, 匿名只读的暴露面只限本机。不需要时把变量 `grafana_anonymous_viewer` 设为 `false`。
**这个配置只对新建集群生效**, 已经在运行的集群需要对 Grafana 的 Helm release 做一次 `terraform apply` (会重启 Grafana Pod)。

## 输出参数 (`terraform -chdir=terraform output`; 记得先 `export TF_WORKSPACE=<集群名>`)

| 输出 | 内容 |
|---|---|
| `eks_login_command` | `aws eks update-kubeconfig ...` |
| `ray_dashboard` | `http://localhost:8265` + `kubectl port-forward` 命令 |
| `grafana_dashboard` | `http://localhost:3000` + port-forward 命令 + admin 密码获取命令 + AMP workspace ID |
| `data_sample_s3` | 输入数据 `s3://<输入桶>/tpch-v2/sf<N>/`、参考结果、测试结果位置 |
| `logs` | Ray 日志目录、作业 driver 日志、raylet 日志、读取命令、S3 结果路径、AMP 指标库 |
| `cluster` | 机型 / 架构 / 模式 / 是否 NVMe / spill 目标 / 镜像 / object store |
| `fsx` | 测试二: FSx 文件系统 ID、DNS 名、挂载名、容量 (测试一为 null) |

## 销毁

```bash
./uninstall.sh                 # 销毁 EKS / 节点 / VPC / AMP (测试二还有 FSx), S3 数据、ECR 镜像、CodeBuild 保留
./uninstall.sh --workers-only  # 仅测试一: 把 worker 缩到 0, 保留系统节点和监控 (约 $1/小时)
```
销毁哪个集群由 `config.env` 的 `CLUSTER_NAME` 决定。想连前置资源一起清理, 手工删除: S3 桶 (`daft-ray-input-*` / `daft-ray-output-*`)、ECR 仓库 (`daft-ray`, `charts/*`, `mirror/*`)、CodeBuild 项目 `daft-ray-build` / `daft-ray-build-x86`、IAM 角色 `daft-ray-codebuild`、ECR Pull-through Cache 规则。

## 仓库结构

```
install.sh / uninstall.sh      一键部署 / 销毁 (按 CLUSTER_MODE 分支)
config.env.example             测试一配置模板
config.test2.env.example       测试二配置模板
scripts/                       lib.sh, bootstrap / build_image / mirror / gen_data / gen_reference, sync_job / run_benchmark, fsx_load_data (测试二), clear_request_resources.py
image/                         Ray+Daft 镜像的 Dockerfile 和 requirements.txt (arm64 / x86_64 同一份)
codebuild/                     buildspec (镜像、chart、镜像中转、数据、参考结果), tpch_gen.py, IAM / 项目模板
job/                           基准作业: tpch_bench.py (主程序) / tpch_queries.py (Q1-Q22) / 校验 / 报告 / 参考结果
elastic/                       测试二: S3 → FSx 复制程序 (copy_data.py) 和 Job 模板
terraform/                     VPC、EKS、节点、IRSA、KubeRay、Prometheus、Grafana、RayCluster; 测试二另有 Karpenter、FSx (详见 terraform/README.md)
```

## 账号网络限制 (为什么有 VPC Endpoint 和 ECR 中转)

本模板按测试所用的受限账号设计: 节点访问不了 AWS 公网 API 端点 (ec2 / sts / ...), 也访问不了 quay.io / registry.k8s.io / Docker Hub。因此用 VPC Interface Endpoint 走私网, 镜像经 ECR (Pull-through Cache 或 CodeBuild 中转)。没有这类限制的账号可在 `terraform/variables.tf` 把 `create_vpc_endpoints` 设为 false。

## 验证范围

| 内容 | 验证方式 |
|---|---|
| `install.sh` 两种模式的 9 步流程 | `--dry-run` 逐步列出; 所有脚本 `bash -n` 语法检查通过 |
| 步骤 1 预检、步骤 3~5 的"已存在则跳过"判断 | 对真实账号做只读验证 (镜像 / chart / 数据 / 参考结果均能被识别) |
| Terraform | `terraform fmt` 和 `validate` 通过; `plan` 测试一 (r8g.4xlarge) 58 个资源, 测试二 (r8g.16xlarge, us-east-1) 76 个资源 |
| 测试一的作业和监控 | 3 节点 EKS 集群上跑过 TPC-H Q1-Q22 (SF100 / SF1000), Prometheus → AMP → Grafana 采集正常 |
| 测试二的作业 | 弹性集群 + FSx 上跑过 10 路并发 SF1000 作业 |

步骤 2 的资源创建、步骤 3~5 的 CodeBuild 构建、步骤 6~9 的 `terraform apply` 在你的账号里会创建真实的计费资源, 结果取决于账号权限和机型容量。
第一次部署建议先用 `DATA_SCALE_FACTORS="1"` 和较小的 `FSX_STORAGE_GIB` (如 2400) 走一遍全流程, 确认各步骤的状态输出后再放大规模。

## 设计说明

- 数据规模 100 / 300 / 500 / 1000 / 3000 分别拆成 32 / 128 / 256 / 512 / 1536 个文件 (1 为 8 个)。
- Grafana 不做持久化 (配置全走 provisioning, 重建不丢); Prometheus 本地只保留 2 小时, 长期指标在 AMP。
- 测试二只用 On-Demand 实例, 因此没有创建 Karpenter 的 SQS 中断队列。
- 基准作业里的集群映射 `CLUSTERS` 为空, 指标报告通过 `--amp-workspace` 取 AMP workspace (`run_benchmark.sh` 自动传入)。

## 第三方内容与说明

第三方内容 (Daft 的 TPC-H 查询实现等) 的归属和许可证见 `THIRD_PARTY_NOTICES.md`。本仓库的测试基于 TPC-H 规范自行实现, 不是 TPC 官方认证的结果, 不可与官方成绩比较。
