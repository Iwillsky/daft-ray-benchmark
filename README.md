# Daft on Ray on EKS: TPC-H 基准测试一键部署

在 AWS 上用 Terraform + CodeBuild + Helm 复建一套 **3 节点静态 EKS 集群** (KubeRay + Daft 0.7.26 + Ray 2.59.0), 并配套 Prometheus → AMP → Grafana 监控和 TPC-H 基准作业。
**机型是输入参数** (r7i / r8i / r8a / r8g / r9g,以及带本地 NVMe 的 r8gd / r8id / r9gd), 架构和 NVMe 由 AWS API 自动识别。

```
./install.sh        # 生成数据 → 构建并上传 ECR 镜像 → 创建 EKS → 安装 Helm chart → 起 Ray 集群 → 冒烟验证
```

## 快速开始

```bash
cp config.env.example config.env     # 改 WORKER_INSTANCE_TYPE 等, 见下表
./install.sh --dry-run               # 先看每一步要做什么 (不调用 AWS)
./install.sh                         # 全流程, 先确认再执行
```

前置: AWS CLI v2、Terraform ≥ 1.6、kubectl、python3、zip, 以及一个有 IAM / S3 / ECR / CodeBuild / EKS / EC2 / AMP 管理权限的 AWS 凭证。
**本机不需要 docker、helm** (镜像和 chart 都在 CodeBuild 里处理)。

## 部署步骤与状态输出

`install.sh` 每一步都显示 `▶ [n/8] 标题` → `✔ 完成 / ↷ 跳过 / ✘ 失败 (耗时)`, 失败时给出排查提示, 完整日志在 `.install/install.log`。
所有步骤**幂等**: 已存在的桶 / 仓库 / 镜像 / 数据会跳过, 失败修复后重跑同一命令即可。

| # | 步骤 | 做什么 | 对应脚本 | 典型耗时 |
|---|---|---|---|---|
| 1 | 预检 | 检查工具、AWS 凭证、机型架构、机型在 AZ1 的供货, 确认费用 | `install.sh` | 数秒 |
| 2 | AWS 前置资源 | S3 输入 / 输出桶, ECR 仓库 (daft-ray、charts/*、mirror/*), ECR Pull-through Cache (quay / k8s / ecr-public), IAM 角色 `daft-ray-codebuild`, CodeBuild 项目 (ARM / x86) | `scripts/bootstrap.sh` | 1 分钟 |
| 3 | Ray+Daft 镜像 | CodeBuild 原生构建 (ARM 或 x86) 并推到 ECR; 构建里做架构 / 版本断言和 DuckDB httpfs 预装 | `scripts/build_image.sh` | 3~5 分钟 |
| 4 | chart 与镜像中转 | Helm chart (KubeRay 1.7.1 / kube-prometheus-stack 92.10.1 / grafana 10.5.15) 以 OCI 推到 ECR; Docker Hub 的 Grafana 镜像中转到 `mirror/grafana/grafana` | `scripts/mirror.sh` | 3 分钟 |
| 5 | TPC-H 数据 | CodeBuild 里用 DuckDB `dbgen` 生成 Parquet 到 S3 (SF1 / SF100 / ... ), 再用 DuckDB 算 Q1-Q22 参考结果 | `scripts/gen_data.sh` `scripts/gen_reference.sh` | 生成: SF100 约 3 分钟, SF1000 约 20 分钟 (XLARGE, 测试一的实测); 参考结果另计 |
| 6 | EKS 集群 | Terraform: VPC + NAT + VPC Endpoint, EKS 1.36, 托管 addon, 系统节点组, worker 节点组 | `terraform/` | 15~20 分钟 |
| 7 | 集群内组件 | Terraform: KubeRay operator、Prometheus (remote write → AMP)、Grafana、PodMonitor、RayCluster、Ray 面板 | `terraform/` | 5~10 分钟 |
| 8 | 验证 | 等 Ray head 和 worker Ready, 同步 `job/` 到 head, 跑 SF1 Q1-Q3 冒烟作业, 打印访问方式 | `install.sh` | 3 分钟 |

常用选项: `--yes` 不确认 · `--from N` 从第 N 步继续 · `--only N` 只做第 N 步 · `--skip 5` 跳过数据生成 · `--config my.env` 用其它配置。

## 配置 (config.env)

| 变量 | 默认 | 说明 |
|---|---|---|
| `WORKER_INSTANCE_TYPE` | `r8g.4xlarge` | **机型** (主要输入参数) |
| `WORKER_COUNT` | 3 | worker 节点数 (静态) |
| `OBJECT_STORE_BYTES` | 68719476736 | 每 worker 的 Ray object store (64 GiB); 80 GB 写 `80000000000` |
| `CLUSTER_NAME` | `daft-ray-static` | 集群名, 也是 kubectl context 名 |
| `AWS_REGION` / `AZ1` / `AZ2` | us-west-2 / 2b / 2c | 节点只放 AZ1, AZ2 仅为满足 EKS 控制面 2 个 AZ 的要求 |
| `INPUT_BUCKET` / `OUTPUT_BUCKET` | 留空 | 留空 = `daft-ray-input/output-<账号ID>-<region>` |
| `DATA_SCALE_FACTORS` | `1 100` | 要生成的 TPC-H 规模 (支持 1 100 300 500 1000 3000) |
| `GENERATE_REFERENCE` | `true` | 是否生成 DuckDB 参考结果 |
| `IMAGE_TAG`, `KUBERAY_CHART_VERSION`, ... | 见文件 | 与测试一的版本一致 |

## 输出参数 (部署完成后 `terraform -chdir=terraform output`)

| 输出 | 内容 |
|---|---|
| `eks_login_command` | `aws eks update-kubeconfig ...` |
| `ray_dashboard` | `http://localhost:8265` + `kubectl port-forward` 命令 |
| `grafana_dashboard` | `http://localhost:3000` + port-forward 命令 + admin 密码获取命令 + AMP workspace ID |
| `data_sample_s3` | 输入数据 `s3://<输入桶>/tpch-v2/sf<N>/`、参考结果、测试结果位置 |
| `logs` | Ray 日志目录 `/tmp/ray/session_latest/logs`、作业 driver 日志、raylet 日志、读取命令、S3 结果路径、AMP 指标库 |
| `cluster` | 机型 / 架构 / 是否 NVMe / spill 目标 / 镜像 / object store |

Ray 和 Grafana 的 Dashboard 仅集群内可达 (ClusterIP, 没有公网 LB), 所以 "URL" 是 localhost 加一条 port-forward 命令。

## 跑基准测试

```bash
scripts/run_benchmark.sh --sf 100 --questions 1-22 -- --s3-max-connections 32 --s3-refresh-creds
```
作业超过 1 小时必须带 `--s3-refresh-creds` (IRSA 的 STS 凭证 1 小时过期, 否则读 S3 会报 `ExpiredToken`)。
结果写到 `s3://<输出桶>/tpch-v2/results/sf<N>/<集群名>-<时间戳>-<种子或pid>/{summary.json,RESULTS.md}`。多流并发、固定随机种子打乱 Q 序等用法见 `job/tpch_bench.py` 顶部的参数说明 (`--shuffle-seed`、`--sequence`、`--order`)。

## 销毁

```bash
./uninstall.sh                 # 销毁 EKS / 节点 / VPC / AMP 等 (S3 数据、ECR 镜像、CodeBuild 保留)
./uninstall.sh --workers-only  # 只把 worker 缩到 0, 保留系统节点和监控 (约 $1/小时)
```

## 仓库结构

```
install.sh / uninstall.sh      一键部署 / 销毁
config.env.example             配置模板
scripts/                       lib.sh (状态输出、CodeBuild 封装), bootstrap / build_image / mirror / gen_data / gen_reference, sync_job / run_benchmark
image/                         Ray+Daft 镜像的 Dockerfile 和 requirements.txt (arm64 / x86_64 同一份)
codebuild/                     buildspec (镜像、chart、镜像中转、数据、参考结果), tpch_gen.py, IAM / 项目模板
job/                           基准作业: tpch_bench.py (主程序) / tpch_queries.py (Q1-Q22) / 校验 / 报告 / 参考结果
terraform/                     VPC、EKS、节点、IRSA、KubeRay、Prometheus、Grafana、RayCluster (详见 terraform/README.md)
```

## 测试一的集群配置 (本模板的默认值)

3 台 4xlarge 静态 worker (每台一个 Ray worker Pod: 15 CPU / 112Gi, object store 64 GiB 放在 `/dev/shm`), 单 AZ;
EBS 机型根盘 600 GiB gp3 (8000 IOPS / 500 MiB/s), Ray spill 走根盘; NVMe 机型根盘 100 GiB + 本地 NVMe RAID0;
head 4 CPU / 24Gi, 不跑任务; 数据直接从 S3 读 (Gateway Endpoint); 凭证走 IRSA。

## 费用 (us-west-2 按需, 约值)

3 台 worker + 2 台系统节点 + EKS 控制面 + NAT + 8 个 Interface Endpoint + AMP ≈ **$4~5/小时**。CodeBuild 按分钟计费 (镜像构建只有几分钟; 数据生成见上表耗时)。
S3 数据: SF100 约 36 GB (195 个文件), SF1000 约 367 GB (3075 个文件), SF3000 约 1.1 TB。**不用时请 `./uninstall.sh`**。

## 账号网络限制 (为什么有 VPC Endpoint 和 ECR 中转)

本模板按测试所用的受限账号设计: 节点访问不了 AWS 公网 API 端点 (ec2 / sts / ...), 也访问不了 quay.io / registry.k8s.io / Docker Hub。
因此用 VPC Interface Endpoint 走私网, 镜像经 ECR (Pull-through Cache 或 CodeBuild 中转)。没有这类限制的账号可在 `terraform/variables.tf` 把 `create_vpc_endpoints` 设为 false。

## 校验状态

已验证:
- `install.sh --dry-run` 和各脚本 `bash -n`;
- 步骤 1 的预检、步骤 3~5 的"已存在则跳过"判断, 都对原始账号做过只读验证 (镜像 / chart / 数据 / 参考结果均能被识别);
- `terraform validate` 通过; `terraform plan` 在 r8g / r7i / r8gd 三种机型上都能生成计划 (58 个资源)。

**尚未在空白账号上完整执行**:
- 步骤 2 的创建动作、步骤 3~5 的 CodeBuild 实际运行 (buildspec 和源码包布局与原测试一致, 只是改了路径和账号参数化);
- 步骤 6~8 的 `terraform apply` 和集群内安装 (provider 首次认证、`kubectl_manifest` 对 CRD 的时序已用 `depends_on` 和分阶段 apply 规避, 但需要真实环境确认)。
首次运行建议先用 `DATA_SCALE_FACTORS="1"` 和较小的规模走一遍。

## 已知限制

- 数据规模 300 / 500 的 `CHILDREN` 取值是按同一规则推算的 (测试一实际用的是 1 / 100 / 1000 / 3000)。
- Grafana 没有持久化 (配置全走 provisioning, 重建不丢); Prometheus 本地只留 2 小时, 长期指标在 AMP。
- 基准作业里的 `CLUSTERS` 映射已清空, 指标报告通过 `--amp-workspace` 取 AMP workspace (run_benchmark.sh 自动传入)。

## 许可证

MIT, 见 `LICENSE` (版权人处目前是占位符 `<COPYRIGHT HOLDER>`, 发布前请填写)。第三方内容 (Daft 的 TPC-H 查询实现等) 的归属和许可证见 `THIRD_PARTY_NOTICES.md`。本仓库的测试基于 TPC-H 规范自行实现, 不是 TPC 官方认证的结果, 不可与官方成绩比较。
