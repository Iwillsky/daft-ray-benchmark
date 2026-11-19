#!/usr/bin/env bash
# 测试二: 把 S3 里的 TPC-H 数据复制到 FSx for Lustre (/fsx/<前缀>/sf<N>), 作业用 --source fsx 读取。
# 做法: 一个 Indexed Job (16 个 Pod, 每个 3 核 / 8Gi, 跑在 Karpenter 拉起的 worker 节点上), 每个 Pod 处理一部分对象, 先写 .part 再改名 (见 elastic/copy_data.py)。
# 用法: scripts/fsx_load_data.sh <SF> [<SF> ...]       例: scripts/fsx_load_data.sh 100 300 500 1000
# 已复制且大小一致的文件会跳过, 可以重复执行。
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config
[ $# -ge 1 ] || { echo "用法: fsx_load_data.sh <SF> [<SF> ...]"; exit 1; }
SFS="$(echo "$*" | tr ' ' ',')"
K="kubectl --context $CLUSTER_NAME"
CL_JSON="$(terraform -chdir="$ROOT_DIR/terraform" output -json cluster 2>/dev/null)" || { echo "读不到 terraform 输出 (集群还没建好?)"; exit 1; }
IMG="$(echo "$CL_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['ray_image'])")"
ARCH_K8S="$(echo "$CL_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['worker_arch'])")"
$K -n ray get pvc fsx-pvc >/dev/null 2>&1 || { echo "没有 PVC fsx-pvc: 这个集群不是 elastic 模式, 或 FSx 还没建好"; exit 1; }

$K -n ray delete job copy-tpch-to-fsx --ignore-not-found >/dev/null
$K -n ray create configmap copy-data-script --from-file=copy_data.py="$ROOT_DIR/elastic/copy_data.py" --dry-run=client -o yaml | $K apply -f - >/dev/null
sed -e "s#@IMAGE@#$IMG#g" -e "s#@ARCH@#$ARCH_K8S#g" -e "s#@BUCKET@#$INPUT_BUCKET#g" -e "s#@PREFIX@#$DATA_PREFIX#g" -e "s#@SFS@#$SFS#g" "$ROOT_DIR/elastic/copy-job.yaml.tpl" | $K apply -f - >/dev/null
info "已提交复制作业 copy-tpch-to-fsx (SF $SFS, 16 个 Pod); Karpenter 需要先拉起 worker 节点, 约 1~2 分钟"
for _ in $(seq 1 180); do
  S=$($K -n ray get job copy-tpch-to-fsx -o jsonpath='{.status.succeeded}/{.spec.completions} 失败={.status.failed}' 2>/dev/null || true)
  info "进度: ${S:-启动中}"
  case "$S" in 16/16*) break ;; esac
  sleep 20
done
case "$S" in 16/16*) info "FSx 数据复制完成: /fsx/$DATA_PREFIX/sf{$SFS}" ;; *) echo "复制作业未在 60 分钟内完成: kubectl --context $CLUSTER_NAME -n ray logs job/copy-tpch-to-fsx"; exit 1 ;; esac
