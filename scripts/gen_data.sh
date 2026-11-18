#!/usr/bin/env bash
# 步骤: 在 CodeBuild 里用 DuckDB 的 tpch 扩展生成 TPC-H Parquet 数据集并写到 s3://<输入桶>/<前缀>/sf<N>/ (见 codebuild/tpch_gen.py)。
# 在 CodeBuild 做是因为 DuckDB 扩展要联网下载, 集群节点访问不了公网。已有且校验通过的规模会跳过。
# 用法: scripts/gen_data.sh <SF> [--force]        例: scripts/gen_data.sh 100
# 每张大表拆成 CHILDREN 个文件 (对齐 Daft 官方 benchmark: SF100=32, SF1000=512; SF3000=1536 来自测试一的实际数据)。
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config
SF="${1:?用法: gen_data.sh <SF> [--force]}"; FORCE="${2:-}"
# 规模 -> CHILDREN PROCS 计算规格 超时(分钟) ; 其中 1 / 100 / 1000 / 3000 来自测试一的实际运行, 300 / 500 为按同一规则的取值
case "$SF" in
  1)    CHILDREN=8;    PROCS=4;  COMPUTE=BUILD_GENERAL1_LARGE;  TMO=60  ;;
  100)  CHILDREN=32;   PROCS=8;  COMPUTE=BUILD_GENERAL1_XLARGE; TMO=180 ;;
  300)  CHILDREN=128;  PROCS=10; COMPUTE=BUILD_GENERAL1_XLARGE; TMO=300 ;;
  500)  CHILDREN=256;  PROCS=10; COMPUTE=BUILD_GENERAL1_XLARGE; TMO=480 ;;
  1000) CHILDREN=512;  PROCS=12; COMPUTE=BUILD_GENERAL1_XLARGE; TMO=480 ;;
  3000) CHILDREN=1536; PROCS=12; COMPUTE=BUILD_GENERAL1_XLARGE; TMO=480 ;;
  *) echo "不支持的规模 SF$SF (支持 1 100 300 500 1000 3000; 其它规模请在本脚本里加一行)"; exit 1 ;;
esac
MANIFEST="s3://$INPUT_BUCKET/$DATA_PREFIX/sf$SF/_manifest.json"
if [ "$FORCE" != "--force" ] && aws s3 cp "$MANIFEST" - 2>/dev/null | python3 -c "import json,sys; m=json.load(sys.stdin); sys.exit(0 if not m.get('problems') else 1)"; then
  info "SF$SF 数据已存在且校验通过: s3://$INPUT_BUCKET/$DATA_PREFIX/sf$SF/ (加 --force 重新生成)"; exit 0
fi
info "生成 SF$SF: children=$CHILDREN procs=$PROCS 规格=$COMPUTE (超时 ${TMO} 分钟)"
[ "$SF" -ge 1000 ] && warn "SF$SF 会写入约 $(( SF * 37 / 100 )) GB 数据到 S3 (SF1000 在 XLARGE 上约 20 分钟)"
package_source
cb_run daft-ray-build codebuild/buildspec-tpch-data.yml \
  "$(cb_env "SF=$SF" "CHILDREN=$CHILDREN" "PROCS=$PROCS" "DATA_BUCKET=$INPUT_BUCKET" "DATA_ROOT=$DATA_PREFIX")" \
  --compute-type-override "$COMPUTE" --timeout-in-minutes-override "$TMO"
aws s3 cp "$MANIFEST" - | python3 -c "import json,sys; m=json.load(sys.stdin); print('    rows:', m['rows']); sys.exit(1 if m.get('problems') else 0)" \
  || { echo "SF$SF 数据校验未通过 (见上或 _manifest.json 的 problems)"; exit 1; }
info "SF$SF 数据已生成: s3://$INPUT_BUCKET/$DATA_PREFIX/sf$SF/"
