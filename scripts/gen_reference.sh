#!/usr/bin/env bash
# 步骤: 用 DuckDB 在 S3 的 TPC-H 数据上独立算出 Q1-Q22 的参考结果, 写到 s3://<输出桶>/<前缀>/reference/sf<N>/q<N>.json
# (tpch_bench.py 用它校验 Daft 的结果; SF1 另有官方标准答案 job/tpch_answers_sf1.json)。已有 22 个结果会跳过。
# 用法: scripts/gen_reference.sh <SF> [--force]
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config
SF="${1:?用法: gen_reference.sh <SF> [--force]}"; FORCE="${2:-}"
# SF -> DuckDB 线程数 / 内存上限(GB) / CodeBuild 规格; SF>=1000 用 2XLARGE (THREADS=32 MEM_GB=60, 来自 buildspec-tpch-ref.yml 的说明)
if [ "$SF" -ge 1000 ]; then THREADS=32; MEM=60; COMPUTE=BUILD_GENERAL1_2XLARGE; TMO=480
elif [ "$SF" -ge 100 ]; then THREADS=16; MEM=28; COMPUTE=BUILD_GENERAL1_XLARGE; TMO=240
else THREADS=8; MEM=12; COMPUTE=BUILD_GENERAL1_LARGE; TMO=60; fi
PREFIX="s3://$OUTPUT_BUCKET/$DATA_PREFIX/reference/sf$SF/"
have=$(aws s3 ls "$PREFIX" 2>/dev/null | grep -c 'q[0-9]*\.json' || true)
if [ "$FORCE" != "--force" ] && [ "${have:-0}" -ge 22 ]; then info "SF$SF 参考结果已存在 ($have 个): $PREFIX"; exit 0; fi
aws s3 ls "s3://$INPUT_BUCKET/$DATA_PREFIX/sf$SF/_manifest.json" >/dev/null 2>&1 || { echo "SF$SF 数据还不存在, 先执行 scripts/gen_data.sh $SF"; exit 1; }
info "计算 SF$SF 参考结果: DuckDB 线程 $THREADS, 内存 ${MEM}GB, 规格 $COMPUTE"
package_source
cb_run daft-ray-build codebuild/buildspec-tpch-ref.yml "$(cb_env "SF=$SF" "THREADS=$THREADS" "MEM_GB=$MEM")" \
  --compute-type-override "$COMPUTE" --timeout-in-minutes-override "$TMO"
info "SF$SF 参考结果已写入 $PREFIX"
