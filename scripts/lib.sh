#!/usr/bin/env bash
# 公共函数: 读配置、状态输出、CodeBuild 启动 / 等待。被 install.sh 和 scripts/*.sh source。
# 兼容 macOS 自带的 bash 3.2 (不用关联数组 / mapfile)。

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$LIB_DIR/.." && pwd)"
STATE_DIR="$ROOT_DIR/.install"
mkdir -p "$STATE_DIR"
LOG_FILE="${LOG_FILE:-$STATE_DIR/install.log}"

# ---------------------------------------------------------------- 颜色
if [ -t 1 ]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_BLU=$'\033[34m'; C_DIM=$'\033[2m'; C_RST=$'\033[0m'; C_BLD=$'\033[1m'
else
  C_RED=""; C_GRN=""; C_YEL=""; C_BLU=""; C_DIM=""; C_RST=""; C_BLD=""
fi

_log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >>"$LOG_FILE"; }
info() { printf '    %s\n' "$*"; _log "INFO $*"; }
warn() { printf '    %s! %s%s\n' "$C_YEL" "$*" "$C_RST"; _log "WARN $*"; }
hint() { printf '    %s→ %s%s\n' "$C_DIM" "$*" "$C_RST"; _log "HINT $*"; }

# ---------------------------------------------------------------- 步骤状态: begin -> ok / skip / fail
_STEP_T0=0
_STEP_TITLE=""
step_begin() { # "3/8" "标题"
  _STEP_TITLE="$1 $2"; _STEP_T0=$(date +%s)
  printf '\n%s▶ [%s] %s%s\n' "$C_BLD$C_BLU" "$1" "$2" "$C_RST"; _log "BEGIN $1 $2"
}
_elapsed() { local s=$(( $(date +%s) - _STEP_T0 )); printf '%dm%02ds' $((s / 60)) $((s % 60)); }
step_ok()   { printf '  %s✔ 完成%s %s(%s)%s %s\n' "$C_GRN" "$C_RST" "$C_DIM" "$(_elapsed)" "$C_RST" "${1:-}"; _log "OK $_STEP_TITLE ${1:-}"; }
step_skip() { printf '  %s↷ 跳过%s %s\n' "$C_YEL" "$C_RST" "${1:-}"; _log "SKIP $_STEP_TITLE ${1:-}"; }
step_fail() { # 消息 [提示...]
  printf '  %s✘ 失败%s %s(%s)%s %s\n' "$C_RED" "$C_RST" "$C_DIM" "$(_elapsed)" "$C_RST" "$1"; _log "FAIL $_STEP_TITLE $1"
  shift; local h; for h in "$@"; do hint "$h"; done
  printf '    日志: %s\n' "$LOG_FILE"
  exit 1
}

# ---------------------------------------------------------------- 配置
load_config() {
  local f="${CONFIG_FILE:-$ROOT_DIR/config.env}"
  if [ ! -f "$f" ]; then
    echo "找不到 $f; 先执行: cp config.env.example config.env 并按需修改" >&2; exit 1
  fi
  # shellcheck disable=SC1090
  set -a; . "$f"; set +a
  export AWS_REGION="${AWS_REGION:-us-west-2}" AWS_DEFAULT_REGION="${AWS_REGION:-us-west-2}"
  if [ -n "${AWS_PROFILE:-}" ]; then export AWS_PROFILE; else unset AWS_PROFILE; fi
  ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
  [ -n "$ACCOUNT_ID" ] || { echo "无法获取 AWS 账号 (aws sts get-caller-identity 失败): 检查 AWS_PROFILE / 凭证是否有效" >&2; exit 1; }
  INPUT_BUCKET="${INPUT_BUCKET:-daft-ray-input-$ACCOUNT_ID-$AWS_REGION}"
  OUTPUT_BUCKET="${OUTPUT_BUCKET:-daft-ray-output-$ACCOUNT_ID-$AWS_REGION}"
  DATA_PREFIX="${DATA_PREFIX:-tpch-v2}"
  ECR_REGISTRY="$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
  export ACCOUNT_ID INPUT_BUCKET OUTPUT_BUCKET DATA_PREFIX ECR_REGISTRY
}

# 由机型推导架构: arm64 / x86_64 (读 EC2 API)
worker_arch() {
  aws ec2 describe-instance-types --instance-types "$WORKER_INSTANCE_TYPE" \
    --query 'InstanceTypes[0].ProcessorInfo.SupportedArchitectures[0]' --output text 2>/dev/null
}

# ---------------------------------------------------------------- CodeBuild
# cb_env KEY=VALUE ... -> 输出 environmentVariablesOverride 的 JSON
cb_env() {
  python3 - "$@" <<'PY'
import json, sys
print(json.dumps([{"name": a.split("=", 1)[0], "value": a.split("=", 1)[1], "type": "PLAINTEXT"} for a in sys.argv[1:]]))
PY
}

# cb_run <项目> <buildspec 路径(源码 zip 内)> <env JSON> [额外 start-build 参数...] -> 启动并等待, 失败退出非 0
cb_run() {
  local project="$1" spec="$2" envjson="$3"; shift 3
  local id
  id="$(aws codebuild start-build --project-name "$project" --buildspec-override "$spec" \
        --environment-variables-override "$envjson" "$@" --query 'build.id' --output text)" || return 1
  info "CodeBuild 已启动: $id"
  hint "实时日志: aws logs tail /aws/codebuild/$project --follow   (或控制台 CodeBuild -> $project)"
  local st phase last="" t0
  t0=$(date +%s)
  while :; do
    sleep 15
    st="$(aws codebuild batch-get-builds --ids "$id" --query 'builds[0].[buildStatus,currentPhase]' --output text 2>/dev/null)" || continue
    set -- $st
    if [ "$2" != "$last" ]; then info "阶段: $2 (已 $(( ($(date +%s) - t0) / 60 )) 分钟)"; last="$2"; fi
    case "$1" in
      SUCCEEDED) return 0 ;;
      FAILED|FAULT|STOPPED|TIMED_OUT)
        warn "CodeBuild 状态 $1, 日志末尾:"
        aws logs tail "/aws/codebuild/$project" --since 2h 2>/dev/null | tail -25 | sed 's/^/        /'
        return 1 ;;
    esac
  done
}

package_source() { # 生成 CodeBuild 用的源码包并上传到 s3://<输入桶>/build/src.zip
  local stage="$STATE_DIR/src" zip="$STATE_DIR/src.zip"
  rm -rf "$stage" "$zip"; mkdir -p "$stage"
  cp "$ROOT_DIR/image/Dockerfile" "$ROOT_DIR/image/requirements.txt" "$stage/"
  cp -R "$ROOT_DIR/codebuild" "$stage/codebuild"; rm -rf "$stage/codebuild/templates"
  cp -R "$ROOT_DIR/job" "$stage/job"
  (cd "$stage" && zip -q -r "$zip" . -x '*/__pycache__/*')
  aws s3 cp "$zip" "s3://$INPUT_BUCKET/build/src.zip" --only-show-errors
}

ecr_tag_exists() { # repo tag
  aws ecr describe-images --repository-name "$1" --image-ids "imageTag=$2" --query 'imageDetails[0].imageTags' --output text >/dev/null 2>&1
}
