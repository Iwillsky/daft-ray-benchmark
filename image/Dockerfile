# Ray + Daft 镜像, arm64 (Graviton) 与 x86_64 (Intel) 共用同一个 Dockerfile, 在各自架构的 CodeBuild 上原生构建。
#
#   arm64 (默认): BASE_IMAGE=rayproject/ray:2.59.0-py311-aarch64  EXPECT_ARCH=aarch64   (CodeBuild ARM_CONTAINER)
#   x86_64      : BASE_IMAGE=rayproject/ray:2.59.0-py311          EXPECT_ARCH=x86_64    (CodeBuild LINUX_CONTAINER)
# 见 codebuild/buildspec-image.yml (构建参数 BASE_IMAGE / EXPECT_ARCH)。
#
# 版本关系 (均已核对):
# - Daft 0.7.26 的 `ray` extra 要求 ray[client,data] >=2.11,<2.60, 所以 Ray 2.59.0 在支持范围内。
# - head / worker / 提交作业的客户端三方 Ray 与 Python 版本必须一致 (这里 Ray 2.59.0 + Python 3.11)。
# - Ray 官方镜像 tag 格式 {ray版本}-{python版本}[-{架构}], arm64 带 -aarch64 后缀, x86_64 不带后缀。
ARG BASE_IMAGE=rayproject/ray:2.59.0-py311-aarch64
FROM ${BASE_IMAGE}

# FROM 之后重新声明才能在本阶段使用; 构建机架构与目标架构一致 (原生构建), 冒烟校验里会断言
ARG EXPECT_ARCH=aarch64
ENV EXPECT_ARCH=${EXPECT_ARCH}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

# --only-binary=:all: 保证只装预编译 wheel; 任何没有目标架构 wheel 的包会让构建直接失败,
# 而不是悄悄回退到源码编译。constraint 把 Ray 钉在基础镜像版本, 防止 pip 顺带升级/降级 Ray。
RUN echo "ray==2.59.0" > /tmp/constraints.txt \
    && python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir --only-binary=:all: \
        -c /tmp/constraints.txt -r /tmp/requirements.txt

# 冒烟校验: 架构、库版本、Daft 能起 Ray runner 所需的依赖
RUN python -c "import os, platform, ray, daft, pyarrow, boto3; \
assert platform.machine() == os.environ['EXPECT_ARCH'], (platform.machine(), os.environ['EXPECT_ARCH']); \
assert ray.__version__ == '2.59.0', ray.__version__; \
assert daft.__version__ == '0.7.26', daft.__version__; \
print('arch', platform.machine(), '| ray', ray.__version__, '| daft', daft.__version__, '| pyarrow', pyarrow.__version__)"

USER ray

# DuckDB 的 httpfs 扩展在构建期下载 (节点运行时访问不了公网), 装到 ray 用户的 ~/.duckdb, 运行时 LOAD httpfs 即可离线使用
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('LOAD httpfs'); print('duckdb', duckdb.__version__, 'httpfs ok')"
