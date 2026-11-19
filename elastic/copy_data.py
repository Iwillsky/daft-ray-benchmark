"""把输入桶里的 TPC-H Parquet (<前缀>/sf<N>) 复制到 FSx (/fsx/<前缀>/...), 目录布局不变。
作为 Indexed Job 运行: 每个 Pod 按 (对象序号 % 总数 == 本 Pod 序号) 处理一部分对象; 先写 .part 临时文件, 大小一致后再 rename。
环境变量: BUCKET (输入桶), DATA_PREFIX (默认 tpch-v2), SFS (默认 100,300,500,1000), JOB_COMPLETION_INDEX (K8s 注入), N_PODS。
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from boto3.s3.transfer import TransferConfig

BUCKET = os.environ["BUCKET"]
SFS = os.environ.get("SFS", "100,300,500,1000").split(",")
PREFIX = os.environ.get("DATA_PREFIX", "tpch-v2")
IDX, N = int(os.environ["JOB_COMPLETION_INDEX"]), int(os.environ["N_PODS"])
DEST = "/fsx/" + PREFIX
s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2")
cfg = TransferConfig(multipart_threshold=64 * 1024 * 1024, multipart_chunksize=64 * 1024 * 1024, max_concurrency=8, use_threads=True)
keys = []
for sf in SFS:
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{PREFIX}/sf{sf}/"):
        for o in page.get("Contents", []):
            if not o["Key"].endswith("/"):
                keys.append((o["Key"], o["Size"]))
keys.sort()
mine = [k for i, k in enumerate(keys) if i % N == IDX]
print(f"[{IDX}] 总对象 {len(keys)} 个, 本 Pod {len(mine)} 个, {sum(s for _, s in mine) / 1e9:.1f} GB", flush=True)


def copy(item):
    key, size = item
    dst = os.path.join(DEST, key[len(PREFIX) + 1:])
    if os.path.exists(dst) and os.path.getsize(dst) == size:
        return size, 0
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    tmp = dst + ".part"
    for attempt in range(5):
        try:
            s3.download_file(BUCKET, key, tmp, Config=cfg)
            if os.path.getsize(tmp) != size:
                raise IOError(f"size mismatch {os.path.getsize(tmp)} != {size}")
            os.replace(tmp, dst)
            return size, 1
        except Exception as e:  # noqa: BLE001
            print(f"[{IDX}] {key} 第 {attempt + 1} 次失败: {e!r}", flush=True)
            time.sleep(5)
    raise RuntimeError(f"copy failed: {key}")


t0 = time.time()
done = copied = 0
with ThreadPoolExecutor(max_workers=6) as ex:
    for size, c in ex.map(copy, mine):
        done += size
        copied += c
print(f"[{IDX}] 完成: {len(mine)} 个对象, {done / 1e9:.1f} GB, 实际新复制 {copied} 个, 用时 {time.time() - t0:.0f}s ({done / 1e6 / max(time.time() - t0, 1):.0f} MB/s)", flush=True)
