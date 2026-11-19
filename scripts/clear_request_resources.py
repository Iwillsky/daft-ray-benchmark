"""清除 Ray autoscaler 里残留的 request_resources 请求。

背景: Daft 的分布式执行 (flotilla) 会调用 ray.autoscaler.sdk.request_resources 预留 CPU (ray status 里显示为
"From request_resources: {'CPU': 1.0}: N from request_resources()")。作业被 stop 后这个请求不会自动撤销,
autoscaler 就认为一直"需要"这些 CPU, 不会回收 worker, 也就不会缩容。把请求重置为 0 即可。
用法: kubectl -n ray exec -i <head-pod> -c ray-head -- python - < scripts/clear_request_resources.py
"""
import ray

ray.init(address="auto", logging_level="ERROR")
from ray.autoscaler.sdk import request_resources  # noqa: E402

request_resources(num_cpus=0)
print("request_resources cleared")
