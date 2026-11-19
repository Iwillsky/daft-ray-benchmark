# 把 S3 数据复制到 FSx 的 Indexed Job (16 个 Pod, 每个 3 核 / 8Gi), 由 scripts/fsx_load_data.sh 渲染并提交。
# 占位符: @IMAGE@ (Ray 镜像, 里面有 boto3) @ARCH@ @BUCKET@ (输入桶) @PREFIX@ (数据前缀) @SFS@ (逗号分隔的规模)。
# ConfigMap copy-data-script 由 elastic/copy_data.py 生成。
apiVersion: batch/v1
kind: Job
metadata:
  name: copy-tpch-to-fsx
  namespace: ray
spec:
  completionMode: Indexed
  completions: 16
  parallelism: 16
  backoffLimit: 4
  template:
    spec:
      serviceAccountName: ray-s3-access
      restartPolicy: OnFailure
      nodeSelector:
        role: ray-worker
        kubernetes.io/arch: @ARCH@
      tolerations:
        - { key: workload, operator: Equal, value: ray-worker, effect: NoSchedule }
      containers:
        - name: copy
          image: @IMAGE@
          command: ["python", "/script/copy_data.py"]
          env:
            - { name: N_PODS, value: "16" }
            - { name: BUCKET, value: "@BUCKET@" }
            - { name: DATA_PREFIX, value: "@PREFIX@" }
            - { name: SFS, value: "@SFS@" }
          resources:
            requests: { cpu: "3", memory: 8Gi }
            limits:   { cpu: "3", memory: 8Gi }
          volumeMounts:
            - { name: fsx, mountPath: /fsx }
            - { name: script, mountPath: /script }
      volumes:
        - name: fsx
          persistentVolumeClaim: { claimName: fsx-pvc }
        - name: script
          configMap: { name: copy-data-script }
