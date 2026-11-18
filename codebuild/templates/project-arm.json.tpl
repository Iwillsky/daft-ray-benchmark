{
  "name": "daft-ray-build",
  "description": "daft-ray: arm64 镜像构建 / Helm chart 与镜像中转 / TPC-H 数据与参考结果生成 (ARM 原生构建环境)",
  "source": {
    "type": "S3",
    "location": "@INPUT_BUCKET@/build/src.zip"
  },
  "artifacts": { "type": "NO_ARTIFACTS" },
  "environment": {
    "type": "ARM_CONTAINER",
    "image": "aws/codebuild/amazonlinux-aarch64-standard:3.0",
    "computeType": "BUILD_GENERAL1_LARGE",
    "privilegedMode": true,
    "environmentVariables": [
      { "name": "ECR_REGISTRY", "value": "@ECR_REGISTRY@", "type": "PLAINTEXT" }
    ]
  },
  "serviceRole": "arn:aws:iam::@ACCOUNT@:role/daft-ray-codebuild",
  "timeoutInMinutes": 60,
  "logsConfig": { "cloudWatchLogs": { "status": "ENABLED", "groupName": "/aws/codebuild/daft-ray-build" } }
}
