{
  "name": "daft-ray-build-x86",
  "description": "daft-ray: x86_64 镜像构建 (x86 原生构建环境)",
  "source": {
    "type": "S3",
    "location": "@INPUT_BUCKET@/build/src.zip"
  },
  "artifacts": { "type": "NO_ARTIFACTS" },
  "environment": {
    "type": "LINUX_CONTAINER",
    "image": "aws/codebuild/amazonlinux-x86_64-standard:5.0",
    "computeType": "BUILD_GENERAL1_LARGE",
    "privilegedMode": true,
    "environmentVariables": [
      { "name": "ECR_REGISTRY", "value": "@ECR_REGISTRY@", "type": "PLAINTEXT" }
    ]
  },
  "serviceRole": "arn:aws:iam::@ACCOUNT@:role/daft-ray-codebuild",
  "timeoutInMinutes": 60,
  "logsConfig": { "cloudWatchLogs": { "status": "ENABLED", "groupName": "/aws/codebuild/daft-ray-build-x86" } }
}
