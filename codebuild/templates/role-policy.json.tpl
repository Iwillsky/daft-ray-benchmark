{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Logs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:@REGION@:@ACCOUNT@:log-group:/aws/codebuild/daft-ray-*"
    },
    {
      "Sid": "ReadBuildSource",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::@INPUT_BUCKET@/build/*"
    },
    {
      "Sid": "WriteTpchData",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:AbortMultipartUpload", "s3:GetObject"],
      "Resource": "arn:aws:s3:::@INPUT_BUCKET@/@DATA_PREFIX@/*"
    },
    {
      "Sid": "ListBuckets",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": ["arn:aws:s3:::@INPUT_BUCKET@", "arn:aws:s3:::@OUTPUT_BUCKET@"]
    },
    {
      "Sid": "EcrAuth",
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Sid": "EcrPushPull",
      "Effect": "Allow",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage",
        "ecr:DescribeRepositories",
        "ecr:DescribeImages"
      ],
      "Resource": [
        "arn:aws:ecr:@REGION@:@ACCOUNT@:repository/daft-ray",
        "arn:aws:ecr:@REGION@:@ACCOUNT@:repository/mirror/*",
        "arn:aws:ecr:@REGION@:@ACCOUNT@:repository/charts/*"
      ]
    },
    {
      "Sid": "WriteTpchReference",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::@OUTPUT_BUCKET@/@DATA_PREFIX@/reference/*"
    }
  ]
}
