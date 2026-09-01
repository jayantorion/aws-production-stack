#!/usr/bin/env bash
# Post-deploy: upload artifacts + wire the S3 event trigger (avoids CFN circular dep)
set -euo pipefail
ENV="${1:-dev}"
STACK="dep-core"

# 1) package lambda
(cd lambda/arrival_validator && zip -r ../../arrival_validator.zip .)
aws s3 cp arrival_validator.zip "s3://my-deploy-bucket/lambda/arrival_validator.zip"
aws s3 cp glue_jobs/retail_silver_job.py "s3://my-deploy-bucket/glue/retail_silver_job.py"

# 2) deploy infra
aws cloudformation deploy --template-file infra/cloudformation.yaml --stack-name "$STACK" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides Environment="$ENV" DeployBucket=my-deploy-bucket GlueJobRoleArn=<glue-role-arn>

# 3) upload pipeline config consumed by Glue/Lambda
RAW_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='RawBucket'].OutputValue" --output text)
aws s3 cp config/entities.yaml "s3://$RAW_BUCKET/config/entities.yaml"
aws s3 cp config/sla.yaml "s3://$RAW_BUCKET/config/sla.yaml"

# 4) wire S3 event notification on manifests -> Lambda
FUNCTION_ARN=$(aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='ValidatorArn'].OutputValue" --output text)
aws s3api put-bucket-notification-configuration --bucket "$RAW_BUCKET" \
  --notification-configuration "{
    \"LambdaFunctionConfigurations\": [{
      \"LambdaFunctionArn\": \"$FUNCTION_ARN\",
      \"Events\": [\"s3:ObjectCreated:*\"],
      \"Filter\": {\"Key\": {\"FilterRules\": [
        {\"Name\": \"suffix\", \"Value\": \"_MANIFEST.json\"}
      ]}}
    }]
  }"

echo "Deploy complete. Redshift DDL: psql/fisql -f sql/redshift_ddl.sql"
