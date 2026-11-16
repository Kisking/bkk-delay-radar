#!/usr/bin/env bash
# Builds the whole BKK Delay Radar from one CloudFormation template.
#
#   export ALERT_EMAIL="you@example.com"      # optional, enables email alerts
#   bash infra/deploy_stack.sh                # create or update the stack
#   bash infra/deploy_stack.sh --delete       # take it down (buckets are kept)
#
# The Lambda code and the Glue script are zipped and uploaded first, because CloudFormation
# can only read them from S3.
set -euo pipefail
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}
cd "$(dirname "$0")/.."
STACK=${STACK:-bkk-delay-radar}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
CODE_BUCKET=${CODE_BUCKET:-bkk-delay-radar-$ACCOUNT-code}
PREFIX=deploy/

if [ "${1:-}" = "--delete" ]; then
  echo "Deleting stack $STACK (the two data buckets are kept on purpose)…"
  aws cloudformation delete-stack --stack-name "$STACK"
  aws cloudformation wait stack-delete-complete --stack-name "$STACK"
  echo "Done. To remove the data too:"
  echo "  aws s3 rb s3://bkk-delay-radar-$ACCOUNT --force"
  echo "  aws s3 rb s3://bkk-delay-radar-$ACCOUNT-site --force"
  exit 0
fi

echo "account=$ACCOUNT region=$AWS_DEFAULT_REGION stack=$STACK"
aws s3api head-bucket --bucket "$CODE_BUCKET" 2>/dev/null || aws s3api create-bucket --bucket "$CODE_BUCKET" >/dev/null

mkdir -p build
# left side = python file, right side = zip name the template expects
for pair in ingest_rt:ingest-rt ingest_weather:ingest-weather ingest_static:ingest-static \
            start_glue:start-glue publish_dashboard:publish healthcheck:healthcheck; do
  f=${pair%%:*}; name=${pair##*:}
  rm -f "build/bkk-$name.zip"
  (cd lambdas && zip -q -j "../build/bkk-$name.zip" "$f.py")
  aws s3 cp "build/bkk-$name.zip" "s3://$CODE_BUCKET/${PREFIX}bkk-$name.zip" --only-show-errors
done
aws s3 cp glue/delay_job.py "s3://$CODE_BUCKET/${PREFIX}delay_job.py" --only-show-errors
echo "code uploaded to s3://$CODE_BUCKET/$PREFIX"

ROLE_ARN=$(aws iam get-role --role-name LabRole --query Role.Arn --output text)
aws cloudformation deploy \
  --stack-name "$STACK" \
  --template-file infra/template.yaml \
  --parameter-overrides \
      LabRoleArn="$ROLE_ARN" \
      CodeBucket="$CODE_BUCKET" \
      CodePrefix="$PREFIX" \
      AlertEmail="${ALERT_EMAIL:-}" \
  --no-fail-on-empty-changeset

echo
aws cloudformation describe-stacks --stack-name "$STACK" \
  --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output text
echo
echo "If this is a fresh stack, set the real API key now:"
echo "  aws secretsmanager put-secret-value --secret-id bkk/api-key --secret-string \"YOUR-KEY\""
echo "Then upload the web page once:"
echo "  aws s3 cp site/index.html s3://bkk-delay-radar-$ACCOUNT-site/index.html --content-type text/html"
