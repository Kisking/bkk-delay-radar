#!/usr/bin/env bash
# BKK Delay Radar - deploy helper for the AWS Academy Learner Lab (us-east-1, LabRole).
# Usage:  export BKK_API_KEY="..."   (only for stage 'ingest')
#         ./deploy.sh ingest | glue | publish
set -euo pipefail
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}
cd "$(dirname "$0")"; mkdir -p build
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=${BUCKET:-bkk-delay-radar-$ACCOUNT}
SITE_BUCKET=${SITE_BUCKET:-$BUCKET-site}
ROLE_ARN=$(aws iam get-role --role-name LabRole --query Role.Arn --output text)
echo "account=$ACCOUNT region=$AWS_DEFAULT_REGION bucket=$BUCKET"

deploy_fn() { # name file timeout memory env
  local name=$1 file=$2 timeout=$3 mem=$4 env=$5 mod=${2%.py}
  rm -f "build/$name.zip"; (cd lambdas && zip -q -j "../build/$name.zip" "$file")
  if aws lambda get-function --function-name "$name" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$name" --zip-file "fileb://build/$name.zip" >/dev/null
    aws lambda wait function-updated --function-name "$name"
    aws lambda update-function-configuration --function-name "$name" --timeout "$timeout" \
      --memory-size "$mem" --environment "Variables={$env}" \
      ${DLQ_ARN:+--dead-letter-config TargetArn=$DLQ_ARN} >/dev/null
  else
    aws lambda create-function --function-name "$name" --runtime python3.12 --role "$ROLE_ARN" \
      --handler "$mod.handler" --zip-file "fileb://build/$name.zip" --timeout "$timeout" \
      --memory-size "$mem" --environment "Variables={$env}" \
      ${DLQ_ARN:+--dead-letter-config TargetArn=$DLQ_ARN} >/dev/null
  fi
  aws lambda wait function-active --function-name "$name"
  echo "deployed $name"
}

schedule() { # rule function expression
  local rule=$1 fn=$2 expr=$3 fn_arn rule_arn
  aws events put-rule --name "$rule" --schedule-expression "$expr" --state ENABLED >/dev/null
  fn_arn=$(aws lambda get-function --function-name "$fn" --query Configuration.FunctionArn --output text)
  rule_arn=$(aws events describe-rule --name "$rule" --query Arn --output text)
  aws lambda add-permission --function-name "$fn" --statement-id "eb-$rule" \
    --action lambda:InvokeFunction --principal events.amazonaws.com --source-arn "$rule_arn" >/dev/null 2>&1 || true
  aws events put-targets --rule "$rule" --targets "Id=1,Arn=$fn_arn" >/dev/null
  echo "scheduled $rule -> $fn ($expr)"
}

stage_ingest() {
  : "${BKK_API_KEY:?export BKK_API_KEY first}"
  aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null || aws s3api create-bucket --bucket "$BUCKET" >/dev/null
  if aws secretsmanager describe-secret --secret-id bkk/api-key >/dev/null 2>&1; then
    aws secretsmanager put-secret-value --secret-id bkk/api-key --secret-string "$BKK_API_KEY" >/dev/null
  else
    aws secretsmanager create-secret --name bkk/api-key --secret-string "$BKK_API_KEY" >/dev/null
  fi
  local dlq_url
  dlq_url=$(aws sqs create-queue --queue-name bkk-ingest-dlq --query QueueUrl --output text)
  DLQ_ARN=$(aws sqs get-queue-attributes --queue-url "$dlq_url" --attribute-names QueueArn \
            --query Attributes.QueueArn --output text)
  export DLQ_ARN
  deploy_fn bkk-ingest-rt      ingest_rt.py      60  256 "BUCKET=$BUCKET,KINDS=TripUpdates+Alerts+VehiclePositions,SECRET_ID=bkk/api-key"
  deploy_fn bkk-ingest-weather ingest_weather.py 30  128 "BUCKET=$BUCKET"
  deploy_fn bkk-ingest-static  ingest_static.py  300 512 "BUCKET=$BUCKET"
  schedule bkk-rt-5min        bkk-ingest-rt      "rate(5 minutes)"
  schedule bkk-weather-hourly bkk-ingest-weather "rate(1 hour)"
  schedule bkk-static-daily   bkk-ingest-static  "cron(0 19 * * ? *)"
  # first runs right away (async), so there is data to look at
  for f in bkk-ingest-static bkk-ingest-weather bkk-ingest-rt; do
    aws lambda invoke --function-name "$f" --invocation-type Event build/out.json >/dev/null
  done
  echo "Done. Check in ~1 min:  aws s3 ls s3://$BUCKET/raw/ --recursive | head"
}

stage_static() {   # redeploy only the static-GTFS Lambda (no API key needed) and run it once
  deploy_fn bkk-ingest-static ingest_static.py 300 512 "BUCKET=$BUCKET"
  aws lambda invoke --function-name bkk-ingest-static --invocation-type Event build/out.json >/dev/null
  echo "Started. After ~2 min:  aws s3 ls s3://$BUCKET/raw/gtfs_static_txt/ --recursive --human-readable"
}

stage_alerts() { # email alerts: SNS topic + hourly health check Lambda
  : "${ALERT_EMAIL:?export ALERT_EMAIL=\"you@example.com\" first}"
  local topic
  topic=$(aws sns create-topic --name bkk-alerts --query TopicArn --output text)
  if aws sns list-subscriptions-by-topic --topic-arn "$topic" \
       --query "Subscriptions[?Endpoint=='$ALERT_EMAIL'].SubscriptionArn" --output text | grep -q .; then
    echo "already subscribed: $ALERT_EMAIL"
  else
    aws sns subscribe --topic-arn "$topic" --protocol email --notification-endpoint "$ALERT_EMAIL" >/dev/null
    echo "CONFIRM THE EMAIL: AWS just sent a confirmation link to $ALERT_EMAIL. Alerts only arrive after you click it."
  fi
  deploy_fn bkk-healthcheck healthcheck.py 60 256 \
    "BUCKET=$BUCKET,SITE_BUCKET=$SITE_BUCKET,TOPIC_ARN=$topic,COLLECT_WINDOW_MIN=20,EXPECTED_PER_WINDOW=4,DASHBOARD_MAX_AGE_H=30"
  schedule bkk-healthcheck-hourly bkk-healthcheck "rate(1 hour)"
  aws lambda invoke --function-name bkk-healthcheck /tmp/hc.json >/dev/null && cat /tmp/hc.json && echo
  echo "Test an email any time:  aws sns publish --topic-arn $topic --subject test --message hello"
}

stage_weather() {  # redeploy only the weather Lambda (no API key needed) and run it once
  deploy_fn bkk-ingest-weather ingest_weather.py 30 128 "BUCKET=$BUCKET"
  aws lambda invoke --function-name bkk-ingest-weather build/out.json >/dev/null && cat build/out.json && echo
}

stage_merge() {  # fill collection gaps from teammates' shared snapshots (they run scripts/share_bucket.sh)
  : "${PARTNER_ACCOUNTS:?export PARTNER_ACCOUNTS=\"111122223333 444455556666\" first (the account ids of your teammates)}"
  local buckets=""
  for a in ${PARTNER_ACCOUNTS//,/ }; do buckets="${buckets:+$buckets+}bkk-delay-radar-$a"; done
  deploy_fn bkk-merge merge_partners.py 300 256 "BUCKET=$BUCKET,PARTNER_BUCKETS=$buckets"
  schedule bkk-merge-hourly bkk-merge "rate(1 hour)"
  aws lambda invoke --function-name bkk-merge --cli-read-timeout 0 build/out.json >/dev/null && cat build/out.json && echo
}

stage_glue() {
  aws s3 cp glue/delay_job.py "s3://$BUCKET/glue/delay_job.py"
  aws glue delete-job --job-name bkk-delay-job >/dev/null 2>&1 || true
  aws glue create-job --name bkk-delay-job --role "$ROLE_ARN" --glue-version 4.0 \
    --worker-type G.1X --number-of-workers 2 \
    --command "Name=glueetl,ScriptLocation=s3://$BUCKET/glue/delay_job.py,PythonVersion=3" \
    --default-arguments "{\"--additional-python-modules\":\"gtfs-realtime-bindings\",\"--BUCKET\":\"$BUCKET\",\"--DT\":\"2026-11-03\"}" >/dev/null
  aws athena start-query-execution --query-string "CREATE DATABASE IF NOT EXISTS bkk" \
    --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" >/dev/null
  deploy_fn bkk-start-glue start_glue.py 30 128 "JOB=bkk-delay-job"
  schedule bkk-glue-daily bkk-start-glue "cron(0 20 * * ? *)"
  echo "Glue job ready. Manual test run:  aws glue start-job-run --job-name bkk-delay-job --arguments '{\"--DT\":\"YYYY-MM-DD\"}'"
}

stage_publish() {
  aws s3api head-bucket --bucket "$SITE_BUCKET" 2>/dev/null || aws s3api create-bucket --bucket "$SITE_BUCKET" >/dev/null
  aws s3api put-public-access-block --bucket "$SITE_BUCKET" --public-access-block-configuration \
    BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false \
    || echo "WARN: cannot enable public access in this lab - open the page via a presigned URL instead"
  cat > build/site_policy.json <<POL
{"Version":"2012-10-17","Statement":[{"Sid":"PublicRead","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::$SITE_BUCKET/*"}]}
POL
  aws s3api put-bucket-policy --bucket "$SITE_BUCKET" --policy file://build/site_policy.json \
    || echo "WARN: bucket policy rejected"
  aws s3 website "s3://$SITE_BUCKET/" --index-document index.html
  aws s3 cp site/index.html "s3://$SITE_BUCKET/index.html" --content-type text/html
  deploy_fn bkk-publish publish_dashboard.py 600 512 "BUCKET=$BUCKET,SITE_BUCKET=$SITE_BUCKET,DB=bkk"
  schedule bkk-publish-daily bkk-publish "cron(0 21 * * ? *)"
  echo "Dashboard: http://$SITE_BUCKET.s3-website-$AWS_DEFAULT_REGION.amazonaws.com"
}

# Two optional extras for the dashboard:
#   districts  stop -> district table and district map from OpenStreetMap (one-off; rerun after big timetable changes)
#   tomtom     live road traffic switch on the delay map. The key goes only into the site bucket (tomtom.json),
#              the page loads the tiles straight from TomTom, and no TomTom data is stored anywhere.
#              Usage: TOMTOM_KEY="..." bash deploy.sh tomtom      (TOMTOM_KEY="" removes the switch)
stage_districts() {
  python3 scripts/build_district_reference.py "$BUCKET"
  bash scripts/athena.sh sql/05_create_stop_district.sql
  echo "District table ready. Refresh the dashboard: aws lambda invoke --function-name bkk-publish --cli-read-timeout 0 /tmp/out.json"
}

stage_tomtom() {
  if [ -z "${TOMTOM_KEY:-}" ]; then
    aws s3 rm "s3://$SITE_BUCKET/tomtom.json" >/dev/null 2>&1 || true
    echo "TOMTOM_KEY is empty: the live traffic switch is removed from the dashboard."
    return
  fi
  printf '{"key":"%s"}' "$TOMTOM_KEY" | aws s3 cp - "s3://$SITE_BUCKET/tomtom.json" \
    --content-type application/json --cache-control no-cache
  echo "Live traffic switch enabled. Note: the key is readable by anyone who opens the dashboard,"
  echo "so use a free key without billing details and delete it after the course."
}

case "${1:-}" in
  ingest)  stage_ingest ;;
  static)  stage_static ;;
  weather) stage_weather ;;
  merge)   stage_merge ;;
  alerts)  stage_alerts ;;
  glue)    stage_glue ;;
  publish) stage_publish ;;
  districts) stage_districts ;;
  tomtom)  stage_tomtom ;;
  *) echo "usage: $0 ingest|static|weather|merge|glue|publish|alerts|districts|tomtom"; exit 1 ;;
esac
