#!/usr/bin/env bash
# Runs one SQL file in Athena (database bkk) and prints the result.
# Usage: bash scripts/athena.sh sql/02_hotspots.sql      (the word BUCKET in the file is replaced)
set -euo pipefail
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=${BUCKET:-bkk-delay-radar-$ACCOUNT}
SQL=$(sed "s/BUCKET/$BUCKET/g" "$1")
QID=$(aws athena start-query-execution --query-string "$SQL" \
  --query-execution-context Database=bkk \
  --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" \
  --query QueryExecutionId --output text)
while true; do
  S=$(aws athena get-query-execution --query-execution-id "$QID" --query QueryExecution.Status.State --output text)
  case "$S" in SUCCEEDED|FAILED|CANCELLED) break ;; esac
  sleep 2
done
if [ "$S" != "SUCCEEDED" ]; then
  aws athena get-query-execution --query-execution-id "$QID" --query QueryExecution.Status.StateChangeReason --output text
  exit 1
fi
STATS=$(aws athena get-query-execution --query-execution-id "$QID" \
  --query 'QueryExecution.Statistics.[EngineExecutionTimeInMillis,DataScannedInBytes]' --output text)
aws athena get-query-results --query-execution-id "$QID" --output text \
  --query 'ResultSet.Rows[*].Data[*].VarCharValue' 2>/dev/null || true
echo "-- SUCCEEDED (ms, bytes scanned): $STATS"
