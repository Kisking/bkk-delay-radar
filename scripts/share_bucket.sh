#!/usr/bin/env bash
# Gives teammates' AWS accounts READ-ONLY access to your raw snapshots (raw/ only, nothing else),
# so their bkk-merge function can fill their collection gaps from your data.
#
#   bash scripts/share_bucket.sh 111122223333 444455556666     # the teammates' account ids
#
# Your own account id (send it to your teammates): aws sts get-caller-identity --query Account --output text
set -euo pipefail
[ $# -ge 1 ] || { echo "usage: bash scripts/share_bucket.sh TEAMMATE_ACCOUNT_ID [MORE_IDS ...]"; exit 1; }
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=bkk-delay-radar-$ACCOUNT
PRINCIPALS=$(printf '"arn:aws:iam::%s:root",' "$@"); PRINCIPALS="[${PRINCIPALS%,}]"
mkdir -p build
cat > build/share_policy.json <<POL
{"Version": "2012-10-17", "Statement": [
  {"Sid": "TeamListRaw", "Effect": "Allow", "Principal": {"AWS": $PRINCIPALS},
   "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::$BUCKET",
   "Condition": {"StringLike": {"s3:prefix": ["raw/*"]}}},
  {"Sid": "TeamReadRaw", "Effect": "Allow", "Principal": {"AWS": $PRINCIPALS},
   "Action": "s3:GetObject", "Resource": "arn:aws:s3:::$BUCKET/raw/*"}]}
POL
aws s3api put-bucket-policy --bucket "$BUCKET" --policy file://build/share_policy.json
echo "Shared read-only: s3://$BUCKET/raw/  ->  $*"
echo "Your account id (send it to your teammates): $ACCOUNT"
