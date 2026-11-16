"""Fills this account's collection gaps with snapshots from teammates' labs.

Each Learner Lab sleeps at different times. Teammates share their raw snapshots read-only
(scripts/share_bucket.sh), and this function copies ONLY the 5-minute slots that are missing here,
so nothing is stored twice and the Glue job does not get slower.

Runs hourly; looks back LOOKBACK_DAYS days (UTC), skips the last SETTLE_MIN minutes so it never
races this account's own collector. File names stay the same (YYYYMMDDTHHMMSS.pb), so every other
part of the pipeline (Glue, KPIs, backfill) works unchanged. Copies carry 'copied-from' metadata.
"""
import datetime as dt
import json
import os
import re

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
PARTNERS = [b for b in os.environ.get("PARTNER_BUCKETS", "").split("+") if b]   # '+' separated
KINDS = os.environ.get("KINDS", "tripupdates+alerts+vehiclepositions").split("+")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "2"))
SETTLE_MIN = 15
NAME = re.compile(r"(\d{8}T\d{6})\.pb$")


def slot(stamp):
    """'20261105T120057' -> index of its 5-minute slot."""
    t = dt.datetime.strptime(stamp, "%Y%m%dT%H%M%S").replace(tzinfo=dt.timezone.utc)
    return int(t.timestamp()) // 300


def keys(bucket, prefix):
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        out += [o["Key"] for o in page.get("Contents", [])]
    return out


def handler(event, context):
    now = dt.datetime.now(dt.timezone.utc)
    days = [(now - dt.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(LOOKBACK_DAYS)]
    newest_allowed = int((now - dt.timedelta(minutes=SETTLE_MIN)).timestamp()) // 300
    copied, by_partner, errors = 0, {}, set()
    for kind in KINDS:
        for day in days:
            prefix = f"raw/{kind}/dt={day}/"
            have = {slot(m.group(1)) for k in keys(BUCKET, prefix) if (m := NAME.search(k))}
            for pb in PARTNERS:
                try:
                    theirs = keys(pb, prefix)
                except Exception as e:  # not shared yet, lab never ran, or no permission
                    errors.add(f"{pb}: {type(e).__name__}")
                    continue
                for k in sorted(theirs):
                    m = NAME.search(k)
                    if not m:
                        continue
                    sl = slot(m.group(1))
                    if sl in have or sl > newest_allowed:
                        continue
                    s3.copy_object(Bucket=BUCKET, Key=k, CopySource={"Bucket": pb, "Key": k},
                                   MetadataDirective="REPLACE", Metadata={"copied-from": pb},
                                   ContentType="application/octet-stream")
                    have.add(sl)
                    copied += 1
                    by_partner[pb] = by_partner.get(pb, 0) + 1
    out = {"partners": len(PARTNERS), "copied": copied, "by_partner": by_partner, "errors": sorted(errors)}
    print(json.dumps(out))
    return out
