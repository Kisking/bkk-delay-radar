"""Fetch BKK GTFS-realtime snapshots and store them unchanged in S3 (raw zone).
Pure standard library: no Lambda layer needed. The API key comes from Secrets Manager."""
import datetime as dt
import json
import os
import urllib.request

import boto3

s3 = boto3.client("s3")
sm = boto3.client("secretsmanager")
BUCKET = os.environ["BUCKET"]
SECRET_ID = os.environ.get("SECRET_ID", "bkk/api-key")
KINDS = os.environ.get("KINDS", "TripUpdates").split("+")  # e.g. TripUpdates+VehiclePositions
RT_BASE = "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/"
_key = None


def api_key():
    global _key
    if _key is None:
        _key = sm.get_secret_value(SecretId=SECRET_ID)["SecretString"].strip()
    return _key


def handler(event, context):
    now = dt.datetime.now(dt.timezone.utc)
    stored = []
    for kind in KINDS:
        req = urllib.request.Request(f"{RT_BASE}{kind}.pb?key={api_key()}",
                                     headers={"User-Agent": "corvinus-de-project"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
        key = f"raw/{kind.lower()}/dt={now:%Y-%m-%d}/hour={now:%H}/{now:%Y%m%dT%H%M%S}.pb"
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        stored.append({"kind": kind, "bytes": len(body), "key": key})
    print(json.dumps(stored))  # never print the key
    return stored
