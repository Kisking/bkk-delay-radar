"""Finds days that were collected but never processed, and runs the Glue job for them.

A day needs processing when raw snapshots exist for it but there is no metrics file
(curated/_metrics/dt=<day>.json). Today is skipped, because the day is not over yet.

Usage:
    python3 scripts/backfill.py BUCKET              # show what is missing (dry run)
    python3 scripts/backfill.py BUCKET --run        # run them, oldest first, one at a time
    python3 scripts/backfill.py BUCKET --run --max 3
    python3 scripts/backfill.py BUCKET --upgrade --run   # also re-run days made by an older job version
    python3 scripts/backfill.py BUCKET --only 2026-11-02,2026-11-03 --run   # exactly these days
"""
import argparse
import json
import datetime as dt
import sys
import time

import boto3

JOB = "bkk-delay-job"
CURRENT_VERSION = 8


def day_set(s3, bucket, prefix):
    days = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for p in page.get("CommonPrefixes", []):
            days.add(p["Prefix"].rstrip("/").split("dt=")[-1])
    return days


def processed_days(s3, bucket):
    days = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="curated/_metrics/"):
        for o in page.get("Contents", []):
            name = o["Key"].rsplit("/", 1)[-1]
            if name.startswith("dt=") and name.endswith(".json"):
                days.add(name[3:-5])
    return days


def old_version_days(s3, bucket, days):
    """Processed days whose metrics file was written by an older job version."""
    old = []
    for d in days:
        try:
            m = json.loads(s3.get_object(Bucket=bucket, Key=f"curated/_metrics/dt={d}.json")["Body"].read())
        except Exception:
            continue
        if m.get("job_version", 3) < CURRENT_VERSION:
            old.append(d)
    return old


def start(glue, day, tries=20):
    """Glue allows one run at a time and needs a few seconds to release a finished one."""
    for i in range(tries):
        try:
            return glue.start_job_run(JobName=JOB, Arguments={"--DT": day})["JobRunId"]
        except glue.exceptions.ConcurrentRunsExceededException:
            if i == 0:
                print(f"  {day}: another run is still active, waiting…", flush=True)
            time.sleep(30)
    raise RuntimeError("Glue kept reporting a concurrent run; try again in a few minutes.")


def run_day(glue, day):
    rid = start(glue, day)
    print(f"  {day}: started {rid[:16]}…", end="", flush=True)
    while True:
        time.sleep(15)
        r = glue.get_job_run(JobName=JOB, RunId=rid)["JobRun"]
        if r["JobRunState"] not in ("RUNNING", "STARTING", "WAITING"):
            break
    secs = r.get("ExecutionTime", 0)
    print(f" {r['JobRunState']} in {secs}s")
    if r["JobRunState"] != "SUCCEEDED":
        print(f"    {r.get('ErrorMessage', '')}")
    return r["JobRunState"] == "SUCCEEDED", secs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bucket")
    ap.add_argument("--run", action="store_true", help="actually run the Glue job (default: dry run)")
    ap.add_argument("--max", type=int, default=10, help="at most this many days in one go")
    ap.add_argument("--only", help="comma-separated days to (re)process, e.g. 2026-11-02,2026-11-03")
    ap.add_argument("--upgrade", action="store_true", help=f"also re-run days processed by a job older than v{CURRENT_VERSION}")
    a = ap.parse_args()

    s3, glue = boto3.client("s3"), boto3.client("glue")
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    collected = day_set(s3, a.bucket, "raw/tripupdates/")
    done = processed_days(s3, a.bucket)
    missing = sorted(d for d in collected - done if d < today)
    if a.upgrade:
        old = old_version_days(s3, a.bucket, sorted(done))
        print(f"Days made by an older job version: {len(old)}")
        missing = sorted(set(missing) | {d for d in old if d < today})

    if a.only:
        missing = sorted(d.strip() for d in a.only.split(",") if d.strip() in collected and d.strip() < today)
        print(f"Only these days were asked for: {', '.join(missing) or 'none that can be processed'}")
    print(f"Days with snapshots : {len(collected)} ({min(collected, default='-')} … {max(collected, default='-')})")
    print(f"Days already processed: {len(done)}")
    print(f"Today ({today}) is skipped: the day is not over yet.")
    if not missing:
        print("\nNothing to backfill.")
        return
    print(f"\nMissing ({len(missing)}): {', '.join(missing)}")
    if not a.run:
        print("\nDry run. Add --run to process them.")
        return
    ok = total = 0
    for day in missing[:a.max]:
        good, secs = run_day(glue, day)
        ok += good
        total += secs
        time.sleep(15)  # let Glue release the finished run before the next one
    print(f"\n{ok} of {min(len(missing), a.max)} succeeded, {total}s of Glue time used.")


if __name__ == "__main__":
    main()
