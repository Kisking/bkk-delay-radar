"""Hourly health check: is the pipeline actually producing data?

Looks at the last COLLECT_WINDOW_MIN minutes of raw snapshots and the age of the dashboard file,
and sends one email through SNS when something is wrong. It also sends one "recovered" email when
things are fine again, and stays quiet otherwise, so the inbox does not fill up.

The lab being asleep is normal in this project, so a gap only counts as a problem if the pipeline
was running shortly before (we compare against the previous check, stored in S3)."""
import datetime as dt
import json
import os

import boto3

s3 = boto3.client("s3")
sns = boto3.client("sns")
BUCKET = os.environ["BUCKET"]
SITE = os.environ["SITE_BUCKET"]
TOPIC = os.environ["TOPIC_ARN"]
WINDOW_MIN = int(os.environ.get("COLLECT_WINDOW_MIN", "20"))
EXPECTED = int(os.environ.get("EXPECTED_PER_WINDOW", "4"))   # one snapshot per 5 minutes
DASHBOARD_MAX_AGE_H = int(os.environ.get("DASHBOARD_MAX_AGE_H", "30"))
STATE_KEY = "ops/healthcheck_state.json"


def recent_snapshots(now):
    """Count distinct 5-min snapshots taken in the last WINDOW_MIN minutes. The time comes from the
    file name (YYYYMMDDTHHMMSS.pb), not from LastModified: snapshots copied in from teammates
    (bkk-merge) are old but freshly written, and must not hide a stopped collector."""
    seen = set()
    for day in {now.strftime("%Y-%m-%d"), (now - dt.timedelta(hours=2)).strftime("%Y-%m-%d")}:
        for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=BUCKET, Prefix=f"raw/tripupdates/dt={day}/"):
            for o in page.get("Contents", []):
                stamp = o["Key"].rsplit("/", 1)[-1][:15]          # YYYYMMDDTHHMMSS
                try:
                    taken = dt.datetime.strptime(stamp, "%Y%m%dT%H%M%S").replace(tzinfo=dt.timezone.utc)
                except ValueError:
                    continue
                if 0 <= (now - taken).total_seconds() <= WINDOW_MIN * 60:
                    seen.add(stamp[:13])                          # YYYYMMDDTHHMM
    return len(seen)


def dashboard_age_h(now):
    try:
        o = s3.head_object(Bucket=SITE, Key="data.json")
        return (now - o["LastModified"]).total_seconds() / 3600
    except Exception:
        return None


def load_state():
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
    except Exception:
        return {"status": "unknown", "collecting": False}


def save_state(state):
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(state))


def handler(event, context):
    now = dt.datetime.now(dt.timezone.utc)
    prev = load_state()
    snaps = recent_snapshots(now)
    age = dashboard_age_h(now)
    collecting = snaps >= max(1, EXPECTED - 1)

    problems = []
    # An asleep lab is expected in this project, so we only raise it the moment collection drops out.
    # While it stays down the status is "down": no new email, but not "ok" either, so the recovery
    # email is only sent once collection really comes back.
    if not collecting and prev.get("collecting"):
        problems.append(f"Collection stopped: only {snaps} of about {EXPECTED} snapshots "
                        f"in the last {WINDOW_MIN} minutes. The student lab is probably asleep.")
    if age is None:
        problems.append("The dashboard file (data.json) is missing.")
    elif age > DASHBOARD_MAX_AGE_H:
        problems.append(f"The dashboard has not been refreshed for {age:.1f} hours "
                        f"(limit {DASHBOARD_MAX_AGE_H}).")

    if problems:
        status = "problem"
    elif not collecting:
        status = "down"          # still not collecting, already reported
    else:
        status = "ok"
    sent = None
    if status == "problem" and prev.get("status") not in ("problem", "down"):
        sent = "problem"
        sns.publish(TopicArn=TOPIC, Subject="BKK Delay Radar: something needs attention",
                    Message="\n".join(["The health check found a problem:", ""] + [f"- {p}" for p in problems] +
                                      ["", f"Checked at {now:%Y-%m-%d %H:%M} UTC.",
                                       "If the lab is asleep, press Start Lab; collection resumes on its own."]))
    elif status == "ok" and prev.get("status") in ("problem", "down"):
        sent = "recovered"
        sns.publish(TopicArn=TOPIC, Subject="BKK Delay Radar: back to normal",
                    Message=f"The pipeline is collecting again ({snaps} snapshots in the last "
                            f"{WINDOW_MIN} minutes) and the dashboard is {age:.1f} hours old.\n"
                            f"Checked at {now:%Y-%m-%d %H:%M} UTC.")

    save_state({"status": status, "collecting": collecting, "checked_utc": now.isoformat(timespec="seconds")})
    out = {"status": status, "snapshots_in_window": snaps, "dashboard_age_h": None if age is None else round(age, 1),
           "email_sent": sent, "problems": problems}
    print(json.dumps(out))
    return out
