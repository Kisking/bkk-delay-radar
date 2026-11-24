"""Whose data is in my bucket? Per day: hours of 5-minute snapshots, split into my own and the ones bkk-merge
copied from each teammate (copied files carry 'copied-from' metadata). Also checks that bkk-merge can read
the teammates' buckets and shows its last runs.

Usage (CloudShell):  python3 scripts/check_team_data.py [days]      (default 7, today included)
"""
import collections
import concurrent.futures as cf
import datetime as dt
import subprocess
import sys

import boto3

NAMES = {"063258860606": "Kinga", "767837692825": "Zoli", "324025139266": "Barbi"}
s3, lam = boto3.client("s3"), boto3.client("lambda")
me = boto3.client("sts").get_caller_identity()["Account"]
B = f"bkk-delay-radar-{me}"
DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 7
who = lambda bucket: NAMES.get(bucket.rsplit("-", 1)[-1], bucket)

env = lam.get_function_configuration(FunctionName="bkk-merge").get("Environment", {}).get("Variables", {})
partners = [p for p in env.get("PARTNER_BUCKETS", "").split("+") if p]
print(f"my bucket: {B} ({who(B)})")
print(f"bkk-merge partners: {', '.join(who(p) for p in partners) or 'NONE: run PARTNER_ACCOUNTS=... bash deploy.sh merge'}"
      f" · looks back {env.get('LOOKBACK_DAYS', '2')} days")
for pb in partners:
    try:
        n = s3.list_objects_v2(Bucket=pb, Prefix="raw/tripupdates/", MaxKeys=1).get("KeyCount", 0)
        print(f"  read access to {who(pb):<6} OK" + ("" if n else " (but no snapshots there yet)"))
    except Exception as e:
        code = getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
        print(f"  read access to {who(pb):<6} FAILED: {code} -> {who(pb)} must run scripts/share_bucket.sh with your account id")

print(f"\nTrip-update snapshots in my bucket (5 min each), last {DAYS} days, UTC:")
today = dt.datetime.now(dt.timezone.utc).date()
total = collections.Counter()
for i in range(DAYS - 1, -1, -1):
    day = (today - dt.timedelta(days=i)).isoformat()
    keys = [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=B, Prefix=f"raw/tripupdates/dt={day}/")
            for o in page.get("Contents", [])]
    src = lambda k: who(s3.head_object(Bucket=B, Key=k).get("Metadata", {}).get("copied-from", B))
    with cf.ThreadPoolExecutor(24) as ex:
        counts = collections.Counter(ex.map(src, keys))
    total.update(counts)
    parts = " · ".join(f"{n} {c * 5 / 60:.1f} h" for n, c in sorted(counts.items(), key=lambda x: -x[1]))
    print(f"  {day}  {len(keys) * 5 / 60:5.1f} h of 24" + (f"   ({parts})" if parts else "   (nothing)") + ("  ← today, so far" if i == 0 else ""))
print("  total: " + " · ".join(f"{n} {c * 5 / 60:.1f} h" for n, c in total.most_common()))

print("\nLast bkk-merge runs:")
try:
    out = subprocess.run(["aws", "logs", "tail", "/aws/lambda/bkk-merge", "--since", "24h"], capture_output=True, text=True, timeout=60).stdout
    runs = [l[l.index("{"):] for l in out.splitlines() if '"partners"' in l]
    print("\n".join("  " + r for r in runs[-3:]) or "  no run in the last 24 hours (it runs hourly while your lab is awake)")
except Exception as e:
    print("  could not read the logs:", e)
