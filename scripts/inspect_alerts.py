"""What is in the BKK Alerts feed? Shows causes, effects and which alerts look like
roadworks or events, from the latest snapshot in S3.

Usage (CloudShell): python3 scripts/inspect_alerts.py bkk-delay-radar-<fiokazonosito>
Needs: pip install gtfs-realtime-bindings   (already installed if you ran inspect_rt.py)
"""
import collections
import datetime as dt
import sys

import boto3
from google.transit import gtfs_realtime_pb2 as pb

EVENT_WORDS = ["rendezvény", "mérkőzés", "meccs", "koncert", "fesztivál", "felvonulás", "futóverseny",
               "maraton", "puskás aréna", "mvm dome", "sportaréna", "demonstráció", "tüntetés", "vásár"]
WORKS_WORDS = ["felújítás", "munkálat", "munkavégzés", "építés", "vágányzár", "karbantartás",
               "csőtörés", "útlezárás", "burkolat", "közműépítés", "pályafelújítás"]


def text(ts):
    """Hungarian translation if there is one, otherwise the first one."""
    for t in ts.translation:
        if t.language in ("hu", ""):
            return t.text
    return ts.translation[0].text if ts.translation else ""


def when(p):
    f = lambda s: dt.datetime.fromtimestamp(s, dt.timezone.utc).strftime("%m-%d %H:%M") if s else "…"
    return f"{f(p.start)} → {f(p.end)}"


bucket = sys.argv[1]
s3 = boto3.client("s3")
keys = [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="raw/alerts/")
        for o in page.get("Contents", [])]
if not keys:
    sys.exit("No alerts in S3 yet.")
key = max(keys)
feed = pb.FeedMessage()
feed.ParseFromString(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
alerts = [e.alert for e in feed.entity if e.HasField("alert")]
print(f"file: {key}\nalerts: {len(alerts)}\n")

cause = collections.Counter(pb.Alert.Cause.Name(a.cause) for a in alerts)
effect = collections.Counter(pb.Alert.Effect.Name(a.effect) for a in alerts)
print("=== causes ===");  [print(f"  {k:22} {v}") for k, v in cause.most_common()]
print("=== effects ==="); [print(f"  {k:22} {v}") for k, v in effect.most_common()]

ev = wk = with_route = 0
print("\n=== first 25 alerts ===")
for i, a in enumerate(alerts):
    head, desc = text(a.header_text), text(a.description_text)
    low = (head + " " + desc).lower()
    is_ev, is_wk = any(w in low for w in EVENT_WORDS), any(w in low for w in WORKS_WORDS)
    routes = {x.route_id for x in a.informed_entity if x.route_id}
    ev += is_ev
    wk += is_wk
    with_route += bool(routes)
    if i < 25:
        tag = ("EVENT " if is_ev else "") + ("WORKS " if is_wk else "")
        period = when(a.active_period[0]) if a.active_period else "no period"
        print(f"- [{pb.Alert.Cause.Name(a.cause)} / {pb.Alert.Effect.Name(a.effect)}] {tag}"
              f"{period} · {len(routes)} routes\n    {head[:150]}")
print(f"\nlooks like an event: {ev} · looks like roadworks: {wk} · has route ids: {with_route} of {len(alerts)}")
