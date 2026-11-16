"""Big events (matches, concerts) as reference data for the delay analysis.

There is no machine-readable event calendar for Budapest venues, so the team keeps a small CSV
by hand (reference/events.csv) and this script checks it and uploads it.

Columns:
  date               YYYY-MM-DD
  start_local        HH:MM  (Budapest time, when doors open or the event starts)
  end_local          HH:MM  (roughly when the crowd leaves; if it is after midnight, write e.g. 00:30)
  venue              e.g. Puskás Aréna, MVM Dome, Papp László Budapest Sportaréna
  event              short name, e.g. "Hungary – Georgia"
  type               football | concert | other
  expected_attendance optional number
  nearest_stop       a BKK stop name near the venue, as written in the timetable,
                     e.g. "Puskás Ferenc Stadion" (used to find the coordinates)
  source_url         where you found the event (venue website, MLSZ, news)

The script looks up nearest_stop in the latest stops.txt, adds the coordinates, and uploads to
s3://<bucket>/raw/reference/events/events.csv (plus a dated copy).

Usage (CloudShell):
    python3 scripts/upload_events.py bkk-delay-radar-<fiokazonosito> reference/events.csv --dry-run
    python3 scripts/upload_events.py bkk-delay-radar-<fiokazonosito> reference/events.csv
"""
import argparse
import csv
import datetime as dt
import io
import re
import sys

REQUIRED = ["date", "start_local", "end_local", "venue", "event", "type", "nearest_stop", "source_url"]
TIME = re.compile(r"^(\d{1,2}):(\d{2})$")


def valid_time(t):
    m = TIME.match(t)
    return bool(m) and 0 <= int(m.group(1)) <= 23 and 0 <= int(m.group(2)) <= 59


def latest_stops(s3, bucket):
    days = []
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix="raw/gtfs_static_txt/", Delimiter="/"):
        days += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
    if not days:
        sys.exit("No timetable in S3 yet. Run: bash deploy.sh static")
    text = s3.get_object(Bucket=bucket, Key=sorted(days)[-1] + "stops.txt")["Body"].read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def check(rows, stops):
    errors, out = [], []
    for i, r in enumerate(rows, start=2):  # line 1 is the header
        missing = [c for c in REQUIRED if not (r.get(c) or "").strip()]
        if missing:
            errors.append(f"line {i}: missing {', '.join(missing)}")
            continue
        try:
            dt.date.fromisoformat(r["date"].strip())
        except ValueError:
            errors.append(f"line {i}: date must be YYYY-MM-DD, got {r['date']!r}")
            continue
        if not (valid_time(r["start_local"].strip()) and valid_time(r["end_local"].strip())):
            errors.append(f"line {i}: times must be HH:MM between 00:00 and 23:59 "
                          f"(an end earlier than the start means the next day)")
            continue
        if r["type"].strip() not in ("football", "concert", "other"):
            errors.append(f"line {i}: type must be football, concert or other")
            continue
        hint = r["nearest_stop"].strip().lower()
        hits = [s for s in stops if hint in s.get("stop_name", "").lower()]
        if not hits:
            errors.append(f"line {i}: no stop name contains {r['nearest_stop']!r}")
            continue
        lat = sum(float(s["stop_lat"]) for s in hits) / len(hits)
        lon = sum(float(s["stop_lon"]) for s in hits) / len(hits)
        out.append({**{k: (r.get(k) or "").strip() for k in REQUIRED + ["expected_attendance"]},
                    "lat": round(lat, 6), "lon": round(lon, 6), "matched_stops": len(hits)})
    return out, errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bucket")
    ap.add_argument("csv_path")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    import boto3
    s3 = boto3.client("s3")
    rows = list(csv.DictReader(open(a.csv_path, encoding="utf-8-sig")))
    if not rows:
        sys.exit("The CSV has no events yet. Add one row per event, then run again.")
    good, errors = check(rows, latest_stops(s3, a.bucket))
    for e in errors:
        print("ERROR", e)
    print(f"{len(good)} of {len(rows)} events are valid")
    for g in good:
        print(f"  {g['date']} {g['start_local']}-{g['end_local']}  {g['venue']}: {g['event']}"
              f"  (stop match: {g['matched_stops']})")
    if errors:
        sys.exit("Fix the errors above first; nothing was uploaded.")
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=REQUIRED + ["expected_attendance", "lat", "lon", "matched_stops"])
    w.writeheader()
    w.writerows(good)
    if a.dry_run:
        print("dry run: nothing uploaded")
        return
    body = buf.getvalue().encode("utf-8")
    for key in ("raw/reference/events/events.csv",
                f"raw/reference/events/history/events_{dt.date.today():%Y-%m-%d}.csv"):
        s3.put_object(Bucket=a.bucket, Key=key, Body=body, ContentType="text/csv")
        print(f"uploaded s3://{a.bucket}/{key}")


if __name__ == "__main__":
    main()
