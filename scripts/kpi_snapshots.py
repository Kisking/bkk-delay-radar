"""KPI 'Reliability' for a Learner Lab that sleeps after 4 hours.

Method: collect the snapshot times from S3, count unique 5-minute buckets (manual extra runs
do not inflate the result), split the timeline into 'active segments' wherever the gap between
two snapshots is longer than 15 minutes (= lab asleep), and compare the buckets stored with the
buckets expected inside the active segments.

Usage: python scripts/kpi_snapshots.py BUCKET
Output: per-day coverage hours + reliability inside active segments + list of sleep gaps."""
import datetime as dt
import sys

BUCKET_S = 300
GAP_S = 900


def analyze(times):
    """times: sorted list of datetimes (UTC). Returns (segments, reliability, stored, expected)."""
    buckets = sorted({int(t.timestamp()) // BUCKET_S for t in times})
    if not buckets:
        return [], None, 0, 0
    segments, start, prev = [], buckets[0], buckets[0]
    for b in buckets[1:]:
        if (b - prev) * BUCKET_S > GAP_S:
            segments.append((start, prev))
            start = b
        prev = b
    segments.append((start, prev))
    expected = sum(e - s + 1 for s, e in segments)
    return segments, len(buckets) / expected, len(buckets), expected


def list_times(bucket):
    import boto3
    s3 = boto3.client("s3")
    times = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="raw/tripupdates/"):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1].replace(".pb", "")  # 20261029T222141
            times.append(dt.datetime.strptime(name, "%Y%m%dT%H%M%S").replace(tzinfo=dt.timezone.utc))
    return sorted(times)


def report(times):
    segments, rel, stored, expected = analyze(times)
    if not segments:
        print("no snapshots found")
        return
    per_day = {}
    for s, e in segments:
        t = s
        while t <= e:
            day = dt.datetime.fromtimestamp(t * BUCKET_S, dt.timezone.utc).strftime("%Y-%m-%d")
            per_day[day] = per_day.get(day, 0) + 1
            t += 1
    print("Active coverage per UTC day (hours with the pipeline running):")
    for day in sorted(per_day):
        print(f"  {day}: {per_day[day] * 5 / 60:5.1f} h")
    print(f"\nReliability inside active segments: {stored}/{expected} unique 5-min snapshots"
          f" = {100 * rel:.1f} %  ({'OK' if rel >= 0.95 else 'BELOW 95 %'})")
    print("\nSleep gaps (lab off / pipeline not running):")
    for (_, e1), (s2, _) in zip(segments, segments[1:]):
        a = dt.datetime.fromtimestamp((e1 + 1) * BUCKET_S, dt.timezone.utc)
        b = dt.datetime.fromtimestamp(s2 * BUCKET_S, dt.timezone.utc)
        print(f"  {a:%Y-%m-%d %H:%M} -> {b:%Y-%m-%d %H:%M} UTC  ({(b - a).total_seconds() / 3600:.1f} h)")


if __name__ == "__main__":
    report(list_times(sys.argv[1]))
