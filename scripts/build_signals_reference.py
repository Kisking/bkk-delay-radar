"""Reference data: distance from every BKK stop to the nearest signalised junction (traffic lights).

Source: Budapest Közút public map service, layer "Jelzőlámpás csomópontok" (signalised junctions).
  https://kapu.budapestkozut.hu/arcgis/rest/services/kozutfigyelo/kozutfigyelo/MapServer/0
The layer is public and returns JSON. Its copyright is Budapest Közút Zrt.; no open licence is
stated, so we use it for analysis only and do not republish the raw points.

Output: s3://<bucket>/raw/reference/dt=<today>/stops_signals.csv
  stop_id, stop_name, signal_dist_m, near_signal (within NEAR_M), signals_within_300m

Usage (CloudShell):
    python3 scripts/build_signals_reference.py bkk-delay-radar-<fiokazonosito> --dry-run
    python3 scripts/build_signals_reference.py bkk-delay-radar-<fiokazonosito>
"""
import argparse
import csv
import datetime as dt
import io
import json
import math
import sys
import urllib.parse
import urllib.request

LAYER = "https://kapu.budapestkozut.hu/arcgis/rest/services/kozutfigyelo/kozutfigyelo/MapServer/0/query"
NEAR_M = 50.0
AREA_M = 300.0
CELL_DEG = 0.004


def fetch_signals():
    """All points of the layer in WGS84 (lat, lon). Pages if the server caps the result."""
    pts, offset = [], 0
    while True:
        q = urllib.parse.urlencode({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                                    "outSR": "4326", "f": "json",
                                    "resultOffset": offset, "resultRecordCount": 2000})
        req = urllib.request.Request(f"{LAYER}?{q}", headers={"User-Agent": "corvinus-de-project"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
        if "error" in data:
            sys.exit(f"map service error: {data['error']}")
        feats = data.get("features", [])
        pts += [p for p in (point_of(f.get("geometry") or {}) for f in feats) if p]
        if not data.get("exceededTransferLimit") or not feats:
            break
        offset += len(feats)
    return pts


def point_of(g):
    """Point geometry -> (lat, lon). Lines or polygons -> centre of their vertices."""
    if "x" in g and "y" in g:
        return g["y"], g["x"]
    coords = [c for part in g.get("paths", []) + g.get("rings", []) for c in part]
    if coords:
        return sum(c[1] for c in coords) / len(coords), sum(c[0] for c in coords) / len(coords)
    return None


def dist_m(a, b):
    lat0 = math.radians((a[0] + b[0]) / 2)
    return math.hypot((b[1] - a[1]) * 111320.0 * math.cos(lat0), (b[0] - a[0]) * 110540.0)


def classify(stops, signals):
    idx = {}
    for p in signals:
        idx.setdefault((int(p[1] // CELL_DEG), int(p[0] // CELL_DEG)), []).append(p)
    out = []
    for r in stops:
        try:
            s = (float(r["stop_lat"]), float(r["stop_lon"]))
        except (KeyError, ValueError):
            continue
        cx, cy = int(s[1] // CELL_DEG), int(s[0] // CELL_DEG)
        cand = [p for gx in (cx - 1, cx, cx + 1) for gy in (cy - 1, cy, cy + 1) for p in idx.get((gx, gy), [])]
        ds = [dist_m(s, p) for p in cand]
        best = min(ds) if ds else None
        out.append({"stop_id": r["stop_id"], "stop_name": r.get("stop_name", ""),
                    "signal_dist_m": "" if best is None else round(best, 1),
                    "near_signal": best is not None and best <= NEAR_M,
                    "signals_within_300m": sum(d <= AREA_M for d in ds)})
    return out


def read_stops(s3, bucket):
    days = []
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix="raw/gtfs_static_txt/", Delimiter="/"):
        days += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
    if not days:
        sys.exit("No timetable in S3 yet (raw/gtfs_static_txt/). Run: bash deploy.sh static")
    key = sorted(days)[-1] + "stops.txt"
    text = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(text)))
    print(f"stops: {len(rows)} from {key}")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bucket")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--signals-file", help="saved map-service JSON instead of downloading (testing)")
    a = ap.parse_args()
    import boto3
    s3 = boto3.client("s3")
    stops = read_stops(s3, a.bucket)
    if a.signals_file:
        signals = [p for p in (point_of(f.get("geometry") or {}) for f in json.load(open(a.signals_file))["features"]) if p]
    else:
        signals = fetch_signals()
    print(f"signalised junctions from Budapest Közút: {len(signals)}")
    rows = classify(stops, signals)
    near = sum(r["near_signal"] for r in rows)
    print(f"stops within {NEAR_M:.0f} m of traffic lights: {near} of {len(rows)} ({100 * near / max(1, len(rows)):.1f} %)")
    for r in [r for r in rows if r["near_signal"]][:5]:
        print(f"  e.g. {r['stop_name']} ({r['stop_id']}): {r['signal_dist_m']} m, {r['signals_within_300m']} junctions within 300 m")
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["stop_id", "stop_name", "signal_dist_m", "near_signal", "signals_within_300m"])
    w.writeheader()
    w.writerows(rows)
    if a.dry_run:
        print("dry run: nothing uploaded")
        return
    key = f"raw/reference/dt={dt.date.today():%Y-%m-%d}/stops_signals.csv"
    s3.put_object(Bucket=a.bucket, Key=key, Body=buf.getvalue().encode("utf-8"), ContentType="text/csv",
                  Metadata={"source": "Budapest Kozut Zrt. KAPU map service", "use": "analysis only"})
    print(f"uploaded s3://{a.bucket}/{key}")


if __name__ == "__main__":
    main()
