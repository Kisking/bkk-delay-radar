"""Reference data: which BKK stops lie on a bus lane, according to OpenStreetMap.

Why: buses on their own lane are less affected by traffic, so 'stop on a bus lane' is a useful
variable when explaining delay. OSM is free and open (ODbL licence: credit
"© OpenStreetMap contributors").

How:
  1. reads the latest stops.txt that bkk-ingest-static already put into S3,
  2. downloads every road with a bus lane in the Budapest area from the Overpass API,
  3. for each stop computes the distance to the nearest bus-lane segment,
  4. writes a small CSV to s3://<bucket>/raw/reference/dt=<today>/stops_buslane.csv

Limits: this is a proximity check. A stop within MAX_DIST_M of a bus lane is flagged, even if
the lane runs in the other direction or on a parallel road. Good enough for comparing groups of
stops, not for judging one single stop.

Usage (CloudShell):
    python3 scripts/build_buslane_reference.py bkk-delay-radar-<fiokazonosito>            # build and upload
    python3 scripts/build_buslane_reference.py bkk-delay-radar-<fiokazonosito> --dry-run  # build, print, no upload
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

OVERPASS = "https://overpass-api.de/api/interpreter"
BBOX = "47.34,18.92,47.62,19.35"  # south, west, north, east: Budapest and its edges
MAX_DIST_M = 25.0
CELL_DEG = 0.002                  # spatial index cell, about 150-220 m
QUERY = f"""[out:json][timeout:180];
(
  way["busway"~"lane|opposite_lane"]({BBOX});
  way["busway:left"~"lane|opposite_lane"]({BBOX});
  way["busway:right"~"lane|opposite_lane"]({BBOX});
  way["busway:both"~"lane"]({BBOX});
  way["lanes:bus"]({BBOX});
  way["lanes:psv"]({BBOX});
  way["bus:lanes"~"designated"]({BBOX});
  way["psv:lanes"~"designated"]({BBOX});
  way["highway"="busway"]({BBOX});
);
out geom;"""


def fetch_overpass():
    body = urllib.parse.urlencode({"data": QUERY}).encode()
    req = urllib.request.Request(OVERPASS, data=body, headers={"User-Agent": "corvinus-de-project"})
    with urllib.request.urlopen(req, timeout=240) as resp:
        return json.loads(resp.read())


def segments_from_osm(osm):
    """List of ((lat1, lon1), (lat2, lon2), way_id) for every bus-lane segment."""
    segs = []
    for el in osm.get("elements", []):
        pts = [(p["lat"], p["lon"]) for p in el.get("geometry", [])]
        for a, b in zip(pts, pts[1:]):
            segs.append((a, b, el.get("id")))
    return segs


def to_xy(lat, lon, lat0):
    """Local metres (equirectangular), accurate enough inside one city."""
    return lon * 111320.0 * math.cos(math.radians(lat0)), lat * 110540.0


def point_segment_m(p, a, b, lat0):
    px, py = to_xy(*p, lat0)
    ax, ay = to_xy(*a, lat0)
    bx, by = to_xy(*b, lat0)
    dx, dy = bx - ax, by - ay
    if dx == dy == 0:
        return math.hypot(px - ax, py - ay)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (ax + t * dx), py - (ay + t * dy))


def build_index(segs):
    """Grid index: each segment is listed in every cell its bounding box touches."""
    idx = {}
    for i, (a, b, _) in enumerate(segs):
        for cx in range(int(min(a[1], b[1]) // CELL_DEG), int(max(a[1], b[1]) // CELL_DEG) + 1):
            for cy in range(int(min(a[0], b[0]) // CELL_DEG), int(max(a[0], b[0]) // CELL_DEG) + 1):
                idx.setdefault((cx, cy), []).append(i)
    return idx


def nearest(stop, segs, idx, lat0):
    cx, cy = int(stop[1] // CELL_DEG), int(stop[0] // CELL_DEG)
    best = None
    for gx in (cx - 1, cx, cx + 1):
        for gy in (cy - 1, cy, cy + 1):
            for i in idx.get((gx, gy), []):
                a, b, _ = segs[i]
                d = point_segment_m(stop, a, b, lat0)
                if best is None or d < best:
                    best = d
    return best  # None: no bus lane within roughly one grid cell


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


def classify(stops, segs):
    idx = build_index(segs)
    lat0 = 47.5
    out = []
    for r in stops:
        try:
            p = (float(r["stop_lat"]), float(r["stop_lon"]))
        except (KeyError, ValueError):
            continue
        d = nearest(p, segs, idx, lat0)
        out.append({"stop_id": r["stop_id"], "stop_name": r.get("stop_name", ""),
                    "buslane_dist_m": "" if d is None else round(d, 1),
                    "on_bus_lane": d is not None and d <= MAX_DIST_M})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bucket")
    ap.add_argument("--dry-run", action="store_true", help="do not upload, just print a summary")
    ap.add_argument("--osm-file", help="use a saved Overpass JSON instead of downloading (for testing)")
    a = ap.parse_args()

    import boto3
    s3 = boto3.client("s3")
    stops = read_stops(s3, a.bucket)
    osm = json.load(open(a.osm_file)) if a.osm_file else fetch_overpass()
    segs = segments_from_osm(osm)
    print(f"bus-lane ways from OSM: {len(osm.get('elements', []))}, segments: {len(segs)}")
    rows = classify(stops, segs)
    flagged = sum(r["on_bus_lane"] for r in rows)
    print(f"stops on a bus lane (within {MAX_DIST_M:.0f} m): {flagged} of {len(rows)} "
          f"({100 * flagged / max(1, len(rows)):.1f} %)")
    for r in [r for r in rows if r["on_bus_lane"]][:5]:
        print(f"  e.g. {r['stop_name']} ({r['stop_id']}): {r['buslane_dist_m']} m")

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["stop_id", "stop_name", "buslane_dist_m", "on_bus_lane"])
    w.writeheader()
    w.writerows(rows)
    if a.dry_run:
        print("dry run: nothing uploaded")
        return
    key = f"raw/reference/dt={dt.date.today():%Y-%m-%d}/stops_buslane.csv"
    s3.put_object(Bucket=a.bucket, Key=key, Body=buf.getvalue().encode("utf-8"),
                  ContentType="text/csv",
                  Metadata={"source": "OpenStreetMap via Overpass API", "licence": "ODbL"})
    print(f"uploaded s3://{a.bucket}/{key}")


if __name__ == "__main__":
    main()
