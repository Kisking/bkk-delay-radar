"""Two more reference tables from OpenStreetMap (ODbL: credit "© OpenStreetMap contributors").

1. stops_bank.csv   which bank of the Danube each stop is on (buda / pest), from the river centre line.
                    The Glue job uses it to flag segments that cross the Danube (= a bridge, a bottleneck).
2. stops_malls.csv  distance to the nearest shopping centre and its name; near_mall = within NEAR_MALL_M.

Usage (CloudShell):
    python3 scripts/build_osm_reference.py bkk-delay-radar-<fiokazonosito> --dry-run
    python3 scripts/build_osm_reference.py bkk-delay-radar-<fiokazonosito>

If the free Overpass servers are busy, the script tries four of them, three rounds each, and caches
every successful download in ~/.cache/bkk_osm, so a second run continues where the first stopped.
To force a fresh download: rm -rf ~/.cache/bkk_osm
"""
import argparse
import csv
import datetime as dt
import io
import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request

# Public Overpass servers are free and often busy (502, 504, timeouts), so we try several,
# a few times each, and keep every successful answer in a local cache file.
OVERPASS = ["https://overpass-api.de/api/interpreter",
            "https://lz4.overpass-api.de/api/interpreter",
            "https://z.overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter"]
CACHE_DIR = os.path.expanduser("~/.cache/bkk_osm")
BBOX = "47.34,18.92,47.62,19.35"
NEAR_MALL_M = 400.0
RIVER_Q = f'[out:json][timeout:300];way["waterway"="river"]["name"="Duna"]({BBOX});out geom;'
MALL_Q = f'[out:json][timeout:300];nwr["shop"="mall"]({BBOX});out center tags;'


def overpass(q, name, rounds=3):
    cache = os.path.join(CACHE_DIR, f"{name}.json")
    if os.path.exists(cache):
        print(f"{name}: using cached download ({cache})")
        return json.load(open(cache))
    body = urllib.parse.urlencode({"data": q}).encode()
    last = None
    for attempt in range(1, rounds + 1):
        for url in OVERPASS:
            try:
                print(f"{name}: trying {url.split('/')[2]} (round {attempt}) …", flush=True)
                req = urllib.request.Request(url, data=body, headers={"User-Agent": "corvinus-de-project"})
                with urllib.request.urlopen(req, timeout=300) as r:
                    data = json.loads(r.read())
                os.makedirs(CACHE_DIR, exist_ok=True)
                json.dump(data, open(cache, "w"))
                print(f"{name}: ok, {len(data.get('elements', []))} elements")
                return data
            except Exception as e:  # busy server, timeout, bad gateway: try the next one
                last = e
                print(f"{name}:   failed: {type(e).__name__}: {str(e)[:80]}")
        time.sleep(20 * attempt)
    sys.exit(f"{name}: all Overpass servers failed ({last}). Try again in 15-30 minutes; "
             f"anything already downloaded is cached in {CACHE_DIR}.")


def xy(lat, lon):
    return lon * 111320.0 * math.cos(math.radians(47.5)), lat * 110540.0


def river_segments(osm):
    segs = []
    for el in osm.get("elements", []):
        pts = [(p["lat"], p["lon"]) for p in el.get("geometry", [])]
        segs += list(zip(pts, pts[1:]))
    return segs


def bank(stop, segs):
    """OSM rivers point downstream. Right of the flow = right bank = Buda, left = Pest."""
    px, py = xy(*stop)
    best, side = None, None
    for a, b in segs:
        ax, ay = xy(*a)
        bx, by = xy(*b)
        dx, dy = bx - ax, by - ay
        L = dx * dx + dy * dy
        t = 0.0 if L == 0 else max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / L))
        d = math.hypot(px - (ax + t * dx), py - (ay + t * dy))
        if best is None or d < best:
            best, side = d, ("buda" if dx * (py - ay) - dy * (px - ax) < 0 else "pest")
    return side, best


def malls_from(osm):
    out = []
    for el in osm.get("elements", []):
        c = el if "lat" in el else el.get("center")
        if c and "lat" in c:
            out.append(((c["lat"], c["lon"]), el.get("tags", {}).get("name", "")))
    return out


def dist(a, b):
    ax, ay = xy(*a)
    bx, by = xy(*b)
    return math.hypot(ax - bx, ay - by)


def read_stops(s3, bucket):
    days = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="raw/gtfs_static_txt/", Delimiter="/"):
        days += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
    if not days:
        sys.exit("No timetable in S3 yet. Run: bash deploy.sh static")
    key = sorted(days)[-1] + "stops.txt"
    rows = list(csv.DictReader(io.StringIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8-sig"))))
    print(f"stops: {len(rows)} from {key}")
    return rows


def build(stops, river_osm, mall_osm):
    segs, malls = river_segments(river_osm), malls_from(mall_osm)
    bank_rows, mall_rows = [], []
    for r in stops:
        try:
            s = (float(r["stop_lat"]), float(r["stop_lon"]))
        except (KeyError, ValueError):
            continue
        side, d = bank(s, segs) if segs else (None, None)
        bank_rows.append({"stop_id": r["stop_id"], "stop_name": r.get("stop_name", ""), "bank": side or "",
                          "river_dist_m": "" if d is None else round(d)})
        near = min(((dist(s, p), n) for p, n in malls), default=(None, ""))
        mall_rows.append({"stop_id": r["stop_id"], "stop_name": r.get("stop_name", ""),
                          "mall_dist_m": "" if near[0] is None else round(near[0]), "mall_name": near[1],
                          "near_mall": near[0] is not None and near[0] <= NEAR_MALL_M})
    return segs, malls, bank_rows, mall_rows


def to_csv(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bucket")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--river-file")
    ap.add_argument("--mall-file")
    a = ap.parse_args()
    import boto3
    s3 = boto3.client("s3")
    stops = read_stops(s3, a.bucket)
    river = json.load(open(a.river_file)) if a.river_file else overpass(RIVER_Q, "danube")
    mallj = json.load(open(a.mall_file)) if a.mall_file else overpass(MALL_Q, "malls")
    segs, malls, banks, mrows = build(stops, river, mallj)
    b = sum(r["bank"] == "buda" for r in banks)
    p = sum(r["bank"] == "pest" for r in banks)
    print(f"Danube centre line: {len(segs)} segments · stops on Buda side: {b} · on Pest side: {p}")
    for name in ("Széll Kálmán tér", "Deák Ferenc tér", "Móricz Zsigmond körtér", "Örs vezér tere"):
        hit = next((r for r in banks if name.lower() in r["stop_name"].lower()), None)
        if hit:
            print(f"  check: {hit['stop_name']} -> {hit['bank']}")
    near = [r for r in mrows if r["near_mall"]]
    print(f"shopping centres: {len(malls)} · stops within {NEAR_MALL_M:.0f} m of one: {len(near)}")
    for r in near[:5]:
        print(f"  e.g. {r['stop_name']}: {r['mall_name']} ({r['mall_dist_m']} m)")
    if a.dry_run:
        print("dry run: nothing uploaded")
        return
    day = f"dt={dt.date.today():%Y-%m-%d}"
    for fname, rows in (("stops_bank.csv", banks), ("stops_malls.csv", mrows)):
        key = f"raw/reference/{day}/{fname}"
        s3.put_object(Bucket=a.bucket, Key=key, Body=to_csv(rows), ContentType="text/csv",
                      Metadata={"source": "OpenStreetMap via Overpass API", "licence": "ODbL"})
        print(f"uploaded s3://{a.bucket}/{key}")


if __name__ == "__main__":
    main()
