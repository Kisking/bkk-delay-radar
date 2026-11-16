"""District reference from OpenStreetMap (ODbL: credit "© OpenStreetMap contributors").

Places every BKK stop in one of Budapest's 23 districts, for the district reliability index.

1. raw/reference/districts/stops_district.csv   stop_id, district_num, district   (Athena table bkk.stop_district)
2. districts.geojson in the site bucket          simplified boundaries for the dashboard map

Stops outside Budapest (Budaörs, Gyál, Vecsés …) get no row: the index covers the 23 districts only.

Usage (CloudShell, from ~/kit):
    python3 scripts/build_district_reference.py bkk-delay-radar-<fiokazonosito> --dry-run
    python3 scripts/build_district_reference.py bkk-delay-radar-<fiokazonosito>
    bash scripts/athena.sh sql/05_create_stop_district.sql

Downloads are cached in ~/.cache/bkk_osm like the other OSM reference scripts (rm -rf it to refresh).
"""
import argparse
import json
import math
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_osm_reference import overpass, read_stops, to_csv   # same servers, cache and stop list

BBOX = "47.34,18.92,47.62,19.35"
DISTRICT_Q = (f'[out:json][timeout:300];'
              f'relation["boundary"="administrative"]["admin_level"="9"]["name"~"kerület"]({BBOX});out geom;')
ROMAN = {"I": 1, "V": 5, "X": 10}
# common names; districts without a widely used one are shown by number only
NAMES = {1: "Várkerület", 3: "Óbuda-Békásmegyer", 4: "Újpest", 5: "Belváros-Lipótváros", 6: "Terézváros",
         7: "Erzsébetváros", 8: "Józsefváros", 9: "Ferencváros", 10: "Kőbánya", 11: "Újbuda", 12: "Hegyvidék",
         14: "Zugló", 17: "Rákosmente", 18: "Pestszentlőrinc-Pestszentimre", 19: "Kispest", 20: "Pesterzsébet",
         21: "Csepel", 22: "Budafok-Tétény", 23: "Soroksár"}


def roman(s):
    total, prev = 0, 0
    for ch in reversed(s):
        v = ROMAN[ch]
        total, prev = (total - v, prev) if v < prev else (total + v, v)
    return total


def district_number(tags):
    for key in ("name", "official_name", "name:hu", "alt_name"):
        m = re.search(r"\b([IVX]{1,5})\.\s*kerület", tags.get(key, ""))
        if m:
            n = roman(m.group(1))
            if 1 <= n <= 23:
                return n
    return None


def label(n):
    r = ["", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII", "XIII", "XIV", "XV",
         "XVI", "XVII", "XVIII", "XIX", "XX", "XXI", "XXII", "XXIII"][n]
    return f"{r}. {NAMES[n]}" if n in NAMES else f"{r}. kerület"


def join_rings(ways):
    """Joins way pieces (lists of (lat, lon)) end to end into closed rings."""
    pieces = [list(w) for w in ways if len(w) >= 2]
    rings = []
    while pieces:
        ring = pieces.pop(0)
        changed = True
        while ring[0] != ring[-1] and changed:
            changed = False
            for i, p in enumerate(pieces):
                if p[0] == ring[-1]:
                    ring += p[1:]
                elif p[-1] == ring[-1]:
                    ring += p[::-1][1:]
                elif p[-1] == ring[0]:
                    ring = p + ring[1:]
                elif p[0] == ring[0]:
                    ring = p[::-1] + ring[1:]
                else:
                    continue
                pieces.pop(i)
                changed = True
                break
        if ring[0] == ring[-1] and len(ring) >= 4:
            rings.append(ring)
    return rings


def polygons_from(osm):
    """{district_num: {"outer": [rings], "inner": [rings]}} from an Overpass 'out geom' answer."""
    out = {}
    for el in osm.get("elements", []):
        if el.get("type") != "relation":
            continue
        n = district_number(el.get("tags", {}))
        if n is None:
            continue
        parts = {"outer": [], "inner": []}
        for m in el.get("members", []):
            if m.get("type") == "way" and m.get("geometry"):
                parts["inner" if m.get("role") == "inner" else "outer"].append(
                    [(round(p["lat"], 7), round(p["lon"], 7)) for p in m["geometry"]])
        outer, inner = join_rings(parts["outer"]), join_rings(parts["inner"])
        if outer:
            out[n] = {"outer": outer, "inner": inner}
    return out


def inside(lat, lon, ring):
    """Ray casting: is the point inside the closed ring?"""
    hit = False
    for (y1, x1), (y2, x2) in zip(ring, ring[1:]):
        if (y1 > lat) != (y2 > lat) and lon < x1 + (lat - y1) * (x2 - x1) / (y2 - y1):
            hit = not hit
    return hit


def bbox(rings):
    lats = [p[0] for r in rings for p in r]
    lons = [p[1] for r in rings for p in r]
    return min(lats), min(lons), max(lats), max(lons)


def locate(lat, lon, polys, boxes):
    for n, poly in polys.items():
        b = boxes[n]
        if not (b[0] <= lat <= b[2] and b[1] <= lon <= b[3]):
            continue
        if any(inside(lat, lon, r) for r in poly["outer"]) and not any(inside(lat, lon, r) for r in poly["inner"]):
            return n
    return None


def xy(p):
    return p[1] * 111320.0 * math.cos(math.radians(47.5)), p[0] * 110540.0


def _dp(line, tol_m):
    """Douglas-Peucker on an open line, in metres."""
    if len(line) < 3:
        return line
    pts = [xy(p) for p in line]
    keep = [False] * len(line)
    keep[0] = keep[-1] = True
    stack = [(0, len(line) - 1)]
    while stack:
        a, b = stack.pop()
        (ax, ay), (bx, by) = pts[a], pts[b]
        L = math.hypot(bx - ax, by - ay) or 1e-9
        best, idx = 0.0, None
        for i in range(a + 1, b):
            px, py = pts[i]
            d = abs((bx - ax) * (ay - py) - (ax - px) * (by - ay)) / L
            if d > best:
                best, idx = d, i
        if idx is not None and best > tol_m:
            keep[idx] = True
            stack += [(a, idx), (idx, b)]
    return [p for p, k in zip(line, keep) if k]


def simplify(ring, tol_m=30.0):
    """Keeps the shape, drops most points (the map needs no 1 m detail). A closed ring is cut at the
    point farthest from its start, because Douglas-Peucker needs two different end points."""
    if len(ring) < 5:
        return ring
    s0 = xy(ring[0])
    k = max(range(len(ring)), key=lambda i: math.hypot(xy(ring[i])[0] - s0[0], xy(ring[i])[1] - s0[1]))
    out = _dp(ring[:k + 1], tol_m) + _dp(ring[k:], tol_m)[1:]
    return out if len(out) >= 4 else ring


def geojson(polys):
    feats = []
    for n in sorted(polys):
        rings = [[[round(p[1], 5), round(p[0], 5)] for p in simplify(r)] for r in polys[n]["outer"]]
        feats.append({"type": "Feature", "properties": {"district_num": n, "district": label(n)},
                      "geometry": {"type": "MultiPolygon", "coordinates": [[r] for r in rings]}})
    return {"type": "FeatureCollection", "features": feats,
            "attribution": "District boundaries © OpenStreetMap contributors (ODbL)"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bucket")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--district-file", help="a saved Overpass answer instead of downloading")
    a = ap.parse_args()
    import boto3
    s3 = boto3.client("s3")
    stops = read_stops(s3, a.bucket)
    osm = json.load(open(a.district_file)) if a.district_file else overpass(DISTRICT_Q, "districts")
    polys = polygons_from(osm)
    missing = sorted(set(range(1, 24)) - set(polys))
    print(f"districts with a closed boundary: {len(polys)} of 23" + (f" · missing: {missing}" if missing else ""))
    if len(polys) < 20:
        sys.exit("Too few district boundaries came back; try again later (rm -rf ~/.cache/bkk_osm/districts.json).")
    boxes = {n: bbox(p["outer"]) for n, p in polys.items()}
    rows, outside = [], 0
    for r in stops:
        try:
            lat, lon = float(r["stop_lat"]), float(r["stop_lon"])
        except (KeyError, ValueError):
            continue
        n = locate(lat, lon, polys, boxes)
        if n is None:
            outside += 1
            continue
        rows.append({"stop_id": r["stop_id"], "district_num": n, "district": label(n)})
    per = {}
    for r in rows:
        per[r["district_num"]] = per.get(r["district_num"], 0) + 1
    print(f"stops placed in a district: {len(rows)} · outside Budapest: {outside}")
    print("  " + " · ".join(f"{label(n).split('.')[0]}: {per.get(n, 0)}" for n in range(1, 24)))
    for name, want in (("Deák Ferenc tér", 5), ("Móricz Zsigmond körtér", 11), ("Újpest-központ", 4), ("Szent Imre tér", 21)):
        hit = next((r for r in stops if name.lower() in r.get("stop_name", "").lower()), None)
        if hit:
            got = locate(float(hit["stop_lat"]), float(hit["stop_lon"]), polys, boxes)
            print(f"  check: {hit['stop_name']} -> {label(got) if got else 'outside'}"
                  + ("" if got == want else f"   (expected {label(want)}: please check)"))
    gj = geojson(polys)
    size = len(json.dumps(gj, separators=(",", ":")))
    print(f"districts.geojson: {size / 1024:.0f} kB after simplification")
    if a.dry_run:
        print("dry run: nothing uploaded")
        return
    key = "raw/reference/districts/stops_district.csv"
    s3.put_object(Bucket=a.bucket, Key=key, Body=to_csv(rows), ContentType="text/csv",
                  Metadata={"source": "OpenStreetMap via Overpass API", "licence": "ODbL"})
    print(f"uploaded s3://{a.bucket}/{key}")
    site = f"{a.bucket}-site"
    s3.put_object(Bucket=site, Key="districts.geojson", Body=json.dumps(gj, separators=(",", ":"), ensure_ascii=False),
                  ContentType="application/geo+json", CacheControl="no-cache")
    print(f"uploaded s3://{site}/districts.geojson")
    print("next: bash scripts/athena.sh sql/05_create_stop_district.sql")


if __name__ == "__main__":
    main()
