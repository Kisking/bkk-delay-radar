"""Which optional fields does BKK fill in? Decides how we compute some new variables.

  block_id            (trips.txt)  -> delay carried over from the vehicle's previous trip
  agency_id + names   (zip)        -> operator (BKV or a subcontractor)
  shape_dist_traveled (stop_times) -> route length in metres
  VehiclePositions: vehicle.id, trip, stop sequence, status, speed -> speed per segment and
                    the previous-trip link if block_id is missing

Usage (CloudShell): python3 scripts/check_feed_fields.py bkk-delay-radar-<fiokazonosito>
Needs: pip install gtfs-realtime-bindings
"""
import collections
import csv
import io
import sys
import zipfile

import boto3


def latest(s3, bucket, prefix, suffix):
    keys = [o["Key"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
            for o in p.get("Contents", []) if o["Key"].endswith(suffix)]
    return max(keys) if keys else None


def fill(rows, col):
    if not rows or col not in rows[0]:
        return "column missing"
    n = sum(1 for r in rows if (r.get(col) or "").strip())
    return f"{n}/{len(rows)} filled ({100 * n / len(rows):.0f} %)"


def main():
    bucket = sys.argv[1]
    s3 = boto3.client("s3")
    zkey = latest(s3, bucket, "raw/gtfs_static/", ".zip")
    if not zkey:
        sys.exit("no timetable zip in S3")
    z = zipfile.ZipFile(io.BytesIO(s3.get_object(Bucket=bucket, Key=zkey)["Body"].read()))
    print(f"timetable: {zkey}\nfiles in zip: {', '.join(sorted(z.namelist()))}\n")

    def table(name, limit=None):
        if name not in z.namelist():
            return []
        with z.open(name) as fh:
            rd = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig"))
            return [r for _, r in zip(range(limit or 10 ** 9), rd)]

    trips, routes, agency = table("trips.txt"), table("routes.txt"), table("agency.txt")
    st = table("stop_times.txt", limit=200000)
    print("trips.txt   columns:", ", ".join(trips[0].keys()) if trips else "-")
    print("  block_id:", fill(trips, "block_id"), "· shape_id:", fill(trips, "shape_id"))
    print("routes.txt  columns:", ", ".join(routes[0].keys()) if routes else "-")
    print("  agency_id:", fill(routes, "agency_id"))
    print("agency.txt:", "; ".join(f"{a.get('agency_id')}={a.get('agency_name')}" for a in agency) or "-")
    if routes and "agency_id" in routes[0]:
        print("  routes per agency:", dict(collections.Counter(r["agency_id"] for r in routes)))
    print("stop_times (first 200k rows) shape_dist_traveled:", fill(st, "shape_dist_traveled"))
    if trips and "block_id" in trips[0]:
        blocks = collections.Counter(r["block_id"] for r in trips if r.get("block_id"))
        multi = sum(1 for c in blocks.values() if c > 1)
        print(f"  blocks with more than one trip: {multi} of {len(blocks)}")

    from google.transit import gtfs_realtime_pb2 as pb
    vkey = latest(s3, bucket, "raw/vehiclepositions/", ".pb")
    if not vkey:
        print("\nno VehiclePositions yet")
        return
    f = pb.FeedMessage()
    f.ParseFromString(s3.get_object(Bucket=bucket, Key=vkey)["Body"].read())
    vs = [e.vehicle for e in f.entity if e.HasField("vehicle")]
    c = collections.Counter()
    for v in vs:
        c["vehicle.id"] += bool(v.vehicle.id)
        c["vehicle.label"] += bool(v.vehicle.label)
        c["trip_id"] += bool(v.trip.trip_id)
        c["current_stop_sequence"] += v.HasField("current_stop_sequence")
        c["stop_id"] += bool(v.stop_id)
        c["current_status"] += v.HasField("current_status")
        c["position.speed"] += v.position.HasField("speed")
        c["position.bearing"] += v.position.HasField("bearing")
        c["timestamp"] += v.HasField("timestamp")
    print(f"\nVehiclePositions: {vkey}\nvehicles: {len(vs)}")
    for k in ["vehicle.id", "vehicle.label", "trip_id", "current_stop_sequence", "stop_id",
              "current_status", "position.speed", "position.bearing", "timestamp"]:
        print(f"  {k:22} {c[k]}/{len(vs)}")
    ex = next((v for v in vs if v.trip.trip_id), None)
    if ex:
        print(f"  example: vehicle {ex.vehicle.id} label {ex.vehicle.label!r} trip {ex.trip.trip_id} "
              f"route {ex.trip.route_id} lat {ex.position.latitude:.5f} lon {ex.position.longitude:.5f}")


if __name__ == "__main__":
    main()
