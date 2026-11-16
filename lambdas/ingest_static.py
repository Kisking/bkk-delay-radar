"""Daily download of the public BKK static GTFS zip (~62 MB).
Stores the zip AND streams the four files Glue needs (stop_times, trips, routes, stops) unzipped
into S3, so Spark can read them directly (stop_times.txt alone is ~460 MB)."""
import datetime as dt
import os
import shutil
import urllib.request
import zipfile

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
URL = "https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip"
WANTED = ["stop_times.txt", "trips.txt", "routes.txt", "stops.txt"]


def handler(event, context):
    now = dt.datetime.now(dt.timezone.utc)
    day = f"dt={now:%Y-%m-%d}"
    tmp = "/tmp/budapest_gtfs.zip"
    req = urllib.request.Request(URL, headers={"User-Agent": "corvinus-de-project"})
    with urllib.request.urlopen(req, timeout=60) as resp, open(tmp, "wb") as f:
        shutil.copyfileobj(resp, f)
    out = {"zip_bytes": os.path.getsize(tmp)}
    s3.upload_file(tmp, BUCKET, f"raw/gtfs_static/{day}/budapest_gtfs.zip")
    with zipfile.ZipFile(tmp) as z:
        for name in WANTED:
            with z.open(name) as fh:  # streamed, never fully on disk or in memory
                s3.upload_fileobj(fh, BUCKET, f"raw/gtfs_static_txt/{day}/{name}")
            out[name] = z.getinfo(name).file_size
    os.remove(tmp)
    print(out)
    return out
