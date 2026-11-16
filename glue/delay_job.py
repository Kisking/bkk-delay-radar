"""Glue (PySpark) job v4: one day of BKK TripUpdates -> curated delay table with explanatory variables.

delay_s = predicted arrival time - scheduled arrival time (the feed has predicted times, not delays).
Only real predictions are used: trip SCHEDULED and stop update SCHEDULED. Trains of other operators
in the BKK feed are counted but not written.

New in v4 (every extra source is optional: if it is missing the day still runs and the columns are NULL):
  time      service date, day of week, day type (workday / saturday / sunday_holiday), school break,
            special day, season (advent), dark or light (sunrise/sunset from Open-Meteo)
  place     stop coordinates, distance from Deák Ferenc tér, zone, Buda/Pest bank, Danube crossing,
            bus lane, traffic lights nearby, shopping centre nearby (reference tables in raw/reference/)
  route     trip length, stops in trip, position on the route, segment length, previous stop,
            delay at the first stop, delay gained on the segment, scheduled and actual headway, bunching
  weather   rain, snowfall, snow depth, wind, gusts, visibility, weather code
  alerts    disruption category and duration active on the route/trip at arrival time (BKK Alerts)
  vehicles  measured speed on the segment, vehicle id, delay at the end of the vehicle's previous trip
  quality   prediction lead time (seconds between the last prediction and the predicted arrival)
v5 quality rules: a row is unreliable (quality_flag set, reliable = false) when
  stale_prediction  the last prediction was made more than 10 minutes before the arrival
                    (lab asleep, or the UTC-day cut at midnight; such trips reappear the next day)
  shifted_trip      the whole trip is moved: at least 5 stops, every stop at least 10 minutes late
                    and the delay changes by less than 2 minutes (a vehicle matched to the wrong trip)
Unreliable rows stay in the table but are left out of delay gained, headways and carry-over.

Job parameters: --BUCKET, --DT (YYYY-MM-DD, UTC day of the raw partition).
Job needs:      --additional-python-modules gtfs-realtime-bindings"""
import csv
import datetime as dt
import io
import json
import os
import sys

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["BUCKET", "DT"])
BUCKET, DT = args["BUCKET"], args["DT"]
LOCAL = os.environ.get("DELAY_JOB_LOCAL_ROOT")  # only used by the offline test
ROOT = LOCAL or f"s3://{BUCKET}"
WARNINGS = []

builder = (SparkSession.builder.appName("bkk-delay")
           .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
           .config("spark.sql.session.timeZone", "UTC"))
if LOCAL:
    builder = builder.master("local[2]").config("spark.ui.enabled", "false")
spark = builder.getOrCreate()
sc = spark.sparkContext


# ---------------------------------------------------------------- storage helpers
def day_exists(prefix):
    """True if the prefix has at least one file."""
    if LOCAL:
        p = f"{LOCAL}/{prefix}"
        return os.path.isdir(p) and any(fs for _, _, fs in os.walk(p) if fs)
    r = boto3.client("s3").list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    return r.get("KeyCount", 0) > 0


def subfolders(prefix):
    """Names of the immediate sub-folders of a prefix, e.g. ['dt=2026-11-01', ...]."""
    if LOCAL:
        p = f"{LOCAL}/{prefix}"
        return sorted(d for d in os.listdir(p) if os.path.isdir(f"{p}/{d}")) if os.path.isdir(p) else []
    out = []
    for page in boto3.client("s3").get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        out += [c["Prefix"][len(prefix):].rstrip("/") for c in page.get("CommonPrefixes", [])]
    return sorted(out)


def exists(key):
    if LOCAL:
        return os.path.exists(f"{LOCAL}/{key}")
    try:
        boto3.client("s3").head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def read_small(key):
    if LOCAL:
        return open(f"{LOCAL}/{key}", encoding="utf-8-sig").read()
    return boto3.client("s3").get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8-sig")


def latest_reference(filename):
    """Newest raw/reference/dt=*/<filename> not newer than DT (or the oldest one if all are newer)."""
    days = [d for d in subfolders("raw/reference/") if d.startswith("dt=") and exists(f"raw/reference/{d}/{filename}")]
    if not days:
        return None
    ok = [d for d in days if d <= f"dt={DT}"]
    return f"raw/reference/{ok[-1] if ok else days[0]}/{filename}"  # none older: the earliest is closest


def put_metrics(obj):
    body = json.dumps(obj)
    if LOCAL:
        os.makedirs(f"{LOCAL}/curated/_metrics", exist_ok=True)
        open(f"{LOCAL}/curated/_metrics/dt={DT}.json", "w").write(body)
    else:
        boto3.client("s3").put_object(Bucket=BUCKET, Key=f"curated/_metrics/dt={DT}.json", Body=body)


def read_csv(path, cols, optional=()):
    """CSV with header; strips a BOM; columns listed in `optional` become NULL when missing."""
    df = spark.read.option("header", True).csv(path)
    df = df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])
    for c in optional:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df.select(*cols, *optional)


def secs(col):  # GTFS time 'HH:MM:SS', hours may exceed 24
    p = F.split(F.col(col), ":")
    return p[0].cast("int") * 3600 + p[1].cast("int") * 60 + p[2].cast("int")


# ---------------------------------------------------------------- 1. TripUpdates
def decode(pair):
    from google.transit import gtfs_realtime_pb2
    path, raw = pair
    snap = path.rsplit("/", 1)[-1].replace(".pb", "")
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(bytes(raw))
    rows = []
    for e in feed.entity:
        if not e.HasField("trip_update"):
            continue
        tu = e.trip_update
        trip_rel = int(tu.trip.schedule_relationship)  # 0 SCHEDULED, 1 ADDED, 2 UNSCHEDULED, 3 CANCELED
        for s in tu.stop_time_update:
            rows.append((snap, tu.trip.trip_id, tu.trip.route_id or None, tu.trip.start_date or None,
                         s.stop_id or None, int(s.stop_sequence),
                         int(s.schedule_relationship),  # 0 SCHEDULED, 1 SKIPPED, 2 NO_DATA
                         trip_rel,
                         int(s.arrival.time) if s.arrival.HasField("time") else None,
                         int(s.departure.time) if s.departure.HasField("time") else None))
    return rows


if not day_exists(f"raw/tripupdates/dt={DT}/"):
    raise SystemExit(f"No TripUpdates snapshots for dt={DT}: nothing to process (the lab was probably asleep).")

RT_SCHEMA = ("snapshot_ts string, trip_id string, rt_route_id string, start_date string, "
             "stop_id string, stop_sequence int, stu_rel int, trip_rel int, arr_time long, dep_time long")
rt_all = spark.createDataFrame(
    sc.binaryFiles(f"{ROOT}/raw/tripupdates/dt={DT}/*/*.pb").flatMap(decode), RT_SCHEMA).cache()

rt = rt_all.filter((F.col("trip_rel") == 0) & (F.col("stu_rel") == 0) & F.col("arr_time").isNotNull())
w_last = Window.partitionBy("start_date", "trip_id", "stop_sequence").orderBy(F.col("snapshot_ts").desc())
rt = rt.withColumn("rn", F.row_number().over(w_last)).filter("rn = 1").drop("rn")

# ---------------------------------------------------------------- 2. timetable
days = subfolders("raw/gtfs_static_txt/")
# newest timetable not newer than DT; if there is none, the EARLIEST one (closest in time).
# Taking the newest instead breaks old days, because BKK renumbers trip ids between timetables.
eligible = [d for d in days if d <= f"dt={DT}"] or days[:1]
base = f"{ROOT}/raw/gtfs_static_txt/{eligible[-1]}"

trips = (read_csv(f"{base}/trips.txt", ["trip_id", "route_id", "trip_headsign"], optional=["direction_id"])
         .withColumnRenamed("route_id", "trip_route_id"))
routes = read_csv(f"{base}/routes.txt", ["route_id", "route_short_name", "route_type"])
stops = (read_csv(f"{base}/stops.txt", ["stop_id", "stop_name"], optional=["stop_lat", "stop_lon"])
         .withColumn("stop_lat", F.col("stop_lat").cast("double"))
         .withColumn("stop_lon", F.col("stop_lon").cast("double")))
needed = rt.select("trip_id").distinct()
w_trip = Window.partitionBy("trip_id").orderBy("stop_sequence")
w_trip_all = Window.partitionBy("trip_id")
st = (read_csv(f"{base}/stop_times.txt", ["trip_id", "stop_sequence", "arrival_time", "departure_time"],
               optional=["stop_id", "shape_dist_traveled"])
      .withColumnRenamed("stop_id", "tt_stop_id")
      .join(F.broadcast(needed), "trip_id")
      .withColumn("stop_sequence", F.col("stop_sequence").cast("int"))
      .withColumn("sched_arr_s", secs("arrival_time"))
      .withColumn("sched_dep_s", secs("departure_time"))
      .withColumn("shape_dist", F.col("shape_dist_traveled").cast("double"))
      .withColumn("prev_stop_id", F.lag("tt_stop_id").over(w_trip))
      .withColumn("prev_shape_dist", F.lag("shape_dist").over(w_trip))
      .withColumn("trip_length_raw", F.max("shape_dist").over(w_trip_all))
      .withColumn("stops_in_trip", F.count(F.lit(1)).over(w_trip_all).cast("int"))
      .cache())
# shape_dist_traveled may be in metres or kilometres: decide from the median trip length
med = st.select("trip_id", "trip_length_raw").dropDuplicates(["trip_id"]).approxQuantile("trip_length_raw", [0.5], 0.05)
DIST_FACTOR = 1.0 if (med and med[0] and med[0] > 1000) else 1000.0
stop_times = st.select(
    "trip_id", "stop_sequence", "sched_arr_s", "sched_dep_s", "prev_stop_id", "stops_in_trip",
    (F.col("trip_length_raw") * DIST_FACTOR).alias("trip_length_m"),
    (F.col("shape_dist") / F.col("trip_length_raw")).alias("position_ratio"),
    ((F.col("shape_dist") - F.col("prev_shape_dist")) * DIST_FACTOR).alias("segment_length_m"))

# ---------------------------------------------------------------- 3. reference tables (optional)
loaded_refs = []


def ref(filename, cols, casts):
    key = latest_reference(filename)
    if not key:
        WARNINGS.append(f"reference {filename} not found")
        return None
    try:
        df = read_csv(f"{ROOT}/{key}", ["stop_id"] + cols)
        for c, t in casts.items():
            df = df.withColumn(c, F.lower(F.col(c)) == "true" if t == "bool" else F.col(c).cast(t))
        loaded_refs.append(key)
        return F.broadcast(df.dropDuplicates(["stop_id"]))
    except Exception as e:
        WARNINGS.append(f"reference {filename} unreadable: {str(e)[:120]}")
        return None


buslane = ref("stops_buslane.csv", ["on_bus_lane"], {"on_bus_lane": "bool"})
signals = ref("stops_signals.csv", ["near_signal", "signals_within_300m"], {"near_signal": "bool", "signals_within_300m": "int"})
banks = ref("stops_bank.csv", ["bank"], {})
malls = ref("stops_malls.csv", ["near_mall"], {"near_mall": "bool"})

# calendar: expanded to one row per date for the days around DT (night trips belong to the day before)
cal_rows = {}
if exists("raw/reference/calendar/calendar.csv"):
    for r in csv.DictReader(io.StringIO(read_small("raw/reference/calendar/calendar.csv"))):
        try:
            a, b = dt.date.fromisoformat(r["start_date"]), dt.date.fromisoformat(r["end_date"])
        except (KeyError, ValueError):
            continue
        d = a
        while d <= b:
            e = cal_rows.setdefault(d.strftime("%Y%m%d"), {"holiday": False, "working_saturday": False,
                                                           "school_break": False, "special": None, "season": None})
            cat = r.get("category")
            if cat in ("public_holiday", "rest_day"):
                e["holiday"] = True
            elif cat == "working_saturday":
                e["working_saturday"] = True
            elif cat == "school_break":
                e["school_break"] = True
            elif cat == "special":
                e["special"] = r.get("label")
            elif cat == "season":
                e["season"] = r.get("label")
            d += dt.timedelta(days=1)
else:
    WARNINGS.append("calendar not found")
calendar = spark.createDataFrame(
    [(k, v["holiday"], v["working_saturday"], v["school_break"], v["special"], v["season"]) for k, v in cal_rows.items()]
    or [("00000000", False, False, False, None, None)],
    "start_date string, is_holiday boolean, is_working_saturday boolean, is_school_break boolean, "
    "special_day string, season string")

# city centre = the Deák Ferenc tér stops (fallback: fixed point)
c = stops.filter(F.col("stop_name").startswith("Deák Ferenc tér")).agg(F.avg("stop_lat"), F.avg("stop_lon")).first()
LAT0, LON0 = (c[0], c[1]) if c and c[0] else (47.4977, 19.0544)

# ---------------------------------------------------------------- 4. weather (optional parts)
HOURLY = ["temperature_2m", "precipitation", "rain", "snowfall", "snow_depth",
          "wind_speed_10m", "wind_gusts_10m", "visibility", "weather_code"]
weather = spark.createDataFrame([], "hour_key string, " + ", ".join(f"{h} double" for h in HOURLY))
sun = spark.createDataFrame([], "sun_date string, sunrise_local timestamp, sunset_local timestamp")
has_weather = extended_weather = has_sun = False
if day_exists(f"raw/weather/dt={DT}/"):
    wj = spark.read.json(f"{ROOT}/raw/weather/dt={DT}/*/*.json")
    hourly_fields = wj.schema["hourly"].dataType.names
    present = [h for h in HOURLY if h in hourly_fields]
    weather = (wj.select(F.explode(F.arrays_zip(F.col("hourly.time").alias("time"),
                                                *[F.col(f"hourly.{h}").alias(h) for h in present])).alias("h"))
               .select(F.col("h.time").alias("time"), *[F.col(f"h.{h}").cast("double").alias(h) for h in present])
               .dropDuplicates(["time"])
               .withColumn("hour_key", F.date_format(F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"), "yyyy-MM-dd HH")))
    for h in HOURLY:
        if h not in present:
            weather = weather.withColumn(h, F.lit(None).cast("double"))
    weather = weather.select("hour_key", *HOURLY)
    has_weather, extended_weather = True, "snowfall" in present
    if "daily" in wj.columns and {"time", "sunrise", "sunset"} <= set(wj.schema["daily"].dataType.names):
        sun = (wj.select(F.explode(F.arrays_zip(F.col("daily.time").alias("d"), F.col("daily.sunrise").alias("r"),
                                                F.col("daily.sunset").alias("s"))).alias("x"))
               .select(F.col("x.d").alias("sun_date"),
                       F.to_timestamp("x.r", "yyyy-MM-dd'T'HH:mm").alias("sunrise_local"),
                       F.to_timestamp("x.s", "yyyy-MM-dd'T'HH:mm").alias("sunset_local"))
               .dropDuplicates(["sun_date"]))
        has_sun = True
else:
    WARNINGS.append(f"no weather files for dt={DT}")

# ---------------------------------------------------------------- 5. core join
service_midnight = F.unix_timestamp(F.to_utc_timestamp(F.to_timestamp("start_date", "yyyyMMdd"), "Europe/Budapest"))
snap_ts = F.to_timestamp("snapshot_ts", "yyyyMMdd'T'HHmmss")
arr_local = F.from_utc_timestamp(F.timestamp_seconds("arr_time"), "Europe/Budapest")
dist_km = F.sqrt(F.pow((F.col("stop_lon") - LON0) * 111.32 * F.cos(F.radians(F.lit(LAT0))), 2)
                 + F.pow((F.col("stop_lat") - LAT0) * 110.54, 2))

df = (rt.join(stop_times, ["trip_id", "stop_sequence"], "left")
      .withColumn("sched_arr_epoch", service_midnight + F.col("sched_arr_s"))
      .withColumn("arrival_delay_s", (F.col("arr_time") - F.col("sched_arr_epoch")).cast("int"))
      .withColumn("departure_delay_s", (F.col("dep_time") - (service_midnight + F.col("sched_dep_s"))).cast("int"))
      .withColumn("prediction_lead_s", (F.col("arr_time") - F.unix_timestamp(snap_ts)).cast("int"))
      .withColumn("arrival_local", arr_local)
      .withColumn("hour_local", F.hour(F.from_utc_timestamp(snap_ts, "Europe/Budapest")))
      .withColumn("arrival_hour_local", F.hour("arrival_local"))
      .withColumn("hour_key", F.date_format("arrival_local", "yyyy-MM-dd HH"))
      .join(trips, "trip_id", "left")
      .withColumn("route_id", F.coalesce("rt_route_id", "trip_route_id"))
      .join(routes, "route_id", "left")
      .join(stops, "stop_id", "left")
      .join(F.broadcast(weather), "hour_key", "left")
      .join(F.broadcast(calendar), "start_date", "left"))
if has_sun:
    df = (df.withColumn("sun_date", F.date_format("arrival_local", "yyyy-MM-dd"))
          .join(F.broadcast(sun), "sun_date", "left")
          .withColumn("is_dark", (F.col("arrival_local") < F.col("sunrise_local")) | (F.col("arrival_local") > F.col("sunset_local"))))
else:
    df = df.withColumn("is_dark", F.lit(None).cast("boolean"))

iso_dow = ((F.dayofweek(F.to_date("start_date", "yyyyMMdd")) + 5) % 7) + 1  # Monday = 1 ... Sunday = 7
df = (df.withColumn("service_date", F.date_format(F.to_date("start_date", "yyyyMMdd"), "yyyy-MM-dd"))
      .withColumn("day_of_week", iso_dow.cast("int"))
      .withColumn("day_type", F.when(F.coalesce("is_holiday", F.lit(False)) | (iso_dow == 7), "sunday_holiday")
                  .when(F.coalesce("is_working_saturday", F.lit(False)), "workday")
                  .when(iso_dow == 6, "saturday").otherwise("workday"))
      .withColumn("is_school_break", F.coalesce("is_school_break", F.lit(False)))
      .withColumn("dist_center_km", F.round(dist_km, 2))
      .withColumn("zone", F.when(F.col("dist_center_km").isNull(), None).when(F.col("dist_center_km") < 3, "inner")
                  .when(F.col("dist_center_km") < 8, "middle").otherwise("outer")))

prev_names = stops.select(F.col("stop_id").alias("prev_stop_id"), F.col("stop_name").alias("prev_stop_name"))
df = df.join(F.broadcast(prev_names), "prev_stop_id", "left")
for table, fill in ((buslane, ["on_bus_lane"]), (signals, ["near_signal", "signals_within_300m"]),
                    (malls, ["near_mall"])):
    if table is not None:
        df = df.join(table, "stop_id", "left")
    else:
        for col in fill:
            df = df.withColumn(col, F.lit(None).cast("int" if col == "signals_within_300m" else "boolean"))
if banks is not None:
    df = (df.join(banks, "stop_id", "left")
          .join(banks.select(F.col("stop_id").alias("prev_stop_id"), F.col("bank").alias("prev_bank")), "prev_stop_id", "left")
          .withColumn("crosses_danube", F.when(F.col("bank").isNull() | F.col("prev_bank").isNull(), None)
                      .otherwise(F.col("bank") != F.col("prev_bank"))))
else:
    df = df.withColumn("bank", F.lit(None).cast("string")).withColumn("crosses_danube", F.lit(None).cast("boolean"))

# trains of other operators (not in the BKK timetable and not on a BKK route) are counted, not written
df = df.cache()
is_foreign = F.col("trip_route_id").isNull() & F.col("route_short_name").isNull()
n_all = df.count()
foreign = df.filter(is_foreign).count()
bkk = df.filter(~is_foreign)

# ---------------------------------------------------------------- 6. BKK Alerts (optional; before the quality flags, which use them)
ALERT_PRIORITY = {"works": 1, "event": 2, "accident": 3, "technical": 4, "traffic": 5, "detour": 6,
                  "no service": 7, "reduced service": 8, "other": 9, "stop_change": 99}
# v6: when BKK gives no usable cause, the effect tells us what happened on the line
EFFECT_CATEGORY = {"DETOUR": "detour", "NO_SERVICE": "no service", "REDUCED_SERVICE": "reduced service",
                   "SIGNIFICANT_DELAYS": "traffic"}
EVENT_WORDS = ["rendezvény", "mérkőzés", "meccs", "koncert", "fesztivál", "felvonulás", "futóverseny",
               "maraton", "puskás aréna", "mvm dome", "sportaréna", "demonstráció", "tüntetés"]


def decode_alerts(pair):
    from google.transit import gtfs_realtime_pb2 as pb
    feed = pb.FeedMessage()
    feed.ParseFromString(bytes(pair[1]))

    def text(ts):
        for t in ts.translation:
            if t.language in ("hu", ""):
                return t.text
        return ts.translation[0].text if ts.translation else ""

    rows = []
    for e in feed.entity:
        if not e.HasField("alert"):
            continue
        a = e.alert
        cause = pb.Alert.Cause.Name(a.cause)
        low = (text(a.header_text) + " " + text(a.description_text)).lower()
        if any(w in low for w in ("megállóáthelyezés", "ideiglenes megálló", "új megálló")):
            cat = "stop_change"
        elif cause in ("CONSTRUCTION", "MAINTENANCE"):
            cat = "works"
        elif cause == "DEMONSTRATION" or any(w in low for w in EVENT_WORDS):
            cat = "event"
        elif cause == "ACCIDENT":
            cat = "accident"
        elif cause == "TECHNICAL_PROBLEM":
            cat = "technical"
        elif "forgalmi ok" in low or "torlódás" in low:
            cat = "traffic"
        else:
            cat = EFFECT_CATEGORY.get(pb.Alert.Effect.Name(a.effect), "other")
        periods = [(p.start or 0, p.end or 0) for p in a.active_period] or [(0, 0)]
        for start, end in periods:
            span = (end - start) if start and end else None
            dur = "long" if span is None or span > 30 * 86400 else ("short" if span <= 86400 else "medium")
            for x in a.informed_entity:
                rows.append((e.id, start, end or 4102444800, cat, dur, x.route_id or None, x.trip.trip_id or None))
    return rows


alert_stats = {"alerts_loaded": 0, "alert_matched_rows_pct": None}
alerts = None
if day_exists(f"raw/alerts/dt={DT}/"):
    try:
        alerts = (spark.createDataFrame(sc.binaryFiles(f"{ROOT}/raw/alerts/dt={DT}/*/*.pb").flatMap(decode_alerts),
                                        "alert_id string, a_start long, a_end long, category string, duration string, "
                                        "a_route_id string, a_trip_id string")
                  .distinct().cache())
        alert_stats["alerts_loaded"] = alerts.select("alert_id").distinct().count()
    except Exception as e:
        WARNINGS.append(f"alerts unreadable: {str(e)[:120]}")
        alerts = None

key_cols = ["start_date", "trip_id", "stop_sequence"]
if alerts is not None and alert_stats["alerts_loaded"]:
    prio = F.create_map(*[x for k, v in ALERT_PRIORITY.items() for x in (F.lit(k), F.lit(v))])
    a = F.broadcast(alerts.withColumn("prio", prio[F.col("category")]))
    base_rows = bkk.select(*key_cols, "route_id", "arr_time")
    in_time = (F.col("arr_time") >= F.col("a_start")) & (F.col("arr_time") <= F.col("a_end"))
    by_route = base_rows.join(a.filter(F.col("a_route_id").isNotNull()), (F.col("route_id") == F.col("a_route_id")) & in_time)
    by_trip = base_rows.join(a.filter(F.col("a_trip_id").isNotNull()), (F.col("trip_id") == F.col("a_trip_id")) & in_time)
    hits = by_route.unionByName(by_trip)
    agg = (hits.groupBy(*key_cols)
           .agg(F.min(F.when(F.col("category") != "stop_change", F.struct("prio", "category", "duration"))).alias("top"),
                F.max((F.col("category") == "stop_change").cast("int")).alias("sc")))
    agg = agg.select(*key_cols, F.col("top.category").alias("alert_category"),
                     F.col("top.duration").alias("alert_duration"), (F.col("sc") == 1).alias("alert_stop_change"))
    bkk = bkk.join(agg, key_cols, "left")
else:
    bkk = (bkk.withColumn("alert_category", F.lit(None).cast("string"))
           .withColumn("alert_duration", F.lit(None).cast("string"))
           .withColumn("alert_stop_change", F.lit(None).cast("boolean")))


# ---------------------------------------------------------------- 7. quality flags
STALE_S, SHIFT_MIN_S, SHIFT_SPREAD_S, SHIFT_MIN_STOPS = 600, 600, 120, 5
w_obs = Window.partitionBy("start_date", "trip_id").orderBy("stop_sequence")
w_obs_all = Window.partitionBy("start_date", "trip_id")
# v7: the first stop is left out of the trip statistics (it shows the timetable until the vehicle
# leaves, so a shifted trip looked like it had a 0-minute stop), and the allowed spread grows with
# the shift: 2 minutes, or 10 % of the smallest delay if that is more (a 100-minute shift may wobble)
after_first = F.when(F.col("stop_sequence") > 1, F.col("arrival_delay_s"))
bkk = (bkk.withColumn("trip_n", F.count(after_first).over(w_obs_all))
       .withColumn("trip_min", F.min(after_first).over(w_obs_all))
       .withColumn("trip_max", F.max(after_first).over(w_obs_all))
       .withColumn("shift_like", (F.col("trip_n") >= SHIFT_MIN_STOPS) & (F.col("trip_min") >= SHIFT_MIN_S)
                   & ((F.col("trip_max") - F.col("trip_min")) < F.greatest(F.lit(SHIFT_SPREAD_S), 0.1 * F.col("trip_min"))))
       # v8: if BKK itself reports a traffic jam or an accident on the trip, the big delay is real
       .withColumn("real_disruption", F.max(F.when(F.col("alert_category").isin("traffic", "accident"), 1).otherwise(0))
                   .over(w_obs_all) == 1)
       .withColumn("is_shifted", F.col("shift_like") & ~F.col("real_disruption"))
       .withColumn("is_stale", F.coalesce(F.col("prediction_lead_s") > STALE_S, F.lit(False)))
       .withColumn("quality_flag", F.when(F.col("is_shifted"), "shifted_trip")
                   .when(F.col("is_stale"), "stale_prediction"))
       .withColumn("reliable", F.col("quality_flag").isNull())
       .withColumn("is_constant_zero", (F.col("trip_n") >= SHIFT_MIN_STOPS) & (F.col("trip_min") == 0) & (F.col("trip_max") == 0)))

# ---------------------------------------------------------------- 8. along the trip and at the stop
w_head_s = Window.partitionBy("stop_id", "route_id", "direction_id").orderBy("sched_arr_epoch")
w_head_a = Window.partitionBy("stop_id", "route_id", "direction_id").orderBy("arr_time")
bkk = (bkk.withColumn("prev_obs_seq", F.lag("stop_sequence").over(w_obs))
       .withColumn("prev_obs_delay", F.lag("arrival_delay_s").over(w_obs))
       .withColumn("prev_obs_reliable", F.lag("reliable").over(w_obs))
       .withColumn("delay_increment_s", F.when((F.col("prev_obs_seq") == F.col("stop_sequence") - 1)
                                               & F.col("reliable") & F.col("prev_obs_reliable"),
                                               F.col("arrival_delay_s") - F.col("prev_obs_delay")).cast("int"))
       .withColumn("origin_delay_s", F.max(F.when(F.col("stop_sequence") == 1,
                                                  F.coalesce("departure_delay_s", "arrival_delay_s"))).over(w_obs_all).cast("int"))
       .withColumn("sched_headway_s", (F.col("sched_arr_epoch") - F.lag("sched_arr_epoch").over(w_head_s)).cast("int"))
       .withColumn("actual_headway_s", (F.col("arr_time") - F.lag("arr_time").over(w_head_a)).cast("int"))
       .withColumn("bunched", F.when(F.col("sched_headway_s").between(60, 900) & F.col("actual_headway_s").isNotNull()
                                     & F.col("reliable"), F.col("actual_headway_s") < 0.5 * F.col("sched_headway_s"))))


# ---------------------------------------------------------------- 9. VehiclePositions (optional)
def decode_vp(pair):
    from google.transit import gtfs_realtime_pb2 as pb
    path, raw = pair
    snap = path.rsplit("/", 1)[-1].replace(".pb", "")
    feed = pb.FeedMessage()
    feed.ParseFromString(bytes(raw))
    rows = []
    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle
        rows.append((snap, v.vehicle.id or None, v.trip.trip_id or None,
                     int(v.current_stop_sequence) if v.HasField("current_stop_sequence") else None,
                     int(v.current_status) if v.HasField("current_status") else None,
                     float(v.position.speed) if v.position.HasField("speed") else None))
    return rows


vp_stats = {"vp_snapshots": 0, "vp_trip_match_pct": None}
seg_speed = trip_vehicle = None
if day_exists(f"raw/vehiclepositions/dt={DT}/"):
    try:
        vp = (spark.createDataFrame(sc.binaryFiles(f"{ROOT}/raw/vehiclepositions/dt={DT}/*/*.pb").flatMap(decode_vp),
                                    "snapshot_ts string, vehicle_id string, trip_id string, stop_seq int, status int, speed double")
              .filter(F.col("trip_id").isNotNull()).cache())
        vp_stats["vp_snapshots"] = vp.select("snapshot_ts").distinct().count()
        vp_trips = vp.select("trip_id").distinct()
        n_vp_trips = vp_trips.count()
        matched = vp_trips.join(needed, "trip_id").count()
        vp_stats["vp_trip_match_pct"] = round(100.0 * matched / n_vp_trips, 2) if n_vp_trips else None
        # GTFS-RT speed is in metres per second; status 0 INCOMING_AT and 2 IN_TRANSIT_TO = driving to that stop
        seg_speed = (vp.filter(F.col("status").isin(0, 2) & F.col("speed").isNotNull() & F.col("stop_seq").isNotNull())
                     .groupBy("trip_id", F.col("stop_seq").alias("stop_sequence"))
                     .agg(F.round(F.avg("speed") * 3.6, 1).alias("segment_speed_kmh"),
                          F.count(F.lit(1)).cast("int").alias("segment_speed_obs"))
                     .cache())
        seg_speed.count()
        first_seen = vp.groupBy("vehicle_id", "trip_id").agg(F.min("snapshot_ts").alias("first_seen"),
                                                             F.count(F.lit(1)).alias("n"))
        w_v = Window.partitionBy("vehicle_id").orderBy("first_seen")
        w_t = Window.partitionBy("trip_id").orderBy(F.col("n").desc())
        trip_vehicle = (first_seen.withColumn("prev_trip_id", F.lag("trip_id").over(w_v))
                        .withColumn("rk", F.row_number().over(w_t)).filter("rk = 1")
                        .select("trip_id", "vehicle_id", "prev_trip_id").cache())
        trip_vehicle.count()
    except Exception as e:
        WARNINGS.append(f"vehicle positions unreadable: {str(e)[:120]}")
        seg_speed = trip_vehicle = None

if seg_speed is not None:
    bkk = bkk.join(F.broadcast(seg_speed), ["trip_id", "stop_sequence"], "left")
else:
    bkk = bkk.withColumn("segment_speed_kmh", F.lit(None).cast("double")).withColumn("segment_speed_obs", F.lit(None).cast("int"))
if trip_vehicle is not None:
    # v8: scheduled turnaround (layover) from the timetable; only plausible values (0 to 4 hours) are kept
    bounds = st.groupBy("trip_id").agg(F.min(F.coalesce("sched_dep_s", "sched_arr_s")).alias("trip_start_s"),
                                       F.max("sched_arr_s").alias("trip_end_s"))
    layover = (trip_vehicle.join(bounds.select("trip_id", "trip_start_s"), "trip_id")
               .join(bounds.select(F.col("trip_id").alias("prev_trip_id"), F.col("trip_end_s").alias("prev_end_s")), "prev_trip_id")
               .withColumn("gap", F.col("trip_start_s") - F.col("prev_end_s"))
               .select("trip_id", F.when(F.col("gap").between(0, 4 * 3600), F.col("gap")).cast("int").alias("sched_layover_s")))
    trip_vehicle = trip_vehicle.join(layover, "trip_id", "left")
    trip_end = (bkk.filter("reliable").groupBy("trip_id")
                .agg(F.max_by("arrival_delay_s", "stop_sequence").alias("prev_trip_end_delay_s"))
                .withColumnRenamed("trip_id", "prev_trip_id"))
    bkk = (bkk.join(F.broadcast(trip_vehicle), "trip_id", "left")
           .join(trip_end, "prev_trip_id", "left"))
else:
    bkk = (bkk.withColumn("vehicle_id", F.lit(None).cast("string")).withColumn("prev_trip_end_delay_s", F.lit(None).cast("int"))
           .withColumn("sched_layover_s", F.lit(None).cast("int")))

# ---------------------------------------------------------------- 10. metrics and output
OUT = ["snapshot_ts", "trip_id", "route_id", "route_short_name",
       F.col("route_type").cast("int").alias("route_type"), F.col("trip_headsign").alias("headsign"),
       "stop_id", "stop_name", "stop_sequence", "arrival_delay_s", "departure_delay_s", "hour_local",
       "temperature_2m", "precipitation",
       # v4
       F.col("direction_id").cast("int").alias("direction_id"), "service_date", "day_of_week", "day_type",
       "is_school_break", "special_day", "season", "arrival_hour_local", "is_dark",
       "stop_lat", "stop_lon", "dist_center_km", "zone", "bank", "crosses_danube",
       "on_bus_lane", "near_signal", "signals_within_300m", "near_mall",
       F.round("trip_length_m", 0).alias("trip_length_m"), "stops_in_trip", F.round("position_ratio", 3).alias("position_ratio"),
       F.round("segment_length_m", 0).alias("segment_length_m"), "prev_stop_id", "prev_stop_name",
       "origin_delay_s", "delay_increment_s", "prediction_lead_s", "sched_headway_s", "actual_headway_s", "bunched",
       "rain", "snowfall", "snow_depth", "wind_speed_10m", "wind_gusts_10m", "visibility",
       F.col("weather_code").cast("int").alias("weather_code"),
       "alert_category", "alert_duration", "alert_stop_change",
       "segment_speed_kmh", "segment_speed_obs", "vehicle_id", F.col("prev_trip_end_delay_s").cast("int").alias("prev_trip_end_delay_s"),
       "quality_flag", "reliable", "sched_layover_s",
       "trip_route_id", "sched_arr_s", "is_constant_zero", "start_date", "shift_like"]
out = bkk.select(*OUT).cache()

total_rows = rt_all.count()
nonsched_trip_rows = rt_all.filter(F.col("trip_rel") != 0).count()
sched_trip_rows = total_rows - nonsched_trip_rows
no_data_rows = rt_all.filter((F.col("trip_rel") == 0) & (F.col("stu_rel") == 2)).count()
n = out.count()


def cnt(cond):
    return out.filter(cond).count()


def pct(a, b):
    return round(100.0 * a / b, 2) if b else None


unmatched_trip = cnt(F.col("trip_route_id").isNull())
unmatched_stop = cnt(F.col("sched_arr_s").isNull())
with_delay = cnt(F.col("arrival_delay_s").isNotNull())
extreme = cnt(F.abs(F.col("arrival_delay_s")) > 10800)
q = out.approxQuantile("arrival_delay_s", [0.5, 0.95], 0.01) if with_delay else [None, None]
if alert_stats["alerts_loaded"]:
    alert_stats["alert_matched_rows_pct"] = pct(cnt(F.col("alert_category").isNotNull()), n)

trips = out.select("start_date", "trip_id", "quality_flag", "is_constant_zero").groupBy("start_date", "trip_id").agg(
    F.max((F.col("quality_flag") == "shifted_trip").cast("int")).alias("shifted"), F.max(F.col("is_constant_zero").cast("int")).alias("zero"))
trip_counts = trips.agg(F.count(F.lit(1)), F.sum("shifted"), F.sum("zero")).first()
n_trips, n_shifted, n_zero = trip_counts[0], int(trip_counts[1] or 0), int(trip_counts[2] or 0)

exempt_trips = (out.filter(F.col("shift_like") & F.col("reliable")).select("start_date", "trip_id").distinct().count())

metrics = {"dt": DT, "job_version": 8, "raw_stop_updates": total_rows, "curated_rows": n,
           "added_or_canceled_trip_rows_pct": pct(nonsched_trip_rows, total_rows),
           "no_data_rows_pct_of_scheduled_trips": pct(no_data_rows, sched_trip_rows),
           "other_operator_rows_pct": pct(foreign, n_all),
           "unmatched_bkk_trip_pct": pct(unmatched_trip, n), "unmatched_bkk_stop_pct": pct(unmatched_stop, n),
           "rows_with_delay_pct": pct(with_delay, n), "abs_delay_over_3h_pct": pct(extreme, n),
           "delay_median_s": q[0], "delay_p95_s": q[1], "timetable_used": eligible[-1],
           "weather_joined": has_weather, "weather_extended": extended_weather, "sun_times": has_sun,
           "shape_dist_unit": "m" if DIST_FACTOR == 1.0 else "km",
           "rows_with_zone_pct": pct(cnt(F.col("zone").isNotNull()), n),
           "rows_with_increment_pct": pct(cnt(F.col("delay_increment_s").isNotNull()), n),
           "rows_with_headway_pct": pct(cnt(F.col("sched_headway_s").isNotNull()), n),
           "rows_with_speed_pct": pct(cnt(F.col("segment_speed_kmh").isNotNull()), n),
           "rows_with_prev_trip_pct": pct(cnt(F.col("prev_trip_end_delay_s").isNotNull()), n),
           "unreliable_rows_pct": pct(cnt(~F.col("reliable")), n),
           "stale_prediction_rows_pct": pct(cnt(F.col("quality_flag") == "stale_prediction"), n),
           "trips": n_trips, "shifted_trips": n_shifted, "kept_real_disruption_trips": exempt_trips,
           "rows_with_layover_pct": pct(cnt(F.col("sched_layover_s").isNotNull()), n), "shifted_trips_pct": pct(n_shifted, n_trips),
           "constant_zero_trips": n_zero, "constant_zero_trips_pct": pct(n_zero, n_trips),
           "calendar_days_known": len(cal_rows), "references_used": loaded_refs,
           **alert_stats, **vp_stats, "warnings": WARNINGS}
print(json.dumps(metrics))
put_metrics(metrics)

(out.drop("trip_route_id", "sched_arr_s", "is_constant_zero", "start_date", "shift_like").withColumn("dt", F.lit(DT))
    .write.mode("overwrite").partitionBy("dt").parquet(f"{ROOT}/curated/delays/"))
