"""Runs the Athena queries and writes data.json to the dashboard bucket.
v2.1: stop 1 and the first segment are left out (terminal effect), plus late starts,
why lines are late, and pipeline health from the daily metrics files.
v3.2: turnaround (scheduled layover x previous delay) and congestion (peak against night speed).
v3: two views (data.json = last DAYS days, data_all.json = everything since START), queries run in
parallel, extended GTFS route types, terminal-approach segments left out of the segment lists,
plus hourly profile by day kind, least reliable lines, early running and rain by vehicle type.

v2: punctuality, peak vs off-peak, zones, where delay builds up, infrastructure (bus lane, traffic
lights, Danube crossing, shopping centres), disruptions, weather conditions, light/dark, bunching,
delay carried over from the previous trip, slowest segments and the daily trend.
Questions that need the v4 job columns simply return no rows for days made by the older job."""
import datetime as dt
import json
import os
import re
import time

import boto3

athena = boto3.client("athena")
s3 = boto3.client("s3")
glue = boto3.client("glue")
BUCKET = os.environ["BUCKET"]
SITE = os.environ["SITE_BUCKET"]
DB = os.environ.get("DB", "bkk")
DAYS = int(os.environ.get("DAYS", "7"))
START = os.environ.get("START", "2026-10-29")   # first day of the whole-period view
BATCH = 10                                       # Athena queries running at the same time
THIS_WEEK = "cast(current_date - interval '7' day as varchar)"
LAST_14 = "dt >= cast(current_date - interval '14' day as varchar)"
SINCE_START = f"dt >= '{START}'"
ALL_SAINTS = "lower(special_day) LIKE '%mindenszentek%'"
NIGHT = "arrival_hour_local IN (22, 23, 0, 1, 2, 3, 4)"                       # free-flowing roads
PEAK = "arrival_hour_local IN (7, 8, 16, 17) AND day_type = 'workday'"
# night lines (9xx) wait for each other at the night hubs by timetable: their standing time is not traffic
DAY_LINE = "NOT regexp_like(coalesce(route_short_name, ''), '^9[0-9][0-9]')"
ALERTS_FROM = os.environ.get("ALERTS_FROM", "2026-11-02")
GLUE_JOB = os.environ.get("GLUE_JOB", "bkk-delay-job")
GLUE_DPU_HOUR_USD = 0.44           # us-east-1 list price per DPU-hour
ATHENA_TB_USD = 5.0                # us-east-1 list price per TB scanned
DPU_PER_WORKER = {"G.025X": 0.25, "G.1X": 1, "G.2X": 2, "G.4X": 4, "G.8X": 8, "Standard": 1}  # first day with BKK Alerts collected
HAS_DELAY = "arrival_delay_s IS NOT NULL AND stop_sequence > 1 AND coalesce(reliable, true)"
ON_TIME = "CASE WHEN arrival_delay_s BETWEEN -60 AND 180 THEN 1 ELSE 0 END"   # at most 1 min early, 3 min late
# Until a vehicle leaves its first stop, the feed predicts the timetable there, so stop 1 always
# looks on time and the real late departure shows up as a jump at stop 2. Therefore:
REL = "coalesce(reliable, true)"   # v5 quality rules: stale predictions and shifted trips are left out
NOT_FIRST = f"stop_sequence > 1 AND {REL}"                                    # averages skip stop 1
INC = ("delay_increment_s IS NOT NULL AND abs(delay_increment_s) < 1800 "     # drop broken pairs
       "AND stop_sequence > 2 AND coalesce(reliable, true)")                  # and the 1 -> 2 segment
ROAD = "(route_type IN (3, 11, 800) OR route_type BETWEEN 700 AND 799)"       # bus and trolleybus share the road
# not the terminal approach, not two platforms of the same place (one name starts with the other),
# not a depot or garage (vehicles pulling out of the garage are not traffic)
EN_ROUTE = ("stop_sequence < coalesce(stops_in_trip, 9999) AND prev_stop_name <> stop_name "
            "AND position(stop_name IN prev_stop_name) <> 1 AND position(prev_stop_name IN stop_name) <> 1 "
            "AND NOT regexp_like(lower(prev_stop_name || ' ' || stop_name), 'garázs|kocsiszín|telephely')")

# basic and extended GTFS route types (the extended ones are three-digit codes)
TYPE_CASE = ("CASE WHEN route_type = 0 OR route_type BETWEEN 900 AND 999 THEN 'tram' "
             "WHEN route_type = 1 OR route_type BETWEEN 400 AND 499 THEN 'metro' "
             "WHEN route_type = 2 OR route_type BETWEEN 100 AND 199 THEN 'rail' "
             "WHEN route_type = 3 OR route_type BETWEEN 700 AND 799 THEN 'bus' "
             "WHEN route_type IN (11, 800) THEN 'trolleybus' "
             "WHEN route_type IN (4, 1000, 1200) THEN 'ferry' "
             "WHEN route_type IN (7, 1400) THEN 'funicular' ELSE 'other' END")


def build_queries(WINDOW):
  def comparison(factor, group_expr, where):
      """Delay gained per segment and per km, for all stops and within each zone.
      Comparing inside one zone removes much of the 'busy areas have everything' effect.
      Per km = sum of delay gained / sum of segment length (segments over 50 m), so long
      segments are not over-counted."""
      return f"""
          SELECT '{factor}' AS factor, coalesce(zone, 'all') AS zone, grp, count(*) AS observations,
                 round(avg(delay_increment_s), 1) AS avg_gain_s,
                 round(1000.0 * sum(CASE WHEN segment_length_m > 50 THEN delay_increment_s END)
                       / nullif(sum(CASE WHEN segment_length_m > 50 THEN segment_length_m END), 0), 1) AS gain_per_km_s
          FROM (SELECT {group_expr} AS grp, zone, delay_increment_s, segment_length_m
                FROM delays WHERE {WINDOW} AND {ROAD} AND {INC} AND {where} AND zone IS NOT NULL)
          GROUP BY GROUPING SETS ((grp), (zone, grp))"""


  return {
      "coverage": f"""
          SELECT dt, count(DISTINCT snapshot_ts) AS snapshots, count(*) AS rows_stored,
                 round(count(DISTINCT snapshot_ts) * 5 / 60.0, 1) AS hours_collected,
                 round(100.0 * avg(CASE WHEN {NOT_FIRST} THEN {ON_TIME} END), 1) AS on_time_pct,
                 round(avg(CASE WHEN {NOT_FIRST} THEN arrival_delay_s END) / 60.0, 2) AS avg_delay_min
          FROM delays WHERE {WINDOW} GROUP BY dt ORDER BY dt""",
      "hotspots": f"""
          SELECT route_short_name AS route, {TYPE_CASE} AS type, count(*) AS observations,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min,
                 round(approx_percentile(arrival_delay_s, 0.95) / 60.0, 2) AS p95_delay_min
          FROM delays WHERE {WINDOW} AND {HAS_DELAY}
          GROUP BY 1, 2 HAVING count(*) > 200
          ORDER BY avg_delay_min DESC LIMIT 10""",
      "punctuality": f"""
          SELECT coalesce(t, 'all') AS type, count(*) AS observations,
                 round(100.0 * avg(ot), 1) AS on_time_pct, round(100.0 * avg(late5), 1) AS late_5min_pct
          FROM (SELECT {TYPE_CASE} AS t, {ON_TIME} AS ot,
                       CASE WHEN arrival_delay_s > 300 THEN 1 ELSE 0 END AS late5
                FROM delays WHERE {WINDOW} AND {HAS_DELAY})
          GROUP BY GROUPING SETS ((t), ()) ORDER BY observations DESC""",
      "by_type": f"""
          SELECT {TYPE_CASE} AS type, count(*) AS observations,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min,
                 round(approx_percentile(arrival_delay_s, 0.95) / 60.0, 2) AS p95_delay_min
          FROM delays WHERE {WINDOW} AND {HAS_DELAY}
          GROUP BY 1 HAVING count(*) > 500 ORDER BY avg_delay_min DESC""",
      "peak": f"""
          SELECT CASE WHEN day_type = 'workday' AND arrival_hour_local IN (7, 8, 16, 17) THEN 'workday peak (7-9, 16-18)'
                      WHEN day_type = 'workday' THEN 'workday off-peak'
                      WHEN day_type = 'saturday' THEN 'saturday' ELSE 'sunday / holiday' END AS period,
                 count(*) AS observations, round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min,
                 round(100.0 * avg({ON_TIME}), 1) AS on_time_pct
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND day_type IS NOT NULL
          GROUP BY 1 ORDER BY avg_delay_min DESC""",
      "by_zone": f"""
          SELECT zone, count(*) AS observations, round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min,
                 round(avg(CASE WHEN {INC} THEN delay_increment_s END), 1) AS avg_gain_s,
                 round(100.0 * avg({ON_TIME}), 1) AS on_time_pct
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND zone IS NOT NULL
          GROUP BY 1 ORDER BY CASE zone WHEN 'inner' THEN 1 WHEN 'middle' THEN 2 ELSE 3 END""",
      "segments": f"""
          SELECT prev_stop_name || ' → ' || stop_name AS segment, route_short_name AS route,
                 count(*) AS observations, round(avg(delay_increment_s), 0) AS avg_gain_s
          FROM delays WHERE {WINDOW} AND {INC} AND prev_stop_name IS NOT NULL AND {EN_ROUTE}
          GROUP BY 1, 2 HAVING count(*) > 100 ORDER BY avg_gain_s DESC LIMIT 10""",
      "infrastructure": " UNION ALL ".join([
          comparison("bus lane at the stop", "CASE WHEN on_bus_lane THEN 'yes' ELSE 'no' END", "on_bus_lane IS NOT NULL"),
          comparison("traffic lights within 300 m",
                     "CASE WHEN signals_within_300m = 0 THEN '0' WHEN signals_within_300m <= 2 THEN '1-2' "
                     "WHEN signals_within_300m <= 5 THEN '3-5' ELSE '6+' END", "signals_within_300m IS NOT NULL"),
          comparison("segment crosses the Danube", "CASE WHEN crosses_danube THEN 'yes' ELSE 'no' END", "crosses_danube IS NOT NULL"),
          comparison("shopping centre within 400 m", "CASE WHEN near_mall THEN 'yes' ELSE 'no' END", "near_mall IS NOT NULL"),
      ]) + " ORDER BY 1, 3, 2",
      "disruptions": f"""
          SELECT coalesce(alert_category, 'none') AS category, count(*) AS observations,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min,
                 round(100.0 * avg({ON_TIME}), 1) AS on_time_pct
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND dt >= '{ALERTS_FROM}' AND service_date IS NOT NULL
          GROUP BY 1 ORDER BY observations DESC""",
      "weather_conditions": f"""
          SELECT CASE WHEN snowfall > 0 THEN 'snow' WHEN weather_code IN (45, 48) OR visibility < 1000 THEN 'fog'
                      WHEN precipitation > 0.1 THEN 'rain' WHEN wind_gusts_10m >= 50 THEN 'strong wind'
                      ELSE 'dry' END AS weather,
                 count(*) AS observations, round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min,
                 round(avg(CASE WHEN {INC} THEN delay_increment_s END), 1) AS avg_gain_s
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND weather_code IS NOT NULL
          GROUP BY 1 ORDER BY observations DESC""",
      "weather_effect": f"""
          SELECT CASE WHEN precipitation > 0.1 THEN 'rain' ELSE 'dry' END AS weather,
                 count(*) AS observations, round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND precipitation IS NOT NULL
          GROUP BY 1""",
      "light": f"""
          SELECT CASE WHEN is_dark THEN 'dark' ELSE 'daylight' END AS light, count(*) AS observations,
                 round(avg(delay_increment_s), 1) AS avg_gain_s
          FROM delays WHERE {WINDOW} AND {ROAD} AND {INC} AND is_dark IS NOT NULL
          GROUP BY 1""",
      "bunching": f"""
          SELECT route_short_name AS route, {TYPE_CASE} AS type, count(*) AS arrivals,
                 round(100.0 * avg(CASE WHEN bunched THEN 1 ELSE 0 END), 1) AS bunched_pct
          FROM delays WHERE {WINDOW} AND bunched IS NOT NULL AND {NOT_FIRST}
          GROUP BY 1, 2 HAVING count(*) > 200 ORDER BY bunched_pct DESC LIMIT 10""",
      "carry_over": f"""
          SELECT CASE WHEN prev_trip_end_delay_s < 120 THEN '1: under 2 min'
                      WHEN prev_trip_end_delay_s < 300 THEN '2: 2-5 min'
                      WHEN prev_trip_end_delay_s < 600 THEN '3: 5-10 min' ELSE '4: over 10 min' END AS previous_trip_late,
                 count(*) AS trips, round(avg(arrival_delay_s) / 60.0, 2) AS avg_start_delay_min
          FROM delays WHERE {WINDOW} AND stop_sequence = 2 AND prev_trip_end_delay_s IS NOT NULL
               AND arrival_delay_s IS NOT NULL AND {REL}
          GROUP BY 1 ORDER BY 1""",
      "late_starts": f"""
          SELECT route_short_name AS route, {TYPE_CASE} AS type, count(*) AS trips,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_start_delay_min,
                 round(100.0 * avg(CASE WHEN arrival_delay_s > 180 THEN 1 ELSE 0 END), 1) AS late_start_pct
          FROM delays WHERE {WINDOW} AND stop_sequence = 2 AND arrival_delay_s IS NOT NULL AND {REL}
          GROUP BY 1, 2 HAVING count(*) > 50 ORDER BY avg_start_delay_min DESC LIMIT 10""",
      "delay_sources": f"""
          WITH t AS (
              SELECT route_short_name AS route, dt, trip_id,
                     max(CASE WHEN stop_sequence = 2 THEN arrival_delay_s END) AS start_delay,
                     max_by(arrival_delay_s, stop_sequence) AS end_delay, count(*) AS stops
              FROM delays WHERE {WINDOW} AND arrival_delay_s IS NOT NULL AND {REL}
              GROUP BY 1, 2, 3 HAVING count(*) >= 5)
          SELECT route, count(*) AS trips, round(avg(start_delay) / 60.0, 2) AS start_min,
                 round(avg(end_delay - start_delay) / 60.0, 2) AS en_route_min,
                 round(avg(end_delay) / 60.0, 2) AS end_min
          FROM t WHERE start_delay IS NOT NULL
          GROUP BY 1 HAVING count(*) > 30 ORDER BY end_min DESC LIMIT 10""",
      "slowest_segments": f"""
          SELECT prev_stop_name || ' → ' || stop_name AS segment, route_short_name AS route,
                 sum(segment_speed_obs) AS speed_samples, round(avg(segment_speed_kmh), 1) AS avg_speed_kmh
          FROM delays WHERE {WINDOW} AND {ROAD} AND segment_speed_kmh IS NOT NULL AND prev_stop_name IS NOT NULL
               AND stop_sequence > 2 AND {REL} AND {EN_ROUTE} AND {DAY_LINE}
          GROUP BY 1, 2 HAVING sum(segment_speed_obs) >= 20 ORDER BY avg_speed_kmh ASC LIMIT 10""",
      "worst_stops": f"""
          SELECT stop_name AS stop, route_short_name AS route, count(*) AS observations,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND stop_name IS NOT NULL
          GROUP BY 1, 2 HAVING count(*) > 100
          ORDER BY avg_delay_min DESC LIMIT 10""",
      "by_hour": f"""
          SELECT arrival_hour_local AS hour_local,
                 CASE WHEN day_type = 'workday' THEN 'workday' ELSE 'weekend / holiday' END AS day_kind,
                 count(*) AS observations, round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND arrival_hour_local IS NOT NULL AND day_type IS NOT NULL
          GROUP BY 1, 2 ORDER BY 1, 2""",
      "reliability": f"""
          SELECT route_short_name AS route, {TYPE_CASE} AS type, count(*) AS observations,
                 round(approx_percentile(arrival_delay_s, 0.5) / 60.0, 2) AS median_min,
                 round(approx_percentile(arrival_delay_s, 0.95) / 60.0, 2) AS p95_min,
                 round((approx_percentile(arrival_delay_s, 0.95) - approx_percentile(arrival_delay_s, 0.5)) / 60.0, 2) AS buffer_min
          FROM delays WHERE {WINDOW} AND {HAS_DELAY}
          GROUP BY 1, 2 HAVING count(*) > 1000 ORDER BY buffer_min DESC LIMIT 10""",
      "early": f"""
          SELECT route_short_name AS route, {TYPE_CASE} AS type, count(*) AS observations,
                 round(100.0 * avg(CASE WHEN arrival_delay_s < -60 THEN 1 ELSE 0 END), 1) AS early_pct,
                 round(avg(CASE WHEN arrival_delay_s < -60 THEN arrival_delay_s END) / 60.0, 2) AS avg_early_min
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND stop_sequence < coalesce(stops_in_trip, 9999)
          GROUP BY 1, 2 HAVING count(*) > 1000 ORDER BY early_pct DESC LIMIT 10""",
      "unusual_hours": f"""
          SELECT service_date, arrival_hour_local AS hour_local, route_short_name AS route, {TYPE_CASE} AS type,
                 count(*) AS observations, round(approx_percentile(arrival_delay_s, 0.5) / 60.0, 1) AS median_min,
                 round(approx_percentile(arrival_delay_s, 0.95) / 60.0, 1) AS p95_min,
                 max(alert_category) AS notice
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND arrival_hour_local IS NOT NULL
          GROUP BY 1, 2, 3, 4 HAVING count(*) >= 20 AND approx_percentile(arrival_delay_s, 0.5) >= 600
          ORDER BY median_min DESC LIMIT 15""",
      "school_break": f"""
          SELECT CASE WHEN is_school_break THEN 'school break' ELSE 'school days' END AS period,
                 count(DISTINCT service_date) AS days, count(*) AS observations,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min, round(100.0 * avg({ON_TIME}), 1) AS on_time_pct,
                 round(100.0 * avg(CASE WHEN arrival_hour_local IN (7, 8) THEN {ON_TIME} END), 1) AS morning_peak_on_time_pct
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND day_type = 'workday' AND special_day IS NULL
               AND is_school_break IS NOT NULL
          GROUP BY 1 ORDER BY 1""",
      "busy_stops": f"""
          WITH s AS (SELECT stop_id, max(stop_name) AS name, round(avg(stop_lat) * 20) / 20 AS la, round(avg(stop_lon) * 20) / 20 AS lo   -- about 5 km cells: all platforms of a hub
                     FROM delays WHERE {WINDOW} AND stop_lat IS NOT NULL GROUP BY 1),
               pl AS (SELECT s.name, s.la, s.lo, count(DISTINCT d.route_short_name) AS n
                      FROM delays d JOIN s ON s.stop_id = d.stop_id WHERE {WINDOW} GROUP BY 1, 2, 3)
          SELECT CASE WHEN pl.n = 1 THEN '1: 1 line' WHEN pl.n <= 3 THEN '2: 2-3 lines' WHEN pl.n <= 6 THEN '3: 4-6 lines'
                      WHEN pl.n <= 12 THEN '4: 7-12 lines' ELSE '5: 13+ lines' END AS lines_at_stop,
                 CASE WHEN regexp_like(s.name, ' (M|H|M[+]H)( |,|$)') THEN 'yes' ELSE 'no' END AS metro_or_hev,
                 count(*) AS observations, round(avg(d.delay_increment_s), 1) AS avg_gain_s
          FROM delays d JOIN s ON s.stop_id = d.prev_stop_id
          JOIN pl ON pl.name = s.name AND pl.la = s.la AND pl.lo = s.lo
          WHERE {WINDOW} AND {ROAD} AND {INC}
          GROUP BY 1, 2 ORDER BY 1, 2""",
      "districts": f"""
          SELECT d.district_num, d.district,
                 count(*) FILTER (WHERE {HAS_DELAY}) AS observations,
                 round(100.0 * avg({ON_TIME}) FILTER (WHERE {HAS_DELAY}), 1) AS on_time_pct,
                 round(approx_percentile(arrival_delay_s, 0.5) FILTER (WHERE {HAS_DELAY}) / 60.0, 2) AS median_min,
                 round((approx_percentile(arrival_delay_s, 0.95) FILTER (WHERE {HAS_DELAY})
                        - approx_percentile(arrival_delay_s, 0.5) FILTER (WHERE {HAS_DELAY})) / 60.0, 2) AS buffer_min,
                 round(100.0 * avg(CASE WHEN bunched THEN 1 ELSE 0 END) FILTER (WHERE bunched IS NOT NULL AND {NOT_FIRST}), 1) AS bunched_pct,
                 round(1000.0 * sum(delay_increment_s) FILTER (WHERE {INC} AND {EN_ROUTE} AND segment_length_m > 50)
                       / nullif(sum(segment_length_m) FILTER (WHERE {INC} AND {EN_ROUTE} AND segment_length_m > 50), 0), 1) AS gain_per_km_s
          FROM (SELECT * FROM delays WHERE {WINDOW}) x JOIN stop_district d ON x.stop_id = d.stop_id
          GROUP BY 1, 2 HAVING count(*) FILTER (WHERE {HAS_DELAY}) >= 5000 ORDER BY 1""",
      "stop_density": f"""
          SELECT CASE WHEN sd < 1.5 THEN '1: under 1.5' WHEN sd < 2.5 THEN '2: 1.5-2.5' WHEN sd < 3.5 THEN '3: 2.5-3.5'
                      ELSE '4: 3.5 or more' END AS stops_per_km,
                 count(DISTINCT route_short_name) AS lines, count(*) AS observations,
                 round(1000.0 * sum(CASE WHEN segment_length_m > 50 THEN delay_increment_s END)
                       / nullif(sum(CASE WHEN segment_length_m > 50 THEN segment_length_m END), 0), 1) AS gain_per_km_s,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min
          FROM (SELECT *, 1000.0 * stops_in_trip / nullif(trip_length_m, 0) AS sd FROM delays
                WHERE {WINDOW} AND {ROAD} AND {INC} AND trip_length_m > 1000)
          GROUP BY 1 ORDER BY 1""",
      "temperature": f"""
          SELECT CASE WHEN temperature_2m < 0 THEN '1: below 0 °C' WHEN temperature_2m < 5 THEN '2: 0-5 °C'
                      WHEN temperature_2m < 10 THEN '3: 5-10 °C' ELSE '4: 10 °C or more' END AS temperature,
                 count(*) AS observations, round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min,
                 round(avg(CASE WHEN {INC} THEN delay_increment_s END), 1) AS avg_gain_s
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND temperature_2m IS NOT NULL
          GROUP BY 1 ORDER BY 1""",
      "turnaround": f"""
          SELECT CASE WHEN sched_layover_s < 300 THEN '1: under 5 min' WHEN sched_layover_s < 600 THEN '2: 5-10 min'
                      WHEN sched_layover_s < 1200 THEN '3: 10-20 min' ELSE '4: 20 min or more' END AS turnaround,
                 CASE WHEN prev_trip_end_delay_s < 120 THEN 'p1' WHEN prev_trip_end_delay_s < 300 THEN 'p2'
                      WHEN prev_trip_end_delay_s < 600 THEN 'p3' ELSE 'p4' END AS previous_late,
                 count(*) AS trips, round(avg(arrival_delay_s) / 60.0, 2) AS start_delay_min
          FROM delays WHERE {WINDOW} AND stop_sequence = 2 AND sched_layover_s IS NOT NULL
               AND prev_trip_end_delay_s IS NOT NULL AND arrival_delay_s IS NOT NULL AND {REL}
          GROUP BY 1, 2 ORDER BY 1, 2""",
      "congestion": f"""
          WITH s AS (
            SELECT prev_stop_name AS a, stop_name AS b, max(route_short_name) AS line,
                   avg(CASE WHEN {NIGHT} THEN segment_speed_kmh END) AS night,
                   avg(CASE WHEN {PEAK} THEN segment_speed_kmh END) AS peak,
                   sum(CASE WHEN {NIGHT} THEN 1 ELSE 0 END) AS n_night,
                   sum(CASE WHEN {PEAK} THEN 1 ELSE 0 END) AS n_peak
            FROM delays WHERE {WINDOW} AND {ROAD} AND segment_speed_kmh IS NOT NULL AND prev_stop_name IS NOT NULL
                 AND stop_sequence > 2 AND {REL} AND {EN_ROUTE} AND {DAY_LINE}
            GROUP BY 1, 2)
          SELECT a || ' → ' || b AS segment, line, round(night, 1) AS night_kmh, round(peak, 1) AS peak_kmh,
                 round(100.0 * (1 - peak / night), 0) AS slowdown_pct, n_peak AS peak_samples
          FROM s WHERE n_night >= 10 AND n_peak >= 10 AND night >= 10
          ORDER BY slowdown_pct DESC LIMIT 12""",
      "congestion_total": f"""
          SELECT {TYPE_CASE} AS type,
                 round(avg(CASE WHEN {NIGHT} THEN segment_speed_kmh END), 1) AS night_kmh,
                 round(avg(CASE WHEN {PEAK} THEN segment_speed_kmh END), 1) AS peak_kmh,
                 round(100.0 * (1 - avg(CASE WHEN {PEAK} THEN segment_speed_kmh END)
                                    / avg(CASE WHEN {NIGHT} THEN segment_speed_kmh END)), 0) AS slowdown_pct
          FROM delays WHERE {WINDOW} AND segment_speed_kmh IS NOT NULL AND stop_sequence > 2 AND {REL} AND {DAY_LINE}
          GROUP BY 1 HAVING sum(CASE WHEN {NIGHT} THEN 1 ELSE 0 END) >= 100 AND sum(CASE WHEN {PEAK} THEN 1 ELSE 0 END) >= 100
          ORDER BY 1""",
      "weekdays": f"""
          SELECT day_of_week, count(DISTINCT service_date) AS days, count(*) AS observations,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min,
                 round(100.0 * avg({ON_TIME}), 1) AS on_time_pct,
                 round(100.0 * avg(CASE WHEN arrival_hour_local IN (7, 8, 16, 17) THEN {ON_TIME} END), 1) AS peak_on_time_pct
          FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND day_of_week IS NOT NULL AND special_day IS NULL
               AND NOT (day_type = 'sunday_holiday' AND day_of_week < 7)
               AND service_date IN (SELECT service_date FROM delays WHERE {WINDOW}
                                    GROUP BY 1 HAVING count(*) >= 50000)   -- full days only
          GROUP BY 1 ORDER BY 1""",
      "week_over_week": f"""
          SELECT route, type, this_week_min, previous_week_min,
                 round(this_week_min - previous_week_min, 2) AS change_min, this_obs, previous_obs
          FROM (SELECT route_short_name AS route, {TYPE_CASE} AS type,
                       round(avg(CASE WHEN dt >= {THIS_WEEK} THEN arrival_delay_s END) / 60.0, 2) AS this_week_min,
                       round(avg(CASE WHEN dt < {THIS_WEEK} THEN arrival_delay_s END) / 60.0, 2) AS previous_week_min,
                       sum(CASE WHEN dt >= {THIS_WEEK} THEN 1 ELSE 0 END) AS this_obs,
                       sum(CASE WHEN dt < {THIS_WEEK} THEN 1 ELSE 0 END) AS previous_obs
                FROM delays WHERE {LAST_14} AND {HAS_DELAY}
                GROUP BY 1, 2)
          WHERE this_obs >= 500 AND previous_obs >= 500
          ORDER BY abs(this_week_min - previous_week_min) DESC LIMIT 12""",
      "week_over_week_total": f"""
          SELECT CASE WHEN dt >= {THIS_WEEK} THEN 'this week' ELSE 'previous week' END AS week,
                 count(DISTINCT dt) AS days, count(*) AS observations,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min, round(100.0 * avg({ON_TIME}), 1) AS on_time_pct
          FROM delays WHERE {LAST_14} AND {HAS_DELAY}
          GROUP BY 1 ORDER BY 1 DESC""",
      "special_days": f"""
          SELECT coalesce(special_day, 'normal day') AS day,
                 CASE WHEN day_type = 'workday' THEN 'workday' WHEN day_type = 'saturday' THEN 'saturday'
                      ELSE 'sunday / holiday' END AS day_type,
                 count(DISTINCT service_date) AS days, count(*) AS observations,
                 round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min, round(100.0 * avg({ON_TIME}), 1) AS on_time_pct
          FROM delays WHERE {SINCE_START} AND {HAS_DELAY} AND day_type IS NOT NULL
          GROUP BY 1, 2 ORDER BY 2, 1""",
      "cemetery_lines": f"""
          WITH c AS (SELECT DISTINCT route_short_name FROM delays
                     WHERE {SINCE_START} AND (lower(stop_name) LIKE '%temető%' OR lower(stop_name) LIKE '%sírkert%'))
          SELECT route, holiday_min, other_holidays_min, workday_min, other_workdays_min,
                 round(holiday_min - other_holidays_min, 2) AS holiday_extra_min,
                 round(workday_min - other_workdays_min, 2) AS workday_extra_min, special_obs
          FROM (SELECT d.route_short_name AS route,
                       round(avg(CASE WHEN {ALL_SAINTS} AND day_type = 'sunday_holiday' THEN arrival_delay_s END) / 60.0, 2) AS holiday_min,
                       round(avg(CASE WHEN special_day IS NULL AND day_type = 'sunday_holiday' THEN arrival_delay_s END) / 60.0, 2) AS other_holidays_min,
                       round(avg(CASE WHEN {ALL_SAINTS} AND day_type = 'workday' THEN arrival_delay_s END) / 60.0, 2) AS workday_min,
                       round(avg(CASE WHEN special_day IS NULL AND day_type = 'workday' THEN arrival_delay_s END) / 60.0, 2) AS other_workdays_min,
                       sum(CASE WHEN {ALL_SAINTS} THEN 1 ELSE 0 END) AS special_obs
                FROM delays d JOIN c ON d.route_short_name = c.route_short_name
                WHERE {SINCE_START} AND {HAS_DELAY} AND day_type IS NOT NULL
                GROUP BY 1)
          WHERE special_obs >= 100
          ORDER BY coalesce(holiday_min - other_holidays_min, 0) + coalesce(workday_min - other_workdays_min, 0) DESC LIMIT 12""",
      "rain_by_type": f"""
          WITH c AS (
            SELECT {TYPE_CASE} AS type, day_type, arrival_hour_local AS h,
                   avg(CASE WHEN precipitation > 0.1 THEN arrival_delay_s END) AS r,
                   avg(CASE WHEN precipitation <= 0.1 THEN arrival_delay_s END) AS d,
                   sum(CASE WHEN precipitation > 0.1 THEN 1 ELSE 0 END) AS nr,
                   sum(CASE WHEN precipitation <= 0.1 THEN 1 ELSE 0 END) AS nd
            FROM delays WHERE {WINDOW} AND {HAS_DELAY} AND precipitation IS NOT NULL AND day_type IS NOT NULL
            GROUP BY 1, 2, 3)
          SELECT type, sum(nr) AS rain_observations,
                 round(sum(d * nr) / sum(nr) / 60.0, 2) AS dry_min,
                 round(sum(r * nr) / sum(nr) / 60.0, 2) AS rain_min,
                 round(sum((r - d) * nr) / sum(nr) / 60.0, 2) AS extra_min
          FROM c WHERE nr >= 50 AND nd >= 50
          GROUP BY 1 HAVING sum(nr) > 1000 ORDER BY rain_observations DESC""",
  }


# Trips that are 30+ minutes late at EVERY stop on two or more days are data errors (a vehicle matched
# to the wrong trip, night after night), not delays. A real incident happens once; these come back.
SUSPECT_SQL = f"""
    WITH t AS (SELECT route_short_name AS r, trip_id, dt, min(arrival_delay_s) AS mn
               FROM delays WHERE dt >= '{START}' AND stop_sequence > 1 AND coalesce(reliable, true)
                    AND arrival_delay_s IS NOT NULL
               GROUP BY 1, 2, 3 HAVING count(*) >= 3)
    SELECT r AS route, trip_id, count(*) AS days, round(avg(mn) / 60.0, 1) AS avg_min_delay
    FROM t WHERE mn >= 1800 GROUP BY 1, 2 HAVING count(*) >= 2
    ORDER BY days DESC, avg_min_delay DESC LIMIT 500"""


def exclude_suspects(queries, suspects):
    """Adds 'AND trip_id NOT IN (...)' wherever a query filters on reliable rows."""
    ids = sorted({re.sub(r"[^A-Za-z0-9_.:-]", "", r["trip_id"]) for r in suspects if r.get("trip_id")})
    if not ids:
        return queries
    clause = "(coalesce(reliable, true) AND trip_id NOT IN (" + ", ".join(f"'{i}'" for i in ids) + "))"
    return {k: v.replace("coalesce(reliable, true)", clause) for k, v in queries.items()}


def build_heavy(WINDOW):
    """Big result sets: per line and stop for the line explorer, per segment for the map."""
    return {
        "lines": f"""
            -- the most common variant of each line and direction (most trips; on a tie the longer one),
            -- shown stop by stop in timetable order: loops that pass a stop twice keep both visits
            WITH pat AS (
                SELECT route_short_name AS r, coalesce(direction_id, 0) AS d, {TYPE_CASE} AS t, stops_in_trip AS n,
                       count(DISTINCT dt || trip_id) AS trips
                FROM delays WHERE {WINDOW} AND route_short_name IS NOT NULL AND stops_in_trip IS NOT NULL
                GROUP BY 1, 2, 3, 4),
                 main AS (SELECT r, d, t, max_by(n, trips * 1000 + n) AS n FROM pat GROUP BY 1, 2, 3)
            SELECT route_short_name AS route, coalesce(direction_id, 0) AS dir, {TYPE_CASE} AS type,
                   stop_sequence AS seq, max(stop_name) AS stop, count(*) AS obs,
                   -- no delay at the first stop: until the vehicle leaves, the feed shows the timetable there
                   round(avg(CASE WHEN stop_sequence > 1 THEN arrival_delay_s END) / 60.0, 2) AS delay_min,
                   round(avg(CASE WHEN {INC} THEN delay_increment_s END), 0) AS gain_s
            FROM delays
            JOIN main m ON m.r = route_short_name AND m.d = coalesce(direction_id, 0) AND m.t = {TYPE_CASE}
                       AND m.n = stops_in_trip
            WHERE {WINDOW} AND arrival_delay_s IS NOT NULL AND {REL}
            GROUP BY 1, 2, 3, 4 HAVING count(*) >= 10""",
        "map": f"""
            WITH s AS (SELECT stop_id, avg(stop_lat) AS lat, avg(stop_lon) AS lon
                       FROM delays WHERE {WINDOW} AND stop_lat IS NOT NULL GROUP BY 1),
                 g AS (SELECT prev_stop_id, stop_id, count(*) AS obs, round(avg(delay_increment_s), 1) AS gain_s,
                              count(DISTINCT route_short_name) AS lines, max(prev_stop_name) AS a, max(stop_name) AS b
                       FROM delays WHERE {WINDOW} AND {INC} AND {EN_ROUTE} AND prev_stop_id IS NOT NULL
                       GROUP BY 1, 2 HAVING count(*) >= 100)
            SELECT round(p.lat, 5) AS lat1, round(p.lon, 5) AS lon1, round(q.lat, 5) AS lat2, round(q.lon, 5) AS lon2,
                   g.gain_s, g.obs, g.lines, g.a, g.b
            FROM g JOIN s p ON p.stop_id = g.prev_stop_id JOIN s q ON q.stop_id = g.stop_id""",
    }


def start(sql):
    return athena.start_query_execution(
        QueryString=sql, QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": f"s3://{BUCKET}/athena-results/"})["QueryExecutionId"]


def fetch(qid):
    rows, token, head = [], None, None
    while True:
        res = athena.get_query_results(QueryExecutionId=qid, **({"NextToken": token} if token else {}))
        page = res["ResultSet"]["Rows"]
        if head is None:
            head, page = [c.get("VarCharValue") for c in page[0]["Data"]], page[1:]
        rows += [dict(zip(head, [c.get("VarCharValue") for c in r["Data"]])) for r in page]
        token = res.get("NextToken")
        if not token:
            return rows


def fetch_csv(qid):
    import csv
    import io
    body = s3.get_object(Bucket=BUCKET, Key=f"athena-results/{qid}.csv")["Body"].read().decode("utf-8")
    return [dict(r) for r in csv.DictReader(io.StringIO(body))]


def run_many(queries, from_csv=False):
    """Runs the queries BATCH at a time and waits for each batch: much faster than one by one."""
    results, scanned, failed = {}, 0, []
    names = list(queries)
    for k in range(0, len(names), BATCH):
        ids = {}
        for n in names[k:k + BATCH]:
            try:
                ids[n] = start(queries[n])
            except Exception as e:
                results[n] = []
                failed.append(f"{n}: {str(e)[:160]}")
        pending = dict(ids)
        for _ in range(150):
            if not pending:
                break
            for n, qid in list(pending.items()):
                q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
                state = q["Status"]["State"]
                if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    del pending[n]
                    if state == "SUCCEEDED":
                        results[n] = fetch_csv(qid) if from_csv else fetch(qid)
                        scanned += q.get("Statistics", {}).get("DataScannedInBytes", 0)
                    else:
                        results[n] = []
                        failed.append(f"{n}: {state} {str(q['Status'].get('StateChangeReason'))[:160]}")
            time.sleep(2)
        for n in pending:  # still running after five minutes
            results[n] = []
            failed.append(f"{n}: timed out")
    return results, scanned, failed


def pipeline_health(last_n=None):
    """Daily metrics written by the Glue job (curated/_metrics/dt=YYYY-MM-DD.json)."""
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="curated/_metrics/"):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json")]
    keys = sorted(keys)
    rows = []
    for k in (keys[-last_n:] if last_n else keys):
        try:
            m = json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
        except Exception:
            continue
        rows.append({"dt": m.get("dt"), "job_version": m.get("job_version", 3),
                     "raw_stop_updates": m.get("raw_stop_updates"), "curated_rows": m.get("curated_rows"),
                     "unmatched_bkk_trip_pct": m.get("unmatched_bkk_trip_pct"),
                     "rows_with_delay_pct": m.get("rows_with_delay_pct"),
                     "alerts_loaded": m.get("alerts_loaded"), "vp_trip_match_pct": m.get("vp_trip_match_pct"),
                     "unreliable_rows_pct": m.get("unreliable_rows_pct"), "shifted_trips": m.get("shifted_trips"),
                     "constant_zero_trips_pct": m.get("constant_zero_trips_pct"),
                     "warnings": len(m.get("warnings", []))})
    return rows


VIEWS = {  # file name -> (label, window filter, metrics days)
    "data.json": (f"last {DAYS} days", f"dt >= cast(current_date - interval '{DAYS}' day as varchar)", DAYS + 3),
    "data_all.json": (f"whole period since {START}", f"dt >= '{START}'", None),
}


# District reliability index: each measure scaled 1-10 between the worst and the best district
# (MinMax), then weighted. The weights are a choice, so they are published next to the result.
INDEX_WEIGHTS = [("on_time_pct", 0.30, True), ("median_min", 0.20, False), ("buffer_min", 0.20, False),
                 ("bunched_pct", 0.15, False), ("gain_per_km_s", 0.15, False)]   # (measure, weight, higher is better)


def district_index(rows):
    num = lambda v: None if v in (None, "") else float(v)
    rows = [dict(r) for r in rows]
    for key, _, better_high in INDEX_WEIGHTS:
        vals = [num(r.get(key)) for r in rows if num(r.get(key)) is not None]
        lo, hi = (min(vals), max(vals)) if vals else (0.0, 0.0)
        for r in rows:
            v = num(r.get(key))
            if v is None:
                r["s_" + key] = None
                continue
            x = 0.5 if hi == lo else (v - lo) / (hi - lo)
            r["s_" + key] = round(1 + 9 * (x if better_high else 1 - x), 1)
    for r in rows:
        parts = [(w, r["s_" + k]) for k, w, _ in INDEX_WEIGHTS if r["s_" + k] is not None]
        wsum = sum(w for w, _ in parts)
        r["index"] = round(sum(w * s for w, s in parts) / wsum, 1) if wsum else None
    rows.sort(key=lambda r: -(r["index"] or 0))
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows


# Project KPIs: each one measured by the pipeline itself. "Latest" is the most recent processed day,
# "days on target" counts every day of the view, so an earlier bad day is not hidden.
def glue_runs(n=15):
    runs, token = [], None
    while len(runs) < 100:
        kw = {"JobName": GLUE_JOB, "MaxResults": 50, **({"NextToken": token} if token else {})}
        page = glue.get_job_runs(**kw)
        runs += page.get("JobRuns", [])
        token = page.get("NextToken")
        if not token:
            break
    ok = [r for r in runs if r.get("JobRunState") == "SUCCEEDED" and r.get("ExecutionTime")]
    ok.sort(key=lambda r: r["StartedOn"], reverse=True)
    return ok[:n]


def dpu_hours(run):
    if run.get("DPUSeconds"):
        return run["DPUSeconds"] / 3600.0
    dpus = (run.get("NumberOfWorkers") or 0) * DPU_PER_WORKER.get(run.get("WorkerType"), 1) or run.get("MaxCapacity") or 2
    return dpus * max(run["ExecutionTime"], 60) / 3600.0      # billed per second, at least one minute


def build_kpis(out, runs, athena_bytes_per_day):
    num = lambda v: None if v in (None, "") else float(v)
    cov, pipe = out.get("coverage", []), out.get("pipeline", [])
    rows = []

    def daily(name, target_txt, values, ok, unit):
        vals = [(d, v) for d, v in values if v is not None]
        if not vals:
            rows.append({"kpi": name, "target": target_txt, "latest": None, "on_target": None, "status": None})
            return
        last_d, last_v = vals[-1]
        hit = sum(1 for _, v in vals if ok(v))
        rows.append({"kpi": name, "target": target_txt, "latest": f"{last_v:g}{unit} ({last_d})",
                     "on_target": f"{hit} of {len(vals)} day{'' if len(vals) == 1 else 's'}", "status": ok(last_v)})

    # the first day of the project started at noon, so it is not a full day
    full = [r for r in cov if r.get("dt") != START]
    daily("Feed hours captured a day", "≥ 20 h", [(r["dt"], num(r.get("hours_collected"))) for r in full],
          lambda v: v >= 20, " h")
    daily("Records matched to the timetable", "≥ 98 %",
          [(r["dt"], None if num(r.get("unmatched_bkk_trip_pct")) is None else round(100 - num(r["unmatched_bkk_trip_pct"]), 2)) for r in pipe],
          lambda v: v >= 98, " %")
    daily("Scheduled trips tracked live", "≥ 95 %",
          [(r["dt"], None if num(r.get("constant_zero_trips_pct")) is None else round(100 - num(r["constant_zero_trips_pct"]), 2)) for r in pipe],
          lambda v: v >= 95, " %")
    daily("Rows left out as data errors", "≤ 5 % a day",
          [(r["dt"], num(r.get("unreliable_rows_pct"))) for r in pipe], lambda v: v <= 5, " %")

    # freshness: yesterday (Budapest time) is on the page
    try:
        from zoneinfo import ZoneInfo
        now = dt.datetime.now(ZoneInfo("Europe/Budapest"))
    except Exception:
        now = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)
    yesterday = (now.date() - dt.timedelta(days=1)).isoformat()
    last_day = cov[-1]["dt"] if cov else None
    rows.append({"kpi": "Yesterday on the dashboard", "target": "every evening by 23:00",
                 "latest": f"{last_day or '–'}, published {now:%H:%M}", "on_target": None,
                 "status": None if last_day is None else last_day >= yesterday})

    # processing time and cost from the Glue job's own run history and the data Athena scanned
    if runs:
        mins = [r["ExecutionTime"] / 60.0 for r in runs]
        rows.append({"kpi": "Processing time per day", "target": "≤ 15 min", "latest": f"{mins[0]:.1f} min",
                     "on_target": f"{sum(m <= 15 for m in mins)} of the last {len(mins)} runs", "status": mins[0] <= 15})
        glue_day = sum(dpu_hours(r) for r in runs) / len(runs) * GLUE_DPU_HOUR_USD
        athena_day = athena_bytes_per_day / 1e12 * ATHENA_TB_USD
        month = 30 * (glue_day + athena_day)
        rows.append({"kpi": "Processing cost per month (Glue + Athena)", "target": "≤ $10",
                     "latest": f"${month:.2f} (Glue ${30 * glue_day:.2f}, Athena ${30 * athena_day:.2f})",
                     "on_target": None, "status": month <= 10})
    else:
        rows += [{"kpi": "Processing time per day", "target": "≤ 15 min", "latest": None, "on_target": None, "status": None},
                 {"kpi": "Processing cost per month (Glue + Athena)", "target": "≤ $10", "latest": None, "on_target": None, "status": None}]
    rows.append({"kpi": "Same table in 3 accounts", "target": "≤ 0.01 % difference", "latest": "0.006 % (one-off check, 8–10 Nov)",
                 "on_target": None, "status": True})
    return rows


def build_view(label, window, metrics_days, suspects=()):
    out = {
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "window_days": DAYS, "view": label,
        "attribution": ("Data source: BKK Zrt., CC BY 4.0 · Weather data by Open-Meteo.com · "
                        "Bus lanes, Danube, shopping centres and district boundaries © OpenStreetMap contributors · "
                        "Traffic light locations: Budapest Közút Zrt."),
        "note": ("Delay = predicted arrival time minus scheduled time, from the last prediction per stop. "
                 "On time = at most 1 minute early and 3 minutes late. Delay gained = how much the delay grew "
                 "between the previous stop and this one. The first stop of each trip is left out of averages, "
                 "because until the vehicle leaves, the feed shows the timetable there; late departures are "
                 "measured at the second stop. Stale predictions (made over 10 minutes before arrival) and shifted "
                 "trips (the whole trip moved by 10+ minutes at every stop) are left out."),
    }
    results, scanned, failed = run_many(exclude_suspects(build_queries(window), suspects))
    out["suspect_trips"] = list(suspects)
    out.update(results)
    if out.get("districts"):
        out["districts"] = district_index(out["districts"])
        out["district_weights"] = {k: w for k, w, _ in INDEX_WEIGHTS}
    try:
        out["pipeline"] = pipeline_health(metrics_days)
    except Exception as e:
        out["pipeline"] = []
        failed.append(f"pipeline: {str(e)[:160]}")
    cov = out.get("coverage", [])
    out["summary"] = {
        "days": len(cov),
        "hours_collected": round(sum(float(r["hours_collected"]) for r in cov), 1),
        "rows": sum(int(r["rows_stored"]) for r in cov),
        "first_day": cov[0]["dt"] if cov else None,
        "last_day": cov[-1]["dt"] if cov else None,
        "mb_scanned": round(scanned / 1048576, 1),
        "failed_panels": failed,
    }
    return out


HEAVY_FILES = {"data.json": ("lines.json", "map.json"), "data_all.json": ("lines_all.json", "map_all.json")}


def pack_lines(rows):
    out = {}
    num = lambda v: None if v in (None, "") else float(v)
    for r in rows:
        k = f"{r['route']}|{r['dir']}|{r['type']}"
        e = out.setdefault(k, {"route": r["route"], "dir": int(r["dir"]), "type": r["type"], "stops": []})
        e["stops"].append([r["stop"], int(r["seq"]), int(r["obs"]), num(r["delay_min"]), num(r["gain_s"])])
    for e in out.values():
        e["stops"].sort(key=lambda x: x[1])                 # timetable order of the main variant
        e["first"], e["headsign"] = e["stops"][0][0], e["stops"][-1][0]
        e["circular"] = e["first"] == e["headsign"]
    return out


def pack_map(rows):
    return [[float(r["lat1"]), float(r["lon1"]), float(r["lat2"]), float(r["lon2"]), float(r["gain_s"]),
             int(r["obs"]), int(r["lines"]), r["a"], r["b"]] for r in rows if r.get("lat1") and r.get("lat2")]


def handler(event, context):
    result, scanned_total = {}, 0
    found, sc, sfail = run_many({"suspects": SUSPECT_SQL})
    scanned_total += sc
    suspects = found.get("suspects", [])
    result["suspect_trips"] = {"trips": len(suspects), "failed": sfail}
    for key, (label, window, _) in VIEWS.items():
        heavy, sc, failed = run_many(exclude_suspects(build_heavy(window), suspects), from_csv=True)
        scanned_total += sc
        lines_key, map_key = HEAVY_FILES[key]
        for k, body in ((lines_key, {"view": label, "lines": pack_lines(heavy.get("lines", []))}),
                        (map_key, {"view": label, "segments": pack_map(heavy.get("map", []))})):
            s3.put_object(Bucket=SITE, Key=k, Body=json.dumps(body, separators=(",", ":"), ensure_ascii=False),
                          ContentType="application/json", CacheControl="no-cache")
        result[lines_key] = {"lines": len(heavy.get("lines", [])), "segments": len(heavy.get("map", [])), "failed": failed}
    outs = {}
    for key, (label, window, metrics_days) in VIEWS.items():
        outs[key] = build_view(label, window, metrics_days, suspects)
        scanned_total += outs[key]["summary"]["mb_scanned"] * 1048576
    try:
        runs = glue_runs()
    except Exception as e:                      # no Glue permission or no runs yet: the KPI rows say so
        runs = []
        for o in outs.values():
            o["summary"]["failed_panels"].append(f"kpis: Glue run history: {str(e)[:120]}")
    for key, out in outs.items():
        out["kpis"] = build_kpis(out, runs, scanned_total)
        s3.put_object(Bucket=SITE, Key=key, Body=json.dumps(out),
                      ContentType="application/json", CacheControl="no-cache")
        result[key] = {"days": out["summary"]["days"], "mb_scanned": out["summary"]["mb_scanned"],
                       "failed_panels": out["summary"]["failed_panels"]}
    print(json.dumps(result))
    return result
