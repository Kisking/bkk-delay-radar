-- Rows without a timetable match (delay is NULL): which routes and trips are they?
SELECT route_id, count(DISTINCT trip_id) AS trips, count(*) AS row_count,
       min(trip_id) AS example_trip_id, min(snapshot_ts) AS first_seen, max(snapshot_ts) AS last_seen
FROM bkk.delays
WHERE dt = '2026-10-30' AND arrival_delay_s IS NULL
GROUP BY route_id ORDER BY row_count DESC LIMIT 15;
