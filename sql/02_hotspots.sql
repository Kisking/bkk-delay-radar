-- Top 10 delayed lines, last 7 days (use in the demo and for the "usability" KPI)
SELECT route_short_name AS route, route_type, count(*) AS observations,
       round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min,
       round(approx_percentile(arrival_delay_s, 0.95) / 60.0, 2) AS p95_delay_min
FROM bkk.delays
WHERE dt >= cast(current_date - interval '7' day as varchar) AND arrival_delay_s IS NOT NULL
GROUP BY 1, 2 HAVING count(*) > 200
ORDER BY avg_delay_min DESC LIMIT 10;
