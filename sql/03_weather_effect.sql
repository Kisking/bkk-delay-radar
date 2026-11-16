SELECT CASE WHEN precipitation > 0.1 THEN 'rain' ELSE 'dry' END AS weather,
       count(*) AS observations, round(avg(arrival_delay_s) / 60.0, 2) AS avg_delay_min
FROM bkk.delays
WHERE arrival_delay_s IS NOT NULL AND precipitation IS NOT NULL
GROUP BY 1;
