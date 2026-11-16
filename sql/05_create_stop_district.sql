-- Stop -> Budapest district (from OpenStreetMap boundaries, built by scripts/build_district_reference.py).
-- The dashboard joins it to the delays table for the district reliability index.
CREATE EXTERNAL TABLE IF NOT EXISTS bkk.stop_district (
  stop_id string,
  district_num int,
  district string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://BUCKET/raw/reference/districts/'
TBLPROPERTIES ('skip.header.line.count'='1')
