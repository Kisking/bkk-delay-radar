"""Hourly Open-Meteo call for Budapest (no key). Stores the raw JSON in S3.\nVariables: temperature, rain, snow, wind and gusts, visibility, weather code; daily sunrise and sunset."""
import datetime as dt
import os
import urllib.request

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
# temperature, rain, snow, wind, gusts, visibility (fog) and the WMO weather code
# (45 and 48 = fog, 71-77 = snow, 95-99 = thunderstorm)
HOURLY = ("temperature_2m,precipitation,rain,snowfall,snow_depth,"
          "wind_speed_10m,wind_gusts_10m,visibility,weather_code")
URL = ("https://api.open-meteo.com/v1/forecast?latitude=47.4979&longitude=19.0402"
       f"&hourly={HOURLY}&daily=sunrise,sunset&timezone=Europe%2FBudapest"
       "&past_days=1&forecast_days=1")


def handler(event, context):
    now = dt.datetime.now(dt.timezone.utc)
    with urllib.request.urlopen(URL, timeout=30) as resp:
        body = resp.read()
    key = f"raw/weather/dt={now:%Y-%m-%d}/hour={now:%H}/{now:%Y%m%dT%H%M%S}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")
    return {"key": key, "bytes": len(body)}
