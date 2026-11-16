"""Starts the daily Glue job for yesterday's (UTC) raw partition."""
import datetime as dt
import os

import boto3

glue = boto3.client("glue")


def handler(event, context):
    day = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    run = glue.start_job_run(JobName=os.environ["JOB"], Arguments={"--DT": day})
    return {"job_run_id": run["JobRunId"], "dt": day}
