"""Calendar reference (holidays, rest days, working Saturdays, school breaks, special days, advent).
Checks reference/calendar.csv and uploads it to s3://<bucket>/raw/reference/calendar/calendar.csv

Usage: python3 scripts/upload_calendar.py bkk-delay-radar-<fiokazonosito> reference/calendar.csv [--dry-run]
Categories: public_holiday, rest_day, working_saturday, school_break, special, season
"""
import argparse
import csv
import datetime as dt
import sys

CATS = {"public_holiday", "rest_day", "working_saturday", "school_break", "special", "season"}


def check(rows):
    errors = []
    for i, r in enumerate(rows, start=2):
        try:
            a, b = dt.date.fromisoformat(r["start_date"]), dt.date.fromisoformat(r["end_date"])
        except (KeyError, ValueError):
            errors.append(f"line {i}: dates must be YYYY-MM-DD")
            continue
        if b < a:
            errors.append(f"line {i}: end_date is before start_date")
        if r.get("category") not in CATS:
            errors.append(f"line {i}: category must be one of {sorted(CATS)}")
        if not (r.get("label") or "").strip():
            errors.append(f"line {i}: label is empty")
    return errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bucket")
    ap.add_argument("csv_path")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    text = open(a.csv_path, encoding="utf-8-sig").read()
    rows = list(csv.DictReader(text.splitlines()))
    errors = check(rows)
    for e in errors:
        print("ERROR", e)
    for r in rows:
        print(f"  {r['start_date']} … {r['end_date']}  {r['category']:16} {r['label']}")
    if errors:
        sys.exit("Fix the errors above first; nothing was uploaded.")
    if a.dry_run:
        print(f"{len(rows)} rows OK. dry run: nothing uploaded")
        return
    import boto3
    boto3.client("s3").put_object(Bucket=a.bucket, Key="raw/reference/calendar/calendar.csv",
                                  Body=text.encode("utf-8"), ContentType="text/csv")
    print(f"{len(rows)} rows uploaded to s3://{a.bucket}/raw/reference/calendar/calendar.csv")


if __name__ == "__main__":
    main()
