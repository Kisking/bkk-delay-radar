"""When was each teammate's lab awake (yesterday and today, Budapest time)?

A lab is awake while its own collector writes a snapshot every 5 minutes. Snapshots copied in from
teammates (bkk-merge) are left out: a snapshot written by its own lab is stored within a few
minutes of the time in its name, a copied one hours later.

Usage (CloudShell):
    python3 scripts/team_timeline.py Kinga=063258860606 Zoli=767837692825 Barbi=324025139266
    python3 scripts/team_timeline.py --days 3 Kinga=063258860606 …
Teammates' buckets are readable only after they ran scripts/share_bucket.sh with your account id.
"""
import argparse
import datetime as dt
from zoneinfo import ZoneInfo

import boto3

BP = ZoneInfo("Europe/Budapest")
GAP_MIN = 15          # longer silence = the lab was asleep
OWN_WITHIN_S = 600    # written within 10 minutes of its name = collected by this lab itself


def own_snapshots(s3, bucket, days_utc):
    times = []
    for day in days_utc:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"raw/tripupdates/dt={day}/"):
            for o in page.get("Contents", []):
                stamp = o["Key"].rsplit("/", 1)[-1][:15]
                try:
                    t = dt.datetime.strptime(stamp, "%Y%m%dT%H%M%S").replace(tzinfo=dt.timezone.utc)
                except ValueError:
                    continue
                if abs((o["LastModified"] - t).total_seconds()) <= OWN_WITHIN_S:
                    times.append(t)
    return sorted(times)


def segments(times):
    out = []
    for t in times:
        if out and (t - out[-1][1]).total_seconds() <= GAP_MIN * 60:
            out[-1][1] = t
        else:
            out.append([t, t])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("people", nargs="+", help="Name=account_id")
    ap.add_argument("--days", type=int, default=2, help="how many local days to show (default: yesterday and today)")
    a = ap.parse_args()
    now = dt.datetime.now(BP)
    first_local = (now - dt.timedelta(days=a.days - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    days_utc = sorted({(first_local + dt.timedelta(hours=h)).astimezone(dt.timezone.utc).strftime("%Y-%m-%d")
                       for h in range(0, int((now - first_local).total_seconds() // 3600) + 2)})
    s3 = boto3.client("s3")
    print(f"Ébren töltött idő {first_local:%m.%d.} 00:00 óta (budapesti idő), saját gyűjtés alapján\n")
    for person in a.people:
        name, _, acct = person.partition("=")
        bucket = f"bkk-delay-radar-{acct}"
        try:
            times = [t for t in own_snapshots(s3, bucket, days_utc) if t.astimezone(BP) >= first_local]
        except Exception as e:
            print(f"== {name} ({acct}) ==\n  nem olvasható: {type(e).__name__} (megosztotta veled?)\n")
            continue
        segs = segments(times)
        awake_h = sum((e - s).total_seconds() for s, e in segs) / 3600 + len(segs) * 5 / 60
        print(f"== {name} ({acct}) == összesen kb. {awake_h:.1f} óra ébren, {len(segs)} szakasz")
        if not segs:
            print("  ebben az időszakban nem gyűjtött (a lab aludt, vagy nem indították)\n")
            continue
        for s, e in segs:
            s_l, e_l = s.astimezone(BP), e.astimezone(BP)
            start_txt = "már ébren volt" if s_l <= first_local + dt.timedelta(minutes=10) else f"indult {s_l:%m.%d. %H:%M}"
            end_txt = "most is ébren" if (now - e_l).total_seconds() <= GAP_MIN * 60 else f"elaludt {e_l:%m.%d. %H:%M} után"
            print(f"  {start_txt:<22} → {end_txt:<26} ({(e - s).total_seconds() / 3600 + 5 / 60:.1f} óra)")
        print()


if __name__ == "__main__":
    main()
