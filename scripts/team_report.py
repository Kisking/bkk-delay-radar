"""Team collection report: who collected how much, how long each lab was awake, and when the city was
covered (and by whom) or not covered at all. Budapest time.

Run it in your own CloudShell (anyone in the team):
    python3 ~/kit/scripts/team_report.py              # the last 3 days (today included), the whole team
    python3 ~/kit/scripts/team_report.py --days 7     # the last 7 days
    python3 ~/kit/scripts/team_report.py Kinga=063258860606 Zoli=767837692825   # other people

You see a teammate only if they shared their raw/ folder with you:  bash scripts/share_bucket.sh YOUR_ACCOUNT_ID
Snapshots copied in by bkk-merge are not counted as that lab's own collection: a lab's own snapshot is
written within minutes of the time in its name, a copy hours later.
"""
import argparse
import datetime as dt
from zoneinfo import ZoneInfo

import boto3

TEAM = ["Kinga=063258860606", "Zoli=767837692825", "Barbi=324025139266"]
BP = ZoneInfo("Europe/Budapest")
SLOT = 300                       # one snapshot every 5 minutes
GAP_SLOTS = 3                    # up to 15 minutes of silence still counts as awake
OWN_WITHIN_S = 600               # written within 10 minutes of its name = this lab's own snapshot
KINDS = ("tripupdates", "alerts", "vehiclepositions")
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def scan(s3, bucket, days_utc, since):
    """Own 5-minute slots (from TripUpdates) and own megabytes (all three feeds)."""
    slots, size = set(), 0
    for kind in KINDS:
        for day in days_utc:
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"raw/{kind}/dt={day}/"):
                for o in page.get("Contents", []):
                    stamp = o["Key"].rsplit("/", 1)[-1][:15]
                    try:
                        t = dt.datetime.strptime(stamp, "%Y%m%dT%H%M%S").replace(tzinfo=dt.timezone.utc)
                    except ValueError:
                        continue
                    if abs((o["LastModified"] - t).total_seconds()) > OWN_WITHIN_S:
                        continue                                  # copied in from a teammate
                    if since <= t:
                        size += o["Size"]
                    if kind == "tripupdates":
                        slots.add(int(t.timestamp()) // SLOT)
    return slots, size


def awake(slots):
    """Fill short silences: the lab counts as awake between snapshots up to 15 minutes apart."""
    out, prev = set(), None
    for s in sorted(slots):
        if prev is not None and 1 < s - prev <= GAP_SLOTS:
            out.update(range(prev + 1, s))
        out.add(s)
        prev = s
    return out


def runs(slots_in_order, key):
    """Consecutive slots with the same key -> (first, last, key)."""
    out = []
    for s in slots_in_order:
        k = key(s)
        if out and out[-1][2] == k and out[-1][1] == s - 1:
            out[-1][1] = s
        else:
            out.append([s, s, k])
    return out


def hm(slot):
    return dt.datetime.fromtimestamp(slot * SLOT, dt.timezone.utc).astimezone(BP).strftime("%H:%M")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("people", nargs="*", default=TEAM, help="Name=account_id (default: the whole team)")
    ap.add_argument("--days", type=int, default=3, help="how many days to show, today included (default 3)")
    a = ap.parse_args()
    now = dt.datetime.now(BP)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    local_days = [today - dt.timedelta(days=i) for i in range(a.days - 1, -1, -1)]
    days_utc = sorted({(d + dt.timedelta(hours=h)).astimezone(dt.timezone.utc).strftime("%Y-%m-%d")
                       for d in local_days for h in (0, 12, 23)})
    s3 = boto3.client("s3")
    me = boto3.client("sts").get_caller_identity()["Account"]

    people, own, mb, missing = [], {}, {}, []
    for p in a.people:
        name, _, acct = p.partition("=")
        try:
            slots, size = scan(s3, f"bkk-delay-radar-{acct}", days_utc, local_days[0])
        except Exception as e:
            missing.append((name, acct, type(e).__name__))
            continue
        people.append(name)
        own[name], mb[name] = awake(slots), size / 1e6
    width = max([len(n) for n in people] + [6])
    now_slot = int(now.timestamp()) // SLOT

    for d in local_days:
        lo = int(d.timestamp()) // SLOT
        hi = min(int((d + dt.timedelta(days=1)).timestamp()) // SLOT, now_slot)   # the current 5 minutes are not over yet
        day_slots = range(lo, hi)
        team = {s for s in day_slots if any(s in own[n] for n in people)}
        total_h = len(day_slots) * SLOT / 3600
        print(f"\n================ {d:%Y-%m-%d} {DAYS[d.weekday()]}{' (so far)' if d == today else ''} ================")
        print(f"Team coverage: {len(team) * SLOT / 3600:.1f} / {total_h:.1f} h "
              f"({100 * len(team) / max(1, len(day_slots)):.0f}%), not covered: {(len(day_slots) - len(team)) * SLOT / 3600:.1f} h\n")

        print(f"{'':{width}}   awake  starts  snapshots  covered alone")
        for n in people:
            mine = [s for s in day_slots if s in own[n]]
            starts = sum(1 for r in runs(mine, lambda s: 1) if r[0] - 1 not in own[n])   # waking up, not just midnight
            alone = sum(1 for s in mine if not any(s in own[m] for m in people if m != n))
            print(f"{n:{width}}  {len(mine) * SLOT / 3600:4.1f} h  {starts:6d}  {len(mine):9d}  {alone * SLOT / 3600:9.1f} h")

        # hour strip: one character = 30 minutes;  █ awake  ▒ partly awake  · asleep
        print(f"\n{'':{width}}  " + "".join(f"{h:<6d}" for h in range(0, 24, 3)) + "   █ awake  ▒ partly  · asleep")
        for n in people + ["TEAM"]:
            line = ""
            for b in range(48):
                block = range(lo + b * 6, lo + b * 6 + 6)
                if block[0] >= hi:
                    line += " "
                    continue
                cov = sum(1 for s in block if (s in team if n == "TEAM" else s in own[n]))
                line += "█" if cov >= 5 else ("▒" if cov else "·")
            print(f"{n:{width}}  {line}")

        print("\nWho covered when:")
        for first, last, who in runs(list(day_slots), lambda s: tuple(n for n in people if s in own[n])):
            length = (last - first + 1) * SLOT / 60
            label = " + ".join(who) if who else "NOT COVERED"
            end = "now" if last == now_slot - 1 else hm(last + 1)
            print(f"  {hm(first)}–{end:5}  {label:<{width * 3 + 6}}  ({length / 60:.1f} h)" if length >= 60
                  else f"  {hm(first)}–{end:5}  {label:<{width * 3 + 6}}  ({length:.0f} min)")

    print(f"\n================ total ({a.days} day{'s' if a.days > 1 else ''}) ================")
    first_slot = int(local_days[0].timestamp()) // SLOT
    for n in people:
        hours = sum(1 for s in own[n] if first_slot <= s < now_slot) * SLOT / 3600
        print(f"{n:{width}}  {hours:5.1f} h awake, {mb[n]:7.0f} MB collected by this lab")
    for name, acct, err in missing:
        hint = "is this your own account?" if acct == me else f"ask them to run: bash scripts/share_bucket.sh {me}"
        print(f"{name:{width}}  not readable ({err}): {hint}")


if __name__ == "__main__":
    main()
