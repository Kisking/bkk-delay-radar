#!/usr/bin/env bash
# Checks a teammate's BKK Delay Radar from your own CloudShell.
#   bash scripts/check_partner.sh 767837692825
# 1-2 use the public dashboard (always works), 3 needs the teammate's share_bucket.sh.
P=${1:?usage: bash scripts/check_partner.sh TEAMMATE_ACCOUNT_ID}
SITE=http://bkk-delay-radar-$P-site.s3-website-us-east-1.amazonaws.com
echo "== 1. Dashboard oldal =="
code=$(curl -s -o /dev/null -w "%{http_code}" "$SITE/"); echo "HTTP $code  (200 = fent van)"
echo "== 2. Dashboard adatai =="
if curl -s -f "$SITE/data.json" -o /tmp/partner.json; then
python3 - <<'EOF'
import json
d = json.load(open("/tmp/partner.json")); s = d.get("summary", {})
print(f"utolsó frissítés: {d.get('generated_utc')} UTC")
print(f"napok: {s.get('days')}, gyűjtött órák: {s.get('hours_collected')}, időszak: {s.get('first_day')} – {s.get('last_day')}")
print("hibás panelek:", s.get("failed_panels") or "nincs")
for r in (d.get("pipeline") or [])[-3:]:
    print(f"  {r.get('dt')}: v{r.get('job_version')}, {r.get('curated_rows')} sor, párosítatlan {r.get('unmatched_bkk_trip_pct')} %, kiszűrt {r.get('unreliable_rows_pct')} %")
EOF
else echo "még nincs data.json: az első frissítés a következő teljes nap után, este 22:00-kor (helyi idő) készül el"; fi
echo "== 3. Nyers pillanatképek (csak ha megosztotta) =="
out=$(aws s3 ls "s3://bkk-delay-radar-$P/raw/tripupdates/dt=$(date -u +%F)/" --recursive 2>&1)
if echo "$out" | grep -q "AccessDenied"; then echo "még nincs megosztva (AccessDenied): neki kell futtatnia a share_bucket.sh-t"
elif echo "$out" | grep -q "NoSuchBucket"; then echo "nincs ilyen bucket: rossz azonosító, vagy még nem telepített"
elif [ -z "$out" ]; then echo "ma még nincs pillanatkép (alszik a labja?)"
else echo "ma $(echo "$out" | wc -l) pillanatkép, a legutóbbi: $(echo "$out" | tail -1 | awk '{print $1, $2}') UTC"; fi
