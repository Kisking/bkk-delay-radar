#!/usr/bin/env bash
# BKK Delay Radar - teljes telepítés egy saját AWS Academy Learner Labba, egyetlen paranccsal.
#
#   export BKK_API_KEY="a-sajat-kulcsod"          # kötelező (opendata.bkk.hu, Kulcskezelés)
#   export ALERT_EMAIL="nev@stud.uni-corvinus.hu"  # nem kötelező: e-mail riasztás
#   bash install.sh
#
# Újrafuttatható: ami már létezik, azt frissíti. Kb. 10-15 perc.
set -uo pipefail
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}
export AWS_PAGER=""
cd "$(dirname "$0")"
: "${BKK_API_KEY:?Előbb: export BKK_API_KEY=\"a-sajat-kulcsod\"}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text) || { echo "Nincs AWS-kapcsolat: fut a lab?"; exit 1; }
BUCKET=bkk-delay-radar-$ACCOUNT
SITE=$BUCKET-site
step() { echo; echo "=================== $1 ==================="; }
warn() { echo "FIGYELEM: $1 (a rendszer enélkül is működik, később pótolható)"; }

step "1/7 Adatgyűjtés (BKK élő feedek, időjárás, menetrend)"
bash deploy.sh ingest || { echo "Az adatgyűjtés telepítése nem sikerült, itt megállok."; exit 1; }

step "2/7 Várakozás az első menetrendre (max. 6 perc)"
for i in $(seq 1 24); do
  if aws s3 ls "s3://$BUCKET/raw/gtfs_static_txt/" --recursive | grep -q "stops.txt"; then echo "menetrend megérkezett"; break; fi
  sleep 15; echo "  még töltődik… ($((i * 15)) s)"
done
aws s3 ls "s3://$BUCKET/raw/gtfs_static_txt/" --recursive | grep -q "stops.txt" \
  || { echo "A menetrend nem érkezett meg. Futtasd: bash deploy.sh static, majd újra: bash install.sh"; exit 1; }

step "3/7 Referenciatáblák (naptár, buszsávok, jelzőlámpák, Duna-part, bevásárlóközpontok)"
python3 scripts/upload_calendar.py "$BUCKET" reference/calendar.csv || warn "a naptár feltöltése nem sikerült"
python3 scripts/build_buslane_reference.py "$BUCKET" || warn "buszsávok: az OpenStreetMap szerver foglalt volt"
python3 scripts/build_signals_reference.py "$BUCKET" || warn "jelzőlámpák: a Budapest Közút szerver nem válaszolt"
python3 scripts/build_osm_reference.py "$BUCKET" || warn "Duna-part / bevásárlóközpontok: az OpenStreetMap szerver foglalt volt"

step "4/7 Feldolgozás (Glue) és Athena-tábla"
bash deploy.sh glue || { echo "A Glue telepítése nem sikerült."; exit 1; }
sleep 5
bash scripts/athena.sh sql/01_create_table.sql || warn "az Athena-tábla létrehozása nem sikerült; futtasd: bash scripts/athena.sh sql/01_create_table.sql"

step "5/7 Dashboard"
bash deploy.sh publish || { echo "A dashboard telepítése nem sikerült."; exit 1; }

step "6/7 E-mail riasztás"
if [ -n "${ALERT_EMAIL:-}" ]; then
  bash deploy.sh alerts || warn "a riasztás telepítése nem sikerült"
else
  echo "kihagyva (nincs ALERT_EMAIL). Később: export ALERT_EMAIL=\"…\" && bash deploy.sh alerts"
fi

step "7/7 Kész"
echo "Az adatgyűjtés fut: 5 percenként BKK-pillanatkép, óránként időjárás."
echo "Az első feldolgozás a következő teljes nap után, este 21:00-kor (helyi idő) fut, a dashboard 22:00-kor frissül."
echo "Ellenőrzés 10 perc múlva:  python3 scripts/kpi_snapshots.py $BUCKET"
echo "Dashboard:                http://$SITE.s3-website-$AWS_DEFAULT_REGION.amazonaws.com"
[ -n "${ALERT_EMAIL:-}" ] && echo "Ne felejtsd el megerősíteni az AWS e-mailjét (lehet, hogy a spamben van)."
echo "Biztonság: unset BKK_API_KEY"
