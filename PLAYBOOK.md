> **Gyors telepítés egy új fiókba:** lásd a `TELEPITES.md`-t (`bash install.sh`).

# BKK Delay Radar – építési playbook

Csapat: Kinga Kisfaludy (KK), Barbara Pecsenye (BP), Zoltán Borbély (ZB). Környezet: AWS Academy Learner Lab, `us-east-1`, `LabRole`.
A csomagban lévő kódot szintaxisra ellenőriztem, az ingest Lambda és a protobuf-dekódoló logika mockolt teszten lefutott. **Éles AWS-en még semmit nem futtattam**, ezért minden fázisnál ott az "Kész, ha" ellenőrzés. A Glue job, az Athena-tábla és a publikáló Lambda az első futásnál javításra szorulhat.

## 0. Szabályok, mielőtt bármit csinálsz

1. **Az API-kulcs titok.** Csak környezeti változóban (`BKK_API_KEY`) és AWS Secrets Managerben él. Ne kerüljön fájlba, GitHubra, diára vagy chatbe. Ha kiszivárog, kérj újat a Kulcskezelésben.
2. **Minden azonnal induljon el az 1. héten.** A "késési hotspot" csak akkor értelmes, ha legalább 1–2 hétnyi gyűjtött adat van. Minden nap, amit a gyűjtés indításával késtek, hiányzik a végső bemutatóról.
3. **Learner Lab korlátai:** csak `us-east-1` és `us-west-2`, IAM-szerepet nem hozhatsz létre (a `LabRole`-t használd mindenhez), a lab kreditkerete véges. **Mérve (2026. okt. 30.): a lab 4 óra után elalszik, ilyenkor a Lambdák nem futnak, és az EventBridge-szabályok sem indulnak. A **Start Lab** újranyomása után minden magától folytatódik, újratelepíteni nem kell.** Ezért kell a lent leírt menetrend (0/b szakasz).
4. A költségbecslés a Calculatorból jön, ez a terv `eu-central-1`-re szól, a megvalósítás `us-east-1`-en fut. Ezt mondd el a bemutatón.

## 0/b. Learner Lab menetrend (a gyűjtés életben tartása)

A **Start Lab** gomb újra megnyomása a futó labot nem szakítja meg, csak az időzítőt állítja vissza 4 órára.

| Mikor (helyi idő) | Teendő | Miért |
|---|---|---|
| kb. 07:30 | Start Lab | a nappali forgalom elejét lefedi |
| kb. 11:30 | Start Lab | |
| kb. 15:30 | Start Lab | |
| kb. 19:30 | Start Lab | a 20:00/21:00 UTC-s napi futások (21:00 és 22:00 helyi idő) ide esnek |
| kb. 23:30 | Start Lab | az éjszakai 4 órát is fedi (03:30-ig) |

- A napi folyamat az esti ablakban fut: statikus GTFS letöltése 19:00 UTC, Glue 20:00 UTC (a tegnapi UTC napot dolgozza fel), publikálás 21:00 UTC. A deck ennek megfelelően nem 06:00-ra, hanem **22:00 CET-re** ígéri a táblázatot.
- Ha egy napot kihagysz, a gyűjtésben hézag lesz, a mérés (`kpi_snapshots.py`) ezt külön kimutatja és a KPI-be nem számítja bele, de a deckre írd ki a lefedettséget őszintén.
- **Megjegyzés:** a statikus GTFS ütemezését át kell állítani, mert az első telepítés éjszakára (02:30 UTC) tette: `aws events put-rule --name bkk-static-daily --schedule-expression "cron(0 19 * * ? *)"`

## 1. A csomag tartalma

| Fájl | Mire jó |
|---|---|
| `deploy.sh` | Lépésenkénti telepítés: `ingest`, `static`, `glue`, `publish`, `alerts` |
| `infra/template.yaml` + `infra/deploy_stack.sh` | A teljes rendszer egyetlen CloudFormation stackként |
| `lambdas/ingest_rt.py` | 5 percenként lekéri a TripUpdates-t, nyersen S3-ba menti (csak standard library, nem kell layer) |
| `lambdas/ingest_weather.py` | Óránként Open-Meteo hívás |
| `lambdas/ingest_static.py` | Naponta letölti a statikus GTFS zip-et (kb. 62 MB), és a 4 szükséges fájlt (stop_times, trips, routes, stops) kicsomagolva is S3-ba menti |
| `lambdas/start_glue.py` | Naponta indítja a Glue jobot a tegnapi (UTC) napra |
| `glue/delay_job.py` | PySpark: dekódol, a késést a menetrendből számolja (előrejelzett idő − menetrendi idő), időjárást kapcsol, Parquetet és metrikafájlt ír |
| `sql/*.sql` | Athena-tábla és lekérdezések |
| `lambdas/publish_dashboard.py` + `site/index.html` | Napi JSON és statikus dashboard S3-on (késéslista, óránkénti bontás, járműtípus, legrosszabb megállók, időjárás, lefedettség) |
| `lambdas/healthcheck.py` | Óránkénti ellenőrzés, e-mail riasztás SNS-en át |
| `scripts/kpi_snapshots.py` | KPI-mérés: aktív órák, megbízhatóság, alvási hézagok |
| `scripts/backfill.py` | Hiányzó napok megkeresése és pótlása |
| `scripts/athena.sh` | Egy SQL fájl futtatása Athenában |

Adatfolyam: EventBridge → Lambda → `raw/` (S3) → Glue → `curated/delays/` (Parquet) → Athena → publikáló Lambda → statikus oldal.

## 2. Ütemezés és felelősök

| Fázis | Mikor | Ki | Eredmény |
|---|---|---|---|
| F0 Helyi PoC | 3 nov. | KK | `bkk_poc.py` kimenete, mért snapshot-méret |
| F1 Gyűjtés indítása | 3–5 nov. | KK | Lambdák és ütemezések élnek |
| F2 Ellenőrzés | 5–8 nov. | KK, ZB | Nyers adat S3-ban, riasztások |
| F3 Glue | 9–15 nov. | BP | Parquet a curated zónában |
| F4 Athena | 16–17 nov. | BP | Lekérdezhető tábla, hotspot-lista |
| F5 Dashboard | 16–22 nov. | ZB | Statikus oldal élő adattal |
| F6 Monitoring, biztonság | 23–29 nov. | ZB, KK | Riasztások, DLQ, titkosítás |
| F7 KPI és prezentáció | 24 nov. – 6 dec. | Mind | Mért KPI-k, képernyőképek, végső deck |

## 3. Fázisok lépésről lépésre

### F0 – Helyi PoC (KK, 3 nov.)
1. `export BKK_API_KEY="..."` (PowerShell: `$env:BKK_API_KEY="..."`), majd `python bkk_poc.py`.
2. Jegyezd fel: a TripUpdates snapshot mérete, az entitások száma, hány rekordban van `delay` mező.
3. Írd át a decken és a Calculatorban a snapshot-méretet (most 1 MB-os feltevés). Ha az üzenet nagyobb, mint ~3 MB, állítsd a mintavételt 10 percre.

**Kész, ha:** a script lefut, és látsz dekódolt `trip_id`-ket.

### F1 – Gyűjtés indítása (KK, 3–5 nov.)
1. Learner Lab: *Start Lab*, az "AWS Details"-ből a CLI-hitelesítő adatok bemásolása (`~/.aws/credentials`), majd `aws sts get-caller-identity`.
2. `./deploy.sh ingest` a csomag mappájában.
3. Egy perc múlva: `aws s3 ls s3://bkk-delay-radar-<account>/raw/ --recursive | head`.

**Kész, ha:** `raw/tripupdates/…`, `raw/weather/…` és `raw/gtfs_static/…` alatt vannak fájlok.

### F2 – Ellenőrzés és riasztások (KK, ZB, 5–8 nov.)
1. Lefedettség és megbízhatóság: `python3 scripts/kpi_snapshots.py bkk-delay-radar-<account>`. A kimenet napi aktív órákat, a megbízhatóságot az aktív szakaszokon belül (cél ≥ 95 %) és a lab-alvási hézagokat adja meg.
2. Lambda hibafigyelő riasztás:
```bash
aws cloudwatch put-metric-alarm --alarm-name bkk-ingest-rt-errors \
  --namespace AWS/Lambda --metric-name Errors --dimensions Name=FunctionName,Value=bkk-ingest-rt \
  --statistic Sum --period 900 --evaluation-periods 1 --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold --treat-missing-data notBreaching
aws cloudwatch put-metric-alarm --alarm-name bkk-ingest-rt-missing \
  --namespace AWS/Lambda --metric-name Invocations --dimensions Name=FunctionName,Value=bkk-ingest-rt \
  --statistic Sum --period 900 --evaluation-periods 1 --threshold 3 \
  --comparison-operator LessThanThreshold --treat-missing-data breaching
```
3. **Lab-lezárási teszt: lefutott.** Eredmény: a gyűjtés a lab lejártakor leáll, az újraindítás után magától folytatódik. Lásd a 0/b szakaszt.
4. DLQ ellenőrzése: `aws sqs get-queue-attributes --queue-url <bkk-ingest-dlq> --attribute-names ApproximateNumberOfMessages`.

**Kész, ha:** legalább egy nap gyűjtése megvan, a riasztások léteznek, és a `kpi_snapshots.py` kimenete mutatja az aktív szakaszokat.

### F3 – Glue job (BP, 9–15 nov.; az első próbát már lefuttattuk)
**Mit tudtunk meg a feedről (2026. okt. 30., 08:00 UTC snapshot):** a BKK feedben **nincs `delay` mező**, csak előrejelzett idő (`arrival.time`, `departure.time`) minden megállónál. A késést ezért a menetrendből számoljuk: `késés = előrejelzett idő − menetrendi idő` (stop_times.txt + a járat szolgálati napja). Csak a valódi előrejelzéseket használjuk: a járat `SCHEDULED`, a megállóbejegyzés is `SCHEDULED` (a `NO_DATA` a menetrend másolata, a `SKIPPED` kihagyott megálló, az `ADDED` és `CANCELED` járatok nincsenek a menetrendben).

1. Statikus fájlok kicsomagolása S3-ba (egyszer most, utána naponta magától): `bash deploy.sh static`, majd 2 perc múlva ellenőrzés: `aws s3 ls s3://bkk-delay-radar-<account>/raw/gtfs_static_txt/ --recursive --human-readable` (a `stop_times.txt` kb. 460 MB legyen).
2. `bash deploy.sh glue` (feltölti a scriptet, létrehozza a jobot és az Athena-adatbázist).
3. Kézi teszt egy napra: `aws glue start-job-run --job-name bkk-delay-job --arguments '{"--DT":"2026-10-30"}'`.
4. Állapot: `aws glue get-job-run --job-name bkk-delay-job --run-id <id> --query 'JobRun.[JobRunState,ExecutionTime,ErrorMessage]' --output text`.
5. Metrika: `aws s3 cp s3://bkk-delay-radar-<account>/curated/_metrics/dt=2026-10-30.json -`

A metrikafájl fontos mezői: `rows_with_delay_pct` (cél: a legtöbb sor), `unmatched_bkk_trip_pct` és `unmatched_bkk_stop_pct` (KPI: < 2 %, csak a BKK-járatokra), `other_operator_rows_pct` (más üzemeltető vonatai, pl. IC, S, Z jelzésűek, ezeket nem írja a curated táblába), `added_or_canceled_trip_rows_pct`, `no_data_rows_pct_of_scheduled_trips`, `abs_delay_over_3h_pct` (hibajelző: ha magas, a menetrendi idő vagy a szolgálati nap rossz), `delay_median_s`, `delay_p95_s`.

**Kész, ha:** a Parquet létrejön, `rows_with_delay_pct` nagy, és `unmatched_bkk_trip_pct` < 2 %. A futásidőt (`ExecutionTime`, másodperc) írd fel a költségdiához.

### F4 – Athena (BP, 16–17 nov.)
1. `sql/01_create_table.sql`: a `BUCKET` szót cseréld a saját bucket nevedre, majd futtasd az Athena konzolban (adatbázis: `bkk`, a lekérdezési eredmény helye: `s3://<bucket>/athena-results/`).
2. `sql/02_hotspots.sql` és `sql/03_weather_effect.sql` futtatása.

**Kész, ha:** a hotspot-lekérdezés 10 sornyi vonalat ad vissza, és 10 másodpercnél gyorsabb (KPI).

### F5 – Dashboard (ZB, 16–22 nov.)
1. `./deploy.sh publish`.
2. Teszt: `aws lambda invoke --function-name bkk-publish build/out.json`, majd nyisd meg a kiírt URL-t.
3. Ha a lab nem engedi a publikus elérést (WARN üzenet), a bucket tartalmát presigned URL-lel vagy képernyőképpel mutasd be a prezentáción.

**Kész, ha:** az oldalon látszik a top 10, az óránkénti átlag és a rain/dry táblázat, a láblécben pedig a hivatkozás („Data source: BKK Zrt., CC BY 4.0").

### F6 – Monitoring és biztonság (ZB, KK, 23–29 nov.)
1. Napi folyamat riasztása: Glue job hiba (`aws cloudwatch put-metric-alarm` a Glue metrikára, vagy kézi napi ellenőrzés a konzolon).
2. Titkosítás (opcionális, a deck KMS-t ígér): hozz létre egy kulcsot (`aws kms create-key`), és állítsd be a bucket alapértelmezett titkosítását (`aws s3api put-bucket-encryption`). Először teszteld egy külön teszt-bucketen, mert hibás kulcs esetén a Glue és a Lambda olvasása leállhat.
3. IAM: a Learner Labben csak a `LabRole` használható, ezt a bemutatón írd le (a tervben "least-privilege szerepek" szerepelnek).

### F7 – KPI-k és prezentáció (mind, 24 nov. – 6 dec.)
| KPI (deck) | Hogyan mérd |
|---|---|
| ≥ 95 % snapshot az aktív lab-szakaszokban | `scripts/kpi_snapshots.py` (egyedi 5 perces sávok az aktív szakaszokon belül) |
| Előző napi táblázat 22:00 CET-ig kész | A publikáló Lambda ideje a CloudWatch-ban (Glue 20:00 UTC, publikálás 21:00 UTC; téli időben 21:00 és 22:00 CET) |
| < 2 % nem párosítható `trip_id` | `curated/_metrics/*.json` |
| Lekérdezés < 10 s, oldal < 5 s | Athena futási idő, kézi mérés |
| Költség ≤ $10 | Learner Lab kreditegyenleg a futások előtt és után |
| Használhatóság | Az elemző a top 10-et kigyűjti kézi lépés nélkül (demó) |

Képernyőképek a prezentációhoz: S3 `raw/` mappa, EventBridge szabályok, Lambda monitor, Glue futás, Athena eredmény, a dashboard, a CloudWatch riasztások, a KPI-szkript kimenete.

## 3/b. Hiányzó napok pótlása és a hibatűrés

**Mi történik, ha a lab aludt?** A napi Glue-futás (20:00 UTC) elmarad, és magától nem pótlódik. A `backfill.py` megkeresi azokat a napokat, amelyekre van nyers adat, de nincs feldolgozás, és sorban lefuttatja őket.

```bash
cd ~/kit
python3 scripts/backfill.py bkk-delay-radar-<account>              # csak megmutatja, mi hiányzik
python3 scripts/backfill.py bkk-delay-radar-<account> --run        # le is futtatja, legrégebbitől
python3 scripts/backfill.py bkk-delay-radar-<account> --run --max 3
```

A mai napot mindig kihagyja, mert még nem ért véget. Egy-egy futás Glue-kreditbe kerül, ezért a `--max` korlátozza, hány napot indít egyszerre.

**A Glue-job hibatűrése (v3):**
- Ha egy napra **nincs időjárásadat**, a job nem áll le: figyelmeztetést ír, és a késéseket időjárás nélkül számolja. A metrikában a `weather_joined: false` jelzi.
- Ha egy napra **nincs járatadat** (a lab egész nap aludt), a job világos üzenettel áll le: `No TripUpdates snapshots for dt=…`. Így a naplóból azonnal látszik az ok.

## 3/c. E-mail riasztás

Óránként fut egy ellenőrzés, ami megnézi, érkeznek-e friss snapshotok, és frissült-e a dashboard. Ha gond van, egy e-mailt küld, és csak egyet: amíg a helyzet tart, nem spammel. Amikor helyreáll, küld egy "back to normal" levelet.

```bash
cd ~/kit
export ALERT_EMAIL="sajat@email.hu"
bash deploy.sh alerts
```

**Fontos:** az AWS küld egy megerősítő levelet, és a riasztások csak azután érkeznek, hogy rákattintottál a linkre.

Mit figyel:
- **Gyűjtés leállt:** 20 percen belül kevesebb mint 3 snapshot érkezett, pedig az előző ellenőrzéskor még ment. A lab alvása ilyen, ezért ez várható és normális üzenet.
- **A dashboard elavult:** 30 óránál régebbi a `data.json`.
- **A dashboard hiányzik.**

Teszt e-mail bármikor: `aws sns publish --topic-arn <arn> --subject test --message hello`

A riasztás kikapcsolása: `aws events disable-rule --name bkk-healthcheck-hourly`

## 3/d. A teljes rendszer egy paranccsal (CloudFormation)

A `deploy.sh` lépésenként telepít, és ez jó a fejlesztéshez. A `infra/template.yaml` viszont **egyetlen stackként** írja le az egészet: bucketek, titok, 6 Lambda, 5 ütemezés, Glue job és adatbázis, Athena tábla és workgroup, riasztások, SNS. Ez a reprodukálható változat.

```bash
cd ~/kit
export ALERT_EMAIL="sajat@email.hu"      # elhagyható, akkor nincs riasztás
bash infra/deploy_stack.sh
```

A script becsomagolja és feltölti a Lambda-kódot és a Glue scriptet (a CloudFormation csak S3-ból tud olvasni), majd létrehozza vagy frissíti a stacket. A végén kiírja a dashboard URL-jét.

**Friss stack után két lépés kell még:**
```bash
aws secretsmanager put-secret-value --secret-id bkk/api-key --secret-string "A-KULCSOD"
aws s3 cp site/index.html s3://bkk-delay-radar-<account>-site/index.html --content-type text/html
```

**Leállítás:** `bash infra/deploy_stack.sh --delete`. A két adatbucket szándékosan megmarad (`DeletionPolicy: Retain`), hogy a gyűjtött adat ne vesszen el. Ezeket külön kell törölni, ha akarod.

**Hasznos paraméterek:** a `SchedulesEnabled=DISABLED` az összes ütemezést leállítja a stack törlése nélkül, a `PollMinutes` a mintavételi gyakoriságot állítja.

**Figyelem:** ha a meglévő erőforrásokat a `deploy.sh`-val hoztad létre, a CloudFormation nem veszi át őket: ugyanazon a néven ütközne. Vagy a meglévőket töröld előbb, vagy adj a stacknek más nevet (`STACK=...`). A sablont `cfn-lint`-tel ellenőriztem, de éles AWS-en még nem futott.

## 3/e. Referenciaadatok (buszsáv, jelzőlámpa, rendezvények)

Ezek ritkán változó táblák, amelyeket a Glue a késésekhez kapcsol. Nem kell naponta frissíteni őket.

| Tábla | Script | Forrás | Mikor frissítsd |
|---|---|---|---|
| Buszsáv melletti megállók | `scripts/build_buslane_reference.py` | OpenStreetMap (ODbL) | havonta |
| Jelzőlámpás csomópontok közelsége | `scripts/build_signals_reference.py` | Budapest Közút térképszolgáltatás | havonta |
| Nagy rendezvények | `scripts/upload_events.py` + `reference/events.csv` | kézi lista | ha új esemény kerül be |
| Duna-partoldal (Buda/Pest) | `scripts/build_osm_reference.py` | OpenStreetMap (ODbL) | havonta |
| Bevásárlóközpontok közelsége | `scripts/build_osm_reference.py` | OpenStreetMap (ODbL) | havonta |
| Naptár (ünnepek, szünetek, advent) | `scripts/upload_calendar.py` + `reference/calendar.csv` | rendeletek, oktatási naptár | évente |

Mindegyik `--dry-run` kapcsolóval csak kiírja az eredményt, anélkül pedig feltölti a `raw/reference/` alá.

**Rendezvénylista kitöltése:** egy sor egy esemény. A `nearest_stop` oszlopba egy közeli BKK-megálló nevét írd, ahogy a menetrendben szerepel (pl. `Puskás Ferenc Stadion`), a script ebből keresi ki a koordinátát. Forrásnak a helyszínek oldalai, az MLSZ műsora vagy hírek jók.

**Licencek:** az OpenStreetMap adatnál a dashboardon fel kell tüntetni: „© OpenStreetMap contributors". A Budapest Közút rétegéhez nem találtunk nyílt licencet, ezért csak elemzésre használjuk, a nyers pontokat nem tesszük közzé.

**Mezőellenőrzés:** a `scripts/check_feed_fields.py` megmutatja, kitölti-e a BKK a `block_id`, `agency_id`, `shape_dist_traveled` mezőket és a járműpozíciók adatait. Ebből dől el, hogyan számoljuk a késés átöröklődését, az üzemeltetőt és az útvonalhosszt.

**Útlezárások:** gépi formában a Nemzeti Adathozzáférési Ponton (napportal.kozut.hu) érhetők el, regisztráció után. Hogy a budapesti utcák lezárásai benne vannak-e, regisztráció nélkül nem ellenőrizhető. A közösségi közlekedést érintő fennakadásokat a BKK Alerts feedje úgyis lefedi.

## 3/f. Miért nem használunk TomTom-forgalmi adatot

Megvizsgáltuk a TomTom Traffic Flow API-t (ingyenes keret, regisztrációval), de a felhasználási feltételek kizárják a mi felhasználásunkat:

- az ingyenes használat csak a TomTom termékeinek belső kipróbálására és tesztelésére vonatkozik (2.2. pont, „Evaluation Use"),
- az eredmények tárolása tilos, csak a válasz fejlécében megadott ideig lehet gyorsítótárazni (11.4. pont),
- tilos származtatott adatbázist létrehozni, amelyben a TomTom-adat más adattal keveredik (11.6.1. pont).

A gyűjtést ezért leállítottuk, az adatot és a kulcsot töröltük. A forgalom közelítésére a BKK saját járműpozícióit (sebesség) és a szakaszonkénti késésnövekedést használjuk, ezek a BKK nyílt licence (CC BY 4.0) alá tartoznak.

## 3/g. Feldolgozás v4 és dashboard v2 (2026. november)

A Glue-job 4-es verziója a késés mellé 45 új oszlopot számol: naptár és napszak, sötétség, zóna, Duna-oldal és hídszakasz, buszsáv, lámpák, bevásárlóközpont, útvonalhossz és pozíció, induló késés, szakaszonkénti késésnövekedés, járatköz és összetorlódás, bővített időjárás, fennakadás-kategória (Alerts), mért sebesség és az előző járat késése (járműpozíciók), valamint az előrejelzés frissessége. Minden új forrás opcionális: ha egy napra hiányzik, a job lefut, és az adott oszlopok üresek maradnak. A napi metrikafájlban (`curated/_metrics/`) a `job_version: 4` és a `warnings` mező mutatja, mi hiányzott.

**Élesítés, sorrendben:**
1. `bash deploy.sh glue` – feltölti az új scriptet és újra létrehozza a jobot.
2. `bash scripts/athena.sh sql/00_drop_table.sql`, majd `bash scripts/athena.sh sql/01_create_table.sql` – az Athena-tábla az új sémával. A Parquet-fájlokhoz nem nyúl.
3. Próbafutás egy napra: `aws glue start-job-run --job-name bkk-delay-job --arguments '{"--DT":"2026-11-01"}'`, utána a metrikafájl ellenőrzése.
4. `python3 scripts/backfill.py <bucket> --upgrade --run` – a régebbi verzióval feldolgozott napokat is újrafuttatja.
5. `bash deploy.sh publish`, majd `aws lambda invoke --function-name bkk-publish --cli-read-timeout 0 /tmp/out.json && cat /tmp/out.json` – az új dashboard.

**A dashboard új részei:** pontosság (legfeljebb 1 perc korán, 3 perc késve), napi trend, csúcs és csúcson kívül, a legtöbb késést termelő és a leglassabb szakaszok, zónák, buszsáv / lámpák / Duna / bevásárlóközpont hatása, fennakadások, időjárás és sötétség, késés átöröklődése, összetorlódás, valamint lefedettségi figyelmeztetés és „few data" jelölés a kevés megfigyelésű vonalaknál.

**Adatminőségi szabályok (v5):** egy sor kimarad az átlagokból, ha
- az utolsó előrejelzés 10 percnél régebbi volt az érkezéshez képest (a lab alvása vagy az UTC-éjfél vágása miatt; a sorok kb. 1,7%-a), vagy
- a teljes járat el van tolva: legalább 5 megálló, mindenhol legalább 10 perc késés, és a késés 2 percnél kevesebbet változik (a járatok kb. 0,4%-a; jellemzően rossz járathoz rendelt jármű, pl. a 60B éjszakai járatai 98–122 perces „késéssel").

A sorok a táblában maradnak (`quality_flag`, `reliable`), a Pipeline health pedig naponta mutatja, mennyi esett ki. A pontosan 0 késésű járatokat („untracked") csak számoljuk: ezek valószínűleg nem valós idejű követésből jönnek.

**Dashboard v3 (2026. november):** két nézet (utolsó 7 nap / teljes időszak, gomb a fejlécben, a `#all` link a teljes időszakot nyitja), párhuzamos Athena-lekérdezések, és új részek: a hét napjai, ez a hét és az előző, különleges napok, temetői vonalak Mindenszentek körül, legkevésbé megbízható és korán járó vonalak, eső járműtípusonként, vonalválasztó (megállónkénti késés) és térkép (Leaflet, CARTO-alaptérkép, szakaszok a késésnövekedés szerint színezve). A vonalválasztó és a térkép adata külön fájlokban van (`lines*.json`, `map*.json`), és csak akkor töltődik be, amikor a panel láthatóvá válik. A publikáló Lambda időkorlátja 600 s, memóriája 512 MB.

**Glue v6 és dashboard v3.1:** az ok nélküli fennakadásokat a hatásuk szerint soroljuk be (terelés, szünetel, csökkentett szolgáltatás). A dashboardon új: szokatlan órák (incidensek), iskolai szünet, csomópontok és metró/HÉV-csatlakozás, megállósűrűség, hőmérséklet, és az eső hatása azonos naptípuson és órán belül. A szakaszlistákból kimaradnak a végállomási peronpárok (pl. „Új köztemető (Kozma utca) → Új köztemető") és a garázsok; a hét napjainál csak a teljes (legalább 50 000 soros) napok számítanak. A metró nem szerepel a BKK valós idejű adatában.

**Hézagkitöltés a csapattársakkal (`bkk-merge`):** a csapattársak csak olvasásra megosztják egymással a `raw/` pillanatképeiket (`scripts/share_bucket.sh`), és mindenki `bkk-merge` Lambdája óránként átmásolja azokat az 5 perces időpontokat, amelyek nála hiányoznak (2 nap visszamenőleg, a legfrissebb 15 percet kihagyva). A fájlnevek változatlanok, ezért a Glue, a KPI-mérés és a backfill módosítás nélkül működik; az átmásolt fájlok `copied-from` metaadatot kapnak. A health check a fájlnévben lévő időpont alapján méri a gyűjtést, így az átmásolt régi fájlok nem takarják el, ha a saját gyűjtő leállt.

**Glue v7 és az ismétlődő adathibák szabálya:** az eltolt járat szabálya az első megállót már nem számolja (ott a menetrendi idő látszik), és a megengedett szórás 2 perc vagy a késés 10%-a, amelyik nagyobb. A dashboard emellett kizárja azokat a járatokat, amelyek legalább két különböző napon minden megállóban legalább 30 percet késtek (pl. a 937-es éjszakai busz hajnali járatai éjszakáról éjszakára 40–100 perccel), mert egy valódi esemény nem ismétlődik így; ezek a „Trips left out as data errors" táblában látszanak.

**Glue v8 és dashboard v3.2 (2026. nov. 14.):** ha egy járatra a BKK forgalmi akadályt vagy balesetet jelez, a nagy késése valódi, ezért nem számít eltolt járatnak (nov. 11-én a 98E, 198, 200E stb. járatai így kerültek vissza; a 4-6-os villamos „szünetel" értesítéses járatai továbbra is kiesnek). Új oszlop: `sched_layover_s`, a jármű előző járatának menetrendi vége és a következő járat menetrendi indulása közötti idő. Új panelek: fordulóidő (az indulási késés az előző járat késése és a fordulóidő szerint) és torlódás (csúcsidei sebesség az éjszakaihoz képest, szakaszonként és járműtípusonként). A Pipeline health metrikában: `kept_real_disruption_trips`.

**Fontos a bemutatón:** ezek összefüggések, nem ok-okozati bizonyítékok (pl. a lámpás csomópontok a sűrű belvárosban vannak, ahol amúgy is nagyobb a forgalom).

## 4. Költségőr és leállítás

- Szabályok kikapcsolása: `aws events disable-rule --name bkk-rt-5min` (ugyanígy `bkk-weather-hourly`, `bkk-static-daily`, `bkk-glue-daily`, `bkk-publish-daily`, `bkk-healthcheck-hourly`).
- Ha a Glue több mint 10 percig fut, vagy a kredit gyorsan fogy, kapcsold ki a napi szabályt, és kézzel indítsd.
- Törlés a projekt végén: szabályok, Lambdák, Glue job, bucketek (`aws s3 rb s3://… --force`), titok (`aws secretsmanager delete-secret --force-delete-without-recovery --secret-id bkk/api-key`).

## 5. Hibaelhárítás

| Tünet | Valószínű ok | Teendő |
|---|---|---|
| `HTTP 401/403` az ingest Lambdában | Kulcs még nincs jóváhagyva vagy rossz a titok | Kulcsellenőrzés `bkk_poc.py`-vel, titok frissítése: `./deploy.sh ingest` |
| `AccessDenied` bucket vagy titok műveletnél | Nem a `LabRole` van beállítva | Szerepnév ellenőrzése a függvény konfigurációjában |
| Az ütemezés nem indítja a Lambdát | Hiányzik az `add-permission` | `./deploy.sh ingest` újrafuttatása |
| Athena üres | A projekciós tartomány vagy a `BUCKET` név rossz | A tábla újralétrehozása |
| Glue: `OutOfMemory` a statikus zipnél | Túl kicsi a worker | `--number-of-workers 4` vagy `G.2X` |

## 6. Prompt egy AI kódolóasszisztensnek (pl. Claude Code)

Másold be a csomag mappájában:

```
Szerep: senior AWS adatmérnök. Kontextus: egyetemi projekt (BKK Delay Radar), AWS Academy Learner Lab,
us-east-1, csak a LabRole használható, új IAM-szerepet nem hozhatunk létre. Olvasd el a PLAYBOOK.md-t,
a lambdas/, glue/, sql/ és site/ mappákat, és a deploy.sh-t.
Feladat: [F3 Glue / F4 Athena / F5 Dashboard / hibakeresés] fázis. Futtasd a leírt ellenőrzést, javítsd a hibát
a legkisebb szükséges módosítással, és mondd meg pontosan, mit változtattál.
Szabályok: a BKK API-kulcsot soha ne írd ki, ne naplózd, ne commitold; csak Secrets Managerből/környezeti
változóból olvasd. Ne találj ki API-végpontokat vagy árakat. Ha valamit nem tudsz ellenőrizni, jelöld NEM ELLENŐRZÖTT-nek.
Kimenet: a módosított fájlok, a futtatott parancsok és a "Kész, ha" ellenőrzés eredménye.
```
