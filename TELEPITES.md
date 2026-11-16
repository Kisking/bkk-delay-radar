# BKK Delay Radar – telepítés a saját Learner Labodba

Ezzel a leírással a teljes rendszert felépíted a saját AWS Academy Learner Lab fiókodban: az adatgyűjtést, a napi feldolgozást, az Athena-táblát, a dashboardot és az e-mail riasztást. Egy parancs, kb. 10–15 perc.

## Mi kell hozzá

- **AWS Academy Learner Lab hozzáférés** (a kurzusból).
- **Saját BKK API-kulcs:** regisztrálj a BKK OpenData oldalán (opendata.bkk.hu), és a Kulcskezelés menüben igényelj egy kulcsot. Ingyenes. Ne a csapattársad kulcsát használd.
- **E-mail cím** a riasztásokhoz (nem kötelező).

## Telepítés

1. Nyisd meg a Learner Labot, és nyomd meg a **Start Lab** gombot. Ha a jelző zöld, kattints az **AWS** feliratra.
2. Az AWS-konzolon jobb felül nyisd meg a **CloudShellt** (a `>_` ikon). Ellenőrizd, hogy a régió **N. Virginia (us-east-1)**.
3. Töltsd fel a `bkk_delay_radar_kit.zip` fájlt: **Actions → Upload file**.
4. Másold be a CloudShellbe (a saját kulcsoddal és e-mail címeddel):

```bash
cd ~ && unzip -o "$(ls -t ~/bkk_delay_radar_kit*.zip | head -1)"
cd ~/kit
export BKK_API_KEY="ide-a-sajat-kulcsod"
export ALERT_EMAIL="nev@stud.uni-corvinus.hu"
bash install.sh
unset BKK_API_KEY
```

5. Ha megadtál e-mail címet, az AWS küld egy megerősítő levelet. **Kattints a benne lévő linkre**, különben nem kapsz riasztást. A spam mappát is nézd meg.

A telepítő a végén kiírja a dashboardod címét. Ha közben „FIGYELEM" sort látsz (pl. az OpenStreetMap szervere foglalt), az nem állítja meg a telepítést. Az adott lépést később újra futtathatod, a parancsot ugyanez a sor írja ki.

## Mi történik utána

- **Azonnal:** 5 percenként menti a BKK élő adatait (késések, fennakadások, járműpozíciók), óránként az időjárást, naponta a menetrendet.
- **A következő teljes nap után, este 21:00-kor (helyi idő):** a Glue feldolgozza az előző napot.
- **22:00-kor:** frissül a dashboard. Az első napon még üres, ez normális.

**Ellenőrzés kb. 10 perc múlva:**

```bash
python3 ~/kit/scripts/kpi_snapshots.py bkk-delay-radar-$(aws sts get-caller-identity --query Account --output text)
```

Ha a „Reliability" sorban 1 vagy több pillanatkép szerepel, a gyűjtés fut.

## Napi teendő: ébren tartani a labot

A Learner Lab 4 óra után elalszik, és amíg alszik, semmi nem fut. A **Start Lab** újbóli megnyomása 4 órára visszaállítja az időzítőt, és nem szakítja meg a futó rendszert.

- Napközben kb. 4 óránként nyomd meg.
- **Este 19:00 és 20:00 között mindenképp**, mert 21:00-kor fut a feldolgozás, 22:00-kor a dashboard-frissítés.
- Az **End Lab** és a **Reset** gombot ne nyomd meg: az egyik leállítja, a másik törli a rendszert.

## Reggeli ellenőrzés

Minden újraindítás után hasznos. Másold be egyben:

```bash
B=bkk-delay-radar-$(aws sts get-caller-identity --query Account --output text)
cd ~/kit
python3 scripts/kpi_snapshots.py $B | grep -E "Reliability|UTC" | tail -3
python3 scripts/backfill.py $B | tail -3
aws s3 ls s3://$B-site/data.json
```

- Ha a második parancs egy hiányzó napot ír ki (mert este aludt a lab), pótold: `python3 scripts/backfill.py $B --run`, majd frissítsd a dashboardot: `aws lambda invoke --function-name bkk-publish --cli-read-timeout 0 /tmp/out.json && tail -c 300 /tmp/out.json`

## Hézagok kitöltése a csapattársak adataival

Mindenki labja máskor alszik el. Ha megosztjátok egymással a nyers pillanatképeket, mindenki rendszere óránként átmásolja azokat az 5 perces időpontokat, amelyek nála hiányoznak, de egy csapattársnál megvannak. Amit már magad is gyűjtöttél, azt nem duplázza.

**1. Cseréljétek ki a fiókazonosítókat.** Mindenki futtassa, és küldje el a többieknek az eredményt:

```bash
aws sts get-caller-identity --query Account --output text
```

**2. Mindenki ossza meg a saját adatait a többiekkel** (csak olvasási jog, csak a nyers pillanatképekre):

```bash
cd ~/kit
bash scripts/share_bucket.sh CSAPATTARS1_AZONOSITO CSAPATTARS2_AZONOSITO
```

**3. Próbáld ki, hogy látod-e egy csapattárs adatait:**

```bash
aws s3 ls s3://bkk-delay-radar-CSAPATTARS1_AZONOSITO/raw/tripupdates/ | head -3
```

Ha napokat (`PRE dt=2026-…`) látsz, működik. Ha `AccessDenied` jön, vagy a csapattárs még nem futtatta a 2. lépést, vagy a Learner Lab nem engedi a fiókok közötti hozzáférést. Ilyenkor szólj.

**Egy csapattárs rendszerének ellenőrzése** bármikor, a saját CloudShelledből (a dashboardja nyilvános, a nyers adatai a megosztás után látszanak):

```bash
bash ~/kit/scripts/check_partner.sh CSAPATTARS1_AZONOSITO
```

**4. Kapcsold be az átmásolást:**

```bash
export PARTNER_ACCOUNTS="CSAPATTARS1_AZONOSITO CSAPATTARS2_AZONOSITO"
bash deploy.sh merge
```

A végén ezt látod: `{"partners": 2, "copied": …, "by_partner": {…}, "errors": []}`. Az első futás a hézagos óráidat rögtön kitölti, utána óránként fut.

**Az eredmény** a következő reggeli ellenőrzésnél látszik: a `kpi_snapshots.py` kevesebb vagy rövidebb hézagot mutat, mert a csapattársak által gyűjtött időpontok is beleszámítanak.

## Ki mennyit gyűjtött, és mikor volt lefedve a nap

Bárki futtathatja a saját CloudShelljében:

```bash
python3 ~/kit/scripts/team_report.py            # az utolsó 3 nap (a mai is), az egész csapat
python3 ~/kit/scripts/team_report.py --days 7   # az utolsó 7 nap
```

Naponta megmutatja, ki hány órát volt ébren, hányszor indította a labot, hány pillanatképet gyűjtött, egy óránkénti idősávot (█ ébren, ▒ részben, · aludt), és szakaszonként, hogy mikor ki fedte le a napot, és mikor nem fedte le senki. A merge által átmásolt pillanatképek nem számítanak bele a gyűjtésbe.

Egy csapattársat csak akkor látsz, ha megosztotta veled az adatait. Hogy mindenki mindenkit lásson, mindenki futtassa a `share_bucket.sh`-t **a másik kettő** azonosítójával (egy parancsban, mert a parancs a korábbi megosztást felülírja).

## Gyakori hibák

| Tünet | Megoldás |
|---|---|
| A CloudShell nem csinál semmit, `(END)` látszik | nyomd meg a `q` betűt |
| `ModuleNotFoundError: google` | `pip install gtfs-realtime-bindings` (csak az `inspect_*` és `check_*` scriptekhez kell) |
| `HTTP 401` vagy `403` a BKK-tól | rossz vagy még nem aktív a kulcs: `export BKK_API_KEY="…"` és `bash deploy.sh ingest` |
| A feltöltött fájl neve `(1)`-et kapott | idézőjelben add meg: `"bkk_delay_radar_kit (1).zip"` |
| A CloudShell hosszú futás közben kilép | a Glue a háttérben tovább fut; a backfillt futtasd `--max 2` kapcsolóval, két naponként |
| Üres a dashboard | az első teljes napot csak másnap este dolgozza fel |

## Költség

A Learner Lab kreditjéből megy, kb. 6–7 dollár havonta (a legnagyobb tétel a napi Glue-feldolgozás). A lab felületén látod, mennyi kredited maradt.

## Biztonság

- A BKK-kulcsot **ne másold** chatbe, e-mailbe vagy GitHubra. A rendszer a Secrets Managerben tárolja, a kódban nincs benne.
- Ha mégis kikerült, igényelj újat, és futtasd újra: `bash deploy.sh ingest`.

## Részletesen

A teljes leírás (architektúra, minden komponens, adatminőségi szabályok, döntések) a `PLAYBOOK.md`-ben van.
