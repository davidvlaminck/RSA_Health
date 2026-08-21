# JSON File Queue voor Dedicated SQLite Writer

## Doel

Deze aanpak zorgt ervoor dat meerdere Python-processen veilig schrijfopdrachten kunnen aanbieden, terwijl slechts één proces effectief naar SQLite schrijft.

De bestaande situatie:

```text
proces_1 leest intensief uit SQLite
proces_2 schrijft elke 10 à 20 seconden naar SQLite
proces_3 schrijft occasioneel naar SQLite
proces_4 schrijft occasioneel naar SQLite
proces_5 schrijft occasioneel naar SQLite
```

Het probleem is dat SQLite maar één actieve writer tegelijk aankan. WAL helpt vooral om readers en één writer beter samen te laten werken, maar lost meerdere gelijktijdige writers niet volledig op.

De gekozen oplossing:

```text
producer processen
        |
        v
JSON-bestandjes in queue-map
        |
        v
dedicated sqlite_file_writer.py
        |
        v
SQLite database
```

Alle processen die vroeger rechtstreeks naar SQLite schreven, maken voortaan kleine JSON-bestandjes aan. Eén aparte writer-service verwerkt die bestanden en schrijft naar SQLite.

---

## Waarom JSON-bestandjes in plaats van Redis?

Voor deze situatie is de schrijffrequentie relatief beperkt: ongeveer één intensieve write-run elke 10 à 20 seconden, plus enkele occasionele writers.

Daarom is een file-based queue met JSON-bestanden een goede keuze:

- geen extra Redis-service nodig;
- zeer weinig overhead;
- makkelijk te inspecteren met `ls`, `cat`, `tail`, enzovoort;
- jobs blijven zichtbaar op disk;
- goed te combineren met systemd;
- robuust bij crashes als het correct wordt geïmplementeerd;
- ideaal voor lage tot matige volumes.

Redis blijft een goed alternatief voor hogere volumes, zeer lage latency of echte queue-semantiek met blocking reads. Voor deze use-case is dat waarschijnlijk overkill.

---

## Basisstructuur op disk

Gebruik één vaste queue-map, bijvoorbeeld:

```text
/opt/data-platform/sqlite_queue/
├── pending/
├── processing/
├── done/
└── failed/
```

Betekenis:

```text
pending/     jobs die klaarstaan om verwerkt te worden
processing/  jobs die momenteel verwerkt worden of waar de writer crashte
done/        succesvol verwerkte jobs
failed/      jobs die niet verwerkt konden worden
```

Maak de directories aan:

```bash
sudo mkdir -p /opt/data-platform/sqlite_queue/pending
sudo mkdir -p /opt/data-platform/sqlite_queue/processing
sudo mkdir -p /opt/data-platform/sqlite_queue/done
sudo mkdir -p /opt/data-platform/sqlite_queue/failed
sudo chown -R deploy:deploy /opt/data-platform/sqlite_queue
```

Pas `deploy:deploy` aan als jouw services onder een andere user draaien.

---

# Sectie 1: Proces dat JSON-bestandjes aanmaakt

## Verantwoordelijkheid van een producer-proces

Een producer-proces is elk Python-script dat vroeger rechtstreeks naar SQLite schreef.

Vanaf nu mag zo'n proces niet meer rechtstreeks naar SQLite schrijven. Het doet enkel dit:

```text
1. maak een Python dictionary met de gewenste actie;
2. schrijf die dictionary als JSON naar een tijdelijk .tmp-bestand;
3. flush en fsync het bestand;
4. rename het .tmp-bestand atomisch naar .json;
5. stop.
```

De dedicated writer verwerkt de `.json` daarna later.

---

## Waarom eerst `.tmp` en daarna rename naar `.json`?

De writer scant alleen naar bestanden met extensie `.json`.

Als een producer rechtstreeks naar `job123.json` zou schrijven, kan de writer dat bestand zien terwijl het nog maar half geschreven is. Dan kan de writer corrupte of onvolledige JSON proberen te lezen.

Daarom schrijft de producer eerst naar:

```text
job123.tmp
```

Zolang het bestand `.tmp` heet, negeert de writer het.

Pas wanneer het bestand volledig geschreven en geflusht is, wordt het atomisch hernoemd naar:

```text
job123.json
```

Op Linux is een rename binnen hetzelfde filesystem atomisch. Dat betekent dat de writer nooit een half bestand ziet. De writer ziet ofwel geen job, ofwel een volledig kant-en-klare `.json` job.

Gebruik hiervoor bij voorkeur `os.replace()`.

Belangrijk:

- `tmp_path` en `final_path` moeten op hetzelfde filesystem staan;
- niet eerst naar `/tmp` schrijven en daarna verplaatsen naar `/opt/...` als dat mogelijk een andere mount is;
- schrijf de `.tmp` direct in dezelfde `pending/` directory.

---

## JSON-formaat voor een job

Gebruik een uniform formaat:

```json
{
  "job_id": "20260806_224501_123456_abcd...",
  "action": "insert_measurement",
  "payload": {
    "source": "proces_2",
    "value": 123,
    "timestamp": "2026-08-06T22:45:00"
  },
  "created_at": "2026-08-06T22:45:01.123456"
}
```

Aanbevolen velden:

```text
job_id      unieke id van de job
action      type actie die de writer moet uitvoeren
payload     data die nodig is voor de actie
created_at  moment waarop de job werd aangemaakt
```

---

## Herbruikbare producer-helper

Maak bijvoorbeeld een bestand:

```bash
nano /opt/data-platform/sqlite_writer/pipeline_state.py
```

Inhoud:

```python
import json
import os
import uuid
from datetime import datetime
from pathlib import Path

QUEUE_PENDING_DIR = Path("/opt/data-platform/sqlite_queue/pending")


def enqueue_sqlite_job(action: str, payload: dict) -> str:
    """
    Zet een SQLite schrijfopdracht klaar als JSON-bestand.

    Deze functie schrijft bewust eerst naar .tmp en hernoemt daarna
    atomisch naar .json, zodat de writer nooit halfgeschreven bestanden ziet.
    """

    QUEUE_PENDING_DIR.mkdir(parents=True, exist_ok=True)

    job_id = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4().hex}"

    tmp_path = QUEUE_PENDING_DIR / f"{job_id}.tmp"
    final_path = QUEUE_PENDING_DIR / f"{job_id}.json"

    message = {
        "job_id": job_id,
        "action": action,
        "payload": payload,
        "created_at": datetime.utcnow().isoformat()
    }

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(message, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, final_path)

    return job_id
```

---

## Voorbeeldgebruik in een producer-proces

In een bestaand script vervang je directe SQLite-writes door een queue-job.

Oude aanpak:

```python
import sqlite3

conn = sqlite3.connect("/opt/data-platform/data.db")
cursor = conn.cursor()

cursor.execute(
    """
    INSERT INTO measurements (source, value, timestamp)
    VALUES (?, ?, ?)
    """,
    ("proces_2", 123, "2026-08-06T22:45:00")
)

conn.commit()
conn.close()
```

Nieuwe aanpak:

```python
from sqlite_writer.pipeline_state import enqueue_sqlite_job

job_id = enqueue_sqlite_job(
    action="insert_measurement",
    payload={
        "source": "proces_2",
        "value": 123,
        "timestamp": "2026-08-06T22:45:00"
    }
)

print(f"SQLite job aangemaakt: {job_id}")
```

Belangrijk:

```text
Producer-processen openen SQLite niet meer om te schrijven.
Ze maken alleen nog JSON-jobs aan.
```

---

## Meerdere acties ondersteunen

Je kan verschillende acties definiëren:

```python
enqueue_sqlite_job(
    action="insert_measurement",
    payload={
        "source": "sensor_a",
        "value": 123,
        "timestamp": "2026-08-06T22:45:00"
    }
)
```

```python
enqueue_sqlite_job(
    action="update_status",
    payload={
        "object_id": 42,
        "status": "processed",
        "updated_at": "2026-08-06T22:46:00"
    }
)
```

De writer beslist op basis van `action` welke SQL moet worden uitgevoerd.

---

# Sectie 2: Proces dat JSON-bestanden verwerkt

## Verantwoordelijkheid van de dedicated writer

De dedicated writer is het enige proces dat schrijft naar SQLite.

Het doet dit:

```text
1. scan pending/ naar .json-bestanden;
2. verplaats een job naar processing/;
3. lees de JSON;
4. voer de correcte SQL uit;
5. commit de transactie;
6. verplaats de job naar done/;
7. bij fout: rollback en verplaats naar failed/.
```

Bij opstart zet de writer eventueel achtergebleven bestanden uit `processing/` terug naar `pending/`. Dat is belangrijk als de writer-service midden in een verwerking crashte.

---

## Voorbeeld dedicated writer

Maak het bestand:

```bash
nano /opt/data-platform/sqlite_file_writer.py
```

Inhoud:

```python
import json
import os
import shutil
import sqlite3
import time
import logging
from pathlib import Path

BASE_DIR = Path("/opt/data-platform/sqlite_queue")
PENDING_DIR = BASE_DIR / "pending"
PROCESSING_DIR = BASE_DIR / "processing"
DONE_DIR = BASE_DIR / "done"
FAILED_DIR = BASE_DIR / "failed"

DB_PATH = "/opt/data-platform/data.db"
POLL_INTERVAL_SECONDS = 1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


def ensure_directories() -> None:
    for directory in [PENDING_DIR, PROCESSING_DIR, DONE_DIR, FAILED_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def recover_processing_jobs() -> None:
    """
    Als de writer eerder crashte, kunnen er jobs in processing/ zijn blijven staan.
    We zetten die terug naar pending/, zodat ze opnieuw verwerkt worden.
    """

    for stuck_file in PROCESSING_DIR.glob("*.json"):
        target = PENDING_DIR / stuck_file.name
        logging.warning("Herstel processing job naar pending: %s", stuck_file.name)
        os.replace(stuck_file, target)


def open_database() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")

    return conn


def ensure_processed_jobs_table(conn: sqlite3.Connection) -> None:
    """
    Deze tabel voorkomt dubbele verwerking van dezelfde job_id.

    Dit is belangrijk bij crashes. Bijvoorbeeld:
    - SQL commit is gelukt;
    - writer crasht voor het JSON-bestand naar done/ verplaatst is;
    - bij restart komt dezelfde job terug in pending/.

    Dankzij processed_jobs kan de writer zien dat die job al verwerkt werd.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_jobs (
            job_id TEXT PRIMARY KEY,
            processed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def mark_job_as_processing_if_new(conn: sqlite3.Connection, job_id: str) -> bool:
    """
    Registreert job_id in processed_jobs.

    Return:
    - True als dit een nieuwe job is;
    - False als de job_id al bestond en dus al verwerkt werd.
    """

    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO processed_jobs (job_id) VALUES (?)",
        (job_id,)
    )

    return cursor.rowcount == 1


def process_message(conn: sqlite3.Connection, message: dict) -> None:
    action = message["action"]
    payload = message["payload"]

    cursor = conn.cursor()

    if action == "insert_measurement":
        cursor.execute(
            """
            INSERT INTO measurements (source, value, timestamp)
            VALUES (?, ?, ?)
            """,
            (
                payload["source"],
                payload["value"],
                payload["timestamp"]
            )
        )

    elif action == "update_status":
        cursor.execute(
            """
            UPDATE objects
            SET status = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                payload["status"],
                payload["updated_at"],
                payload["object_id"]
            )
        )

    else:
        raise ValueError(f"Onbekende action: {action}")


def handle_job(conn: sqlite3.Connection, job_path: Path) -> None:
    processing_path = PROCESSING_DIR / job_path.name
    done_path = DONE_DIR / job_path.name
    failed_path = FAILED_DIR / job_path.name

    try:
        os.replace(job_path, processing_path)

        with open(processing_path, "r", encoding="utf-8") as f:
            message = json.load(f)

        job_id = message["job_id"]

        is_new_job = mark_job_as_processing_if_new(conn, job_id)

        if not is_new_job:
            conn.rollback()
            logging.warning("Job was al verwerkt, verplaats naar done: %s", job_id)
            shutil.move(processing_path, done_path)
            return

        process_message(conn, message)
        conn.commit()

        shutil.move(processing_path, done_path)
        logging.info("Job verwerkt: %s", job_id)

    except Exception:
        conn.rollback()
        logging.exception("Fout bij verwerken van job: %s", job_path.name)

        if processing_path.exists():
            shutil.move(processing_path, failed_path)

        time.sleep(1)


def main() -> None:
    ensure_directories()
    recover_processing_jobs()

    conn = open_database()
    ensure_processed_jobs_table(conn)

    logging.info("SQLite file queue writer gestart")

    while True:
        jobs = sorted(PENDING_DIR.glob("*.json"))

        if not jobs:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        for job_path in jobs:
            handle_job(conn, job_path)


if __name__ == "__main__":
    main()
```

---

## Belangrijke opmerkingen bij de writer

### 1. Alleen deze writer schrijft naar SQLite

Dit is de kern van de oplossing.

Alle andere scripts mogen nog wel lezen uit SQLite, maar niet meer schrijven.

```text
Writes -> altijd via JSON queue
Reads  -> mogen rechtstreeks naar SQLite
```

---

### 2. WAL blijft zinvol

De writer zet:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=30000;
```

Ook readers mogen deze instellingen gebruiken.

Voor readers is minimaal dit nuttig:

```python
conn = sqlite3.connect("/opt/data-platform/data.db", timeout=30)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA busy_timeout=30000;")
```

Hou read-transacties kort. Laat geen cursor minutenlang open staan terwijl een writer wil schrijven.

---

### 3. Idempotency is belangrijk

De tabel `processed_jobs` voorkomt dubbele verwerking.

Zonder zo'n mechanisme kan dit gebeuren:

```text
1. writer verwerkt job;
2. SQLite commit lukt;
3. writer crasht voor het JSON-bestand naar done/ verplaatst is;
4. bij restart wordt dezelfde job opnieuw verwerkt.
```

Met `processed_jobs` ziet de writer dat de `job_id` al verwerkt is.

Voor extra veiligheid kan je daarnaast ook natuurlijke unieke constraints gebruiken in je echte tabellen, bijvoorbeeld:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_measurements_unique
ON measurements (source, timestamp);
```

Of bij inserts:

```sql
INSERT OR IGNORE INTO measurements (source, value, timestamp)
VALUES (?, ?, ?)
```

Gebruik dit alleen als het inhoudelijk klopt dat dubbele records genegeerd mogen worden.

---

### 4. Failed jobs bewust manueel bekijken

Alles in `failed/` moet je kunnen inspecteren.

Voorbeeld:

```bash
ls -lh /opt/data-platform/sqlite_queue/failed
cat /opt/data-platform/sqlite_queue/failed/jobnaam.json
```

Mogelijke oorzaken:

- onbekende `action`;
- ontbrekend veld in `payload`;
- constraint error in SQLite;
- verkeerde datatypes;
- databasepad fout;
- permissieprobleem.

Verplaats een gecorrigeerde job eventueel terug naar pending:

```bash
mv /opt/data-platform/sqlite_queue/failed/jobnaam.json /opt/data-platform/sqlite_queue/pending/
```

---

# Systemd service voor de writer

Maak een servicebestand:

```bash
sudo nano /etc/systemd/system/sqlite_file_writer.service
```

Inhoud:

```ini
[Unit]
Description=SQLite File Queue Writer
After=network.target

[Service]
User=deploy
WorkingDirectory=/opt/data-platform
ExecStart=/usr/bin/python3 /opt/data-platform/sqlite_file_writer.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Activeer de service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable sqlite_file_writer.service
sudo systemctl start sqlite_file_writer.service
```

Status bekijken:

```bash
sudo systemctl status sqlite_file_writer.service
```

Live logs bekijken:

```bash
journalctl -u sqlite_file_writer.service -f
```

Laatste 100 logregels:

```bash
journalctl -u sqlite_file_writer.service -n 100 --no-pager
```

---

# Cleanup van oude done-bestanden

De map `done/` zal blijven groeien.

Je kan oude verwerkte jobs periodiek verwijderen, bijvoorbeeld jobs ouder dan 14 dagen:

```bash
find /opt/data-platform/sqlite_queue/done -type f -name "*.json" -mtime +14 -delete
```

Zet dit eventueel in een cronjob of systemd timer.

Voor een eenvoudige cronjob:

```bash
crontab -e
```

Voeg toe:

```cron
0 3 * * * find /opt/data-platform/sqlite_queue/done -type f -name "*.json" -mtime +14 -delete
```

Dit verwijdert elke nacht om 03:00 oude done-jobs.

Verwijder `failed/` niet automatisch. Die wil je manueel kunnen controleren.

---

# Testprocedure

## 1. Maak een testjob

```bash
cd /opt/data-platform
python3 - <<'PY'
from sqlite_writer.pipeline_state import enqueue_sqlite_job

job_id = enqueue_sqlite_job(
    action="insert_measurement",
    payload={
        "source": "manual_test",
        "value": 123,
        "timestamp": "2026-08-06T22:45:00"
    }
)

print(job_id)
PY
```

## 2. Controleer pending

```bash
ls -lh /opt/data-platform/sqlite_queue/pending
```

## 3. Start of check de writer

```bash
sudo systemctl restart sqlite_file_writer.service
journalctl -u sqlite_file_writer.service -f
```

## 4. Controleer done

```bash
ls -lh /opt/data-platform/sqlite_queue/done
```

## 5. Controleer failed

```bash
ls -lh /opt/data-platform/sqlite_queue/failed
```

Als de job in `failed/` staat, bekijk dan de logs:

```bash
journalctl -u sqlite_file_writer.service -n 100 --no-pager
```

---

# Implementatiechecklist

## Aan producer-kant

- [ ] Alle SQLite write-code verwijderen uit producer-processen.
- [ ] `pipeline_state.py` (met enqueue_sqlite_job) toevoegen.
- [ ] Producers gebruiken `enqueue_sqlite_job(...)`.
- [ ] Jobs worden eerst als `.tmp` geschreven.
- [ ] Jobs worden daarna met `os.replace()` naar `.json` hernoemd.
- [ ] Producers schrijven naar `/opt/data-platform/sqlite_queue/pending`.
- [ ] Producer-user heeft schrijfrechten op de queue-map.

## Aan writer-kant

- [ ] `sqlite_file_writer.py` toevoegen.
- [ ] Writer is het enige proces dat naar SQLite schrijft.
- [ ] Writer maakt directories aan als ze ontbreken.
- [ ] Writer herstelt oude `processing/` jobs naar `pending/` bij startup.
- [ ] Writer gebruikt WAL en busy_timeout.
- [ ] Writer verplaatst jobs correct naar `processing/`, `done/` of `failed/`.
- [ ] Writer gebruikt `processed_jobs` voor idempotency.
- [ ] Writer draait als systemd service.
- [ ] Logs zijn zichtbaar via `journalctl`.

## Aan beheer-kant

- [ ] Cleanup voorzien voor oude `done/` jobs.
- [ ] `failed/` jobs worden niet automatisch verwijderd.
- [ ] Er is een procedure om failed jobs te inspecteren en eventueel terug naar pending te zetten.
- [ ] Readers houden transacties kort.
- [ ] Er schrijft geen enkel ander proces rechtstreeks naar SQLite.

---

# Samenvatting

De gekozen oplossing is:

```text
Alle schrijvende processen maken JSON-jobbestanden aan.
De JSON-bestanden komen in pending/.
Eén dedicated writer-service verwerkt die jobs.
Alleen die writer schrijft naar SQLite.
Readers mogen rechtstreeks uit SQLite blijven lezen.
```

Dit past goed omdat de zogenaamde intensieve writer maar elke 10 à 20 seconden schrijft. Voor die frequentie is Redis waarschijnlijk niet nodig. Een JSON file queue is eenvoudiger, transparanter en heeft weinig overhead.

De belangrijkste veiligheidsregels zijn:

```text
1 job = 1 JSON-bestand
schrijf eerst naar .tmp
rename daarna atomisch naar .json
writer verwerkt alleen .json
writer gebruikt processing/done/failed
writer is idempotent via job_id
```

Als dit correct wordt toegepast, vermijd je SQLite write-lock conflicten zonder meteen naar PostgreSQL of Redis te migreren.
