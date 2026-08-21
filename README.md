## Purpose

This repository serves two main goals:

1. **Health page and API** — It exposes a health endpoint that serves a web page showing server health parameters, as well as an API endpoint that returns those parameters and their values in JSON format so they can be fetched programmatically.

2. **Pipeline orchestration** — It coordinates a nightly data pipeline by tracking the status of every phase in a central SQLite database and signaling independent services when to start, pause, or resume.

## Kernconcept

Elke pipeline-run start om **middernacht lokale tijd (Europe/Brussels)**. Bij een reset of crash bevragen alle scripts eerst SQLite (`health.db`) om te weten waar in de pipeline ze zitten. SQLite is de **enige bron van waarheid** voor de huidige fase, status en bericht.

Er lopen **verschillende processen naast elkaar** (Arango-sync, PostGIS-sync, RSA-queries). De orchestrator observeert alleen de status en coördineert de overgangen en Drive-stappen. Het is dus niet zo dat alleen de laatste fase telt — elke fase rapporteert zelf zijn status.

## Communicatiemodel

### Reader vs Writer

**Lezen (reading)** — Externe scripts en API-endpoints mogen **rechtstreeks** uit `health.db` lezen. Dit is veilig en vereist geen tussenlaag.

**Schrijven (writing)** — Alle schrijfacties naar SQLite gaan via een **JSON file queue**. Dit voorkomt write-lock conflicten als meerdere processen tegelijk schrijven. Er is precies één dedicated writer die de queue consumeert en naar SQLite schrijft.

```text
producer processen (rsa_health, orchestrator, externe scripts)
        |
        v
JSON-bestandjes in /opt/data-platform/sqlite_queue/pending/
        |
        v
sqlite_file_writer.py (enigste proces dat naar SQLite schrijft)
        |
        v
health.db
```

### Wat de writer verwerkt

De writer verwerkt alleen de volgende acties:

- `insert_snapshot` — server health snapshot
- `insert_db_snapshot` — database health snapshot
- `prune_snapshots` — oude snapshots opschonen
- `update_pipeline_state` — pipeline status updates

### Wat externe repos/scripts doen

Externe repos en scripts die de pipeline willen bijwerken, gebruiken **alleen** `update_pipeline_state`. Ze moeten **niet** direct naar SQLite schrijven.

Gebruik de producer-helper:

```python
from sqlite_writer.sqlite_queue_client import enqueue_sqlite_job

enqueue_sqlite_job(
    action="update_pipeline_state",
    payload={
        "phase": "rsa_queries",
        "status": "completed",
        "updated_at": "2026-08-09T20:00:00Z",
        "message": "RSA queries voltooid",
    },
)
```

De writer verwerkt deze job later asynchroon.

### Waarom een file queue?

SQLite ondersteunt maar één actieve writer tegelijk. WAL helpt readers en één writer, maar lost meerdere gelijktijdige writers niet volledig op. Voor deze use-case (lage schrijffrequentie, geen extra afhankelijkheden) is een file-based queue met JSON-bestanden eenvoudig, transparant en robuust.

## Herstellen na crash of reset

Wanneer een script hervat na een crash of reset, moet het eerst SQLite bevragen om de huidige pipeline-status te weten. Hiervoor is een functie beschikbaar die alle status-updates van **vandaag** ophaalt, zodat externe repos eenvoudig kunnen bepalen waar de pipeline staat.

```python
from sqlite_writer.pipeline_state import PipelineState

pipeline = PipelineState("/opt/data-platform/RSA_Health/health.db")
today_updates = pipeline.get_today_updates()
```

Deze functie retourneert een overzicht van alle fase-updates van de huidige dag (local Brussels time), zodat elke service weet:
- welke fasen al zijn gestart,
- welke fase nu actief is,
- welke fasen nog moeten gebeuren.

## Pipeline overzicht

### Dagelijkse cyclus

Elke nacht om middernacht (Europe/Brussels) start een nieuwe cyclus. De orchestrator reset de pipeline naar de starttoestand en iedere service begint zijn nieuwe cyclus.

### De fasen

| Fase                       | Betekenis                                              |
|----------------------------|--------------------------------------------------------|
| idle                       | Wacht op volgende pipeline run                         |
| sharepoint_to_drive        | SharePoint → Drive marker detectie                     |
| drive_download             | Drive download gestart                                 |
| arango_sync                | ArangoDB synchronisatie                                |
| postgis_sync_pausing       | PostGIS sync pauzeren                                  |
| postgis_sync_paused        | PostGIS sync gepauzeerd                                |
| rsa_queries                | RSA queries uitvoeren                                  |
| postgis_sync_resuming      | PostGIS sync hervatten                                 |
| postgis_sync_running       | PostGIS sync actief                                    |
| drive_upload               | Drive upload gestart                                   |
| drive_to_sharepoint        | Drive → SharePoint marker detectie                     |

### Geldige statussen

| Status    | Betekenis                                    |
|-----------|----------------------------------------------|
| starting  | Actie wordt gestart                          |
| running   | Actie is bezig                               |
| completed | Actie succesvol voltooid                     |
| failed    | Actie mislukt                                |
| aborted   | Actie werd afgebroken (zeldzaam, intern)     |
| time-out  | Actie overschreef tijdslimiet; pipeline gaat verder (alleen rsa_queries) |

### Timeouts

| Fase                      | Timeout | Toelichting                                    |
|---------------------------|---------|-----------------------------------------------|
| arango_sync               | 4 uur   | Geen time-out in normale loop (= safety-net)   |
| postgis_sync_paused       | 10 min  | Bij time-out: gaat verder zonder pauze         |
| rsa_queries               | 3 uur   | RSA rapporteert zelf time-out; orchestrator safety-net 3.5 uur |
| postgis_sync_running      | 10 min  | Bij time-out: gaat verder naar drive_upload    |

## Volledige nachtelijke sequentie

De sequentie is **signaal-based**. De orchestrator observeert `pipeline_state` in SQLite en coördineert alleen de overgangen en de Drive-stappen. Arango-sync, PostGIS-sync en RSA draaien onafhankelijk als services; de orchestrator wacht steeds met een timeout op het verwachte signaal.

```text
00:00  Elke service start een nieuwe cyclus
       (orchestrator reset naar idle/completed)

00:00  Orchestrator              controleert sharepoint_to_drive status
       (Power Automate plaatst marker op Drive)

00:01  Orchestrator              ziet marker → sharepoint_to_drive / running
00:30  Power Automate            sharepoint_to_drive / completed
       (marker verwijderd na verwerking)

00:31  Orchestrator              start drive_download
       RSA                        ziet drive_download / running → start download
00:35  RSA                        drive_download / completed
       (of: failed → stop)

       ~ RSA wacht op arango_sync = completed ~
       (geen time-out in normale loop)

03:00  Arango-sync (onafhankelijk) start → arango_sync / running
       ~ rapporteert vordering per sub-stap (fase blijft running) ~
04:45  Arango-sync               arango_sync / completed

   04:50  Orchestrator              ziet 'completed' → postgis_sync_pausing / running
   04:51  PostGIS-sync              ziet 'pausing' → onderbreekt schrijven
        → schakelt over naar **view maken** (read-only modus voor RSA)
        → postgis_sync_paused / completed

        ~ Orchestrator wacht op 'paused' (max. 10 min) ~
        ~ Bij timeout: orchestrator gaat door zonder pauze (risico acceptabel ~05:00) ~
        ~ PostGIS wacht tot 08:00 op resume signaal (max. 4u interne safety-net) ~

   05:00  RSA ReportLoopRunner (onafhankelijk) start query'n
        → rsa_queries / running
        (alleen als postgis_sync_paused is geregistreerd, of na 10-min timeout)
        ~ rapporteert via PipelineStatusReporter ~

        ~ RSA heeft 3 uur (tot 08:00) ~
        ~ RSA rapporteert zelf: completed of time-out ~
        ~ Bij time-out: stopt rapporten, maakt overzicht,
          zet rsa_queries = time-out → orchestrator gaat door ~

   08:00  Orchestrator              → postgis_sync_resuming / running
        (of: PostGIS start zelf resume na 08:00 bij time-out)
        ~ PostGIS documenteert self-resume als time-out in pipeline_state ~

   08:01  PostGIS-sync              ziet 'resuming' → hervat schrijven
        → postgis_sync_running / completed

   08:02  Orchestrator              start drive_upload
        RSA                        ziet drive_upload / running → start upload
        ~ RSA uploadt het (voor-geassemblede) overzicht ~
   08:10  RSA                        drive_upload / completed

08:10+ Power Automate            start Drive → SharePoint (pollend)
10:00  Orchestrator              marker gedetecteerd → drive_to_sharepoint / completed
       (einde cyclus)

midnight  Orchestrator            reset → idle / completed
```

### Belangrijke gedetailleerde regels

1. **Herstart na crash** — Bij herstart vragen alle scripts eerst SQLite op (`get_today_updates()`) om de huidige fase te weten. Er wordt niet vanaf het begin gestart, maar vanaf waar de pipeline staat.

2. **Parallelle processen** — Arango-sync, PostGIS-sync en RSA draaien onafhankelijk. De orchestrator coördineert alleen overgangen. Het is dus mogelijk dat `arango_sync = completed` is terwijl `rsa_queries` nog steeds `running` is.

3. **SharePoint → Drive** — De orchestrator detecteert de marker op Google Drive en zet de fase op `running` en later `completed`. Power Automate plaatst de marker; RSA_Health verwijdert deze na verwerking.

4. **Drive download** — Wordt door de orchestrator getriggerd. RSA voert de daadwerkelijke download uit en rapporteert `drive_download / completed` of `failed`.

5. **Arango-sync** — Is een volledig onafhankelijke service. De orchestrator wacht op het signaal `arango_sync / completed`. In de normale loop geen time-out; de orchestrator blijft wachten tot Arango klaar is.

6. **PostGIS pauzeren + view maken** — Zodra Arango klaar is, geeft de orchestrator het signaal `postgis_sync_pausing / running`. PostGIS-sync onderbreekt zijn schrijven, schakelt over naar **view maken** (read-only modus, zodat RSA veilig kan readen) en rapporteert `postgis_sync_paused / completed`. De orchestrator wacht max 10 minuten op dit signaal. Loopt deze time-out, gaat de orchestrator door zonder pauze — dit is acceptabel rond 05:00 omdat de data al 5 uur gesynct is en PostgreSQL zelf altijd draait.

7. **RSA-queries starten** — Pas wanneer `postgis_sync_paused / completed` is geregistreerd, start RSA met de queries. RSA controleert zelf eerst SQLite om te zien of de voorwaarde is voldaan.

8. **RSA time-out (3 uur)** — RSA rapporteert zelf zijn time-out (niet de orchestrator). Als RSA niet binnen 3 uur klaar is, stopt het met rapporten uitvoeren, maar maakt het wel overzichtssamenstelling. RSA zet vervolgens `rsa_queries = time-out`. De orchestrator behandelt `time-out` gelijk aan `completed`: PostGIS resume wordt getriggerd, daarna `drive_upload`. RSA voert de upload uit van de voor-geassembleerde overzicht. Het `time-out` signaal is de sleutel om de orchestrator naar de volgende fase te bewegen — zonder deze status blijft de orchestrator hangen.

9. **PostGIS wacht op resume** — PostGIS-sync luistert vanaf het moment van pauzeren naar het resume-signaal in `pipeline_state` (fase `postgis_sync_resuming`). Na 08:00 (local time) start het vanzelf het resume als de orchestrator nog niet heeft getriggerd. Deze self-resume wordt gedocumenteerd in de pipeline_state als `time-out` in het bericht. Maximale pauzetijd is 4 uur (tot max 09:00).

10. **Drive upload** — Wordt door de orchestrator getriggerd. RSA voert de upload uit en rapporteert `drive_upload / completed`.

11. **Drive → SharePoint** — Na afloop plaatst Power Automate een marker. De orchestrator detecteert deze en rondt de cyclus af met `drive_to_sharepoint / completed`.

## Services

### rsa_health (FastAPI service)

De FastAPI-app serveert het dashboard en de health API. Hij voert zelf health-checks uit en plaatst snapshots in de queue via `enqueue_sqlite_job()`.

### sqlite_file_writer

De writer consumeert de JSON file queue en schrijft naar `health.db`. Geen andere component mag direct schrijven naar SQLite.

### Orchestrator

De orchestrator observeert `pipeline_state` in `health.db` en coördineert de pipeline-overgangen. Hij draait als **afzonderlijke service**.

Systemd unit files:

#### rsa_health.service

```ini
[Unit]
Description=RSA Health FastAPI
After=network.target

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/opt/data-platform/RSA_Health
ExecStart=/home/deploy/.local/bin/uv run uvicorn main:app --host 127.0.0.1 --port 8000
StandardOutput=append:/opt/data-platform/logs/rsa_health.log
StandardError=append:/opt/data-platform/logs/rsa_health.log
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### sqlite_file_writer.service

```ini
[Unit]
Description=SQLite File Queue Writer
After=network.target

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/opt/data-platform
Environment="DB_PATH=/opt/data-platform/RSA_Health/health.db"
Environment="SQLITE_QUEUE_DIR=/opt/data-platform/sqlite_queue"
ExecStart=/usr/bin/python3 /opt/data-platform/sqlite_file_writer.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### rsa-health-orchestrator.service

```ini
[Unit]
Description=RSA Health Pipeline Orchestrator
After=network.target

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/opt/data-platform/RSA_Health
ExecStart=/home/deploy/.local/bin/uv run python -m orchestrator.orchestrator --db /opt/data-platform/RSA_Health/health.db
StandardOutput=append:/opt/data-platform/logs/rsa_orchestrator.log
StandardError=append:/opt/data-platform/logs/rsa_orchestrator.log
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Deployment

### 1. Kopieer project naar server

```bash
# Op je lokale machine
scp -r /home/david/PycharmProjects/RSA_Health deploy@server:/opt/data-platform/
```

Of gebruik git:
```bash
ssh deploy@server
cd /opt/data-platform
git clone <repo-url> RSA_Health
```

### 2. Installeer dependencies

```bash
ssh deploy@server
cd /opt/data-platform/RSA_Health
uv sync
```

### 3. Maak config aan

```bash
cp config.example.json config_rsa_health.json
# Bewerk met je database credentials, Drive config, etc.
nano config_rsa_health.json
```

### 4. Genereer Google Drive token (eenmalig)

```bash
uv run python -m orchestrator.orchestrator --db health.db
```

Log in met je Google-account in de browser. Daarna wordt `gdrive_token.pkl` automatisch gebruikt.

### 5. Maak directory structuur aan voor de queue

```bash
sudo mkdir -p /opt/data-platform/sqlite_queue/pending
sudo mkdir -p /opt/data-platform/sqlite_queue/processing
sudo mkdir -p /opt/data-platform/sqlite_queue/done
sudo mkdir -p /opt/data-platform/sqlite_queue/failed
sudo chown -R deploy:deploy /opt/data-platform/sqlite_queue
```

### 6. Installeer systemd services

Kopieer de servicebestanden:

```bash
sudo cp sqlite_writer/sqlite_file_writer.service /etc/systemd/system/
sudo cp rsa_health.service /etc/systemd/system/
sudo cp orchestrator/rsa_orchestrator.service /etc/systemd/system/
```

### 7. Activeer en start services

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sqlite_file_writer.service
sudo systemctl enable --now rsa-health.service
sudo systemctl enable --now rsa-health-orchestrator.service
```

Controleer status:
```bash
sudo systemctl status sqlite_file_writer.service
sudo systemctl status rsa-health.service
sudo systemctl status rsa-health-orchestrator.service
```

Logs:
```bash
journalctl -u sqlite_file_writer.service -f
journalctl -u rsa-health.service -f
journalctl -u rsa-health-orchestrator.service -f
```

## Configuration

Zie `config.example.json` voor de structuur. Belangrijke onderdelen:

- `databases` — lijst van te monitoren databases (ArangoDB, PostgreSQL)
- `logs.directory` — directory waar logbestanden staan voor de `/logs` endpoint
- `drive` — Google Drive configuratie voor marker-bestanden (orchestrator)

## Google Drive OAuth setup (voor Power Automate markers)

De orchestrator gebruikt Google Drive om marker-bestanden te detecteren. Dit vereist een eenmalige OAuth-authenticatie.

### Stap 1: Maak OAuth client credentials aan

1. Ga naar [Google Cloud Console](https://console.cloud.google.com/) → project `rsa-api-492010`
2. **APIs & Services** → **Credentials**
3. **Create Credentials** → **OAuth client ID**
4. Type: **Desktop app** (of **Other**)
5. Download het JSON-bestand en sla op als bijvoorbeeld `/home/deploy/Documenten/AWV/resources/client_secret_RSA-API.json`

### Stap 2: Voeg toe aan `config_rsa_health.json`

```json
{
  "drive": {
    "credentials_file": "/home/deploy/Documenten/AWV/resources/client_secret_RSA-API.json",
    "token_file": "/home/deploy/Documenten/AWV/resources/gdrive_token.pkl",
    "folder_id": "je-drive-folder-id",
    "poll_interval_seconds": 60
  }
}
```

De `folder_id` vind je door de Drive-map `PipelineStatus/` te openen in de browser — de ID staat in de URL: `https://drive.google.com/drive/folders/<folder_id>`

### Stap 3: Genereer het token (eenmalig)

```bash
uv run python -m orchestrator.orchestrator --db health.db
```

De eerste keer opent er een browser venster. Log in met je Google-account en geef toegang. Daarna wordt het token opgeslagen in `gdrive_token.pkl` en automatisch vernieuwd.

**Opmerking:** De service account aanpak (`service_account_file`) werkt niet voor persoonlijke Google-accounts. Gebruik de OAuth flow hierboven.
