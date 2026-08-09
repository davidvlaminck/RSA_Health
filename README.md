## Purpose

This repository serves two main goals:

1. **Health page and API** — It exposes a health endpoint that serves a web page showing server health parameters, as well as an API endpoint that returns those parameters and their values in JSON format so they can be fetched programmatically.

2. **Monitoring service** — It runs a background service on the server that continuously checks those health parameters and sends a notification to a Teams webhook if one or more parameters fall below a configured threshold.

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

## Componenten

### rsa_health (FastAPI service)

De FastAPI-app serveert het dashboard en de health API. Hij voert zelf ook health-checks uit en plaatst snapshots in de queue via `enqueue_sqlite_job()`.

```bash
uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

### sqlite_file_writer (aparte service)

De writer consumeert de JSON file queue en schrijft naar `health.db`. Hij draait als **afzonderlijke systemd service**.

```bash
sudo systemctl start sqlite_file_writer.service
```

### Orchestrator

De orchestrator observeert `pipeline_state` in `health.db` en coördineert de pipeline-overgangen. Hij draait als **afzonderlijke service**.

```bash
uv run python -m orchestrator.orchestrator --db health.db
```

## Pipeline Orchestrator

De orchestrator draait als **afzonderlijke service**. Hij observeert de `pipeline_state` tabel in `health.db` (SQLite) en coördineert de overgangen.

### Pipeline Fases

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

### Volgorde

```
idle → sharepoint_to_drive → drive_download → arango_sync →
postgis_sync_pausing → postgis_sync_paused → rsa_queries →
postgis_sync_resuming → postgis_sync_running → drive_upload →
drive_to_sharepoint → (reset om middernacht)
```

### Timeouts

| Fase                      | Timeout | Toelichting                                    |
|---------------------------|---------|-----------------------------------------------|
| arango_sync               | 4 uur   | Geen time-out in normale loop                  |
| postgis_sync_paused       | 10 min  | Bij time-out: gaat verder zonder pauze         |
| rsa_queries               | 3 uur   |                                               |
| postgis_sync_running      | 10 min  |                                               |

### Geldige statussen

| Status    | Betekenis                                    |
|-----------|----------------------------------------------|
| starting  | Actie wordt gestart                          |
| running   | Actie is bezig                               |
| completed | Actie succesvol voltooid                     |
| failed    | Actie mislukt                                |
| aborted   | Actie werd afgebroken (zeldzaam, intern)     |

De orchestrator zelf zet `starting` wanneer een fase begint. Externe scripts en services rapporteren meestal `running`, `completed` of `failed`.

Voorbeeld van een complete status-update voor een externe script:

```python
from sqlite_writer.sqlite_queue_client import enqueue_sqlite_job

enqueue_sqlite_job(
    action="update_pipeline_state",
    payload={
        "phase": "rsa_queries",
        "status": "running",
        "updated_at": "2026-08-09T20:00:00Z",
        "message": "RSA queries starten",
    },
)
```

Wanneer de taak klaar is:

```python
enqueue_sqlite_job(
    action="update_pipeline_state",
    payload={
        "phase": "rsa_queries",
        "status": "completed",
        "updated_at": "2026-08-09T21:30:00Z",
        "message": "RSA queries voltooid",
    },
)
```

Bij een fout:

```python
enqueue_sqlite_job(
    action="update_pipeline_state",
    payload={
        "phase": "rsa_queries",
        "status": "failed",
        "updated_at": "2026-08-09T21:35:00Z",
        "message": "Database time-out",
    },
)
```

## Services

### rsa_health.service

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
After=network.target rsa-health.service

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/opt/data-platform/RSA_Health
ExecStart=/opt/data-platform/RSA_Health/.venv/bin/python -m orchestrator.orchestrator --db /opt/data-platform/RSA_Health/health.db
Restart=on-failure
RestartSec=10

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
