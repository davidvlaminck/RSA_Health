## Purpose

This repository serves two main goals:

1. **Health page and API** — It exposes a health endpoint that serves a web page showing server health parameters, as well as an API endpoint that returns those parameters and their values in JSON format so they can be fetched programmatically.

2. **Monitoring service** — It runs a background service on the server that continuously checks those health parameters and sends a notification to a Teams webhook if one or more parameters fall below a configured threshold.

## Communicatiemodel

Alle pipeline-componenten schrijven hun status **rechtstreeks** naar de SQLite-database
(`health.db` → tabel `pipeline_state`). De FastAPI-API (`/pipeline/update`) is alleen bedoeld
voor externe tools (Power Automate, handmatige diagnostiek).

Als de `rsa-health` service down is, kunnen de scripts dus nog steeds rapporteren en kan de
orchestrator (in een apart proces) nog altijd functioneren.

### Componenten die rechtstreeks naar SQLite schrijven

- **Arango-sync** (`run_arango_sync()` in `main.py`) — starten via subprocess, status updates
  direct naar `pipeline_state`
- **PostGIS-sync** (AWVInfraPostGISSyncer) — eigen code, moet `update_pipeline_state()` aanroepen
- **RSA** (`ReportLoopRunner`) — gebruikt `PipelineStatusReporter` die direct naar SQLite schrijft

### Orchestrator

De orchestrator observeert `pipeline_state` en coördineert de overgangen. Hij draait
als **afzonderlijke service** (zie hieronder). De FastAPI-app start de orchestrator
niet standaard — dit kan via de `ORCHESTRATOR_ENABLED` environment variable als ze
alsnog samen willen draaien.

## Pipeline Orchestrator

De orchestrator draait als **afzonderlijke service**. Hij observeert de
`pipeline_state` tabel in `health.db` (SQLite) en coördineert de overgangen.
Externe services rapporteren hun status via directe SQLite-updates.

Pipeline-fases (volgorde):

```
idle → sharepoint_to_drive → drive_download → arango_sync →
postgis_sync_pausing → postgis_sync_paused → rsa_queries →
postgis_sync_resuming → postgis_sync_running → drive_upload →
drive_to_sharepoint → (reset om middernacht)
```

Timeouts:

| Fase                      | Timeout | Toelichting                                    |
|---------------------------|---------|-----------------------------------------------|
| arango_sync               | 4 uur   | Geen time-out in normale loop                  |
| postgis_sync_paused       | 10 min  | Bij time-out: gaat verder zonder pauze         |
| rsa_queries               | 3 uur   |                                               |
| postgis_sync_running      | 10 min  |                                               |

### RSA Health (FastAPI service)

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

Bezoek `http://<server-ip>:8000/` — elk pad redirect naar het dashboard (`/`), terwijl
`/health` en `/history` toegankelijk blijven als API endpoints.

De orchestrator wordt **niet** gestart als onderdeel van deze service.

### Pipeline Orchestrator (losse service)

```bash
uv run python -m lib.orchestrator --db health.db
```

Met systemd (`/etc/systemd/system/rsa-health-orchestrator.service`):

```ini
[Unit]
Description=RSA Health Pipeline Orchestrator
After=network.target

[Service]
Type=simple
User=rsahealth
WorkingDirectory=/opt/RSA_Health
ExecStart=/opt/RSA_Health/.venv/bin/python -m lib.orchestrator --db /opt/RSA_Health/health.db
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Activeren en starten:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now rsa-health-orchestrator.service
```

### Samen draaien (optioneel)

Als je de orchestrator wél als onderdeel van de FastAPI service wilt starten,
zet dan `ORCHESTRATOR_ENABLED=true`:

```bash
ORCHESTRATOR_ENABLED=true uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

### Opmerking

Alle services delen dezelfde `health.db` SQLite-database voor `pipeline_state`.
Als de orchestrator crasht, blijven de andere services (Arango-sync, PostGIS-sync,
RSA) hun status rapporteren. De orchestrator kan na herstart de pipeline
volledig hervatten.

## Running

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

Bezoek `http://<server-ip>:8000/` — elk pad redirect naar het dashboard (`/`), terwijl
`/health` en `/history` toegankelijk blijven als API endpoints.

## Configuration

Maak een `config.toml` aan aan de hand van `config.example.toml` en vul je database-gegevens in.
Het bestand wordt genegeerd door git.

```bash
cp config.example.toml config.toml
```

## Google Drive OAuth setup (voor Power Automate markers)

De orchestrator gebruikt Google Drive om marker-bestanden te detecteren. Dit vereist een eenmalige OAuth-authenticatie.

### Stap 1: Maak OAuth client credentials aan

1. Ga naar [Google Cloud Console](https://console.cloud.google.com/) → project `rsa-api-492010`
2. **APIs & Services** → **Credentials**
3. **Create Credentials** → **OAuth client ID**
4. Type: **Desktop app** (of **Other**)
5. Download het JSON-bestand en sla op als bijvoorbeeld `/home/davidlinux/Documenten/AWV/resources/client_secret_RSA-API.json`

### Stap 2: Voeg toe aan `config.toml`

```toml
[drive]
credentials_file = "/home/davidlinux/Documenten/AWV/resources/client_secret_RSA-API.json"
token_file = "/home/davidlinux/Documenten/AWV/resources/gdrive_token.pkl"
folder_id = "je-drive-folder-id"
poll_interval_seconds = 60
```

De `folder_id` vind je door de Drive-map `PipelineStatus/` te openen in de browser — de ID staat in de URL: `https://drive.google.com/drive/folders/<folder_id>`

### Stap 3: Genereer het token (eenmalig)

```bash
uv run python -m lib.orchestrator --db health.db
```

De eerste keer opent er een browser venster. Log in met je Google-account en geef toegang. Daarna wordt het token opgeslagen in `gdrive_token.pkl` en automatisch vernieuwd.

**Opmerking:** De service account aanpak (`service_account_file`) werkt niet voor persoonlijke Google-accounts. Gebruik de OAuth flow hierboven.
