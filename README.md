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

De orchestrator observeert `pipeline_state` en coördineert de overgangen. Hij loopt als
achtergrond-thread binnen de FastAPI service (zie hieronder), of kan apart draaien.

## Pipeline Orchestrator

De orchestrator is een background thread die **binnen dezelfde process** als de FastAPI
application draait (`rsa-health`). Hij wordt automatisch gestart bij opstarten via FastAPI's
`lifespan` mechanismus (`PipelineOrchestrator` class in `main.py`) en stopt bij afsluiten.

### Hoe het werkt

De orchestrator observeert de `pipeline_state` tabel in `health.db` (SQLite). Hij is
**signaal-gebaseerd** — externe services rapporteren hun status via directe SQLite-updates,
en de orchestrator reageert op die statussen om de volgende stap te coördineren.

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

### Apart draaien (optioneel)

Als je de orchestrator liever als **aparte systemd-service** wilt laten draaien:

1. Start `rsa-health` zonder orchestrator, óf
2. Draai de orchestrator in een eigen process.

Voorbeeld systemd-service (`/etc/systemd/system/rsa-health-orchestrator.service`):

```ini
[Unit]
Description=RSA Health Pipeline Orchestrator
After=network.target

[Service]
Type=simple
User=rsahealth
WorkingDirectory=/opt/RSA_Health
ExecStart=/opt/RSA_Health/.venv/bin/python -m main --orchestrator-only
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

Opmerking: indien apart gedraaid, moet ook de hoofd-`rsa-health` service actief zijn. Ze
delen dezelfde `health.db` SQLite-database voor `pipeline_state`.

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
