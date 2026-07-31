# Voorstel: Centrale Pipeline Status & Workflow Database

## Doel

Momenteel werkt de nachtelijke verwerking volledig op vaste tijdstippen:

- 00:00 Power Automate: SharePoint → Drive
- 01:00 Drive download
- 03:00 Arango sync
- 05:30 PostGIS pauzeren + RSA query's
- Daarna upload naar Drive
- 10:00 Power Automate: Drive → SharePoint

Doel van deze wijziging:

1. De huidige fase zichtbaar maken op de health pagina.
2. Historiek van de verschillende fases bewaren.
3. Fouten beter detecteren.
4. Op termijn minder afhankelijk worden van vaste tijdstippen.
5. Eén centrale bron van waarheid hebben voor de status van de pipeline.

---

## Architectuur

### SQLite als centrale statusdatabase

In plaats van een los tekstbestand wordt de bestaande SQLite database gebruikt.

Voordelen:

- Historiek blijft beschikbaar.
- Status is persistent.
- Geen synchronisatieproblemen tussen bestanden.
- De health pagina gebruikt dezelfde bron als de scripts.

---

## Concept

Elke stap van de pipeline rapporteert zijn status aan de SQLite database.

Voorbeelden van fases:

- SharePoint → Drive
- Drive Download
- Arango Sync
- PostGIS-sync pauzeren
- RSA Queries
- PostGIS-sync hervatten
- Drive Upload
- Drive → SharePoint

Mogelijke statussen:

- running
- completed
- failed
- aborted

---

## Communicatie: directe SQLite-toegang

Elke component van de pipeline schrijft zijn status **rechtstreeks** naar de SQLite-database
(`health.db` → tabel `pipeline_state`). Er is geen tussenkomst van een HTTP-API nodig.

Reden: als de `rsa-health` API down zou zijn, moet de rest van de pipeline nog steeds kunnen
functioneren. De API is alleen bedoeld voor:
- het dashboard (`/pipeline/state`, `/health`, `/history`)
- externe toegang (bv. Power Automate marker-bestanden via Drive)
- handmatige diagnostiek

De onderstaande diagrammen tonen de SQLite-toegang expliciet.

```text
                     ┌───────────────────────────────────────────────┐
                     │  rsa-health (FastAPI)  ← systemd service      │
                     │  - /health, /history, /pipeline/state       │
                     │  - achtergrond-orchestrator (stap 8)          │
                     └────────────────────┬──────────────────────────┘
                                          │ pipeline_state (SQLite: health.db)
           ┌──────────────┬───────────────┼──────────────┬─────────────┐
           ▼              ▼               ▼              ▼             ▼
┌────────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐
│ Power Automate │ │ Arango-sync  │ │ PostGIS-sync │ │ RSA (ReportLoop)     │
│ (SP <-> Drive) │ │ schrijft ->  │ │ schrijft ->  │ │ leest -> ArangoDB +  │
│ marker files   │ │ ArangoDB     │ │ PostGIS      │ │          -> PostGIS  │
│ (extern)       │ │ (direct SQL) │ │ (direct SQL) │ │ (direct SQL)         │
│                │ │ rapporteert  │ │ + pauw/resume│ │ rapporteert          │
└────────────────┘ └──────────────┘ └──────────────┘ └──────────────────────┘
                          │ schrijft          │ schrijft         │ leest (RSA)
                          ▼                   ▼                  │
               ┌──────────────────┐  ┌─────────────────────┐     │
               │ ArangoDB         │  │ PostGIS (PostgreSQL)│     │
               │ (arangod service)│  │ (systemd service)   │     │
               │ NIET stoppen     │  │ NIET stoppen        │     │
               └──────────────────┘  └─────────────────────┘     │
                        ▲                     ▲                  │
                        └─────────────────────┴──────────────────┘
                                 RSA leest beide databases
```

### Volledige nachtelijke sequentie (signal-based)

Elke component schrijft direct naar `pipeline_state` in SQLite. De orchestrator observeert
deze tabel en coördineert alleen de overgangen en de drive-stappen. Arango-sync, PostGIS-sync
en RSA draaien onafhankelijk; de orchestrator wacht steeds met een timeout op het verwachte signaal.

```text
00:00  Power Automate            kopiert SharePoint → Drive
00:30  Power Automate            marker: sharepoint_to_drive
00:31  Orchestrator              marker gedetecteerd → sharepoint_to_drive / completed
00:31  Orchestrator              start sync_drive_to_local → drive_download / running
00:35  Orchestrator              drive_download / completed   (of: failed → stop)
        ~ wacht op arango_sync = completed (T1; in de normale loop geen time-out) ~
03:00  Arango-sync (onafhankelijk) start → arango_sync / running
        ~ rapporteert vordering per sub-stap (fase blijft running) ~
04:45  Arango-sync               arango_sync / completed
04:50  Orchestrator              ziet 'completed' → postgis_sync_pausing / running
04:51  PostGIS-sync              ziet 'pausing' → onderbreekt schrijven → postgis_sync_paused / completed
        ~ wacht op 'paused' (max. T2 ≈ 10 min; bij time-out: gaat verder zonder pause) ~
05:00  RSA ReportLoopRunner (onafhankelijk) start query'n → rsa_queries / running
        ~ rapporteert via PipelineStatusReporter ~
08:00  RSA                       rsa_queries / completed   (max. T3 ≈ 3u)
08:00  Orchestrator              → postgis_sync_resuming / running
08:01  PostGIS-sync              ziet 'resuming' → hervat schrijven → postgis_sync_running / completed
08:02  Orchestrator              start sync_local_to_drive → drive_upload / running
08:10  Orchestrator              drive_upload / completed
08:10+ Power Automate            kopiert Drive → SharePoint (start pollend)
10:00  Orchestrator              marker gedetecteerd → drive_to_sharepoint / completed (einde cyclus)
midnight  Orchestrator            reset → (idle / completed)
```

---

## Workflow State

De status kan niet alleen gebruikt worden voor monitoring, maar ook als eenvoudige workflow-state.

Voorbeelden:

- sharepoint_to_drive_running
- sharepoint_to_drive_completed
- drive_download_running
- drive_download_completed
- arango_running
- arango_completed
- postgis_sync_pausing
- postgis_sync_paused
- rsa_running
- rsa_completed
- postgis_sync_resuming
- postgis_sync_running
- drive_upload_completed
- completed
- failed

---

## PostGIS Specifieke Situatie

PostGIS (de PostgreSQL-database) draait continu als systemd-service en blijft altijd draaien. De database wordt niet gestopt of gestart tijdens de nachtelijke verwerking.

Wat wel gebeurt, is dat het PostGIS-syncscript (AWVInfraPostGISSyncer) zijn schrijfactiviteit tijdelijk pauzeert. RSA leest tijdens de rapportagenesis uit PostGIS; terwijl RSA leest, willen we voorkomen dat het syncscript gelijktijdig nieuwe data schrijft. Daarom pauzeert het syncscript alleen, niet de database.

PostGIS wordt daarmee beschouwd als onderdeel van de pipeline — zowel als continue service, als via het pauzeren/hervatten van de sync.

Workflow:

Arango klaar → PostGIS-sync pauzeren → RSA uitvoeren → PostGIS-sync hervatten → Upload

Na elke actie moet gecontroleerd worden of de gewenste toestand effectief bereikt werd. Het syncscript heeft een interne time-out: als de orchestrator nooit signaal geeft om te hervatten, hervat het syncscript vanzelf na een maximaal pauzetijd (bv. 4 uur) zodat de sync niet vastloopt.

---

## Voorstel Datamodel

Voor de eerste versie wordt bewust gekozen voor een eenvoudig model met één centrale tabel.

### Tabel pipeline_state

Deze tabel bevat altijd exact één record en stelt de actuele toestand van de pipeline voor.

Velden:

- id
- phase
- status
- updated_at
- message

Voorbeeld:

```text
id: 1
phase: rsa_queries
status: running
updated_at: 2026-07-30 06:13
message: Query's worden uitgevoerd
```

Mogelijke fases:

- sharepoint_to_drive
- drive_download
- arango_sync
- postgis_sync_pausing
- postgis_sync_paused
- rsa_queries
- postgis_sync_resuming
- postgis_sync_running
- drive_upload
- drive_to_sharepoint

Onafhankelijke services (rapporteer status, eigen logging): `arango_sync`, `postgis_sync`, `rsa_queries`.
Orchestrator-coördineerde fasen: `sharepoint_to_drive`, `drive_download`, `postgis_sync_pausing/resuming`, `drive_upload`, `drive_to_sharepoint`.

Mogelijke statuswaarden:

- running
- completed
- failed
- aborted

### Waarom slechts één tabel?

De health pagina heeft voornamelijk nood aan:

- huidige fase
- huidige status
- laatste update
- foutmelding

Historische gegevens worden reeds bijgehouden door de bestaande health monitoring.

Daarom zijn aparte tabellen zoals pipeline_run en pipeline_event voorlopig niet noodzakelijk.

---

## Controleflow via de Database

De SQLite database wordt de centrale bron van waarheid voor de pipeline-status.

Voorbeeld:

```text
sharepoint_to_drive  completed  (Power Automate marker; RSA_Health detecteert)
↓
drive_download       running    (orchestrator start: sync_drive_to_local)
↓
drive_download       completed  (of: failed)
↓
arango_sync          completed  (onafhankelijke service rapporteert)
↓
postgis_sync         pausing    (orchestrator signaleert: fase = postgis_sync_pausing)
↓
postgis_sync         paused     (syncscript rapporteert: fase = postgis_sync_paused)
↓
rsa_queries          running    (onafhankelijke service; wacht op drive_download + paused)
↓
rsa_queries          completed  (RSA rapporteert zelf via PipelineStatusReporter)
↓
postgis_sync         resuming   (orchestrator signaleert: fase = postgis_sync_resuming)
↓
postgis_sync         running    (syncscript hervat; rapporteert fase = postgis_sync_running)
↓
drive_upload         completed  (orchestrator: sync_local_to_drive)
↓
drive_to_sharepoint  completed  (Power Automate marker; einde cyclus)
```

Belangrijke richtlijnen:

- nooit oneindig wachten op een status
- altijd timeouts voorzien
- failed status registreren
- dagelijks resetten naar een nieuwe starttoestand
- health pagina gebruikt dezelfde statusbron

---

## Dagelijkse Reset

Om middernacht wordt de pipeline terug naar de starttoestand gebracht.

Voorbeeld:

```text
phase = sharepoint_to_drive
status = running
```

Indien de vorige nacht vastgelopen is, vormt dit geen blokkade voor de volgende run.

---

## Implementatiestatus

De volgende tabel geeft de voortgang per implementatiestap:

| Stap | Omschrijving | Status | Details |
|------|-------------|--------|---------|
| 1 | SQLite `pipeline_state` tabel | ✅ Gereed | Tabel bestaat in `main.py` (init_db). Initiële rij `idle/completed`. |
| 2 | FastAPI endpoint `POST /pipeline/update` | ✅ Gereed | Bestaat in `main.py` (pipeline_update). Accepteert `phase`, `status`, `message`. |
| 3 | Health pagina uitbreiden | ✅ Gereed | `static/index.html` toont pipeline fase, status, tijdstempel en bericht. |
| 4 | Arango sync integreren | ⚠️ Gedeeltelijk | `run_arango_sync()` in `main.py` schrijft direct naar `pipeline_state` (goed). `arangolooprunner.py` in extern project rapporteert nog niet — moet ook directe SQLite-updates doen in plaats van via API. |
| 5 | RSA integreren | ⚠️ Gedeeltelijk | `PipelineStatusReporter` rapporteert via `/pipeline/update` (API). Ideaal: directe SQLite-updates in plaats van API-calls. |
| 6 | Power Automate via marker-bestanden | ❌ Nog niet | Google Drive marker-file polling is een TODO-placeholder in `PipelineOrchestrator._find_drive_marker()`. |
| 7 | PostGIS-sync integreren | ❌ Nog niet | PostGIS-sync heeft geen pauze/resume-mechanisme. TODO-placeholder in `_start_postgis_pause()`/`_start_postgis_resume()`. |
| 8 | Orchestrator (achtergrond-taak) | ⚠️ Boilerplate geschreven | `PipelineOrchestrator` class en lifespan-startup in `main.py`. State-machine logica, timeouts, daily-reset en marker-detection zijn opgenomen. Drive-sync en PostGIS-signalen zijn placeholders. |

Zie [Stap 8 — Orchestrator](#stap-8) voor details over de boilerplate-implementatie.

---

## Stappen

### Stap 1 — ✅ Gereed

SQLite uitbreiden met:

```sql
CREATE TABLE pipeline_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    phase TEXT,
    status TEXT,
    updated_at DATETIME,
    message TEXT
);
```

Initiële rij:

```text
id = 1
phase = idle
status = completed
```

### Stap 2 — ✅ Gereed

FastAPI endpoint voorzien:

```http
POST /pipeline/update
```

### Stap 3 — ✅ Gereed

Health pagina uitbreiden:

- huidige fase tonen
- huidige status tonen
- laatste update tonen
- foutmelding tonen

### Stap 4 — ⚠️ Gedeeltelijk

Arango sync (arangolooprunner.py) integreren met statusupdates: signaleer `arango_sync` running bij start en `arango_sync` completed/failed bij einde. Het script blijft een onafhankelijke service draaien; de orchestrator wacht op de statussignaal in plaats van het script zelf te starten.

### Stap 5 — ✅ Gereed

RSA (ReportLoopRunner) volgt hetzelfde patroon als postgis_sync en arango_sync: onafhankelijke service met eigen logging, die zijn status rapporteert via `PipelineStatusReporter` aan `/pipeline/update`. RSA wacht intern op de voorwaarden in `pipeline_state` (drive_download completed en postgis_sync gepauseerd) voordat rapportage start; de orchestrator start RSA niet zelf.

### Stap 6 — ❌ Nog niet

Power Automate integreren via marker-bestanden op Google Drive (zie [Aanvulling: Integratie van Power Automate](#aanvulling-integratie-van-power-automate-in-de-pipeline-workflow) hieronder).

### Stap 7 — ❌ Nog niet

PostGIS-syncscript (AWVInfraPostGISSyncer) integreren: rapporteer `postgis_sync` running tijdens normaal bewerken. Voeg een pauze/hervat mechanisme toe: lees fase `postgis_sync_pausing` / `postgis_sync_resuming` uit `pipeline_state` en onderbreek het schrijven tot `postgis_sync_paused` / `postgis_sync_running` wordt gerapporteerd. De PostgreSQL-database zelf wordt niet gestopt.

### Stap 8 — ⚠️ Boilerplate geschreven

Eenvoudige orchestrator toevoegen (achtergrond-taak in RSA_Health) die de volgende stappen coördineert op basis van `pipeline_state` in plaats van vaste tijdstippen, met timeouts en idempotentie:

- **drive_download**: orchestrator voert `sync_drive_to_local` uit en rapporteert `drive_download` completed/failed.
- **drive_upload**: orchestrator voert `sync_local_to_drive` uit en rapporteert `drive_upload` completed/failed.
- **sequencing**: orchestrator observeert `arango_sync`, `postgis_sync`, `rsa_queries` (allemaal onafhankelijke services) en signaleert `postgis_sync_pausing`/`resuming`.
- **power automate markers**: detecteer `sharepoint_to_drive` en `drive_to_sharepoint` marker-bestanden op Google Drive en zet die om naar `pipeline_state`.

---

## Implementatielagen en Services

De componenten zijn opgedeeld in drie lagen. De databases en de orchestrator blijven **altijd** als service draaien. De sync-scripts blijven onafhankelijke services, maar rapporteren hun status (en respecteren pauze-signaling) aan de orchestrator.

```text
                    ┌───────────────────────────────────────────────┐
                    │  rsa-health (FastAPI)  ← systemd service      │
                    │  - /health, /history, /pipeline/update, state │
                    │  - achtergrond-orchestrator (stap 8)          │
                    └────────────────────┬──────────────────────────┘
                                         │ pipeline_state (SQLite: health.db)
          ┌──────────────┬───────────────┼──────────────┬─────────────┐
          ▼              ▼               ▼              ▼             ▼
┌────────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐
│ Power Automate │ │ Arango-sync  │ │ PostGIS-sync │ │ RSA (ReportLoop)     │
│ (SP <-> Drive) │ │ schrijft ->  │ │ schrijft ->  │ │ leest -> ArangoDB +  │
│ marker files   │ │ ArangoDB     │ │ PostGIS      │ │          -> PostGIS  │
│ (extern)       │ │ rapporteert  │ │ + pauw/resume│ │ rapporteert          │
│                │ │ status       │ │ via state    │ │ rsa_queries          │
└────────────────┘ └──────────────┘ └──────────────┘ └──────────────────────┘
                         │ schrijft          │ schrijft         │ leest (RSA)
                         ▼                   ▼                  │
              ┌──────────────────┐  ┌─────────────────────┐     │
              │ ArangoDB         │  │ PostGIS (PostgreSQL)│     │
              │ (arangod service)│  │ (systemd service)   │     │
              │ NIET stoppen     │  │ NIET stoppen        │     │
              └──────────────────┘  └─────────────────────┘     │
                       ▲                     ▲                  │
                       └─────────────────────┴──────────────────┘
                                RSA leest beide databases
```

### Volledige nachtelijke sequentie (signal-based)

Elke component draait onafhankelijk als service en rapporteert zijn status via
`POST /pipeline/update` (of marker-bestanden voor Power Automate). De orchestrator
observeert `pipeline_state` in SQLite en coördineert alleen de overgangen en de
drive-stappen. Arango-sync, PostGIS-sync en RSA draaien onafhankelijk; de
orchestrator wacht steeds met een timeout op het verwachte signaal.

```text
00:00  Power Automate            kopiert SharePoint → Drive
00:30  Power Automate            marker: sharepoint_to_drive
00:31  Orchestrator              marker gedetecteerd → sharepoint_to_drive / completed
00:31  Orchestrator              start sync_drive_to_local → drive_download / running
00:35  Orchestrator              drive_download / completed   (of: failed → stop)
        ~ wacht op arango_sync = completed (T1; in de normale loop geen time-out) ~
03:00  Arango-sync (onafhankelijk) start → arango_sync / running
        ~ rapporteert vordering per sub-stap (fase blijft running) ~
04:45  Arango-sync               arango_sync / completed
04:50  Orchestrator              ziet 'completed' → postgis_sync_pausing / running
04:51  PostGIS-sync              ziet 'pausing' → onderbreekt schrijven → postgis_sync_paused / completed
        ~ wacht op 'paused' (max. T2 ≈ 10 min; bij time-out: gaat verder zonder pause) ~
05:00  RSA ReportLoopRunner (onafhankelijk) start query'n → rsa_queries / running
        ~ rapporteert via PipelineStatusReporter ~
08:00  RSA                       rsa_queries / completed   (max. T3 ≈ 3u)
08:00  Orchestrator              → postgis_sync_resuming / running
08:01  PostGIS-sync              ziet 'resuming' → hervat schrijven → postgis_sync_running / completed
08:02  Orchestrator              start sync_local_to_drive → drive_upload / running
08:10  Orchestrator              drive_upload / completed
08:10+ Power Automate            kopiert Drive → SharePoint (start pollend)
10:00  Orchestrator              marker gedetecteerd → drive_to_sharepoint / completed (einde cyclus)
midnight  Orchestrator            reset → (idle / completed)
```

T1: wacht op arango_sync = completed; in de normale loop geen time-out (optioneel lange time-out)
T2: wacht op postgis_sync_paused; max. ~10 min, bij time-out gaat Orchestrator verder zonder te wachten op de gepauzeerde sync
T3: wacht op rsa_queries = completed; max. ~3 uur
autonome hervatting: PostGIS-sync hervat vanzelf na max. 4 uur, zonder orchestrator-signaal

> Belangrijk verschil t.o.v. trigger-model: Arango-sync, PostGIS-sync en RSA draaien
> onafhankelijk en rapporteren hun status; de orchestrator observeert `pipeline_state` en
> coördineert alleen de overgangen (pauz. en hervat van PostGIS-sync) én de drive-stappen.
> Als de orchestrator crasht, blijven de jobs draaien; de volgende run reset om middernacht.

# Aanvulling: Integratie van Power Automate in de Pipeline Workflow

## Probleemstelling

Power Automate draait buiten de serveromgeving en kan daarom niet eenvoudig communiceren met een interne FastAPI API zonder bijkomende beveiliging, API-keys, firewallregels of publieke endpoints.

Omdat de bestaande pipeline reeds gebruik maakt van Google Drive als uitwisselingsmechanisme, wordt gekozen voor een eenvoudigere oplossing.

---

## Voorgestelde Oplossing

Power Automate communiceert zijn status via marker-bestanden op Google Drive.

De server downloadt deze statusinformatie periodiek en verwerkt ze in de centrale `pipeline_state` tabel.

Hierdoor blijft SQLite de centrale bron van waarheid voor de workflow, terwijl Power Automate geen rechtstreekse toegang nodig heeft tot de server.

---

## Google Drive Structuur

Voorzie een map:

```text
PipelineStatus/
```

Power Automate maakt hierin bestanden aan.

Voorbeelden:

```text
2026-07-30_sharepoint_to_drive.completed
2026-07-30_sharepoint_to_drive.failed

2026-07-30_drive_to_sharepoint.completed
2026-07-30_drive_to_sharepoint.failed
```

---

## Werking

### SharePoint → Drive

Wanneer de Power Automate flow succesvol afgerond is:

```text
2026-07-30_sharepoint_to_drive.completed
```

Bij fout:

```text
2026-07-30_sharepoint_to_drive.failed
```

### Drive → SharePoint

Wanneer deze flow succesvol afgerond is:

```text
2026-07-30_drive_to_sharepoint.completed
```

Bij fout:

```text
2026-07-30_drive_to_sharepoint.failed
```

---

## Controle door RSA_Health

RSA_Health controleert periodiek de PipelineStatus-map.

Bijvoorbeeld:

- elke minuut;
- of telkens wanneer een nieuwe workflowstap geëvalueerd wordt.

Wanneer een markerbestand wordt gevonden:

```text
2026-07-30_sharepoint_to_drive.completed
```

wordt de centrale status bijgewerkt:

```text
phase = sharepoint_to_drive
status = completed
```

Bij:

```text
2026-07-30_sharepoint_to_drive.failed
```

wordt:

```text
phase = sharepoint_to_drive
status = failed
```

geregistreerd.

---

## Waarom geen JSON-bestand?

Een centrale JSON-file werd overwogen maar heeft nadelen:

- voortdurend downloaden van dezelfde informatie;
- risico op gelijktijdige updates;
- extra parsing-logica;
- minder duidelijk historisch overzicht.

Marker-bestanden zijn eenvoudiger:

- aanwezig = status bereikt;
- bestandsnaam bevat alle informatie;
- gemakkelijk te debuggen;
- eenvoudig te controleren via Google Drive UI.

---

## Historiek

De datum wordt opgenomen in de bestandsnaam.

Voorbeeld:

```text
2026-07-30_sharepoint_to_drive.completed
```

Hierdoor:

- blijft historiek automatisch beschikbaar;
- hoeft geen bestand overschreven te worden;
- kunnen oude bestanden later worden opgeruimd.

Een optioneel onderhoudsscript kan bijvoorbeeld bestanden ouder dan 30 dagen verwijderen.

---

## Integratie met de Centrale Workflow

De Google Drive-bestanden worden niet beschouwd als de hoofdbron van waarheid.

Google Drive dient enkel om de status van Power Automate aan RSA_Health door te geven.

De effectieve workflowstatus blijft opgeslagen in:

```text
pipeline_state
```

Workflow:

```text
Power Automate
        ↓
Google Drive markerbestand
        ↓
RSA_Health detecteert status
        ↓
pipeline_state wordt bijgewerkt
        ↓
Volgende workflowstap kan starten
```

---

## Voordelen

- Geen publieke FastAPI endpoint nodig.
- Geen API-keys nodig voor Power Automate.
- Geen SSH-oplossing nodig.
- Eenvoudige implementatie.
- Gemakkelijk te debuggen.
- Past binnen de bestaande Google Drive architectuur.
- Kan later nog uitgebreid worden indien nodig.

---

## Beslissing

Voor de eerste implementatie wordt Power Automate geïntegreerd via marker-bestanden op Google Drive.

RSA_Health verwerkt deze bestanden en zet de informatie om naar de centrale status in `pipeline_state`.

De volledige workflow blijft hierdoor bestuurbaar vanuit één centrale state machine zonder bijkomende netwerkinfrastructuur of beveiligingsconfiguratie.