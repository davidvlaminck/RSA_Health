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
- PostGIS Stop
- RSA Queries
- PostGIS Start
- Drive Upload
- Drive → SharePoint

Mogelijke statussen:

- running
- completed
- failed
- aborted

---

## FastAPI Endpoint

Er wordt een generiek endpoint voorzien:

POST /pipeline/update

Voorbeeld payload:

```json
{
  "phase": "arango_sync",
  "status": "running",
  "message": "Synchronisatie gestart"
}
```

Alle componenten gebruiken hetzelfde endpoint:

- Arango script
- RSA script
- Power Automate
- Eventuele toekomstige scripts

---

## Health Pagina

De health pagina leest om de paar seconden de huidige pipeline-status.

Voorbeeld:

- Fase: RSA Queries
- Status: Running
- Sinds: 05:42
- Laatste update: 05:43

Omdat de status in SQLite wordt bewaard, wordt deze automatisch ook opgenomen in de historiek.

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
- postgis_stopped
- rsa_running
- rsa_completed
- postgis_running
- drive_upload_completed
- completed
- failed

---

## PostGIS Specifieke Situatie

PostGIS draait overdag continu als systemd-service.

Tijdens de nachtelijke verwerking wordt deze tijdelijk gestopt zodat RSA-query's veilig kunnen draaien.

Daarom wordt PostGIS beschouwd als onderdeel van de pipeline.

Workflow:

Arango klaar → PostGIS stoppen → RSA uitvoeren → PostGIS starten → Upload

Na elke actie moet gecontroleerd worden of de gewenste toestand effectief bereikt werd.

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
- postgis_stopping
- postgis_stopped
- rsa_queries
- postgis_starting
- postgis_running
- drive_upload

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
arango completed
↓
postgis stoppen
↓
postgis stopped
↓
rsa starten
↓
rsa completed
↓
postgis starten
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

## Implementatiestappen

### Stap 1

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

### Stap 2

FastAPI endpoint voorzien:

```http
POST /pipeline/update
```

### Stap 3

Health pagina uitbreiden:

- huidige fase tonen
- huidige status tonen
- laatste update tonen
- foutmelding tonen

### Stap 4

Arango script integreren met statusupdates.

### Stap 5

RSA script integreren met statusupdates.

### Stap 6

Power Automate integreren via FastAPI endpoint.

### Stap 7

PostGIS integreren zodat deze rapporteert wanneer ze stopt en opnieuw actief is.

### Stap 8

Optioneel: eenvoudige orchestrator toevoegen die de volgende stap start op basis van pipeline_state in plaats van vaste tijdstippen.

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