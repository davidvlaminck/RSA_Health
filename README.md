## Purpose

This repository serves two main goals:

1. **Health page and API** — It exposes a health endpoint that serves a web page showing server health parameters, as well as an API endpoint that returns those parameters and their values in JSON format so they can be fetched programmatically.

2. **Monitoring service** — It runs a background service on the server that continuously checks those health parameters and sends a notification to a Teams webhook if one or more parameters fall below a configured threshold.

## Running

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

Then visit `http://<server-ip>:8000/` — any path redirects to the dashboard (`/`), while `/health` and `/history` remain accessible as API endpoints.

## Configuration

Create a `config.toml` based on `config.example.toml` and add your database credentials. The file is ignored by git.

```bash
cp config.example.toml config.toml
```
