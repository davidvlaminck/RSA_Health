# RSA Health Security & Pipeline Setup

## 1. Security: Block Scanner IPs

### fail2ban + ufw

Run the setup script as root:

```bash
sudo bash scripts/setup_security.sh
```

This will:
- Install `fail2ban` and `ufw` if missing
- Copy fail2ban filter and jail configs
- Block known scanner IPs with ufw
- Enable and restart fail2ban

### Verify

```bash
# Check fail2ban status
fail2ban-client status rsa-health

# Check ufw blocked IPs
ufw status numbered

# Check rsa_health logs for blocked requests
grep -i "Blocked\|Rate limit" logs/rsa_health.log
```

---

## 2. Pipeline Recovery

The pipeline can get stuck when `rsa_queries` times out waiting for `postgis_sync`.

### Dashboard reset endpoint

```bash
# Reset pipeline state (use when stuck)
curl -X POST http://127.0.0.1:8000/pipeline/reset
```

### Manual recovery steps

If the pipeline is stuck:

1. Check current state:
   ```bash
   curl http://127.0.0.1:8000/pipeline/state
   ```

2. Reset if needed:
   ```bash
   curl -X POST http://127.0.0.1:8000/pipeline/reset
   ```

3. Restart services in order:
   ```bash
   sudo systemctl restart rsa_orchestrator.service
   sudo systemctl restart postgis_sync.service
   ```

---

## 3. Rate Limit Configuration

Edit `config_rsa_health.json`:

```json
{
  "rate_limit": {
    "max_requests": 120,
    "window": 60
  }
}
```

- `max_requests`: max requests per IP per `window` seconds
- `window`: time window in seconds

After changing, restart:
```bash
sudo systemctl restart rsa_health.service
```

---

## 4. Blocked IPs

The following scanner IPs are blocked by default (see `main.py`):

- `93.123.109.228` — `.env` scanner (22 requests)
- `47.114.87.90` — phpMyAdmin scanner
- `5.61.209.44` / `5.61.209.92` — Hikvision SDK scanner
- `160.119.76.24` — robots.txt / security.txt scanner
- `36.255.33.242` — Netgear exploit scanner
- `185.209.15.199` — `.env` scanner
- `45.198.224.26` — `/login` scanner
- `20.65.193.201` — `/portal/redlion` scanner

To add more, edit `_BLOCKED_IPS` in `main.py` or use ufw directly:
```bash
sudo ufw deny from <IP> comment "RSA Health scanner"
```

---

## 5. Instructions for RSA repo

### Quiet hours / Drive Sync Gate

The RSA repo contains `ReportLoopRunner` which has a quiet hours check (15:00-23:00) and a drive sync gate.

**Issue**: During quiet hours, the runner sleeps until 23:00. If the drive sync doesn't complete before the hard deadline (06:00), it proceeds anyway but may miss the optimal window.

**Recommended fix in RSA repo**:

1. **Move quiet hours check earlier** or make it configurable:
   ```python
   # In ReportLoopRunner or config
   QUIET_HOURS_START = 15  # 15:00
   QUIET_HOURS_END = 18    # 18:00 instead of 23:00
   ```

2. **Add a configurable drive sync timeout** with warning:
   ```python
   DRIVE_SYNC_HARD_DEADLINE_HOURS = 10  # instead of fixed 06:00
   ```

3. **Log drive sync status explicitly** so the orchestrator can detect completion:
   ```python
   # After drive download completes
   pipeline.update("drive_download", "completed", f"Downloaded {len(files)} files")
   ```

4. **Ensure `PipelineStatusReporter` uses direct SQLite** instead of HTTP POST to `/pipeline/update` when running in the same process as `rsa_health`:
   ```python
   # Instead of:
   requests.post("http://127.0.0.1:8000/pipeline/update", json={...})
   
   # Use direct SQLite update (see PipelineState class)
   pipeline.update("rsa_queries", "running", "Starting queries")
   ```

This avoids HTTP dependency and makes the pipeline more robust.

---

## 6. Fail2ban Filter Details

The filter matches blocked requests in `logs/rsa_health.log`:

```
2026-08-21 02:34:49 INFO 185.209.15.199:0 - "GET /.env HTTP/1.0" 404
```

Pattern: `IP:PORT - "METHOD PATH PROTOCOL" STATUS`

- Status 403 = blocked IP
- Status 429 = rate limit exceeded
- Status 404 = scanner path not found

Bans after 3 attempts within 10 minutes, for 1 hour.
