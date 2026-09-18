# Security: Health Page Access Control

## Doel
De health endpoint (`/health`) mag enkel toegankelijk zijn na het maken van een SSH tunnel. 
Dit betekent dat directe toegang vanaf het internet moet worden geblokkeerd.

## Huidige Setup
- Nginx draait op poort 80 als reverse proxy
- FastAPI draait op 127.0.0.1:8000
- Nginx stuurt momenteel AL het verkeer door naar FastAPI (inclusief extern)

## Veranderingen op de server

### 1. Nginx configuratie (RSA Health)

Bewerk `/etc/nginx/sites-enabled/rsa_health`:

```nginx
server {
    listen 80;
    server_name _;

    # Alleen toegang vanaf localhost toestaan (via SSH tunnel)
    # Extern verkeer wordt geweigerd
    allow 127.0.0.1;
    deny all;

    location / {
        proxy_pass http://127.0.0.1:8000;

        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Toepassen:
```bash
sudo nano /etc/nginx/sites-enabled/rsa_health
# Of bewerk het bestand met de bovenstaande content
sudo nginx -t  # Test configuratie
sudo systemctl reload nginx  # Herlaad configuratie
```

### 2. Code aanpassing (main.py)

De security middleware is aangepast om IP-adres te halen uit X-Forwarded-For header:

```python
# main.py - security middleware
async def _security_middleware(request: Request, call_next):
    # Get client IP - check X-Forwarded-For header first (for nginx proxy)
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        ip = x_forwarded_for.split(",")[0].strip()
    else:
        ip = request.client.host if request.client else "unknown"

    # Health endpoint: only allow localhost access (via SSH tunnel)
    if request.url.path == "/health" and ip != "127.0.0.1":
        logging.warning("Blocked external access attempt to /health from IP: %s", ip)
        return Response(content="Forbidden", status_code=403)

    if ip in _BLOCKED_IPS:
        logging.warning("Blocked request from banned IP: %s %s", ip, request.url.path)
        return Response(content="Forbidden", status_code=403)
    if not _rate_limiter.allow(ip):
        logging.warning("Rate limit exceeded for IP: %s %s", ip, request.url.path)
        return Response(content="Rate limit exceeded", status_code=429)
    return await call_next(request)
```

### 3. Service herstarten

```bash
sudo systemctl reload nginx
sudo systemctl restart rsa_health.service
```

### 4. SSH Tunnel instructies

Voor toegang tot de health pagina:

```bash
# Vanaf je lokale machine
ssh -L 8000:localhost:8000 user@server-ip -N

# Open dan in je browser
http://localhost:8000/health
```

Of alternatief via SSH tunnel naar poort 80:
```bash
ssh -L 8080:localhost:80 user@server-ip -N
http://localhost:8080/health
```

### 5. Verificatie

Test dat de health endpoint geblokkeerd is vanaf extern:
```bash
curl http://server-ip:80/health
# Verwacht: 403 Forbidden (van nginx)
```

Test dat deze toegankelijk is via SSH tunnel:
```bash
# Na het maken van SSH tunnel
curl http://localhost:8000/health
# Verwacht: JSON response met server status
```

## Log control

Geblokkeerde toegangspogingen worden gelogd in:
- `logs/rsa_health.log` - "Blocked external access attempt to /health from IP: <ip>"

## Configuratie

De health endpoint IP-beperking is hardcoded in `main.py`. 
Geen configuratie nodig in `config_rsa_health.json`.


# Recommendations
Sta SSH toe vanaf elk IP
```bash   
sudo ufw allow 22/tcp
```
Maar beveilig met:
- SSH sleutel authenticatie (geen wachtwoorden)
- fail2ban voor SSH (blokkeert brute force)
- Wijzig SSH-poort (bijv. 2222)
- Alleen specifieke gebruikers toestaan