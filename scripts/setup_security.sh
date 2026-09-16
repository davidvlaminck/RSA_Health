#!/bin/bash
set -e

echo "=== RSA Health Security Setup ==="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root (sudo)"
    exit 1
fi

# Install fail2ban if not present
if ! command -v fail2ban-client &> /dev/null; then
    echo "Installing fail2ban..."
    apt-get update && apt-get install -y fail2ban ufw
else
    echo "fail2ban already installed"
fi

# Create filter directory
mkdir -p /etc/fail2ban/filter.d

# Copy filter and jail configs
echo "Copying fail2ban configurations..."
cp "$(dirname "$0")/../fail2ban/filter-rsa-health.conf" /etc/fail2ban/filter.d/rsa-health.conf
cp "$(dirname "$0")/../fail2ban/jail-rsa-health.conf" /etc/fail2ban/jail.d/rsa-health.conf

# Deploy logrotate config for PostgreSQL/PostGIS logs
echo "Deploying PostgreSQL logrotate config..."
mkdir -p /etc/logrotate.d
cp "$(dirname "$0")/../logrotate/postgresql-common" /etc/logrotate.d/postgresql-common
if ! command -v logrotate &> /dev/null; then
    apt-get update && apt-get install -y logrotate
fi
logrotate -d /etc/logrotate.d/postgresql-common >/dev/null 2>&1 \
    && echo "  logrotate config OK" \
    || echo "  logrotate config: dry-run warning (non-fatal)"

# Block known scanner IPs with ufw
echo "Blocking known scanner IPs..."
BLOCKED_IPS=(
    93.123.109.228
    47.114.87.90
    5.61.209.44
    5.61.209.92
    160.119.76.24
    36.255.33.242
    185.209.15.199
    45.198.224.26
    20.65.193.201
    213.209.159.175
    213.209.159.154
    80.94.95.211
    45.67.211.147
    94.154.46.243
    85.239.151.82
    151.243.18.111
    34.186.7.117
    98.98.47.133
    172.105.69.26
    93.152.221.68
)

for ip in "${BLOCKED_IPS[@]}"; do
    if ! ufw status | grep -q "$ip"; then
        ufw deny from "$ip" comment "RSA Health scanner"
        echo "  Blocked $ip"
    else
        echo "  Already blocked: $ip"
    fi
done

# Ensure ufw is active
echo "Enabling ufw..."
ufw --force enable

# Restart fail2ban
echo "Restarting fail2ban..."
systemctl restart fail2ban
systemctl enable fail2ban

echo ""
echo "=== Setup complete ==="
echo "Check fail2ban status: fail2ban-client status rsa-health"
echo "Check ufw status: ufw status numbered"
