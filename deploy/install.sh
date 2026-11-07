#!/usr/bin/env bash
set -euo pipefail
# Install only; never start trading automatically.
if [[ $(id -u) != 0 ]]; then echo 'Run with sudo on a systemd Linux host'; exit 1; fi
command -v systemctl >/dev/null
id khaled >/dev/null 2>&1 || useradd --create-home --shell /bin/bash khaled
source_dir=$(cd -- "$(dirname -- "$0")/.." && pwd)
target=/home/khaled/Khaled
if [[ "$source_dir" != "$target" ]]; then
  echo "Place the checkout at $target before installation"; exit 1
fi
[[ -f "$target/.env" && -x "$target/.venv/bin/python" ]]
chmod 600 "$target/.env"
chown -R khaled:khaled "$target"
install -m 644 "$target/deploy/khaled-bot.service" /etc/systemd/system/khaled-bot.service
install -m 644 "$target/deploy/logrotate.conf" /etc/logrotate.d/khaled
install -m 644 "$target/deploy/khaled-health.service" /etc/systemd/system/khaled-health.service
systemctl daemon-reload
systemctl enable khaled-bot.service khaled-health.service
echo 'Installed; start explicitly with: sudo systemctl start khaled-health khaled-bot'
