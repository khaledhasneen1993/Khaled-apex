#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$repo_dir"
python3 -c 'import sys; assert sys.version_info >= (3,11), "Python 3.11+ required; use Ubuntu inside Termux"'
fresh_args=()
server_args=()
for argument in "$@"; do
 case "$argument" in
  --fresh) fresh_args=(--fresh) ;;
  --no-autostart) server_args=(--no-autostart) ;;
  *) echo 'Usage: bash run_paper_dashboard.sh [--fresh] [--no-autostart]' >&2; exit 1 ;;
 esac
done
# Reserve/check the UI port before creating a new session or installing packages.
python3 - <<'PY'
import socket
with socket.socket() as s:
 s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
 try: s.bind(('127.0.0.1',8787))
 except OSError: raise SystemExit('Port 8787 is in use. Stop the old dashboard with Ctrl+C first.')
PY
export CASCADE_FINAL_DIR="${CASCADE_FINAL_DIR:-$repo_dir/.runtime/repos/Final}"
if [[ ! -d "$CASCADE_FINAL_DIR/.git" ]]; then
 if [[ -e "$CASCADE_FINAL_DIR" ]]; then
  echo 'Final source path exists without Git metadata; choose another CASCADE_FINAL_DIR.' >&2; exit 1
 fi
 mkdir -p -- "$(dirname -- "$CASCADE_FINAL_DIR")"
 git clone --depth 1 --branch main https://github.com/khaledhasneen1993/Final.git "$CASCADE_FINAL_DIR"
fi
if [[ ! -f "$CASCADE_FINAL_DIR/binance_cascade_hunter_dynamic_v2_paper.py" ]]; then
 echo 'Final Paper entry point is missing.' >&2; exit 1
fi
if [[ ! -x .venv-paper-dashboard/bin/python ]]; then
 python3 -m venv .venv-paper-dashboard
fi
expected_hash="$(sha256sum requirements.txt | cut -d ' ' -f 1)"
installed_hash="$(cat .venv-paper-dashboard/requirements.sha256 2>/dev/null || true)"
if [[ "$installed_hash" != "$expected_hash" ]]; then
 .venv-paper-dashboard/bin/python -m pip install -r requirements.txt
 printf '%s\n' "$expected_hash" > .venv-paper-dashboard/requirements.sha256
fi
export KHALED_PAPER_SESSION
KHALED_PAPER_SESSION="$(python3 paper_dashboard/sessions.py "${fresh_args[@]}")"
printf '\nPAPER ONLY: Final + Khaled | 100 USDT per new account\nSession: %s\nDashboard: http://127.0.0.1:8787\nCtrl+C stops both Paper bots and saves their state.\n\n' "$KHALED_PAPER_SESSION"
exec .venv-paper-dashboard/bin/python -u paper_dashboard/server.py "${server_args[@]}"
