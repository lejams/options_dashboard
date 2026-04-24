#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/options_dashboard}"
DATA_DIR="${DATA_DIR:-/opt/options_data}"
REPO_URL="${REPO_URL:-https://github.com/lejams/options_dashboard.git}"
APP_USER="${APP_USER:-www-data}"
APP_GROUP="${APP_GROUP:-www-data}"

sudo apt update
sudo apt install -y python3-venv python3-pip git nginx

sudo mkdir -p "$APP_DIR" "$DATA_DIR"
sudo chown -R "$USER":"$USER" "$APP_DIR" "$DATA_DIR"

if [ ! -d "$APP_DIR/.git" ]; then
  git clone "$REPO_URL" "$APP_DIR"
else
  git -C "$APP_DIR" pull --ff-only
fi

cd "$APP_DIR"
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

if [ ! -f .env ]; then
  cp .env.example .env
fi

python3 -m py_compile app.py index.py

sudo chown -R "$APP_USER":"$APP_GROUP" "$APP_DIR"
sudo chown -R "$APP_USER":"$APP_GROUP" "$DATA_DIR"

echo "Bootstrap complete."
echo "Edit $APP_DIR/.env and set OPTIONS_DATA_ROOT=$DATA_DIR."
echo "Then install deploy/options-dashboard.service with systemd."
