#!/usr/bin/env bash
set -euo pipefail

APP_USER=${APP_USER:-ubuntu}
APP_DIR=${APP_DIR:-/opt/trading}
APP_ENV_FILE=${APP_ENV_FILE:-/etc/trading/vinayak.production.env}
PYTHON_BIN=${PYTHON_BIN:-python3}
SERVICE_NAME=${SERVICE_NAME:-vinayak-api.service}

sudo apt-get update -y
sudo apt-get install -y git python3 python3-venv python3-pip unzip awscli

sudo mkdir -p "$APP_DIR" /etc/trading /var/log/trading
sudo chown -R "$APP_USER":"$APP_USER" "$APP_DIR" /var/log/trading

if [ ! -d "$APP_DIR/.git" ]; then
  sudo -u "$APP_USER" git clone https://github.com/YOUR_ORG/YOUR_REPO.git "$APP_DIR"
fi

sudo -u "$APP_USER" bash -lc "cd '$APP_DIR' && $PYTHON_BIN -m venv .venv"
sudo -u "$APP_USER" bash -lc "cd '$APP_DIR' && . .venv/bin/activate && python -m pip install --upgrade pip && python -m pip install -r app/vinayak/requirements.txt alembic"

if [ ! -f "$APP_ENV_FILE" ]; then
  sudo cp "$APP_DIR/infra/production/env/vinayak.production.env.example" "$APP_ENV_FILE"
  sudo chmod 600 "$APP_ENV_FILE"
fi

sudo cp "$APP_DIR/infra/production/systemd/vinayak-api.service" /etc/systemd/system/vinayak-api.service
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"

echo "Bootstrap complete. Edit $APP_ENV_FILE, run Alembic migrations, then start $SERVICE_NAME."
