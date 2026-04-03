#!/usr/bin/env bash
set -euo pipefail

APP_USER=${APP_USER:-ubuntu}
APP_DIR=${APP_DIR:-/opt/trading}
APP_ENV_FILE=${APP_ENV_FILE:-/etc/trading/trading.env}
PYTHON_BIN=${PYTHON_BIN:-python3}

sudo apt-get update -y
sudo apt-get install -y git python3 python3-venv python3-pip unzip awscli

sudo mkdir -p "$APP_DIR" /etc/trading /var/log/trading
sudo chown -R "$APP_USER":"$APP_USER" "$APP_DIR" /var/log/trading

if [ ! -d "$APP_DIR/.git" ]; then
  sudo -u "$APP_USER" git clone https://github.com/YOUR_ORG/YOUR_REPO.git "$APP_DIR"
fi

sudo -u "$APP_USER" bash -lc "cd '$APP_DIR' && $PYTHON_BIN -m venv .venv"
sudo -u "$APP_USER" bash -lc "cd '$APP_DIR' && . .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt"

if [ ! -f "$APP_ENV_FILE" ]; then
  sudo cp "$APP_DIR/infra/config/trading.env.example" "$APP_ENV_FILE"
  sudo chmod 600 "$APP_ENV_FILE"
fi

sudo cp "$APP_DIR/infra/systemd/trading-ui.service" /etc/systemd/system/trading-ui.service
sudo cp "$APP_DIR/infra/systemd/trading-daemon.service" /etc/systemd/system/trading-daemon.service
sudo systemctl daemon-reload
sudo systemctl enable trading-ui.service
sudo systemctl enable trading-daemon.service

echo "Bootstrap complete. Edit $APP_ENV_FILE before starting live trading."

