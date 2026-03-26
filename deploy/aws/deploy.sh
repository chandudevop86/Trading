#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/trading"
DOMAIN="chandudevopai.shop"

export APP_ENV="${APP_ENV:-production}"
export LEGACY_DEPLOYMENT_TARGET="production"
python3 -m src.deployment_guard --target production

echo "[1/9] Installing packages..."
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx certbot python3-certbot-nginx git

if [ ! -d "$APP_DIR" ]; then
  echo "[2/9] Cloning repo into $APP_DIR ..."
  sudo git clone REPLACE_GIT_REPO_URL "$APP_DIR"
else
  echo "[2/9] Repo already exists: $APP_DIR"
fi

sudo chown -R "$USER":"$USER" "$APP_DIR"
cd "$APP_DIR"

echo "[3/9] Python env + deps..."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "[4/9] Streamlit config..."
mkdir -p .streamlit
cp deploy/streamlit/config.toml .streamlit/config.toml

echo "[5/9] Systemd service..."
sudo cp deploy/aws/intratrade.service /etc/systemd/system/intratrade.service
sudo systemctl daemon-reload
sudo systemctl enable intratrade
sudo systemctl restart intratrade

echo "[6/9] Nginx config..."
sudo cp deploy/aws/chandudevopai.shop.nginx.conf /etc/nginx/sites-available/$DOMAIN.conf
sudo ln -sf /etc/nginx/sites-available/$DOMAIN.conf /etc/nginx/sites-enabled/$DOMAIN.conf
sudo nginx -t
sudo systemctl reload nginx

echo "[7/9] Certbot SSL..."
sudo certbot --nginx -d "$DOMAIN" -d "www.$DOMAIN" --non-interactive --agree-tos -m REPLACE_EMAIL --redirect || true

echo "[8/9] Health checks..."
sudo systemctl status intratrade --no-pager || true
sudo systemctl status nginx --no-pager || true

echo "[9/9] Done. Open: https://$DOMAIN"
