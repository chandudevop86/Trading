#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/trading"
DOMAIN="chandudevopai.shop"
SERVICE_NAME="vinayak-api"

export APP_ENV="${APP_ENV:-production}"

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
python -m pip install --upgrade pip
python -m pip install -r app/vinayak/requirements.txt alembic

echo "[4/9] Production env..."
sudo mkdir -p /etc/trading
if [ ! -f /etc/trading/vinayak.production.env ]; then
  sudo cp infra/production/env/vinayak.production.env.example /etc/trading/vinayak.production.env
  sudo chmod 600 /etc/trading/vinayak.production.env
fi

echo "[5/9] Database migrations..."
python -m alembic -c app/vinayak/alembic.ini upgrade head

echo "[6/9] Systemd service..."
sudo cp infra/production/systemd/vinayak-api.service /etc/systemd/system/vinayak-api.service
sudo systemctl daemon-reload
sudo systemctl enable vinayak-api.service
sudo systemctl restart vinayak-api.service

echo "[7/9] Nginx config..."
sudo cp infra/aws/chandudevopai.shop.nginx.conf /etc/nginx/sites-available/$DOMAIN.conf
sudo ln -sf /etc/nginx/sites-available/$DOMAIN.conf /etc/nginx/sites-enabled/$DOMAIN.conf
sudo nginx -t
sudo systemctl reload nginx

echo "[8/9] Certbot SSL..."
sudo certbot --nginx -d "$DOMAIN" -d "www.$DOMAIN" --non-interactive --agree-tos -m REPLACE_EMAIL --redirect || true

echo "[9/9] Health checks..."
sudo systemctl status vinayak-api.service --no-pager || true
sudo systemctl status nginx --no-pager || true

echo "Done. Open: https://$DOMAIN"
