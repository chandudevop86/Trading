#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/trading"
DOMAIN="chandudevopai.shop"
REPO_URL="REPLACE_GIT_REPO_URL"
EMAIL="REPLACE_EMAIL"

echo "[1/10] Installing packages (Amazon Linux 2023)..."
sudo dnf update -y
sudo dnf install -y python3 python3-pip nginx git
python3 -m ensurepip --upgrade || true

if [ ! -d "$APP_DIR" ]; then
  echo "[2/10] Cloning repo into $APP_DIR ..."
  sudo git clone "$REPO_URL" "$APP_DIR"
else
  echo "[2/10] Repo already exists: $APP_DIR"
fi

sudo chown -R "$USER":"$USER" "$APP_DIR"
cd "$APP_DIR"

echo "[3/10] Python env + deps..."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "[4/10] Streamlit config..."
mkdir -p .streamlit
cp deploy/streamlit/config.toml .streamlit/config.toml

echo "[5/10] Systemd service..."
sudo cp deploy/aws/intratrade.service /etc/systemd/system/intratrade.service
sudo systemctl daemon-reload
sudo systemctl enable intratrade
sudo systemctl restart intratrade

echo "[6/10] Nginx config..."
sudo cp deploy/aws/chandudevopai.shop.nginx.conf /etc/nginx/conf.d/$DOMAIN.conf
sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx

echo "[7/10] Installing certbot..."
sudo dnf install -y certbot python3-certbot-nginx

echo "[8/10] Requesting SSL cert..."
sudo certbot --nginx -d "$DOMAIN" -d "www.$DOMAIN" --non-interactive --agree-tos -m "$EMAIL" --redirect || true

echo "[9/10] Health checks..."
sudo systemctl status intratrade --no-pager || true
sudo systemctl status nginx --no-pager || true

echo "[10/10] Done. Open: https://$DOMAIN"
