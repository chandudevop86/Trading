#!/usr/bin/env bash
set -euo pipefail

DOMAIN="chandudevopai.shop"
APP_DIR="/opt/trading"

echo "Stopping app service..."
sudo systemctl stop intratrade || true
sudo systemctl disable intratrade || true
sudo rm -f /etc/systemd/system/intratrade.service
sudo systemctl daemon-reload

echo "Removing nginx config..."
sudo rm -f /etc/nginx/sites-enabled/$DOMAIN.conf
sudo rm -f /etc/nginx/sites-available/$DOMAIN.conf
sudo nginx -t || true
sudo systemctl reload nginx || true

echo "Optionally remove certs (commented by default)"
# sudo certbot delete --cert-name $DOMAIN

echo "Optionally remove app directory (commented by default)"
# sudo rm -rf "$APP_DIR"

echo "Undeploy complete (Ubuntu)."
