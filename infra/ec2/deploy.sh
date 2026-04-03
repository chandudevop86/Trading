#!/usr/bin/env bash
set -euo pipefail

APP_DIR=${APP_DIR:-/opt/trading}
BRANCH=${BRANCH:-main}

export APP_ENV="${APP_ENV:-production}"
export LEGACY_DEPLOYMENT_TARGET="production"
python3 -m src.deployment_guard --target production

cd "$APP_DIR"
git fetch --all --prune
git checkout "$BRANCH"
git pull origin "$BRANCH"
. .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -p "test_*.py" -v
sudo systemctl restart trading-ui.service
sudo systemctl restart trading-daemon.service
sudo systemctl status trading-ui.service --no-pager
sudo systemctl status trading-daemon.service --no-pager
