#!/usr/bin/env bash
set -euo pipefail

APP_DIR=${APP_DIR:-/opt/trading}
BRANCH=${BRANCH:-main}
SERVICE_NAME=${SERVICE_NAME:-vinayak-api.service}
PYTHON_BIN=${PYTHON_BIN:-python}

cd "$APP_DIR"
git fetch --all --prune
git checkout "$BRANCH"
git pull origin "$BRANCH"

. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r app/vinayak/requirements.txt pytest alembic

python -m alembic -c app/vinayak/alembic.ini upgrade head
python -m pytest tests/unit/vinayak -q
python -m py_compile app/main.py app/vinayak/api/main.py

sudo systemctl restart "$SERVICE_NAME"
sudo systemctl status "$SERVICE_NAME" --no-pager
