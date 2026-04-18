# Deployment

## Supported runtime

Vinayak is deployed as a FastAPI service via:
- `app.main:app`

Do not deploy deprecated `src/*` runtime surfaces for supported production operation.

## Linux systemd deployment

1. Create the virtual environment and install runtime dependencies:
```bash
python3 -m venv venv
. venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r app/vinayak/requirements.txt alembic
```

2. Create the production environment file from:
- `infra/production/env/vinayak.production.env.example`

3. Run database migrations explicitly before restart:
```bash
python -m alembic -c app/vinayak/alembic.ini upgrade head
```

4. Install the service unit:
- `infra/production/systemd/vinayak-api.service`

5. Reload and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable vinayak-api.service
sudo systemctl restart vinayak-api.service
sudo systemctl status vinayak-api.service --no-pager
```

## EC2 deploy script

Use:
- `infra/ec2/deploy.sh`

It now performs:
- git pull
- dependency install from `app/vinayak/requirements.txt`
- `alembic upgrade head`
- `pytest tests/unit/vinayak -q`
- entrypoint syntax validation
- restart of `vinayak-api.service`

## Docker

Primary image:
- repo root `Dockerfile`

Production API image:
- `infra/production/docker/Dockerfile.api`

Both now target:
```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Migration discipline

Production startup should not be treated as the migration mechanism.

Use Alembic explicitly:
```bash
python -m alembic -c app/vinayak/alembic.ini current
python -m alembic -c app/vinayak/alembic.ini upgrade head
```

Current migration tree lives under:
- `app/vinayak/db/migrations/versions/`

## Pre-deploy checks

Run at minimum:
```bash
python -m pytest tests/unit/vinayak -q
python -m py_compile app/main.py app/vinayak/api/main.py
```

## Reverse proxy expectation

Nginx or ingress should send the supported app web/API paths to the FastAPI service on port `8000`.
