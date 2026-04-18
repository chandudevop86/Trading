# AWS Deployment Architecture

## Supported production surface

Vinayak production deployment is now centered on one runtime:
- FastAPI service via `app.main:app`

Deprecated `src/*` operator surfaces are not supported deployment targets.

## Runtime topology

1. Edge layer
- Nginx listens on `80/443`
- Nginx proxies to the FastAPI service on `127.0.0.1:8000`

2. App layer
- `vinayak-api.service` runs `python -m uvicorn app.main:app --host 0.0.0.0 --port 8000`
- Reviewed-trade approval, execution routing, dashboards, auth, and health endpoints live behind the same FastAPI runtime

3. Data layer
- PostgreSQL for authoritative relational state
- Redis for cache/guard support
- optional RabbitMQ/message bus for async event handling
- local artifact/report storage only as secondary support

## EC2 setup steps
1. Launch Ubuntu 22.04 EC2
2. Clone repo into `/opt/trading`
3. Run:
   - `bash infra/ec2/bootstrap.sh`
4. Create `/etc/trading/vinayak.production.env`
5. Run migrations:
   - `python -m alembic -c app/vinayak/alembic.ini upgrade head`
6. Start service:
   - `sudo systemctl restart vinayak-api.service`
7. Install nginx config from:
   - `infra/aws/chandudevopai.shop.nginx.conf`

## Service operations
- `sudo systemctl restart vinayak-api.service`
- `sudo systemctl status vinayak-api.service --no-pager`
- `curl http://127.0.0.1:8000/health/live`
- `curl http://127.0.0.1:8000/health/ready`

## CI/CD
GitHub Actions file:
- `.github/workflows/deploy.yml`

Expected flow:
1. install dependencies from `app/vinayak/requirements.txt`
2. run `pytest tests/unit/vinayak -q`
3. build Docker image
4. deploy to EC2
5. run Alembic migrations
6. restart `vinayak-api.service`

## Secrets handling
- keep runtime secrets in `/etc/trading/vinayak.production.env`
- `chmod 600`
- prefer IAM role for AWS access
- do not store broker or Telegram secrets in git

## Reverse proxy expectation
Nginx should proxy all supported app web/API paths to the FastAPI service on port `8000`.

## Migration note
Historical Streamlit and `src.operational_daemon` deployment paths are deprecated and retained only as migration reference, not as the supported production architecture.
