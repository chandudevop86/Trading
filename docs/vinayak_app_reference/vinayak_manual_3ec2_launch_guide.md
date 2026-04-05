# Vinayak Manual 3-EC2 Production Launch Guide

## Purpose

This runbook describes the manual, production-first launch process for Vinayak across three EC2 instances before introducing shell automation, Ansible, Terraform, Docker, or Kubernetes.

## Tier Layout

### 1. Web EC2

Responsibilities:
- public HTTPS entrypoint
- Nginx reverse proxy
- TLS termination
- forwarding browser traffic to the App EC2 private IP

Run on this host:
- Nginx only

### 2. App EC2

Responsibilities:
- FastAPI app
- admin and user web pages
- API routes
- outbox worker
- queue worker
- execution orchestration
- observability and report generation

Run on this host:
- Vinayak API
- Vinayak outbox worker
- Vinayak queue worker

### 3. Data EC2

Responsibilities:
- PostgreSQL
- Redis
- RabbitMQ
- persistent state and broker services

Run on this host:
- PostgreSQL
- Redis
- RabbitMQ

## Network Flow

Browser -> Web EC2 Nginx -> App EC2 FastAPI -> Data EC2 services

All traffic between App EC2 and Data EC2 should use private IPs only.

## Security Groups

### Web EC2
- allow TCP 80 from the internet
- allow TCP 443 from the internet
- allow TCP 22 only from your admin IP

### App EC2
- allow TCP 8000 only from the Web EC2 security group
- allow TCP 22 only from your admin IP
- allow outbound TCP 5432, 6379, 5672 to the Data EC2 security group

### Data EC2
- allow TCP 5432 only from the App EC2 security group
- allow TCP 6379 only from the App EC2 security group
- allow TCP 5672 only from the App EC2 security group
- do not expose 15672 publicly unless you intentionally need the RabbitMQ UI
- allow TCP 22 only from your admin IP

## Recommended Hostnames

- Web EC2: vinayak-web
- App EC2: vinayak-app
- Data EC2: vinayak-data

## Manual Build Order

1. Prepare the Data EC2 first.
2. Prepare the App EC2 second.
3. Prepare the Web EC2 last.
4. Run validation checks from App EC2 to Data EC2.
5. Run browser checks through the Web EC2 public endpoint.

## Data EC2 Manual Setup

### Install services
Install manually:
- PostgreSQL
- Redis
- RabbitMQ

### PostgreSQL setup
Create:
- database: vinayak
- role/user: vinayak
- strong password

Bind PostgreSQL to the Data EC2 private IP or to the VPC interface only.

### Redis setup
Bind Redis to the private interface only and disable public access.

### RabbitMQ setup
Create a dedicated user such as `vinayak` with a strong password.
Do not rely on the guest account for shared or production-like environments.

### Data EC2 validation
Confirm from the host that:
- PostgreSQL listens on 5432
- Redis listens on 6379
- RabbitMQ listens on 5672

## App EC2 Manual Setup

### Install base packages
Install manually:
- Python 3.12
- pip
- venv support
- Git
- build tools required by Python dependencies

### Deploy code
Clone the repo into a stable path, for example:
- /opt/vinayak

### Create a Python environment
Create a virtual environment and install dependencies from:
- [requirements.txt](/F:/Trading/requirements.txt)

### Configure production env
Create a real env file based on:
- [prod.env.example](/F:/Trading/infra/app/env/prod.env.example)

Point these values to the Data EC2 private IP:
- `VINAYAK_DATABASE_URL=postgresql+psycopg2://vinayak:<db-password>@<data-private-ip>:5432/vinayak`
- `REDIS_URL=redis://<data-private-ip>:6379/0`
- `MESSAGE_BUS_URL=amqp://vinayak:<rabbitmq-password>@<data-private-ip>:5672/`

Also set:
- `VINAYAK_ADMIN_USERNAME`
- `VINAYAK_ADMIN_PASSWORD`
- `VINAYAK_ADMIN_SECRET`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`
- `VINAYAK_SECURE_COOKIES=true`

### Run migrations
Before opening traffic, run the Alembic upgrade described in:
- [README.md](/F:/Trading/app/vinayak/README.md)

### Start processes manually first
Start these processes manually during the first launch:
- API process
- outbox worker
- queue worker

After validation, move them under `systemd`.

### App EC2 validation
Verify locally on the App EC2:
- `/health`
- `/health/live`
- `/health/ready`
- `/login`
- `/admin`

Also verify the App EC2 can reach the Data EC2 on:
- 5432
- 6379
- 5672

## Web EC2 Manual Setup

### Install Nginx
Install Nginx and keep this host dedicated to web ingress.

### Configure reverse proxy
Use:
- [default.conf](/F:/Trading/infra/app/nginx/default.conf)

For the manual 3-EC2 setup, change the upstream from the compose service name to the App EC2 private IP, for example:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://10.0.2.15:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### HTTPS
For production-style launch, enable HTTPS on the Web EC2 or place an ALB in front later.

### Web EC2 validation
Verify:
- Nginx config syntax passes
- proxy can reach the App EC2 private IP
- browser login works end to end

## First Launch Checklist

1. Data EC2 services are running and private.
2. App EC2 env file has real secrets.
3. App EC2 migrations completed successfully.
4. App EC2 API and workers start without import or connection errors.
5. Web EC2 Nginx points to the App EC2 private IP.
6. HTTPS is enabled before public use.
7. `/health/ready` reports healthy dependencies.
8. `/login`, `/admin`, `/workspace`, and `/dashboard/live-analysis` work in browser.
9. Worker-driven notifications are tested if Telegram is enabled.

## Operational Practice From Now On

Use this maturity path:
1. manual installation and process understanding
2. systemd services
3. backup and log discipline
4. shell automation
5. Ansible
6. Terraform
7. Docker
8. Kubernetes

This order gives you a clean operational baseline before introducing orchestration complexity.

## Immediate Next Improvements After Manual Launch

- convert the API and worker commands into `systemd` unit files
- add daily PostgreSQL backups
- add log rotation and disk monitoring
- document rollback steps
- move Data EC2 services into managed replacements later if needed

## Summary

Vinayak can be launched today in a manual 3-EC2 model with:
- one Web EC2 for Nginx
- one App EC2 for the API and workers
- one Data EC2 for PostgreSQL, Redis, and RabbitMQ

That is the cleanest production-first manual topology for the current project before automation.
