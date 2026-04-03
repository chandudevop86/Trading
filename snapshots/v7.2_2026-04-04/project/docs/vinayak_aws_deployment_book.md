# Vinayak AWS Deployment Book

Date: 2026-04-04
Project: Vinayak
Format: Consolidated architecture, operations, and rollout dossier

## Title Page

Vinayak AWS Deployment Book

Prepared for:
- Vinayak production separation planning
- AWS deployment readiness
- staged live-validation rollout

This book combines the recommended AWS architecture, operational runbook, and rollout/cost planning into one document.

## Table of Contents

1. Executive Summary
2. Chapter 1: AWS 3-Tier Architecture
3. Chapter 2: Security Groups and Network Layout
4. Chapter 3: EC2 and Application Setup
5. Chapter 4: PostgreSQL and Data Layer
6. Chapter 5: Nginx and Systemd
7. Chapter 6: Verification and Operations
8. Chapter 7: Sizing and Cost Planning
9. Chapter 8: Rollout Phases
10. Final Recommendation

## Executive Summary

Vinayak should run as a clean AWS three-tier deployment:

1. Web tier
   Public-facing entry point and reverse proxy

2. Application tier
   FastAPI, trading orchestration, execution, observability

3. Data tier
   PostgreSQL as durable state

This separation improves reliability, security, and production readiness for controlled live validation.

## Chapter 1: AWS 3-Tier Architecture

### Web Tier

Recommended:
- EC2 with Nginx behind an Application Load Balancer

Responsibilities:
- receive public traffic
- reverse proxy to app tier
- avoid storing broker or DB credentials

### Application Tier

Recommended:
- private EC2 running `vinayak.api.main:app`

Responsibilities:
- FastAPI
- strategy evaluation
- broker integration
- observability APIs
- operator actions

### Data Tier

Recommended:
- Amazon RDS PostgreSQL

Responsibilities:
- reviewed trades
- executions
- audit logs
- application state

### Optional Cache Tier

Recommended:
- Amazon ElastiCache Redis

Responsibilities:
- cache
- freshness snapshots
- coordination

## Chapter 2: Security Groups and Network Layout

### VPC Layout

Use:
- 2 public subnets
- 2 private app subnets
- 2 private data subnets

### Public Layer

Place:
- ALB
- optional web EC2

### Private App Layer

Place:
- app EC2 hosts

### Private Data Layer

Place:
- RDS PostgreSQL
- Redis

### Security Groups

`sg-alb`
- inbound `80/443` from internet

`sg-web`
- inbound web traffic only from `sg-alb`
- SSH only from your IP

`sg-app`
- inbound `8000` only from `sg-web` or `sg-alb`
- SSH only from bastion or your IP

`sg-db`
- inbound `5432` only from `sg-app`

`sg-redis`
- inbound `6379` only from `sg-app`

Important:
- database and Redis must not be public

## Chapter 3: EC2 and Application Setup

### Web Host

Recommended size:
- `t3.micro`

Purpose:
- Nginx
- public entry
- low-resource reverse proxy work

### App Host

Minimum:
- `t3.small`

Preferred:
- `t3.medium`

Purpose:
- FastAPI
- pandas work
- execution logic
- observability

Important:
- do not use `t3.micro` for real live-validation workloads

### Ubuntu Setup Commands

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx git curl
cd /opt
sudo git clone <your-repo-url> Trading
sudo chown -R $USER:$USER /opt/Trading
cd /opt/Trading
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install fastapi "uvicorn[standard]" python-multipart
```

### Recommended Swap

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
free -h
```

Persist:

```bash
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

## Chapter 4: PostgreSQL and Data Layer

### Recommended Database

- Amazon RDS PostgreSQL 16

### Baseline Settings

- `db.t3.small` minimum
- private subnets only
- public access disabled
- Multi-AZ enabled for production
- automated backups enabled

### Why PostgreSQL

- durable execution records
- better concurrency than SQLite
- stronger audit trail
- easier scaling

### Example Connection String

```text
postgresql+psycopg2://vinayak_app:YOUR_PASSWORD@YOUR_RDS_ENDPOINT:5432/vinayak
```

### Example Environment File

```env
DATABASE_URL=postgresql+psycopg2://vinayak_app:YOUR_PASSWORD@YOUR_RDS_ENDPOINT:5432/vinayak
REDIS_URL=redis://YOUR_REDIS_ENDPOINT:6379/0
DHAN_CLIENT_ID=your_dhan_client_id
DHAN_ACCESS_TOKEN=your_dhan_access_token
TELEGRAM_TOKEN=your_telegram_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
LIVE_TRADING_ENABLED=false
TRADING_BROKER_MODE=PAPER
```

## Chapter 5: Nginx and Systemd

### Nginx Example

```nginx
server {
    listen 80;
    server_name vinayak.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Systemd Example

```ini
[Unit]
Description=Vinayak FastAPI
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/opt/Trading
EnvironmentFile=/etc/vinayak/vinayak.env
ExecStart=/opt/Trading/venv/bin/python -m uvicorn vinayak.api.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Chapter 6: Verification and Operations

### Health Checks

```bash
curl http://127.0.0.1:8000/health
ss -tulpn | grep 8000
sudo journalctl -u vinayak -n 100 --no-pager
sudo journalctl -u nginx -n 100 --no-pager
```

### Deployment Order

1. Create RDS
2. Create Redis if needed
3. Launch app EC2
4. Install dependencies
5. Configure env file
6. Create systemd service
7. verify `127.0.0.1:8000`
8. configure Nginx
9. connect ALB
10. point Route 53

## Chapter 7: Sizing and Cost Planning

### Lean Validation Stack

- web `t3.micro`
- app `t3.small`
- db `db.t3.small`

Approximate monthly range:
- USD 45 to 90

### Stronger Validation Stack

- web `t3.micro`
- app `t3.medium`
- db `db.t3.small`
- optional Redis

Approximate monthly range:
- USD 80 to 160

### Production-Oriented Baseline

- ALB
- separate web and app
- Multi-AZ RDS
- Redis
- CloudWatch

Approximate monthly range:
- USD 180 to 400 plus transfer and storage growth

## Chapter 8: Rollout Phases

### Phase 1: Controlled Paper Validation

Goal:
- separated AWS tiers
- paper-only unattended execution
- clean deployment behavior

### Phase 2: Small Live Validation

Goal:
- minimum capital
- one strategy
- one symbol group
- strict risk caps

### Phase 3: Hardened Pre-Production

Add:
- Redis
- Secrets Manager
- CloudWatch alarms
- reconciliation automation

### Phase 4: Stronger Production Posture

Add:
- ALB
- Multi-AZ database
- better backups
- optional second app node

## Final Recommendation

For Vinayak, the best near-term AWS production shape is:

- separate web page tier
- separate private application tier
- separate private PostgreSQL database
- optional Redis
- CloudWatch and Secrets Manager
- phased rollout from paper to tiny live validation

This is the right structure for Vinayak if you want it to look and behave like a serious trading platform rather than a single-host experiment.
