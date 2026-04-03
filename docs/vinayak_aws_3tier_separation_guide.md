# Vinayak AWS 3-Tier Separation Guide

Date: 2026-04-04
Project: Vinayak
Goal: Separate the web page, application runtime, and database into clean AWS tiers suitable for controlled production growth.

## Executive Summary

Vinayak should run as a three-tier AWS architecture:

1. Web tier
   Hosts the public-facing web entry point and reverse proxy.

2. Application tier
   Hosts FastAPI, trading orchestration, broker integrations, observability APIs, and execution logic.

3. Data tier
   Hosts PostgreSQL as the durable system of record for executions, reviewed trades, audit logs, and application state.

This separation reduces blast radius, improves security, and creates a much better path toward stable live validation and production hardening.

## Recommended AWS Topology

### Tier 1: Web

Recommended service:
- EC2 running Nginx behind an Application Load Balancer

Purpose:
- serve the Vinayak web entry point
- terminate or forward HTTP traffic
- reverse proxy requests to the private application tier
- avoid placing broker credentials or DB credentials on the web host

### Tier 2: Application

Recommended service:
- Private EC2 running `uvicorn` or `gunicorn + uvicorn workers`

Purpose:
- run `vinayak.api.main:app`
- run trading analysis and execution flows
- connect to Dhan and Telegram
- emit observability metrics
- handle operator actions from the web tier

### Tier 3: Database

Recommended service:
- Amazon RDS for PostgreSQL in private subnets

Purpose:
- store reviewed trades
- store executions and execution claims
- store audit logs and operational state
- replace SQLite for production-style deployment

### Optional Tier: Cache and Coordination

Recommended service:
- Amazon ElastiCache for Redis

Purpose:
- short-lived cache
- freshness snapshots
- event buffering
- lightweight coordination and locking

## Network Design

Use one VPC with:
- 2 public subnets
- 2 private application subnets
- 2 private data subnets

### Public Subnets

Place:
- Application Load Balancer
- optional web EC2 if you want a dedicated Nginx tier

### Private App Subnets

Place:
- `vinayak-app-1`
- `vinayak-app-2` later if you scale out

### Private Data Subnets

Place:
- RDS PostgreSQL
- ElastiCache Redis

## Security Groups

### `sg-alb`

Allow:
- `80/tcp` from `0.0.0.0/0`
- `443/tcp` from `0.0.0.0/0`

### `sg-web`

Allow:
- `80/tcp` or `8080/tcp` only from `sg-alb`
- `22/tcp` only from your office or home IP

### `sg-app`

Allow:
- `8000/tcp` only from `sg-web` or `sg-alb`
- `22/tcp` only from bastion or your IP

### `sg-db`

Allow:
- `5432/tcp` only from `sg-app`

### `sg-redis`

Allow:
- `6379/tcp` only from `sg-app`

Important rule:
- RDS and Redis must not be publicly reachable

## DNS and Traffic Flow

1. User opens `vinayak.yourdomain.com`
2. Route 53 sends traffic to the ALB
3. ALB forwards to Nginx web tier or directly to the app tier
4. App tier connects privately to RDS and Redis

## EC2 Layout

### Web Instance

Name:
- `vinayak-web`

Recommended size:
- `t3.micro`

Responsibilities:
- Nginx
- static assets if needed
- TLS proxy path

### App Instance

Name:
- `vinayak-app`

Recommended size:
- minimum `t3.small`
- preferred `t3.medium`

Responsibilities:
- FastAPI app
- background trading logic
- broker integration
- observability APIs

Important:
- do not use `t3.micro` for serious live validation
- add swap if you stay on a small host

## Database Design

### Recommended Engine

- Amazon RDS PostgreSQL 16

### Recommended Production Settings

- Multi-AZ enabled
- automated backups enabled
- `gp3` storage
- private subnets only
- public access disabled

### Why PostgreSQL

- strong transactional safety
- durable execution records
- better concurrency handling than SQLite
- easier audit and reporting

## Secrets and Credentials

Use:
- AWS Secrets Manager or Parameter Store

Store:
- Dhan credentials
- Telegram token and chat ID
- database URL
- Redis URL
- environment toggles for production

Do not store these on the web tier.

## Deployment Flow

1. Create VPC and subnet layout
2. Create RDS PostgreSQL
3. Create Redis if needed
4. Launch app EC2 in private subnet
5. Launch web EC2 or ALB-linked reverse proxy
6. Configure security groups
7. Configure environment variables and secrets
8. Start `vinayak.api.main:app`
9. Verify `/health`
10. Verify `/dashboard/observability`
11. Run one manual live-analysis cycle in paper mode

## Nginx Reverse Proxy Example

```nginx
server {
    listen 80;
    server_name vinayak.yourdomain.com;

    location / {
        proxy_pass http://APP_PRIVATE_IP:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## Systemd Application Service Example

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

## Observability Recommendations

Use:
- CloudWatch logs
- CloudWatch alarms
- application health endpoint
- database metrics
- instance memory and CPU alerts

Track at minimum:
- execution attempts
- execution success and failures
- execution blocked counts
- daily PnL
- kill-switch state
- market-data freshness

## Production Readiness Notes

For Vinayak, the correct progression is:

1. Separate the tiers
2. Move to PostgreSQL
3. Keep auto execution in paper mode for unattended flows
4. Use very small capital for live validation
5. harden alerts and reconciliation

## Final Recommendation

The best practical AWS architecture for Vinayak is:

- ALB
- separate web tier
- separate private app tier
- separate private RDS PostgreSQL database
- optional Redis
- Secrets Manager
- CloudWatch

This gives you the separation you asked for:

- web page separate
- application separate
- database separate

It is the right structure for moving Vinayak from a single-host deployment toward controlled production validation.
