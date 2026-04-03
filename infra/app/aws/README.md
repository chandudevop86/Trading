# Vinayak AWS Target: ECS + ALB + RDS + ElastiCache + S3 + Secrets Manager

This folder defines the selected target deployment model for Vinayak.

## Selected Architecture

You chose the full AWS path:

- Route 53 for DNS
- ACM for TLS certificates
- ALB as the public web entry point
- ECS Fargate for the Vinayak app tier
- RDS PostgreSQL for persistent relational state
- ElastiCache Redis for hot cache and short-lived runtime state
- S3 for reports, exports, and artifacts
- Secrets Manager for broker, Telegram, and DB credentials
- CloudWatch for logs, metrics, and alarms

## Tier Mapping

### Web Tier

- Route 53
- ACM
- ALB

Responsibilities:
- public HTTPS access
- TLS termination
- forwarding traffic to ECS services

### App Tier

- ECS Fargate service for `vinayak-api`
- optional ECS worker service later for async jobs

Responsibilities:
- FastAPI app
- admin console
- live workspace
- strategy execution workflow
- broker/Telegram integrations

### Data Tier

- RDS PostgreSQL
- ElastiCache Redis
- S3

Responsibilities:
- signals, reviewed trades, executions, audit logs
- OHLCV hot cache and option-chain cache
- HTML/PDF reports, CSV exports, archives

## Network Layout

- VPC across 2 or 3 AZs
- Public subnets: ALB only
- Private app subnets: ECS tasks
- Private data subnets: RDS and Redis
- NAT gateway for ECS outbound internet access when Yahoo, NSE, Telegram, or broker APIs are needed

## Runtime Secrets

Store these in Secrets Manager or SSM Parameter Store:

- `VINAYAK_DATABASE_URL`
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`
- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`
- any future broker or API keys

## Deployment Units In This Folder

- `ecs-task-definition.json`
- `ecs-service-vars.env.example`
- `aws_target_architecture.md`

## Operational Notes

- ALB should be the only public entry point
- ECS tasks should not be assigned public IPs in production unless you intentionally choose that model
- RDS should run Multi-AZ in production
- Redis should be private-only
- S3 should store generated reports and export bundles instead of local-only files
- CloudWatch alarms should cover ECS health, ALB 5xx, RDS CPU/storage, and task restarts
