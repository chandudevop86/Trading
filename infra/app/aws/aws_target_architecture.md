# AWS Target Architecture For Vinayak

## Goal

Deploy Vinayak as a production-style three-tier AWS platform using managed services wherever possible.

## Final Target

1. Web tier
- Route 53
- ACM
- Application Load Balancer

2. App tier
- ECS Fargate service: `vinayak-api`
- optional ECS worker service later: `vinayak-worker`

3. Data tier
- RDS PostgreSQL
- ElastiCache Redis
- S3
- Secrets Manager

## Request Flow

1. User opens the Vinayak domain.
2. Route 53 resolves to the ALB.
3. ACM terminates TLS on the ALB.
4. ALB forwards `/`, `/admin`, `/workspace`, `/dashboard/*`, `/strategies/*`, `/executions/*` to the ECS service.
5. ECS tasks run the FastAPI app.
6. FastAPI reads and writes persistent state in RDS.
7. FastAPI reads and writes hot cache state in Redis.
8. FastAPI stores reports and exports in S3.
9. Broker and Telegram credentials are loaded from Secrets Manager.

## Why This Matches Vinayak Now

Vinayak already has:
- web/admin/workspace layer
- API/service separation
- DB repository layer
- live analysis route
- execution routes
- health endpoints suitable for ALB health checks

That means the codebase now fits the AWS three-tier shape more naturally than the original Streamlit monolith.

## Recommended AWS Build Order

1. Put the current app in ECS Fargate behind ALB.
2. Switch from SQLite/demo DB to RDS PostgreSQL.
3. Add Redis for OHLCV and option metrics cache.
4. Move generated report/export artifacts to S3.
5. Load secrets from Secrets Manager.
6. Add CloudWatch alarms and dashboards.
7. Add a second ECS worker service if async jobs become heavy.

## Security Rules

- ALB in public subnets
- ECS tasks in private subnets
- RDS and Redis in private data subnets
- security groups should only allow required east-west traffic
- no public DB access
- IAM roles for ECS task access to S3, Secrets Manager, and CloudWatch

## Vinayak Environment Variables

Core:
- `VINAYAK_DATABASE_URL`
- `REDIS_URL`
- `REPORTS_S3_BUCKET`
- `AWS_REGION`

Broker and notification:
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## ECS Services

### vinayak-api

Responsibilities:
- serve FastAPI routes
- serve admin console and workspace page
- run live-analysis workflow requests
- execute broker and Telegram integrations

### vinayak-worker (optional later)

Responsibilities:
- async alerting
- scheduled refresh jobs
- report generation
- reconciliation jobs

## Persistence Notes

Current project still has local-file fallbacks for some report/log outputs. For the AWS target, those should gradually move toward:
- RDS for transactional state
- S3 for export/report artifacts
- Redis for short-lived cache

## Health Checks

Use these for ALB target group checks:
- `GET /health/live`
- `GET /health/ready`

