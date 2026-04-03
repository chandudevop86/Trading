# AWS Deployment Architecture

## Architecture Diagram (Text)

1. UI Layer
- Streamlit UI runs on EC2 on port `8501`.
- For production internet access, place the instance behind an Application Load Balancer if public UI access is needed.
- Security Group should allow inbound `80/443` from the internet to the ALB and `8501` only from the ALB security group.

2. Application Layer
- `trading-ui.service` runs the Streamlit operator UI.
- `trading-daemon.service` runs `python -m src.operational_daemon` for background polling, strategy execution, broker routing, and artifact persistence.
- Execution engine routes validated trade candidates to `PaperBroker` by default or `DhanBroker` only when live is explicitly enabled.

3. Data Layer
- Local artifacts are written under `data/` and `logs/` for fast local access.
- CSV outputs are mirrored to S3 when `AWS_S3_BUCKET` is configured.
- CloudWatch Agent ships logs and EC2 host metrics to CloudWatch.
- Future-ready path: replace CSV artifacts with RDS/PostgreSQL or DynamoDB while keeping S3 for archival backtests and logs.

## AWS Services
- EC2: application host
- S3: artifact storage and backups
- IAM Role: S3 + CloudWatch permissions without static AWS credentials
- CloudWatch: metrics, dashboards, log aggregation, alarms
- SNS: alert fan-out to email/SMS/Slack bridge
- Optional ALB: TLS termination and UI scaling
- Optional Auto Scaling Group: future horizontal UI scaling, not required for single-instance trading execution

## Recommended EC2 Sizing
- Free-tier friendly: `t3.micro` or `t2.micro` for paper testing only
- Production baseline: `t3.small`
- Higher reliability / more concurrent jobs: `t3.medium`

## Security Group Rules
- Inbound `22/tcp`: your office/home IP only
- Inbound `8501/tcp`: your IP only if exposing Streamlit directly
- Preferred production setup:
  - ALB inbound `80/443` from internet
  - EC2 inbound `8501` only from ALB security group
- Outbound: allow `443` for AWS APIs, market data, Dhan, Telegram

## IAM Role Permissions
Attach an instance profile with:
- `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` for your trading bucket/prefix
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`
- `cloudwatch:PutMetricData` if you later add custom metrics
- `ssm:GetParameter` if you later move secrets to Parameter Store or Secrets Manager

## EC2 Setup Steps
1. Launch Ubuntu 22.04 EC2
2. Attach IAM role with S3 and CloudWatch permissions
3. Attach Security Group with restricted SSH and UI access
4. SSH into the host
5. Clone the repo into `/opt/trading`
6. Run:
   - `bash deploy/ec2/bootstrap.sh`
7. Edit `/etc/trading/trading.env`
8. Start services:
   - `sudo systemctl start trading-ui.service`
   - `sudo systemctl start trading-daemon.service`
9. Install CloudWatch Agent and apply `deploy/cloudwatch/agent-config.json`

## Running on EC2
- UI:
  - `sudo systemctl restart trading-ui.service`
  - `sudo systemctl status trading-ui.service`
- Trading daemon:
  - `sudo systemctl restart trading-daemon.service`
  - `sudo systemctl status trading-daemon.service`
- One-shot daemon cycle:
  - `.venv/bin/python -m src.operational_daemon --env-file /etc/trading/trading.env --once`

## CI/CD
GitHub Actions file: `.github/workflows/deploy.yml`

Pipeline flow:
1. Checkout
2. Install dependencies
3. Run unit tests
4. SSH to EC2 using GitHub Secrets
5. Run `deploy/ec2/deploy.sh`
6. Pull latest code, reinstall deps, rerun tests on EC2, restart services

Required GitHub Secrets:
- `EC2_HOST`
- `EC2_USER`
- `EC2_SSH_KEY`

## Monitoring and Alerting
### CloudWatch Agent
Use `deploy/cloudwatch/agent-config.json` to ship:
- `logs/app.log`
- `logs/execution.log`
- `logs/broker.log`
- `logs/errors.log`
- systemd redirected UI/daemon stderr logs

### Alarms
Use `deploy/cloudwatch/create-alarms.sh` for baseline host alarms.
Add log metric filters for:
- `BROKER_ERROR`
- `LIVE_DISABLED`
- `KILL_SWITCH_ENABLED`
- `Dhan order placement failed`
- repeated no-trade periods

### Operational alerts to configure
- app/service crash via CloudWatch agent + systemd status checks
- CPU > 80% for 5 minutes
- memory > 85% for 5 minutes
- disk > 85%
- no trades in expected window (log metric filter or custom metric)
- Dhan/API failures (log metric filter on `broker.log`/`errors.log`)

## Secrets Handling
Development:
- use `.env` loaded by `src.env_loader`

Production:
- prefer EC2 IAM role for AWS access
- keep broker/app secrets in `/etc/trading/trading.env`
- restrict to `chmod 600`
- do not store Dhan or Telegram tokens in git

Future hardening:
- move Dhan and Telegram secrets to AWS Systems Manager Parameter Store or AWS Secrets Manager
- inject them at boot via systemd EnvironmentFile generation or SSM agent

## Live Trading Safety Controls
- `TRADING_BROKER_MODE=PAPER` is the default
- `LIVE_TRADING_ENABLED=false` by default
- `MAX_TRADES_PER_DAY`, `MAX_DAILY_LOSS`, `MAX_ORDER_QUANTITY`, `MAX_ORDER_VALUE`
- `LIVE_SYMBOL_ALLOWLIST`
- malformed candidates rejected before broker routing
- kill switch supported through `LIVE_TRADING_KILL_SWITCH=true`

## S3 Artifact Paths
When `AWS_S3_BUCKET` is configured, CSV artifacts are uploaded automatically by the app/runtime path.
Suggested prefixes:
- `trading/prod/data/ohlcv.csv`
- `trading/prod/data/trades.csv`
- `trading/prod/data/executed_trades.csv`
- `trading/prod/data/order_history.csv`
- `trading/prod/data/backtest_trades.csv`
- `trading/prod/data/backtest_summary.csv`

## Future Scaling Path
- Put Streamlit behind ALB for TLS and blue/green migration
- Move daemon to a separate EC2 or ECS service for isolation
- Move state/log indexes to RDS/PostgreSQL
- Use SQS/EventBridge for asynchronous execution triggers
- Containerize UI and daemon separately when you are ready for ECS or EKS

## AWS-Native Secret Loading
The runtime now supports optional secret hydration from:
- AWS Systems Manager Parameter Store via `AWS_SSM_PARAMETER_PATH`
- AWS Secrets Manager via `AWS_SECRETS_MANAGER_ID`

Load order:
1. `.env` or `/etc/trading/trading.env`
2. Parameter Store values
3. Secrets Manager values

This keeps local development simple while letting production use IAM roles instead of static AWS credentials.

Suggested Parameter Store path:
- `/trading/prod/`

Suggested keys under that path:
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`
- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`
- `LIVE_TRADING_ENABLED`

## Docker / ECS Readiness
Container-ready assets added:
- `Dockerfile`
- `.dockerignore`
- `docker-compose.yml`

Local container run:
- `docker compose up --build`

Future ECS split:
- service 1: Streamlit UI task
- service 2: trading daemon task
- shared S3 artifact bucket
- secrets injected through ECS task secrets from SSM or Secrets Manager

## Additional CloudWatch Automation
For application-level alerting, run:
- `bash deploy/cloudwatch/create-log-metric-filters.sh`

This creates metric filters and alarms for:
- Dhan / broker errors
- repeated no-trade cycles
