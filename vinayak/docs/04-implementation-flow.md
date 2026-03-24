# 04-IMPLEMENTATION-FLOW

Use this order to implement Vinayak in a 3-tier style similar to the Roboshop document flow.

## Step 1

Prepare the server or container runtime.

- provision ECS, EC2, or a local Linux host
- open only web ports publicly
- keep app and data layers private

## Step 2

Implement the web tier.

- install Nginx or configure ALB
- point browser traffic to the Vinayak application
- verify `/admin` and `/workspace`

## Step 3

Implement the app tier.

- install Python dependencies
- start FastAPI
- validate health endpoints
- validate `GET /dashboard/candles`
- validate `POST /dashboard/live-analysis`

## Step 4

Implement the data tier.

- configure PostgreSQL or RDS
- configure Redis or ElastiCache
- configure S3 bucket for reports
- configure Secrets Manager for broker and Telegram credentials

## Step 5

Integrate production controls.

- enable CloudWatch logs and alarms
- add CI/CD deployment pipeline
- run database migrations during deploy
- verify report artifact creation and cache usage

## Step 6

Final production validation.

- browser opens workspace through web tier only
- app tier is reachable only from web tier or private network
- data tier is private and not browser-exposed
- live analysis works with Redis and S3 enabled
- broker, Yahoo, NSE, and Telegram integrations are healthy
