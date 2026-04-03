# Vinayak AWS Cost And Rollout Plan

Date: 2026-04-04
Project: Vinayak
Purpose: Provide a practical AWS sizing guide, cost estimate ranges, and phased rollout plan for Vinayak.

## Scope

This document covers:

1. Instance sizing guidance
2. Estimated AWS monthly cost bands
3. Rollout phases from paper validation to stronger production posture

## Sizing Recommendations

### Web Tier

Recommended:
- `t3.micro`

Use case:
- Nginx reverse proxy
- light operator traffic
- low CPU and memory footprint

When to increase:
- heavier concurrent usage
- additional frontend assets
- TLS termination plus heavier request load

### Application Tier

Minimum:
- `t3.small`

Preferred:
- `t3.medium`

Reason:
- Vinayak performs market-data processing, pandas work, API serving, and observability updates
- `t3.micro` is too fragile for stable live validation

When to increase:
- multiple concurrent strategies
- live broker execution
- heavier observability and alerting
- multiple users or dashboards open simultaneously

### Database Tier

Minimum:
- `db.t3.small`

Preferred:
- `db.t3.medium`

Reason:
- PostgreSQL should have headroom for execution inserts, audit logging, and query traffic

### Redis Tier

Recommended:
- `cache.t3.micro`

Use case:
- cache
- freshness snapshots
- short-lived coordination

## Recommended Starting Stack

For controlled live validation:

- web: `t3.micro`
- app: `t3.small` minimum, `t3.medium` preferred
- db: `db.t3.small`
- redis: optional `cache.t3.micro`

## Estimated Monthly Cost Bands

These are rough planning bands, not billing quotes.

### Lean Validation Stack

Components:
- 1 web EC2 `t3.micro`
- 1 app EC2 `t3.small`
- 1 RDS PostgreSQL `db.t3.small`
- small EBS volumes
- low traffic

Estimated monthly range:
- approximately USD 45 to 90

### Stronger Validation Stack

Components:
- 1 web EC2 `t3.micro`
- 1 app EC2 `t3.medium`
- 1 RDS PostgreSQL `db.t3.small`
- optional Redis `cache.t3.micro`
- modest monitoring and log retention

Estimated monthly range:
- approximately USD 80 to 160

### Production-Oriented Baseline

Components:
- ALB
- web tier
- app tier `t3.medium` or better
- Multi-AZ RDS
- Redis
- CloudWatch alarms and logs

Estimated monthly range:
- approximately USD 180 to 400 plus data transfer and storage growth

## Cost Drivers

The main AWS cost drivers for Vinayak are:

1. RDS
2. Multi-AZ database replication
3. ALB
4. CloudWatch logs and retention
5. EC2 instance size
6. EBS storage
7. NAT gateway if used in a private-subnet-heavy design

## Rollout Phases

### Phase 1: Controlled Paper Validation

Goal:
- separate tiers
- validate deployment stability
- keep all unattended execution in paper mode

Stack:
- web EC2
- app EC2
- RDS PostgreSQL

Requirements:
- health endpoint working
- observability page loading
- manual live-analysis updates fresh metrics
- no live unattended orders

Success criteria:
- clean app uptime
- no duplicate execution records
- no stale metrics after manual runs

### Phase 2: Small Live Validation

Goal:
- use minimal capital
- validate broker execution with strict risk limits

Requirements:
- tiny capital allocation
- kill switch enabled
- one strategy only
- one symbol group only
- hard max trades per day
- daily loss cap

Success criteria:
- no wrong symbol, side, or quantity
- no duplicate orders
- clean broker reconciliation
- reliable alerting

### Phase 3: Hardened Pre-Production

Goal:
- raise reliability and observability quality

Add:
- Redis
- Secrets Manager
- CloudWatch alarms
- better runbooks
- automated reconciliation and summaries

Success criteria:
- stable weekly operation
- no unexplained execution failures
- clear operational alert routing

### Phase 4: Stronger Production Posture

Goal:
- improve resilience and failure isolation

Add:
- ALB
- Multi-AZ RDS
- improved backup strategy
- optional second app node
- tighter least-privilege IAM

Success criteria:
- service survives app restarts cleanly
- data tier remains protected
- observability and DB are durable

## Practical Recommendation For Vinayak

Use this order:

1. `t3.micro` web
2. `t3.small` or `t3.medium` app
3. `db.t3.small` PostgreSQL
4. no live scale-out yet
5. add Redis once freshness and cache coordination matter more

If you stay very cost-conscious:
- reduce the web tier if ALB is not needed immediately
- do not reduce the app tier below `t3.small`
- do not try to validate serious live trading on `t3.micro`

## Final Recommendation

For Vinayak, the best near-term AWS posture is:

- separate web and app
- separate PostgreSQL database
- `t3.small` or `t3.medium` app host
- RDS PostgreSQL baseline
- gradual rollout from paper to tiny live validation

This keeps the system affordable while still respecting the operational reality of a trading application.
