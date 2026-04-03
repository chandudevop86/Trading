# Vinayak Monolith To Microservices Roadmap

## Purpose

This document explains how Vinayak should evolve from its current modular monolith into a selective microservices architecture without destabilizing trading behavior.

Current recommendation:

- Keep Vinayak as a modular monolith in the near term
- Deploy it in a clean 3-tier AWS shape
- Extract only the services that provide clear operational value

This is the safest path for a trading platform that still needs continued paper validation and tightly controlled live validation.

## Current State

Vinayak is not a microservices system today.

It is a modular monolith because:

- the main backend still runs as one deployable application
- modules are separated by folders, not by independently deployed services
- most domains still communicate in-process
- data ownership is still mostly centralized

Current monolith modules include:

- `app/vinayak/api`
- `app/vinayak/execution`
- `app/vinayak/observability`
- `app/vinayak/strategies`
- `app/vinayak/db`
- `app/vinayak/notifications`
- `app/vinayak/workers`

## Why Not Split Everything Now

Early microservices adoption would create extra complexity in:

- deployments
- monitoring
- retries and failure handling
- database ownership
- network security
- debugging production incidents

For Vinayak, those costs are not justified until operational boundaries are truly needed.

## Best Near-Term Architecture

The best near-term target is:

- separate web tier
- separate private app tier
- separate private PostgreSQL database
- optional Redis
- CloudWatch and Secrets Manager
- modular monolith application

This gives infrastructure separation without forcing service fragmentation too early.

## Service Extraction Principles

A Vinayak module should become a microservice only if at least one of these becomes true:

- it needs independent scaling
- it needs a different reliability profile
- it has a distinct deployment cadence
- it owns a clear business boundary
- it creates repeated operational friction inside the monolith

If none of those are true, keep it inside the monolith.

## Good Service Candidates

### 1. Observability Service

Source area:

- `app/vinayak/observability`
- observability-focused API surfaces

Why it is a good first candidate:

- low business risk compared with execution
- useful independent dashboards and alerts
- clear separation from core trade execution

Responsibilities:

- metrics aggregation
- alert generation
- health summaries
- dashboard payloads

Benefits:

- can scale independently
- can fail without directly blocking trade execution
- gives cleaner monitoring ownership

### 2. Notification Service

Source area:

- `app/vinayak/notifications`

Why it is a good early candidate:

- naturally asynchronous
- easy event-driven boundary
- limited data ownership

Responsibilities:

- Telegram delivery
- operational alerts
- status notifications

Benefits:

- easy queue integration
- failure handling can be isolated
- retry behavior is simpler outside the main app

### 3. Execution Service

Source area:

- `app/vinayak/execution`

Why it should be extracted later, not first:

- highest operational sensitivity
- broker behavior is critical
- live order safety matters more than structural purity

Responsibilities:

- order submission
- execution state tracking
- broker adapters
- execution audit handling

Benefits after stabilization:

- isolated execution runtime
- stricter security boundary for broker credentials
- more focused scaling and monitoring

### 4. Strategy Runtime Service

Source area:

- `app/vinayak/strategies`

Why it is a later candidate:

- strategies often share common data and validation logic
- premature extraction can increase coupling rather than reduce it

Responsibilities:

- strategy evaluation
- trade candidate generation
- rule execution

Benefits after maturity:

- separate scaling for compute-heavy strategy runs
- cleaner model/version rollout
- easier experimentation

## Areas That Should Stay Central Longer

### Auth

Keep authentication centralized until the platform has multiple independent apps that truly need a shared identity layer.

### Database Core

Do not split database ownership too early. Start with one PostgreSQL database and logical domain separation.

### Config And Secrets

Keep configuration and secrets management centralized using AWS Secrets Manager and environment controls.

## Recommended Extraction Order

### Phase 0. Stabilize The Modular Monolith

Goals:

- finish structure cleanup
- keep runtime stable
- validate paper workflows
- validate observability freshness

Do not extract services in this phase.

### Phase 1. Add Clear Internal Boundaries

Goals:

- define module interfaces
- reduce cross-module leakage
- formalize events and contracts

Examples:

- execution accepts typed requests instead of loose shared state
- notifications consume explicit event payloads
- observability reads published metrics instead of internal object state

### Phase 2. Introduce Event Boundaries

Goals:

- publish internal events
- move non-critical side effects off the synchronous path

Examples:

- trade executed event
- strategy validation failed event
- alert triggered event

This phase prepares the codebase for service extraction without immediate network complexity.

### Phase 3. Extract Notification Service

Why first:

- lowest risk
- easiest async separation
- limited blast radius

Success criteria:

- app publishes notification events
- notification service sends Telegram or alert outputs
- retries no longer block main app flow

### Phase 4. Extract Observability Service

Why second:

- important operationally
- easier than execution extraction
- gives immediate monitoring clarity

Success criteria:

- app publishes metrics or health events
- observability service produces dashboards and alerts
- stale snapshot handling is isolated from trading logic

### Phase 5. Harden Execution Boundaries

Before extracting execution:

- paper trading must be stable
- tiny live validation must be stable
- broker audit logs must be reliable
- kill switch behavior must be proven

Only then should execution be considered for extraction.

### Phase 6. Extract Execution Service

Success criteria:

- dedicated execution API or queue contract
- dedicated credential scope
- dedicated alerting and health checks
- strong idempotency and replay controls

### Phase 7. Evaluate Strategy Runtime Extraction

Do this only if:

- strategy workloads are scaling differently
- research and production runtimes need separation
- strategy deployment cadence differs materially from the rest of the app

## Example Target Service Shape

Near-term selective microservices target:

- `vinayak-web`
- `vinayak-app-core`
- `vinayak-notifications`
- `vinayak-observability`
- `vinayak-db`
- optional `vinayak-redis`

Later-only services:

- `vinayak-execution`
- `vinayak-strategy-runtime`

## Folder To Future Service Mapping

### Keep In Core App

- `app/vinayak/api`
- `app/vinayak/db`
- `app/vinayak/core`
- `app/vinayak/catalog`
- `app/vinayak/auth`

### Early Extraction Candidates

- `app/vinayak/notifications`
- `app/vinayak/observability`

### Later Extraction Candidates

- `app/vinayak/execution`
- `app/vinayak/strategies`

## Risks To Avoid

Do not do these too early:

- split databases per service
- split execution before live controls are stable
- create many services without event contracts
- couple services through direct database writes
- introduce network calls on every internal code path

## Practical Recommendation For Vinayak

Best recommendation:

1. Keep the modular monolith now
2. Finish 3-tier AWS deployment
3. Validate paper mode thoroughly
4. Validate tiny live mode cautiously
5. Introduce event boundaries
6. Extract notifications first
7. Extract observability second
8. Reassess execution extraction only after live stability

## Final Position

Vinayak should not become "microservices everywhere."

Vinayak should become:

- a clean modular monolith first
- a selectively extracted platform later
- a reliability-first trading system at every step

That is the safest and most professional path.
