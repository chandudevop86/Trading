# Monolithic to Microservices Conversion Report

Date: 2026-03-26
Scope: Migration of the legacy trading project under `F:\Trading\src` toward a microservices architecture
Related legacy analysis: `F:\Trading\docs\legacy_src_gap_analysis.md`

## Executive Summary

The current legacy trading project is feature-rich but structurally monolithic. The best path forward is not a direct full rewrite in one step. It is a controlled migration from the current `src/` monolith into a service-oriented platform with clear ownership boundaries, durable persistence, safer execution controls, and better operational scalability.

This report focuses on:

- requirements for converting the monolith to microservices
- current gap analysis
- migration bottlenecks
- recommended implementation steps
- target operating model

The recommended strategy is a staged strangler-style migration. Keep the monolith stable, extract bounded capabilities one by one, and move critical state off CSV and local-process coupling before attempting true scale-out.

## Migration Goal

The migration should transform the current monolithic trading application into a platform where:

- services have clear responsibilities
- execution-critical state is durable and auditable
- live trading controls are isolated from UI concerns
- market data, strategy analysis, execution, reporting, and notification pipelines can evolve independently
- deployment can scale more safely than the current single-runtime model

## Current Monolithic Baseline

The old application currently provides:

- Streamlit operator UI
- multiple strategy execution paths
- backtesting and strategy ranking
- paper and optional live Dhan execution
- Telegram notification flow
- file-based logs and CSV-ledger outputs
- automation scripts and deployment assets

Key baseline files include:

- `F:\Trading\src\Trading.py`
- `F:\Trading\src\strategy_service.py`
- `F:\Trading\src\execution_engine.py`
- `F:\Trading\src\auto_run.py`
- `F:\Trading\README.md`

## Target Microservices Requirements

### Functional Requirements

| Domain | Requirement |
|---|---|
| API Gateway / Web | Provide a single entrypoint for operators and clients |
| Auth / Access | Protect operator and admin actions with explicit authentication and authorization |
| Market Data | Fetch, normalize, cache, and serve OHLCV and instrument data consistently |
| Strategy Service | Run strategies through standardized inputs and outputs |
| Backtest Service | Execute historical strategy validation independently from live execution |
| Execution Service | Handle paper and live order workflows with explicit state transitions |
| Risk Service | Enforce daily loss, duplicate signal, open-position, and trading-limit controls |
| Broker Service | Encapsulate broker-specific connectivity, payload creation, and reconciliation |
| Notification Service | Send Telegram and future channels without blocking core trading workflows |
| Reporting Service | Generate and store summaries, exports, and artifacts |
| Audit Service | Keep immutable execution, risk, and action history |
| Scheduler / Worker | Run scheduled analysis and background jobs safely |
| Config / Secrets | Centralize environment settings and external secrets management |

### Non-Functional Requirements

| Category | Requirement |
|---|---|
| Reliability | Services should fail independently where possible |
| Auditability | Every critical execution event should be traceable |
| Recovery | In-flight execution state should survive process restarts |
| Maintainability | Teams should be able to change one domain without editing the whole platform |
| Scalability | Non-execution workloads should scale independently from execution-critical paths |
| Security | Secrets and trading controls should not be embedded in UI/runtime shortcuts |
| Observability | Logs, metrics, and health checks should exist per service |
| Testability | Each service contract should be independently testable |

## Gap Analysis: Monolith vs Target Microservices

| Area | Current Monolith State | Target State | Gap | Priority |
|---|---|---|---|---|
| Service Boundaries | Most major workflows are concentrated in one runtime | Each domain owned by a bounded service | Very large structural gap | High |
| Persistence | CSV files act as practical system of record in several paths | DB-backed transactional persistence | Very large operational gap | High |
| Execution Isolation | UI and orchestration are closely linked | Execution service isolated from UI | High risk gap | High |
| Risk Controls | Present, but tied to file history and shared runtime | Risk service with durable state checks | High reliability gap | High |
| Broker Abstraction | Dhan integration exists inside the monolith | Dedicated broker integration boundary | Medium to high gap | High |
| Market Data | Data fetching is embedded in broader runtime flows | Dedicated data service with cache and contracts | Medium gap | Medium |
| Notifications | Telegram is part of the main workflow path | Event-driven notification service | Medium gap | Medium |
| Reporting | Reports are generated locally and stored ad hoc | Report service with structured artifact storage | Medium gap | Medium |
| Authentication | Trusted local operator model dominates | Explicit auth and role model | High security gap | High |
| Deployment | Docker and infra assets exist, but core runtime is still monolithic | Service-based deploy topology | High execution mismatch | High |
| Monitoring | Mostly file-based logs | Service health, metrics, and alerts | Medium gap | Medium |
| Recovery | Recovery depends on local logs and files | Durable state and resumable jobs | High gap | High |
| Testing | Strong legacy tests exist at app/module level | Service contract and integration tests | Medium gap | Medium |

## Migration Bottlenecks

### 1. CSV-Centric State

The biggest bottleneck is that several critical flows still depend on CSV files in `F:\Trading\data`.

Why this matters:

- weak concurrency behavior
- poor transactional guarantees
- difficult recovery after partial failures
- limited auditability
- unsuitable for horizontally scaled services

### 2. Centralized Orchestration in `src/Trading.py`

The Streamlit app does not only present data. It also orchestrates strategy runs, backtesting, output handling, and execution flow.

Why this matters:

- UI changes can affect core trading behavior
- business logic is harder to test independently
- service extraction becomes slower because responsibilities are mixed

### 3. Shared Runtime Assumptions

The monolith assumes a local writable filesystem, local logs, shared output paths, and in-process execution sequencing.

Why this matters:

- these assumptions break quickly in containerized or distributed deployments
- multiple services cannot safely share mutable CSV-ledger files

### 4. Live Execution Coupling

Paper and live execution flows are advanced, but they still live too close to the monolith runtime and operator workflow.

Why this matters:

- live trading should be isolated behind stronger state control and approvals
- failures in reporting or UI should not threaten execution integrity

### 5. Mixed Repository Surface

The repo contains the old monolith, the newer `vinayak` platform work, snapshots, archived files, and deployment artifacts together.

Why this matters:

- migration ownership can become unclear
- operators and developers may target the wrong runtime path

## Recommended Target Architecture

### Core Services

| Service | Responsibility |
|---|---|
| Gateway / Admin UI | Public entrypoint, operator workflows, routing to internal services |
| Auth Service | Session, role, and action authorization |
| Market Data Service | Fetch OHLCV, cache data, normalize symbols and candles |
| Strategy Service | Run strategies and return standardized signals |
| Backtest Service | Historical analysis and ranking workloads |
| Risk Service | Enforce policy checks before execution |
| Execution Service | Manage signal-to-order lifecycle and execution states |
| Broker Adapter Service | Dhan connectivity, payload building, order/position sync |
| Notification Service | Telegram and future alerts |
| Reporting Service | Persist summaries, exports, and generated artifacts |
| Scheduler / Worker Service | Recurring jobs, queues, async workflows |
| Audit / Event Service | Immutable event trail and operational visibility |

### Shared Platform Components

| Component | Purpose |
|---|---|
| PostgreSQL | Source of truth for signals, trades, executions, audit logs, jobs |
| Redis | Hot cache, dedupe windows, short-lived workflow state |
| Message Bus | Event distribution for notifications, reporting, async processing |
| Object Storage | Reports, exports, snapshots, artifacts |
| Secrets Manager | Broker credentials and environment secrets |

## Implementation Strategy

The preferred approach is a phased strangler migration, not a big-bang replacement.

### Phase 0: Stabilize the Monolith

Objectives:

- freeze the old monolith scope
- fix the failing legacy test baseline
- identify official entrypoints
- document current data schemas and runtime outputs

Deliverables:

- green legacy test suite
- stable operator runbook
- explicit migration boundary document

### Phase 1: Extract Persistence First

Objectives:

- replace CSV-ledger behavior with database-backed persistence
- preserve CSV as exports only
- introduce normalized tables for signals, executions, orders, reconciliations, and reports

Why first:

- every later service depends on durable state
- persistence is the main blocker to safe service separation

### Phase 2: Extract Strategy and Backtest Services

Objectives:

- move strategy execution behind an API or internal service contract
- move historical backtesting into a separate bounded service
- keep signal output schemas stable across strategies

Benefits:

- lower coupling to the UI
- simpler testing
- easier future strategy expansion

### Phase 3: Extract Execution and Risk Services

Objectives:

- isolate paper and live execution workflows
- move risk-policy enforcement into a pre-execution control layer
- add stronger execution audit states and retries

Benefits:

- safer live trading
- cleaner responsibility boundaries
- stronger recovery behavior

### Phase 4: Extract Notification, Reporting, and Scheduler Services

Objectives:

- move Telegram and future channels out of the critical trading path
- move report generation to async workflows
- separate background jobs from request-response actions

Benefits:

- UI and execution become less fragile
- heavy report work no longer blocks trade workflows

### Phase 5: Introduce Gateway and Auth Boundaries

Objectives:

- place operator UI and API access behind a defined gateway
- add explicit auth and action authorization
- expose only supported internal service routes

Benefits:

- better security posture
- cleaner production routing model

## Step-by-Step Implementation Plan

| Step | Action | Outcome |
|---|---|---|
| 1 | Freeze official monolith entrypoints | Reduces migration confusion |
| 2 | Fix the current failing legacy test | Restores confidence baseline |
| 3 | Define canonical data models for signals, executions, orders, reports, and audit events | Creates migration contract |
| 4 | Move CSV-ledger state into PostgreSQL | Creates durable source of truth |
| 5 | Keep CSV outputs as generated exports only | Preserves operator familiarity without operational risk |
| 6 | Extract strategy execution interface from `src/Trading.py` | Reduces UI coupling |
| 7 | Wrap strategies behind a strategy service contract | Enables service isolation |
| 8 | Extract backtest workflows into a separate service path | Separates research from execution |
| 9 | Extract risk policy checks into a pre-execution service | Strengthens live-trade safety |
| 10 | Extract execution engine into a dedicated service | Isolates trade lifecycle management |
| 11 | Isolate Dhan integration into a broker adapter boundary | Simplifies broker portability and testing |
| 12 | Move notifications to async event-driven processing | Prevents alerting from blocking core flows |
| 13 | Move reporting to artifact and summary services | Improves observability and retention |
| 14 | Add message bus for async workflows | Enables service communication |
| 15 | Add Redis for cache, dedupe, and short-lived policy windows | Improves runtime behavior |
| 16 | Add gateway, auth, and role-based controls | Hardens operator access |
| 17 | Decommission monolithic responsibilities incrementally | Completes strangler migration |

## Risks During Migration

| Risk | Description | Mitigation |
|---|---|---|
| Dual-write inconsistency | Temporary overlap between CSV and DB records can diverge | Use DB as primary as early as possible and make CSV export-only |
| Logic drift | Extracted services may behave differently from the monolith | Preserve contract tests and golden test data |
| Live-trading risk | Refactors around execution can affect safety | Keep live trading gated, isolated, and rollout-controlled |
| Over-fragmentation | Too many services too early can add operational overhead | Start with a small number of high-value service boundaries |
| Incomplete observability | Failures become harder to trace after splitting services | Add structured logs, request IDs, and event IDs from the start |

## Recommended Service Extraction Order

1. Persistence layer
2. Strategy service
3. Backtest service
4. Risk service
5. Execution service
6. Broker adapter service
7. Reporting service
8. Notification service
9. Scheduler and worker service
10. Gateway and auth layer

## Success Criteria

The migration should be considered successful when:

- the monolith is no longer the system of record
- strategy runs can happen without the Streamlit UI
- execution and risk workflows are isolated from presentation logic
- live trading is auditable and recoverable through durable state
- notifications and reports are asynchronous side effects, not core blockers
- deployment can scale non-execution services independently

## Final Recommendation

Do not convert the monolith to microservices by splitting files only. Convert it by first separating responsibility, persistence, and operational control.

The most important first move is not Kubernetes, not more deployment YAML, and not more UI features. The most important first move is replacing CSV-based operational state and extracting execution-critical workflows out of the monolithic app runtime.

That sequence gives the project a realistic path from a capable monolith to a safer and more scalable microservices platform.
