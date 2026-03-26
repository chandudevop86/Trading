# Trading Platform Transformation Proposal

Date: 2026-03-26
Scope: Current monolithic trading platform under `F:\Trading\src`, target microservices direction, and migration roadmap

## Executive Summary

The current trading platform is a capable monolithic application with meaningful real-world functionality already in place. It supports market-data ingestion, multiple strategies, backtesting, paper execution, optional live Dhan execution, operator workflows, and reporting.

Its main limitation is not feature coverage. Its main limitation is structural concentration. The current platform depends heavily on one application runtime, shared local file state, and CSV-led operational records. That creates risk around maintainability, auditability, recovery, and safe production scaling.

The recommended direction is a staged transformation from the current monolith into a bounded microservices platform. This should not be executed as a big-bang rewrite. It should be done through a controlled migration that first stabilizes the monolith, moves critical state into durable persistence, and then extracts services in a risk-aware order.

## Current-State Overview

### What Exists Today

The current `src/` platform already provides:

- a Streamlit-based operator console
- multiple strategy execution paths
- market-data retrieval and normalization
- historical backtesting and strategy comparison
- paper trading execution
- optional live Dhan trading flow
- Telegram notification support
- automation scripts and deployment assets

Key implementation references include:

- `F:\Trading\src\Trading.py`
- `F:\Trading\src\strategy_service.py`
- `F:\Trading\src\execution_engine.py`
- `F:\Trading\src\auto_run.py`
- `F:\Trading\README.md`

### Strengths

- strong feature breadth for a single application
- practical trading workflow support
- meaningful execution safeguards already exist
- good legacy test coverage
- usable operator-facing workflow

### Weaknesses

- too much responsibility is centralized in one runtime path
- local CSV files behave like the practical system of record
- UI, orchestration, reporting, and execution concerns are closely coupled
- production-style deployment ambition exceeds runtime maturity
- audit and recovery controls are not yet strong enough for a more serious live-trading posture

## As-Is Assessment

| Area | As-Is State | Business Impact |
|---|---|---|
| Product Runtime | Monolithic application | Faster early delivery, but harder long-term change control |
| UI Layer | Streamlit UI also orchestrates workflows | User-facing changes can affect core behavior |
| Persistence | Heavy CSV and file-state usage | Weak durability and auditability |
| Strategy Layer | Multiple strategies available | Strong feature value, but growing coupling |
| Execution Layer | Paper and live logic exist | Valuable capability, but operationally sensitive |
| Reporting | Outputs and summaries exist | Useful, but fragmented |
| Notifications | Telegram support exists | Helpful, but tightly coupled |
| Deployment | Docker and cloud assets exist | Signals ambition, but not full production readiness |
| Testing | Strong legacy suite | Good base, but baseline should be fully green |

## To-Be Target State

The target platform should become a service-oriented trading system where:

- each domain has a clear responsibility boundary
- core trading state is persisted in a durable store
- execution and risk policy are isolated from presentation logic
- data, strategy, execution, reporting, and notifications can evolve independently
- asynchronous workloads are moved out of the request path
- production operations rely on structured audit, observability, and recovery

### Target Domains

| Service / Component | Target Responsibility |
|---|---|
| Gateway / Web Layer | Unified entrypoint for users and clients |
| Auth / Access Control | Protect actions and define operator permissions |
| Market Data Service | Fetch, normalize, cache, and serve market data |
| Strategy Service | Run strategies and return standardized signals |
| Backtest Service | Historical validation and ranking workloads |
| Risk Service | Evaluate policy checks before execution |
| Execution Service | Manage paper and live execution state transitions |
| Broker Adapter Service | Dhan integration, payload construction, sync, and reconciliation |
| Notification Service | Telegram and future outbound channels |
| Reporting Service | Summaries, exports, artifacts, and retention |
| Scheduler / Worker Service | Background jobs and recurring workflows |
| Audit / Event Layer | Immutable history and service-event traceability |
| PostgreSQL | Durable source of truth |
| Redis | Cache and dedupe support |
| Message Bus | Event-driven coordination |
| Object Storage | Reports and export artifacts |

## Requirement and Gap Summary

| Dimension | Current State | Target State | Gap Level | Priority |
|---|---|---|---|---|
| Architecture | Monolithic | Bounded services | Large | High |
| Persistence | CSV and local files | Transactional DB-backed model | Large | High |
| Execution Safety | Present but runtime-coupled | Isolated auditable execution service | Large | High |
| Risk Controls | Present but file-history dependent | Durable policy enforcement service | Large | High |
| Scalability | Limited by local-state assumptions | Selective service-scale capability | Large | High |
| Recovery | Partial and local-file based | Durable resumable workflows | Large | High |
| Auditability | Partial through logs and CSVs | Structured immutable event trail | Large | High |
| Maintainability | Centralized code ownership | Clear domain ownership | Large | High |
| Notifications | Inline runtime feature | Async side-effect service | Medium | Medium |
| Reporting | Local outputs and summaries | Structured report lifecycle | Medium | Medium |
| Security | Trusted operator model | Explicit auth and secret management | Large | High |
| Observability | Mostly file logs | Service-level metrics and health | Medium | Medium |

## Bottlenecks

### 1. CSV as Operational State

Critical flows still depend on local CSV files under `F:\Trading\data`.

This limits:

- concurrency safety
- crash recovery
- auditability
- scale-out
- cross-service coordination

### 2. Orchestration Inside the UI Runtime

The operator UI in `F:\Trading\src\Trading.py` is also a major workflow coordinator.

This limits:

- separation of concerns
- independent testing
- safe service extraction
- operational resilience

### 3. Shared Filesystem Assumptions

The monolith assumes:

- local writable files
- shared folders for outputs
- in-process sequencing
- direct local artifact generation

These assumptions do not translate cleanly to distributed services.

### 4. Execution Coupling

Execution logic exists and is valuable, but it remains too close to the same runtime used for UI and broader operator workflow.

This raises migration and operational risk.

### 5. Mixed Codebase Generations

The repository contains:

- the active legacy monolith
- the newer `vinayak` direction
- snapshots
- archived files
- deployment assets

This increases ambiguity during migration unless ownership is made explicit.

## Transformation Strategy

The recommended strategy is a staged strangler migration.

### Phase 0: Stabilize the Current Monolith

Objectives:

- keep the old app functional
- define the official legacy entrypoints
- fix the current failing baseline issue
- document current schemas and outputs

Outputs:

- stable monolith baseline
- green regression suite
- operator-safe runbook

### Phase 1: Introduce Durable Persistence

Objectives:

- move signals, executions, orders, reports, and audit history into a database
- stop treating CSV as the source of truth
- retain CSV only as export output

Why this comes first:

- every later service depends on stable shared data
- it reduces the highest current bottleneck immediately

### Phase 2: Extract Strategy and Backtest Domains

Objectives:

- standardize strategy contracts
- run strategies outside the UI runtime
- separate historical backtesting from live execution paths

Expected benefits:

- cleaner testing
- easier reuse across services
- lower UI coupling

### Phase 3: Extract Risk and Execution Domains

Objectives:

- isolate trade lifecycle management
- make risk policy checks durable and auditable
- contain live broker activity within a controlled boundary

Expected benefits:

- stronger live-trading safety
- cleaner rollback and recovery behavior
- more defensible production operations

### Phase 4: Extract Reporting, Notification, and Jobs

Objectives:

- move report generation out of critical request paths
- shift Telegram and similar actions to async workflows
- isolate schedulers and worker logic

Expected benefits:

- better responsiveness
- lower coupling
- fewer side-effect failures in core trading actions

### Phase 5: Add Gateway and Access Control

Objectives:

- route all interactions through a clear front door
- introduce authentication and role-aware action control
- expose only supported internal APIs

Expected benefits:

- stronger security posture
- simpler operations
- cleaner boundary between users and internal services

## Recommended Implementation Roadmap

| Step | Action | Outcome |
|---|---|---|
| 1 | Freeze legacy entrypoints and unsupported surfaces | Reduces confusion |
| 2 | Fix the remaining failing legacy test | Restores trust in the baseline |
| 3 | Define canonical data models for trading records | Creates migration contracts |
| 4 | Introduce PostgreSQL as the system of record | Removes the biggest bottleneck |
| 5 | Keep CSV outputs only for export/reporting | Preserves familiarity without operational dependence |
| 6 | Extract strategy interfaces out of the UI code | Improves modularity |
| 7 | Stand up a strategy service boundary | Enables clean reuse |
| 8 | Separate backtesting into its own service path | Distinguishes research from execution |
| 9 | Add a dedicated risk evaluation boundary | Strengthens control posture |
| 10 | Extract the execution engine into a service | Isolates trade lifecycle management |
| 11 | Encapsulate Dhan in a broker adapter boundary | Improves portability and testing |
| 12 | Move notifications to event-driven workflows | Reduces runtime coupling |
| 13 | Move reporting to structured artifact handling | Improves retention and observability |
| 14 | Add Redis and message bus support | Enables async coordination |
| 15 | Add gateway and auth controls | Hardens user interaction paths |
| 16 | Decommission monolith responsibilities gradually | Completes transformation safely |

## Risks and Mitigations

| Risk | Description | Mitigation |
|---|---|---|
| Migration drift | Extracted services behave differently from the monolith | Preserve contract tests and golden data |
| Dual-write errors | Temporary overlap of CSV and DB states can diverge | Make DB primary as early as possible |
| Live-trading instability | Execution refactors can affect safety | Roll out in tightly gated stages |
| Too many services too early | Operational overhead increases too fast | Start with only high-value boundaries |
| Weak observability during split | Failures become harder to trace | Add structured logging and IDs from the start |

## Recommended Deliverables

### Short-Term

- stabilized monolith baseline
- green legacy test suite
- canonical DB schema for trading records
- documented migration contracts

### Mid-Term

- strategy service
- backtest service
- execution and risk services
- broker adapter boundary
- DB-backed audit trail

### Long-Term

- gateway and auth boundary
- reporting and notification services
- scheduler and async worker model
- production-grade deployment with observability and secrets management

## Final Recommendation

The current monolithic platform should be treated as a valuable working asset, not discarded. It should become the source system for a controlled extraction program.

The correct transformation order is:

1. stabilize
2. persist durably
3. extract bounded domains
4. isolate live execution
5. add async and operational controls
6. complete service-based routing and governance

This approach reduces delivery risk while preserving the real trading functionality that already exists.
