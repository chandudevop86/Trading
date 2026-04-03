# Legacy `src/` Monolith Requirement and Gap Analysis

Date: 2026-03-26
Scope: Old monolithic trading project under `F:\Trading\src`
Excluded: `F:\Trading\vinayak` rewrite

## Executive Summary

The legacy trading project under `src/` is functionally strong. It already supports market-data ingestion, multiple strategies, backtesting, paper trading, optional live Dhan execution, reporting, and a Streamlit operator console.

The main issue is not missing features. The main issue is architectural concentration: too many responsibilities sit inside one monolithic runtime, with critical operational state still stored in CSV files.

In its current form, the legacy app is suitable for local or operator-led use, but it is not yet aligned with a production-grade architecture. The highest-risk gaps are persistence, auditability, recovery, maintainability, and safe live-execution governance.

## Assessment Basis

This review is based on the current legacy implementation and supporting assets, including:

- `F:\Trading\README.md`
- `F:\Trading\src\Trading.py`
- `F:\Trading\src\strategy_service.py`
- `F:\Trading\src\execution_engine.py`
- `F:\Trading\src\auto_run.py`
- `F:\Trading\tests`

Legacy test baseline observed during review:

- 122 tests executed in the legacy `tests/` suite
- 121 passed
- 1 failed in `F:\Trading\tests\test_breakout_bot.py`

## Current-State Summary

### Strengths

- Broad strategy coverage exists in the monolith.
- Backtesting and execution workflows are both implemented.
- Risk controls are present in execution logic.
- Operator workflow is accessible through a single Streamlit console.
- Automated test coverage is substantial for the legacy code path.

### Weaknesses

- UI and orchestration are tightly coupled.
- CSV files are heavily used as operational records.
- Production deployment ambitions exceed runtime hardening maturity.
- Recovery and auditability are weaker than required for a stronger production profile.
- The repository mixes active legacy code with parallel-generation and archived assets.

## Requirement and Gap Table

| Area | Requirement | Current Implementation | Gap | Priority | Recommendation |
|---|---|---|---|---|---|
| Product Scope | One clear legacy product boundary | Legacy app exists, but repo also contains `vinayak`, snapshots, archives, and duplicate assets | Ownership boundary is blurred | High | Declare `src/` as legacy-maintenance scope and document official entrypoints only |
| UI Entry | Single operator console for trading actions | Streamlit UI in `F:\Trading\src\Trading.py` handles run, backtest, and execution | UI is also orchestration layer | High | Keep UI thin and move orchestration into service modules |
| Strategy Routing | Unified strategy execution contract | Multiple strategies run through shared workflow | Strategy integration depends on centralized routing and UI knowledge | High | Standardize input/output contracts for all strategies |
| Market Data | Fetch, normalize, and reuse OHLCV safely | Live fetch and normalization are implemented | Data flow is runtime-local and file-oriented | Medium | Separate fetch, cache, normalize, and persistence responsibilities |
| Strategy Coverage | Support multiple strategies | Breakout, Demand Supply, Indicator, One Trade/Day, MTF, AMD/FVG/SD are present | Breadth is good, but maintainability declines as coverage grows | Medium | Introduce per-strategy interfaces and test contracts |
| Backtesting | Historical validation with metrics | Backtesting and ranking are implemented | Backtest lifecycle is loosely coupled to execution lifecycle | Medium | Define a formal backtest result schema and persistence path |
| Strategy Ranking | Compare strategies by quality and readiness | Ranking and optimizer-style outputs exist | Ranking logic is file/report dependent | Medium | Persist ranked summaries in a structured store, not only CSV |
| Paper Trading | Default safe execution mode | Paper execution is implemented in `F:\Trading\src\execution_engine.py` | Uses file ledger as source of truth | High | Move paper-trade ledger to DB and keep CSV only as export |
| Live Trading | Controlled live broker execution | Dhan live flow exists with gating and checks | Live execution still shares monolith runtime and file-state assumptions | High | Isolate live execution service and strengthen audit trail |
| Broker Integration | Broker credentials, payload build, positions, and orders | Dhan integration is substantial | Operational controls are still local-tool style, not hardened platform style | High | Add stronger secret management and explicit execution authorization flow |
| Risk Controls | Daily loss, duplicate blocking, and open trade limits | Present in execution flow | Controls depend on file history integrity | High | Store risk-control state in durable transactional storage |
| Reconciliation | Compare expected vs broker/live state | Reconciliation CLI/modules exist | State source is fragmented across files and runtime data | High | Centralize executions, order history, and reconciliation records |
| Persistence | Durable system of record | CSV files in `F:\Trading\data` are heavily used | CSV is weak for concurrency, audit, rollback, and multi-instance use | High | Introduce DB-backed persistence for signals, executions, orders, and summaries |
| Reporting | Trade summaries and downloadable outputs | Implemented via CSV outputs and summaries | Reporting is fragmented and local-file centric | Medium | Create a report service with structured artifacts and retention rules |
| Notifications | Optional Telegram integration | Implemented in `F:\Trading\src\telegram_notifier.py` | Notification flow is tightly coupled to trading runtime | Medium | Decouple notifications from core execution path |
| Automation | End-to-end scheduled pipeline | `F:\Trading\src\auto_run.py` supports pipeline execution | Automation depends on the same monolithic assumptions and local files | Medium | Separate scheduled workflow runner from UI/manual runtime |
| Configuration | Centralized, environment-specific config | Env vars and runtime defaults exist | Config is spread across code, docs, and output paths | Medium | Introduce one config model and environment profiles for the legacy app |
| Deployment | Support local plus production deployment | Docker, K8s, and AWS docs/assets exist | Deployment story is ahead of runtime architecture maturity | High | Reframe docs: local-supported now, production-aspirational until persistence is hardened |
| Security | Safe secret handling and operator control | `.env` pattern is used | Local-secret model is not enough for production-grade execution | High | Use secret manager in deployed environments and separate operator auth from app logic |
| Access Control | Restrict trading actions | Implicitly single-operator trusted-user model | No strong role separation in the legacy monolith | Medium | Add minimal role and action guardrails if the legacy app remains active |
| Logging | Operator and failure visibility | File logs exist in `F:\Trading\logs` | Logging is local and mostly unstructured | Medium | Add structured logs and clearer event categories |
| Observability | Monitor health, failures, and executions | Partial through logs and output files | No strong centralized telemetry model in monolith | Medium | Add health summaries, failure counters, and execution audit reporting |
| Test Coverage | Automated regression protection | Legacy suite is strong: 122 tests ran | Suite is not fully green | High | Fix the failing test in `F:\Trading\tests\test_breakout_bot.py` first |
| Quality Baseline | Stable release baseline | Most legacy tests pass | One precision or rounding failure indicates non-clean baseline | High | Establish a green suite requirement before further legacy enhancements |
| Code Organization | Clear separation of concerns | Many capabilities exist in one codebase | UI, workflows, file IO, and execution are tightly coupled | High | Refactor by domain: data, strategies, execution, reporting, UI |
| Repo Hygiene | Low confusion in active code surface | Repo contains archived, experimental, and snapshot files | Risk of using wrong file or deploying wrong target | Medium | Mark archives clearly and remove non-active files from active docs |
| Documentation | Accurate current-state guidance | README is detailed | Docs mix local workflow, broker flow, deployment, and production claims | Medium | Split docs into Legacy Local Ops, Legacy Deployment Limits, and Migration Notes |
| Scalability | Support higher load or more operators | Some deployment assets suggest this goal | Core monolith and file model are not ready for true scale-out | High | Do not scale horizontally until state is moved off local CSV |
| Recovery | Recover safely after crashes or partial runs | Some logs and outputs survive locally | No strong transactional recovery model | High | Add durable execution states and resumable workflow records |
| Auditability | Trace who, what, and when for trades | CSV and logs provide partial history | Weak audit trail for production live-trade governance | High | Add immutable execution and audit records in DB |
| Maintainability | Safe future enhancement path | Functionality is broad and valuable | Each enhancement increases complexity in centralized runtime code | High | Freeze features briefly and do a hardening and refactor pass |

## Roadmap

### Phase 1: Stabilize

- Fix the failing legacy test in `F:\Trading\tests\test_breakout_bot.py`.
- Define official legacy entrypoints in `F:\Trading\README.md`.
- Mark non-production or archived files as inactive.
- Freeze current CSV schemas so behavior is explicit.

### Phase 2: Harden

- Introduce DB-backed persistence for signals, executions, order history, and summaries.
- Keep CSV generation only for export and reporting.
- Separate execution-state handling from Streamlit UI code.
- Strengthen live-trading approval and audit flow.

### Phase 3: Refactor

- Break orchestration out of `F:\Trading\src\Trading.py`.
- Standardize strategy input and output contracts.
- Separate domains into data, strategy, execution, reporting, and UI layers.
- Decouple Telegram and reporting from core trade workflows.

### Phase 4: Operationalize

- Split local operator app docs from production deployment docs.
- Add structured logs and clearer recovery procedures.
- Define a supportable production profile only after persistence and audit gaps are closed.
- Treat horizontal scaling as out of scope until local-file state is removed from the core path.

## Final Conclusion

The legacy `src/` project is a capable trading application with substantial real functionality already in place. Its biggest gaps are architectural and operational, not feature-related.

The right next move is not adding more strategy breadth. The right next move is hardening the system of record, separating concerns, and re-establishing a fully green baseline for the monolithic code path.
