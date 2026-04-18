> Archived reference: this document describes deprecated `src/`-era surfaces and is not an active operator guide. Use `README.md`, `DEPLOYMENT.md`, `RUNBOOK.md`, and `docs/active_code_surface.md` for the supported FastAPI runtime.

﻿# Legacy `src/` Contracts and Runtime Outputs

Date: 2026-03-26
Scope: Supported contracts for the legacy monolithic trading project under `F:\Trading\src`

## Purpose

This document freezes the current Phase 0 baseline for the old monolithic project so migration work can preserve external behavior while replacing internals.

The contracts below are derived from the active legacy code paths, especially:

- `F:\Trading\src\Trading.py`
- `F:\Trading\src\trading_core.py`
- `F:\Trading\src\breakout_bot.py`
- `F:\Trading\src\execution_engine.py`
- `F:\Trading\src\backtest_engine.py`
- `F:\Trading\src\runtime_config.py`

## Official Legacy Entrypoints

- UI operator console: `streamlit run src/Trading.py`
- Generic CSV rules CLI: `py -3 -m src.main ...`
- NIFTY 50 snapshot CLI: `py -3 -m src.nifty50 ...`
- NIFTY options rules CLI: `py -3 -m src.nifty_options ...`
- NIFTY futures CLI: `py -3 -m src.nifty_futures ...`
- Breakout CLI: `py -3 -m src.breakout_bot ...`
- BTST CLI: `py -3 -m src.btst_bot ...`
- End-to-end pipeline: `py -3 -m src.auto_run ...`
- Backtest workflow: `py -3 -m src.auto_backtest ...`
- Dhan order preview/live CLI: `py -3 -m src.dhan_example ...`
- Dhan account CLI: `py -3 -m src.dhan_account ...`
- Reconciliation CLIs: `py -3 -m src.reconcile_live ...` and `py -3 -m src.reconcile_positions ...`

Blocked non-canonical direct launchers:

- `src/breakout_app.py`
- `src/pattern_detector.py`
- `src/operational_daemon.py`

## Required Input Candle Schema

The canonical input candle columns are validated by `prepare_trading_data()` in `F:\Trading\src\trading_core.py`.

| Column | Type | Notes |
|---|---|---|
| `timestamp` | datetime-like | `datetime`, `date`, and `time` are normalized to this when possible |
| `open` | numeric | Must be non-negative |
| `high` | numeric | Must be greater than or equal to `low` |
| `low` | numeric | Must be numeric |
| `close` | numeric | Must be non-negative |
| `volume` | numeric | Numeric, can be zero |

Normalization rules:

- duplicate timestamps are dropped
- invalid OHLC rows are dropped
- rows are sorted by timestamp

## Runtime Output Files

The main legacy runtime paths are defined in `F:\Trading\src\Trading.py` and `F:\Trading\src\runtime_config.py`.

| File | Purpose |
|---|---|
| `data/ohlcv.csv` | last written operator candle dataset |
| `data/live_ohlcv.csv` | last live-fetched candle dataset |
| `data/trades.csv` | most recent generated trade rows |
| `data/output.csv` | signal-style output mirror |
| `data/executed_trades.csv` | execution ledger used by the UI/runtime flow |
| `data/paper_trading_logs_all.csv` | paper execution log |
| `data/live_trading_logs_all.csv` | live execution log |
| `data/paper_trade_summary.csv` | summarized paper execution metrics |
| `data/order_history.csv` | order submission history |
| `data/paper_order_history.csv` | paper order history |
| `data/backtest_trades.csv` | simulated backtest trade rows |
| `data/backtest_summary.csv` | backtest summary row |
| `data/backtest_validation.csv` | deployment-readiness validation row |
| `data/backtest_results_all.csv` | summarized backtest results |
| `data/strategy_expectancy_report.csv` | strategy ranking output |
| `data/strategy_optimizer_report.csv` | optimizer/readiness output |
| `logs/app.log` | operator app events |
| `logs/execution.log` | execution activity log |
| `logs/broker.log` | broker-side activity log |
| `logs/errors.log` | error log |
| `logs/rejections.log` | trade rejection log |

## Standard Trade Output Contract

Legacy strategy rows are typically normalized through `StandardTrade.to_dict()` in `F:\Trading\src\trading_core.py`.

Core fields:

| Column | Meaning |
|---|---|
| `timestamp` | signal timestamp |
| `entry_time` | normalized entry timestamp mirror |
| `side` | `BUY` or `SELL` |
| `entry` | entry price |
| `entry_price` | entry price mirror |
| `stop_loss` | stop loss |
| `target` | target price |
| `target_price` | target price mirror |
| `strategy` | strategy name |
| `reason` | primary trade rationale |
| `score` | strategy score |
| `risk_per_unit` | displayed entry-to-stop risk distance |
| `quantity` | order quantity |

Common extended fields written by strategy flows:

| Column | Meaning |
|---|---|
| `exit_time` | close timestamp if known |
| `exit_price` | close price if known |
| `exit_reason` | `TARGET`, `STOP_LOSS`, `EOD`, etc. |
| `gross_pnl` | gross PnL before cost |
| `trading_cost` | cost deduction |
| `pnl` | net PnL |
| `rr_achieved` | realized risk-reward multiple |
| `entry_trigger_price` | breakout trigger level |
| `fill_model` | fill model label |
| `market_regime` | `TREND` or `CHOPPY` |
| `first_hour_bias` | derived bias |
| `bias_mode` | `REQUIRED` or `OBSERVE_ONLY` |
| `bias_aligned` | `YES` or `NO` |
| `regime_filter` | `ON` or `OFF` |

Note:

- strategy-specific outputs may append additional fields
- column order for plain CSV strategy outputs follows the first written row

## Execution Log Contract

Execution logs are stabilized through `EXECUTION_SCHEMA` in `F:\Trading\src\execution_engine.py`.

Core ordered fields:

| Column |
|---|
| `trade_id` |
| `trade_key` |
| `trade_status` |
| `position_status` |
| `duplicate_reason` |
| `blocked_reason` |
| `validation_error` |
| `strategy` |
| `symbol` |
| `timeframe` |
| `data_symbol` |
| `trade_symbol` |
| `trading_symbol` |
| `security_id` |
| `exchange_segment` |
| `instrument_type` |
| `signal_time` |
| `duplicate_signal_key` |
| `side` |
| `price` |
| `share_price` |
| `strike_price` |
| `option_expiry` |
| `option_type` |
| `option_strike` |
| `quantity` |
| `execution_type` |
| `execution_status` |
| `executed_at_utc` |
| `reviewed_at_utc` |
| `analyzed_at_utc` |
| `broker_name` |
| `broker_order_id` |
| `broker_status` |
| `broker_message` |
| `broker_response_json` |
| `risk_limit_reason` |
| `pnl` |
| `exit_time` |
| `exit_price` |
| `exit_reason` |

Rules:

- existing files may grow extra appended fields after this base schema
- the base schema order is treated as the canonical legacy contract
- both `data/executed_trades.csv` and `data/paper_trading_logs_all.csv` or `data/live_trading_logs_all.csv` use this execution-style model

## Order History Contract

Paper broker order history is written in `F:\Trading\src\brokers\paper_broker.py`.

Expected core fields:

| Column |
|---|
| `order_id` |
| `trade_id` |
| `broker_name` |
| `status` |
| `message` |
| `symbol` |
| `side` |
| `quantity` |
| `price` |
| `order_type` |
| `product_type` |
| `validity` |
| `execution_type` |
| `submitted_at_utc` |

Live and paper execution paths may append additional broker-specific fields through `F:\Trading\src\execution_engine.py`.

## Backtest Trades Contract

Backtest trades are written by `run_backtest()` in `F:\Trading\src\backtest_engine.py`.

Common fields:

| Column |
|---|
| `trade_index` |
| `strategy` |
| `side` |
| `timestamp` |
| `entry_time` |
| `entry` |
| `entry_price` |
| `stop_loss` |
| `target` |
| `target_price` |
| `quantity` |
| `score` |
| `reason` |
| `trade_status` |
| `execution_status` |
| `position_status` |
| `exit_time` |
| `exit_price` |
| `exit_reason` |
| `gross_pnl` |
| `trading_cost` |
| `pnl` |
| `rr_achieved` |
| `setup_type` |
| `rejection_reason` |

## Backtest Summary Contract

Backtest summary rows are created by `_summary_from_trades()` and then enriched by `_validation_report()` in `F:\Trading\src\backtest_engine.py`.

Expected fields:

| Column |
|---|
| `strategy` |
| `total_trades` |
| `wins` |
| `losses` |
| `win_rate` |
| `total_pnl` |
| `gross_total_pnl` |
| `total_trading_cost` |
| `avg_pnl` |
| `avg_win` |
| `avg_loss` |
| `profit_factor` |
| `expectancy_per_trade` |
| `expectancy_r` |
| `positive_expectancy` |
| `max_drawdown` |
| `max_drawdown_pct` |
| `avg_rr` |
| `pnl_by_strategy` |
| `score_bucket_analysis` |
| `trades_output` |
| `summary_output` |
| `validation_output` |
| `rejected_candidates` |
| `closed_trades` |
| `duplicate_rejections` |
| `risk_rule_rejections` |
| `sample_status` |
| `sample_trade_floor` |
| `sample_trade_target` |
| `sample_trade_cap` |
| `trades_evaluated` |
| `trade_gap_to_target` |
| `deployment_ready` |
| `deployment_blockers` |
| `validation_notes` |

## Paper Trade Summary Contract

`data/paper_trade_summary.csv` is produced by `summarize_trade_log()` in `F:\Trading\src\backtest_engine.py`.

The summary fields align with the backtest summary contract above, but are derived from closed paper-trade rows rather than simulated historical rows.

## Phase 0 Baseline Rules

During Phase 0:

- these file names are treated as canonical legacy outputs
- these schemas are the behavioral baseline to preserve
- internal refactors should not silently rename or repurpose them
- CSV remains allowed as a legacy export format even if later phases move the system of record into a database
