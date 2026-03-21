# Vinayak Sprint 1 Plan

## Sprint Goal

Build the first working backend slice of `Vinayak` by completing these three things:

1. move the `Breakout` strategy into the new architecture
2. create the first database foundation for signals and executions
3. expose one API route that runs the strategy and returns standardized output

This sprint should produce a small but real working path:

`API -> strategy -> normalized signal -> database-ready structure`

---

## Sprint Scope

### In Scope

- `Breakout` strategy only
- strategy result normalization
- API route for running one strategy
- database session setup
- first DB models
- first unit tests

### Out of Scope

- Dhan live execution
- Telegram sending
- Redis cache
- RabbitMQ / SQS
- frontend integration
- all other strategies

---

## Step By Step Tasks

### Step 1: Prepare package imports

Goal: make the new project import cleanly.

Tasks:

- add missing `__init__.py` files where needed in subfolders
- confirm `vinayak.api.main:app` is importable
- keep module names simple and stable

Target folders:

- `vinayak/api/`
- `vinayak/strategies/`
- `vinayak/db/`
- `vinayak/tests/`

Definition of done:

- API app imports without path confusion
- package layout is stable for future modules

---

### Step 2: Create the Breakout strategy module

Goal: move `Breakout` into the new structure.

Source reference:

- `F:\Trading\src\breakout_bot.py`

Target location:

- `vinayak/strategies/breakout/service.py`

Tasks:

- copy only the reusable strategy logic, not Streamlit UI parts
- return normalized signal objects
- keep inputs simple:
  - candles
  - capital
  - risk_pct
  - rr_ratio
  - symbol
- keep outputs standardized using `StrategySignal`

Definition of done:

- strategy runs independently inside `Vinayak`
- strategy output is no longer tied to old app code

---

### Step 3: Standardize strategy output

Goal: all strategy results should follow one internal contract.

Primary file:

- `vinayak/strategies/common/base.py`

Tasks:

- confirm `StrategySignal` fields are enough
- if needed, extend with:
  - `signal_time`
  - `strategy_name`
  - `metadata`
- keep side values only:
  - `BUY`
  - `SELL`
  - optional later: `WATCHLIST`

Definition of done:

- Breakout output maps cleanly to one standard structure
- future strategies can follow the same contract

---

### Step 4: Add first database models

Goal: create the initial persistent data layer.

Target files:

- `vinayak/db/models/signal.py`
- `vinayak/db/models/execution.py`
- `vinayak/db/models/__init__.py`

Tables for Sprint 1:

1. `signals`
- `id`
- `strategy_name`
- `symbol`
- `side`
- `entry_price`
- `stop_loss`
- `target_price`
- `signal_time`
- `status`
- `created_at`

2. `executions`
- `id`
- `signal_id`
- `mode`
- `broker`
- `status`
- `executed_price`
- `executed_at`
- `created_at`

Tasks:

- use SQLAlchemy models
- keep model names simple
- add `Base` export for future migrations

Definition of done:

- models exist
- session can create tables later
- structure is ready for API integration

---

### Step 5: Add DB repository layer

Goal: do not let routes write SQL directly.

Target files:

- `vinayak/db/repositories/signal_repository.py`
- `vinayak/db/repositories/execution_repository.py`

Tasks:

- create `create_signal()`
- create `list_signals()`
- create `create_execution()`
- keep logic minimal and clean

Definition of done:

- route handlers can save and fetch data through repositories

---

### Step 6: Add API schema models

Goal: validate request and response payloads.

Target files:

- `vinayak/api/schemas/strategy.py`
- `vinayak/api/schemas/signal.py`

Tasks:

- request model for breakout run
- response model for normalized signals
- response model for health and status later

Suggested request fields:

- `symbol`
- `capital`
- `risk_pct`
- `rr_ratio`
- `input_path` or direct candle payload later

Definition of done:

- route input is validated
- API output shape is explicit

---

### Step 7: Add first strategy API route

Goal: create one working API endpoint.

Target file:

- `vinayak/api/routes/strategies.py`

Route for Sprint 1:

- `POST /strategies/breakout/run`

Behavior:

- load or receive candle data
- run breakout strategy
- normalize output
- optionally save signals through repository
- return JSON response

Definition of done:

- one route works end-to-end
- response is structured and reusable

---

### Step 8: Register route in API app

Goal: include the strategy route in the main app.

Primary file:

- `vinayak/api/main.py`

Tasks:

- import the strategies router
- include router cleanly
- keep root and health routes untouched

Definition of done:

- app serves:
  - `/`
  - `/health`
  - `/strategies/breakout/run`

---

### Step 9: Add first unit tests

Goal: make Sprint 1 safe and repeatable.

Target files:

- `vinayak/tests/unit/test_breakout_strategy.py`
- `vinayak/tests/unit/test_health_route.py`
- `vinayak/tests/unit/test_strategy_route.py`

Tasks:

- test breakout returns normalized fields
- test health route returns `ok`
- test breakout route responds successfully
- test invalid request payload fails validation

Definition of done:

- first test layer exists
- future refactors are safer

---

### Step 10: Add local run instructions

Goal: make Sprint 1 easy to start.

Update file:

- `vinayak/README.md`

Add:

- install command
- run API command
- test command
- sample breakout route example

Definition of done:

- someone can open the project and run Sprint 1 without guessing

---

## Sprint 1 Deliverables

At the end of Sprint 1, `Vinayak` should have:

- one migrated strategy: `Breakout`
- one standard signal model
- two DB models: `signals`, `executions`
- one API route to run strategy
- first unit tests
- updated README instructions

---

## Suggested File List For Sprint 1

Create or update these files:

- `vinayak/api/main.py`
- `vinayak/api/routes/strategies.py`
- `vinayak/api/schemas/strategy.py`
- `vinayak/api/schemas/signal.py`
- `vinayak/strategies/breakout/service.py`
- `vinayak/strategies/common/base.py`
- `vinayak/db/session.py`
- `vinayak/db/models/__init__.py`
- `vinayak/db/models/signal.py`
- `vinayak/db/models/execution.py`
- `vinayak/db/repositories/signal_repository.py`
- `vinayak/db/repositories/execution_repository.py`
- `vinayak/tests/unit/test_breakout_strategy.py`
- `vinayak/tests/unit/test_health_route.py`
- `vinayak/tests/unit/test_strategy_route.py`
- `vinayak/README.md`

---

## Recommended Execution Order

Do the sprint in this order:

1. package/import cleanup
2. breakout service migration
3. standard signal object cleanup
4. DB models
5. repositories
6. API schemas
7. API route
8. route registration
9. tests
10. README update

---

## Sprint 1 Success Criteria

Sprint 1 is successful if:

- `Vinayak` runs one real strategy through API
- output is standardized
- DB layer foundation exists
- tests cover the first route and strategy
- current `v3` project remains untouched

---

## Next Sprint Preview

Sprint 2 should focus on:

- `Demand Supply` migration
- reviewed trade queue model
- paper execution layer
- Telegram notification service split
- initial PostgreSQL integration wiring
