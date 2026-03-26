# Data Processing and Rule Engine (Python)

This project provides:
- CSV data processing with Python stdlib (`csv`)
- YAML-defined rules (`PyYAML`)
- NIFTY 50 constituent fetch and rule scoring
- NIFTY 50 option-chain rule screening
- First 15-min breakout strategy with optional Telegram alerts

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Generic CSV Rule Run

```bash
py -3 -m src.main --input data/input.csv --rules data/rules.yaml --output data/output.csv
```

## NIFTY 50 Constituents Run

```bash
py -3 -m src.nifty50 --snapshot-output data/nifty50_snapshot.csv --rules data/nifty50_rules.yaml --scored-output data/nifty50_scored.csv
```

## NIFTY 50 Options Run

```bash
py -3 -m src.nifty_options --input data/nifty_options_chain_sample.csv --rules data/nifty_options_rules.yaml --output data/nifty_options_scored.csv
```

## NIFTY Futures Run

```bash
py -3 -m src.nifty_futures --symbol NIFTY --snapshot-output data/nifty_futures_snapshot.csv --rules data/nifty_futures_rules.yaml --scored-output data/nifty_futures_scored.csv
```

If rules exist, output is generated with extra boolean columns per rule.

## Rule format

Each rule in YAML:

```yaml
- name: high_value
  field: amount
  op: ">"
  value: 10000
```

Supported operators:
- `>`
- `<`
- `>=`
- `<=`
- `==`
- `!=`
- `in`
- `not in`

## Notes

Numeric comparisons auto-cast string CSV values to numbers when possible.

## First 15-Min Breakout Algo Bot

Strategy implemented in `src/breakout_bot.py`:
- Trades only after first 15-minute candle range breakout
- Uses first 1-hour candle direction (first four 15-minute candles) as bias
- Uses intraday VWAP confirmation (`close > VWAP` for buy, `close < VWAP` for sell)
- Stop loss on breakout candle extreme
- Target fixed at 1:2 risk-reward
- Position size based on risk percentage of capital

Run:

```bash
py -3 -m src.breakout_bot --input data/intraday.csv --output data/breakout_trades.csv --capital 100000 --risk-pct 0.01 --rr-ratio 2
```

Send Telegram notification from CLI run:

```bash
py -3 -m src.breakout_bot --input data/intraday.csv --output data/breakout_trades.csv --capital 100000 --risk-pct 0.01 --telegram-token "<BOT_TOKEN>" --telegram-chat-id "<CHAT_ID>"
```

Input CSV columns (required):
- `timestamp` (ISO format or `YYYY-MM-DD HH:MM[:SS]`)
- `open`
- `high`
- `low`
- `close`
- `volume`

## Trading App (UI)

Launch web app:

```bash
streamlit run src/Trading.py
```

<<<<<<< HEAD
## Active Legacy Surface

Supported active legacy entrypoints:
- `streamlit run src/Trading.py`
- `py -3 -m src.breakout_bot`
- `tools/run_app.ps1`

Reference-only / non-active files in `src/` are documented in `src/EXPERIMENTAL_SURFACE.md` and should not be used as runtime or deployment targets.
=======
## Legacy Supported Entry Points

During Phase 0 stabilization, the supported legacy `src/` runtime surface is:

- UI operator console: `streamlit run src/Trading.py`
- End-to-end batch pipeline: `py -3 -m src.auto_run ...`
- Backtesting workflow: `py -3 -m src.auto_backtest ...`
- Broker/account CLI reference: `py -3 -m src.dhan_example ...` and `py -3 -m src.dhan_account ...`

These are the supported legacy entrypoints for the old monolithic project.

## Legacy Product Boundary

The legacy maintained product in this repository is the monolithic trading application under `src/`.

In scope for legacy maintenance:

- `src/`
- `tests/`
- `data/`
- `logs/`
- root-level legacy runtime docs and deploy assets that directly support `src/`

Not in scope for legacy runtime ownership:

- `vinayak/` because it is the next-generation platform track
- `snapshots/` because they are rollback/reference artifacts
- `src/_archive/` because it is archived legacy history, not an active runtime surface
- duplicate or historical experimental files that are not part of the supported `src/` entrypoints

For the formal legacy scope definition, see:

- `docs/legacy_scope.md`

Not supported as legacy deployment targets:

- files under `src/_archive/`
- deleted or historical experimental files referenced in old snapshots
- parallel rewrite code under `vinayak/`

For current legacy contracts and runtime outputs, see:

- `docs/legacy_src_contracts.md`
>>>>>>> feature

What app does:
- Upload intraday OHLCV CSV
- Set capital, risk %, and RR ratio
- Auto-select option strike price (ATM/ITM/OTM, 50/100 interval)
- Generate breakout trades
- Show PnL summary and trade table
- Download trades as CSV
- Optionally send a Telegram summary notification

Telegram setup in app:
- Create bot with BotFather and copy bot token
- Get your chat ID
- In sidebar, enable Telegram and provide token + chat ID
- Click `Send Telegram summary`

## Use Custom Domain (chandudevopai.shop)

The app can run behind your domain, but domain routing needs DNS + reverse proxy on your server.

1. Run Streamlit on the server:
```bash
streamlit run src/Trading.py
```

2. Point DNS A record:
- Host: `chandudevopai.shop`
- Value: your server public IP

3. Configure Nginx reverse proxy:
```nginx
server {
    listen 80;
    server_name chandudevopai.shop;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

4. Enable HTTPS (recommended):
- Use Certbot for TLS certs.

After this, open: `https://chandudevopai.shop`

## AWS Integration on Localhost

1. Install dependencies:
   - `py -3 -m pip install -r requirements.txt`
2. Configure local AWS credentials (AWS CLI profile or env vars).
3. Run app locally:
<<<<<<< HEAD
   - streamlit run src/Trading.py`n4. In app sidebar, enable **AWS S3 (localhost)** and fill:
=======
   - `streamlit run src/Trading.py`
4. In app sidebar, enable **AWS S3 (localhost)** and fill:
>>>>>>> feature
   - S3 bucket
   - AWS region
   - S3 key prefix
5. Generate trades and click **Upload Trades CSV to S3**.
## Docker + Kubernetes Deployment

Ready files are included:
- `deploy/docker/Dockerfile`
- `deploy/docker/docker-compose.yml`
- `deploy/k8s/*.yaml`
- `deploy/k8s/DOCKER_KUBERNETES_DHAN_GUIDE.md`

Quick start:
```bash
docker build -f deploy/docker/Dockerfile -t intratrade:latest .
docker compose -f deploy/docker/docker-compose.yml up -d
```

K8s apply:
```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/secret.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/ingress.yaml
```

## Dhan Integration

The app now supports Dhan-based execution and account connectivity.

Current capabilities:
- Preview Dhan order payloads before sending
- Route live orders through Dhan when execution mode is `LIVE`
- Run readiness checks for Dhan credentials and security-map coverage
- Fetch positions and order details from the Dhan account CLI tools
- Use a root `.env` file for `DHAN_CLIENT_ID` and `DHAN_ACCESS_TOKEN`

## Dhan Execution Policy

- Live execution is available when Dhan credentials and the security map are configured correctly.
- Orders are sent only when you choose `LIVE` execution or pass `--place-live` in the CLI.
- Default NIFTY lot size is set to **65**.

## Auto Run (Backtest + Execute + Report)

Run the end-to-end pipeline (fetch OHLCV -> backtest -> execute to log -> HTML/PDF report -> optional Telegram message/PDF):

```bash
py -3 -m src.auto_run --symbol ^NSEI --interval 5m --period 1d --execution-type PAPER --send-telegram --send-telegram-pdf --telegram-token "<BOT_TOKEN>" --telegram-chat-id "<CHAT_ID>"
```

Execution modes:
- `--execution-type PAPER` writes to `data/paper_trading_logs_all.csv`
- `--execution-type LIVE` writes to `data/live_trading_logs_all.csv` when Dhan credentials and the security map are configured
- `--execution-type NONE` skips execution (backtest/report only)


## Dhan HQ CLI Preview / Order

Preview a Dhan order payload safely:

```bash
py -3 -m src.dhan_example --symbol NIFTY --side BUY --quantity 50 --option-strike 27MAR24500CE --option-type CE --strike-price 24500 --security-map data/dhan_security_map.csv
```

Place the same order live after previewing it:

```bash
py -3 -m src.dhan_example --symbol NIFTY --side BUY --quantity 50 --option-strike 27MAR24500CE --option-type CE --strike-price 24500 --security-map data/dhan_security_map.csv --place-live
```

Required environment variables for live use:
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`
- Optional: `DHAN_BASE_URL`
- Optional: `DHAN_SECURITY_MAP`

Notes:
- Default behavior is preview-only and does not hit the broker.
- The security-map CSV must contain Dhan security IDs and symbol aliases used by your contract names.
- Use `--order-type LIMIT --price <value>` for limit orders.

Fetch current Dhan positions:

```bash
py -3 -m src.dhan_account --resource positions
```

Fetch a single Dhan order by ID:

```bash
py -3 -m src.dhan_account --resource order --order-id <ORDER_ID>
```

## Environment File

This repo now auto-loads a root `.env` file whenever a `src.*` module starts, so Dhan credentials can live in one place.

Example `.env` values:

```env
DHAN_CLIENT_ID=your_client_id_here
DHAN_ACCESS_TOKEN=your_access_token_here
DHAN_BASE_URL=https://api-hq.dhan.co
DHAN_SECURITY_MAP=data/dhan_security_map.csv
```

Files added:
- `.env` for your local secrets
- `.env.example` as the shareable template

Because `src/__init__.py` loads `.env` automatically, the same credentials are available to `src.dhan_example`, `src.dhan_account`, `src.auto_run`, and other `src.*` entry points that read `os.getenv(...)`.



## Official App Entry Point

The official production UI entry point is now:

```bash
streamlit run src/Trading.py
```

Legacy or experimental files should not be used as deployment targets.




