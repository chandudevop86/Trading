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

## Breakout Bot App (UI)

Launch web app:

```bash
streamlit run src/breakout_app.py
```

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
streamlit run src/breakout_app.py
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
   - py -3 -m pip install -r requirements.txt`n2. Configure local AWS credentials (AWS CLI profile or env vars).
3. Run app locally:
   - streamlit run src/breakout_app.py`n4. In app sidebar, enable **AWS S3 (localhost)** and fill:
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

## Dhan API Integration Tab (Future)

A future-ready strategy tab is available in app:
- `Dhan API (Future)`

This tab includes:
- Auth config placeholders
- Market feed payload example
- Order router payload example
- Webhook schema example
- Downloadable `dhan_order_template.csv`

## Dhan Execution Policy

- `Dhan API (Future)` strategy now supports **paper execution now**.
- Live execution is locked until **30 days** of paper-trade history exists in `data/dhan_paper_trades.csv`.
- Default NIFTY lot size is set to **65**.

## Auto Run (Backtest + Execute + Report)

Run the end-to-end pipeline (fetch OHLCV → backtest → execute to log → HTML/PDF report → optional Telegram message/PDF):

```bash
py -3 -m src.auto_run --symbol ^NSEI --interval 5m --period 1d --execution-type PAPER --send-telegram --send-telegram-pdf --telegram-token "<BOT_TOKEN>" --telegram-chat-id "<CHAT_ID>"
```

Execution modes:
- `--execution-type PAPER` writes to `data/paper_trading_logs_all.csv`
- `--execution-type LIVE` writes to `data/live_trading_logs_all.csv` (locked until 30 days of PAPER history; see `--min-paper-days`)
- `--execution-type NONE` skips execution (backtest/report only)
