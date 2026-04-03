# 02-APP-TIER

- The app tier/backend tier contains the FastAPI service and trading logic.
- This tier should not be opened directly to the public internet in production.
- It should receive traffic only from the web tier.

## Implement App Tier

Install Python dependencies:

```bash
py -3 -m pip install -r app/vinayak/requirements.txt
```

Start the Vinayak API:

```bash
py -3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Important routes:

- `GET /health`
- `GET /dashboard/candles`
- `POST /dashboard/live-analysis`
- `GET /admin`
- `GET /workspace`

Key app-tier modules:

- `vinayak/api/main.py`
- `vinayak/api/routes/dashboard.py`
- `vinayak/api/services/trading_workspace.py`
- `vinayak/api/services/live_ohlcv.py`
- `vinayak/api/services/report_storage.py`

Recommended environment variables:

```bash
REDIS_URL=redis://localhost:6379/0
REPORTS_DIR=vinayak/data/reports
REPORTS_S3_BUCKET=your-report-bucket
REPORTS_S3_PREFIX=vinayak/reports
AWS_REGION=ap-south-1
YFINANCE_TIMEOUT=15
```

Verify app tier:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/health/ready
```

