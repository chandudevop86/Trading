# 03-DATA-TIER

- The data tier stores transactional data, cached live data, and generated reports.
- In production, this should use managed AWS services where possible.

## Implement Data Tier

### PostgreSQL / RDS

Create or connect the application database.

Run migrations:

```bash
py -3 -m alembic -c vinayak/alembic.ini upgrade head
```

### Redis / ElastiCache

Use Redis for:

- live OHLCV hot cache
- recent analysis artifact cache
- fast temporary state

Set environment variable:

```bash
REDIS_URL=redis://localhost:6379/0
```

### S3 / Report Storage

Vinayak writes local reports first and can upload the same artifacts to S3.

Set environment variables:

```bash
REPORTS_DIR=vinayak/data/reports
REPORTS_S3_BUCKET=your-report-bucket
REPORTS_S3_PREFIX=vinayak/reports
AWS_REGION=ap-south-1
```

Files used by this tier:

- `vinayak/db/`
- `vinayak/cache/redis_client.py`
- `vinayak/api/services/report_storage.py`
- `vinayak/data/`

Verify data tier behavior:

- execute `/dashboard/candles` and confirm Redis cache gets populated
- execute `/dashboard/live-analysis` and confirm JSON/TXT reports are written
- if S3 is configured, confirm uploaded report artifacts are created
