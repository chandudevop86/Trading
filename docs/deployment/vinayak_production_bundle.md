# Vinayak Production Bundle

## Environment File
Path: `infra/production/env/vinayak.production.env.example`

```env
APP_ENV=prod
VINAYAK_DATABASE_URL=postgresql+psycopg://vinayak:REPLACE_DB_PASSWORD@postgres-host:5432/vinayak
REDIS_URL=redis://redis-host:6379/0
REDIS_DEFAULT_TTL_SECONDS=900
VINAYAK_ADMIN_USERNAME=admin
VINAYAK_ADMIN_PASSWORD=REPLACE_STRONG_ADMIN_PASSWORD
VINAYAK_ADMIN_SECRET=REPLACE_LONG_RANDOM_SECRET
VINAYAK_API_BASE_URL=http://127.0.0.1/api
MESSAGE_BUS_ENABLED=false
```

## API systemd Unit
Path: `infra/production/systemd/vinayak-api.service`

```ini
[Unit]
Description=Vinayak FastAPI service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/Trading
EnvironmentFile=/root/Trading/infra/production/env/vinayak.production.env
ExecStart=/root/Trading/venv/bin/python -m uvicorn app.vinayak.api.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5
TimeoutStopSec=20

[Install]
WantedBy=multi-user.target
```

## UI systemd Unit
Path: `infra/production/systemd/vinayak-ui.service`

```ini
[Unit]
Description=Vinayak Streamlit UI
After=network.target vinayak-api.service
Requires=vinayak-api.service

[Service]
Type=simple
User=root
WorkingDirectory=/root/Trading
EnvironmentFile=/root/Trading/infra/production/env/vinayak.production.env
Environment=VINAYAK_API_BASE_URL=http://127.0.0.1/api
ExecStart=/root/Trading/venv/bin/python -m streamlit run app/vinayak/ui/app.py --server.address 0.0.0.0 --server.port 8501
Restart=always
RestartSec=5
TimeoutStopSec=20

[Install]
WantedBy=multi-user.target
```

## Nginx Reverse Proxy
Path: `infra/production/nginx/vinayak.conf`

```nginx
server {
    listen 80;
    server_name _;
    client_max_body_size 20m;

    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

    location /api/ {
        proxy_pass http://127.0.0.1:8000/;
        proxy_read_timeout 120s;
    }

    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        proxy_read_timeout 30s;
    }

    location /health/ready {
        proxy_pass http://127.0.0.1:8000/health/ready;
        proxy_read_timeout 30s;
    }

    location /streamlit/ {
        proxy_pass http://127.0.0.1:8501/;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }

    location / {
        proxy_pass http://127.0.0.1:8501/;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }
}
```
