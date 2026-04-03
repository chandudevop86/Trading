# Vinayak AWS Ops Runbook

Date: 2026-04-04
Project: Vinayak
Purpose: Practical deployment companion for AWS security groups, EC2 setup commands, and PostgreSQL connection configuration.

## Scope

This runbook is the operational companion to the Vinayak AWS 3-tier separation guide.

It covers:

1. Security group rules
2. EC2 setup commands
3. PostgreSQL setup notes
4. Vinayak environment configuration
5. Startup and verification steps

## Security Group Rules

### `sg-alb`

Inbound:
- `80/tcp` from `0.0.0.0/0`
- `443/tcp` from `0.0.0.0/0`

Outbound:
- allow all outbound or restrict to app security group on required app port

### `sg-web`

Inbound:
- `80/tcp` from `sg-alb`
- `443/tcp` from `sg-alb` if terminating TLS on web EC2
- `22/tcp` from your office or home IP only

Outbound:
- `8000/tcp` to `sg-app`

### `sg-app`

Inbound:
- `8000/tcp` from `sg-web` or `sg-alb`
- `22/tcp` from bastion or your IP only

Outbound:
- `5432/tcp` to `sg-db`
- `6379/tcp` to `sg-redis`
- `443/tcp` for Dhan, Telegram, AWS APIs, and package installation

### `sg-db`

Inbound:
- `5432/tcp` only from `sg-app`

Outbound:
- default outbound

### `sg-redis`

Inbound:
- `6379/tcp` only from `sg-app`

## Ubuntu EC2 Setup Commands

Run these on the app EC2 host:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx git curl
cd /opt
sudo git clone <your-repo-url> Trading
sudo chown -R $USER:$USER /opt/Trading
cd /opt/Trading
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install fastapi "uvicorn[standard]" python-multipart
```

## Recommended Swap Setup

For small instances, add swap:

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
free -h
```

To persist after reboot:

```bash
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

## PostgreSQL Database Setup

Use Amazon RDS PostgreSQL.

Recommended baseline:
- engine: PostgreSQL 16
- instance: `db.t3.small` minimum
- storage: `gp3`
- public access: disabled
- Multi-AZ: enabled for production

Create a database:
- name: `vinayak`
- user: `vinayak_app`

## PostgreSQL Connection String

Example:

```text
postgresql+psycopg2://vinayak_app:YOUR_PASSWORD@YOUR_RDS_ENDPOINT:5432/vinayak
```

If the project uses direct psycopg style:

```text
postgresql://vinayak_app:YOUR_PASSWORD@YOUR_RDS_ENDPOINT:5432/vinayak
```

## Environment File Example

Create:

```bash
sudo mkdir -p /etc/vinayak
sudo nano /etc/vinayak/vinayak.env
```

Example contents:

```env
DATABASE_URL=postgresql+psycopg2://vinayak_app:YOUR_PASSWORD@YOUR_RDS_ENDPOINT:5432/vinayak
REDIS_URL=redis://YOUR_REDIS_ENDPOINT:6379/0
DHAN_CLIENT_ID=your_dhan_client_id
DHAN_ACCESS_TOKEN=your_dhan_access_token
TELEGRAM_TOKEN=your_telegram_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
LIVE_TRADING_ENABLED=false
TRADING_BROKER_MODE=PAPER
```

Lock the file:

```bash
sudo chmod 600 /etc/vinayak/vinayak.env
```

## Nginx Reverse Proxy

Example `/etc/nginx/sites-available/vinayak`:

```nginx
server {
    listen 80;
    server_name vinayak.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Enable it:

```bash
sudo ln -s /etc/nginx/sites-available/vinayak /etc/nginx/sites-enabled/vinayak
sudo nginx -t
sudo systemctl restart nginx
```

## Systemd Service

Create:

```bash
sudo nano /etc/systemd/system/vinayak.service
```

Use:

```ini
[Unit]
Description=Vinayak FastAPI
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/opt/Trading
EnvironmentFile=/etc/vinayak/vinayak.env
ExecStart=/opt/Trading/venv/bin/python -m uvicorn vinayak.api.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable vinayak
sudo systemctl start vinayak
sudo systemctl status vinayak
```

## Verification Commands

### App Health

```bash
curl http://127.0.0.1:8000/health
```

### Port Check

```bash
ss -tulpn | grep 8000
```

### Service Logs

```bash
sudo journalctl -u vinayak -n 100 --no-pager
```

### Nginx Logs

```bash
sudo journalctl -u nginx -n 100 --no-pager
```

## Deployment Order

1. Create RDS PostgreSQL
2. Create Redis if needed
3. Launch app EC2
4. Install Python and dependencies
5. configure `/etc/vinayak/vinayak.env`
6. create systemd service
7. verify app on `127.0.0.1:8000`
8. configure Nginx
9. place ALB in front if using public entry
10. point Route 53 record to ALB

## Final Notes

For Vinayak, this operational baseline is the minimum clean AWS setup:

- separate web layer
- separate app layer
- separate PostgreSQL database
- private database access only
- broker credentials only on app tier
- swap enabled or larger app instance

This is the right path for controlled live validation and future production hardening.
