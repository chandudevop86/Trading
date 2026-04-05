# Vinayak Systemd Services Guide

## Purpose

This guide explains how to run Vinayak manually on the App EC2 with `systemd` after the initial validation phase.

## Services To Create

Create three services:
- `vinayak-api`
- `vinayak-outbox-worker`
- `vinayak-queue-worker`

## Common Runtime Assumptions

- app code path: `/opt/vinayak`
- env file path: `/etc/vinayak/vinayak.env`
- Python virtual environment: `/opt/vinayak/.venv`
- service user: `ubuntu` or dedicated `vinayak`

## API Service Unit

```ini
[Unit]
Description=Vinayak API
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/opt/vinayak
EnvironmentFile=/etc/vinayak/vinayak.env
Environment="PYTHONUNBUFFERED=1"
ExecStart=/opt/vinayak/.venv/Scripts/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Outbox Worker Unit

```ini
[Unit]
Description=Vinayak Outbox Worker
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/opt/vinayak
EnvironmentFile=/etc/vinayak/vinayak.env
Environment="PYTHONUNBUFFERED=1"
ExecStart=/opt/vinayak/.venv/Scripts/python -m vinayak.workers.outbox_worker
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Queue Worker Unit

```ini
[Unit]
Description=Vinayak Queue Worker
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/opt/vinayak
EnvironmentFile=/etc/vinayak/vinayak.env
Environment="PYTHONUNBUFFERED=1"
ExecStart=/opt/vinayak/.venv/Scripts/python -m vinayak.workers.event_worker
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Enable And Start

```powershell
sudo systemctl daemon-reload
sudo systemctl enable vinayak-api
sudo systemctl enable vinayak-outbox-worker
sudo systemctl enable vinayak-queue-worker
sudo systemctl start vinayak-api
sudo systemctl start vinayak-outbox-worker
sudo systemctl start vinayak-queue-worker
```

## Health Checks

Run:
- `sudo systemctl status vinayak-api`
- `sudo systemctl status vinayak-outbox-worker`
- `sudo systemctl status vinayak-queue-worker`
- `journalctl -u vinayak-api -n 100 --no-pager`
- `journalctl -u vinayak-outbox-worker -n 100 --no-pager`
- `journalctl -u vinayak-queue-worker -n 100 --no-pager`

## Operational Advice

- restart the API separately from the workers when possible
- after changing env values, restart all three services
- keep the env file readable only by root or the service user
- verify `/health/ready` after every deploy
