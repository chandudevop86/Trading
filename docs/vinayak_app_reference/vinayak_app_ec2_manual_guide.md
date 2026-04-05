# Vinayak App EC2 Manual Guide

## Purpose

This guide covers the manual setup of the App EC2 in the 3-EC2 Vinayak topology.

## Responsibilities

The App EC2 runs:
- Vinayak API
- outbox worker
- queue worker

## Install Base Packages

Install:
- Python 3.12
- pip
- venv support
- git
- build tools for Python dependencies

## Deploy Code

Recommended path:
- `/opt/vinayak`

## Python Environment

- create a virtual environment
- install dependencies from [requirements.txt](/F:/Trading/requirements.txt)

## Env File

Create an env file such as:
- `/etc/vinayak/vinayak.env`

Set:
- `VINAYAK_DATABASE_URL=postgresql+psycopg2://vinayak:<db-password>@<data-private-ip>:5432/vinayak`
- `REDIS_URL=redis://<data-private-ip>:6379/0`
- `MESSAGE_BUS_URL=amqp://vinayak:<rabbitmq-password>@<data-private-ip>:5672/`
- `VINAYAK_ADMIN_USERNAME`
- `VINAYAK_ADMIN_PASSWORD`
- `VINAYAK_ADMIN_SECRET`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`
- `VINAYAK_SECURE_COOKIES=true`

## Database Migration

Run Alembic upgrade before public traffic.

## Manual Validation

Before using `systemd`, start the API and workers manually and verify:
- `/health`
- `/health/live`
- `/health/ready`
- `/login`
- `/admin`

## Security Group

Allow:
- 8000 only from Web EC2
- 22 only from admin IP
- outbound 5432, 6379, and 5672 to Data EC2

## Operations

- keep logs collected after each restart
- restart workers after env changes
- test one live-analysis run after deploy
