#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -f docker/Dockerfile -t krsh-trading:latest .
docker compose -f docker/docker-compose.yml up -d
