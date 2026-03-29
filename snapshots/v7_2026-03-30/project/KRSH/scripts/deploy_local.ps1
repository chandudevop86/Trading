Set-Location $PSScriptRoot\..
docker build -f docker\Dockerfile -t krsh-trading:latest .
docker compose -f docker\docker-compose.yml up -d
