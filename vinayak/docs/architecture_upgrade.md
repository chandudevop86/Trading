# Vinayak Architecture Upgrade

## 1. Web Tier

The web tier is the internet-facing layer.

Technologies that fit here:
- HTML
- CSS
- JavaScript
- React
- Angular
- Node.js for frontend hosting or SSR when needed
- Nginx as the preferred web server
- Apache as an older alternative
- AWS ALB for public entry in cloud deployments

Responsibilities:
- serve frontend pages
- terminate public HTTP or HTTPS traffic
- reverse proxy requests to the APP/API tier
- host static assets
- protect internal services from direct internet access

For Vinayak:
- `Nginx` or `AWS ALB` should be the public web tier
- `/admin` and `/workspace` should be exposed through this layer
- browser traffic should stop here first

## 2. APP/API Tier

The APP/API tier is the backend layer.

Technologies that fit here:
- Java
- .NET
- Python
- Go
- PHP

Older hosting styles:
- Tomcat for Java apps
- JBoss for Java enterprise apps
- IIS for .NET and Windows-hosted apps

Modern hosting styles:
- many frameworks now ship with built-in servers or runtime hosts
- Python uses FastAPI with Uvicorn or Gunicorn
- Java can use Spring Boot embedded Tomcat
- .NET uses Kestrel
- Go services usually ship with their own HTTP server
- PHP can run through PHP-FPM behind Nginx

Responsibilities:
- business logic
- API endpoints
- authentication and authorization
- strategy processing
- execution workflows
- broker integrations
- notification workflows

For Vinayak:
- the current best fit is `Python + FastAPI + Uvicorn`
- this tier should not be directly opened to the internet
- it should be reachable only from the web tier or private network

## 3. Data Tier

The data tier stores application and operational data.

RDBMS technologies that fit here:
- MySQL
- MSSQL
- PostgreSQL

Use RDBMS for:
- users
- orders
- executions
- audit logs
- signals
- reviewed trades
- relational reporting data

NoSQL technologies that fit here:
- MongoDB

Use NoSQL for:
- flexible product-style documents
- semi-structured metadata
- evolving document models

For Vinayak:
- `PostgreSQL` is the best primary database choice
- it fits reviewed trades, signals, executions, users, logs, and admin data
- MongoDB is optional, not required for the current trading workflow

## 4. Cache Tier

Cache technologies that fit here:
- Redis

Use cache for:
- hot market data
- session data if needed
- recent report artifacts
- fast dashboard responses
- temporary workflow state

For Vinayak:
- `Redis` should be used for live OHLCV cache and fast API reads

## 5. Messaging Tier

MQ technologies that fit here:
- RabbitMQ
- ActiveMQ
- Kafka

Use MQ for:
- asynchronous communication
- event-driven workflows
- alert fan-out
- decoupled execution pipelines
- background workers

For Vinayak:
- `RabbitMQ` is the best immediate fit
- use it for signal events, execution events, and notification events
- Kafka is only needed later if traffic becomes very large

## 6. Recommended Vinayak Stack

Recommended upgraded stack for this project:
- Web Tier: `Nginx` or `AWS ALB`
- Frontend: current HTML/CSS/JS pages, with React optional later
- APP/API Tier: `Python + FastAPI + Uvicorn`
- Database: `PostgreSQL`
- Cache: `Redis`
- MQ: `RabbitMQ`
- Object/report storage: `S3` or local reports during development

## 7. What Not To Do

Do not put all technologies into the project together.

Examples:
- do not add Java, .NET, Go, and PHP into the same Vinayak backend without a real need
- do not add Tomcat, JBoss, and IIS unless the selected backend actually requires them
- do not expose FastAPI or internal APIs directly to the internet in production

## 8. Final Vinayak Production Flow

Recommended request flow:

`Browser -> Nginx/ALB Web Tier -> FastAPI APP/API Tier -> PostgreSQL/Redis/RabbitMQ`

Security rule:
- only the web tier should be public
- APP/API tier should stay private
- database, cache, and MQ should stay private
