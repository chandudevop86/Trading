# Vinayak 3-Tier Codebase Restructure Book

Date: 2026-04-04
Project: Vinayak
Purpose: Define how the Vinayak repository should be reorganized so the codebase matches a serious 3-tier architecture.

## Title Page

Vinayak 3-Tier Codebase Restructure Book

Prepared for:
- repository cleanup
- production architecture alignment
- long-term maintainability
- deployment isolation

This book explains how Vinayak should be restructured so the repo layout reflects the same separation as the AWS architecture.

## Table Of Contents

1. Executive Summary
2. Why Repo Separation Matters
3. Target Repository Shape
4. Tier Responsibilities
5. Current-To-Target Migration Map
6. Proposed Directory Tree
7. Migration Phases
8. Naming and Ownership Rules
9. Final Recommendation

## Executive Summary

Vinayak should not only be deployed as a 3-tier platform. It should also be organized as one in the repository.

That means separating:

1. Web concerns
2. Application and domain logic
3. Infrastructure and deployment assets

If the AWS deployment is separated but the repository is still mixed together, the system continues to behave like a monolith in practice.

## Why Repo Separation Matters

If web, app, deployment, and operational files are mixed together:

- changes become harder to reason about
- deployment boundaries remain blurry
- app secrets and infra concerns get mixed into feature work
- testing becomes less clear
- ownership becomes weaker

A serious 3-tier platform should look separated both:

1. in runtime deployment
2. in source layout

## Target Repository Shape

Recommended top-level structure:

```text
vinayak-platform/
  web/
  app/
  infra/
  docs/
  tests/
  data/
  snapshots/
```

This creates clean top-level separation:

- `web/` for public-facing UI and reverse-proxy assets
- `app/` for application logic and domain modules
- `infra/` for deployment and environment assets
- `docs/` for architecture and operations guidance
- `tests/` for organized testing layers

## Tier Responsibilities

### `web/`

Contains:
- Nginx config
- static assets
- templates
- simple frontend helpers

Must not contain:
- broker logic
- database models
- trading execution code
- strategy logic

### `app/`

Contains:
- FastAPI app
- execution logic
- risk logic
- observability logic
- messaging
- workers
- DB access layer
- broker adapters

This is the main business-logic layer.

### `infra/`

Contains:
- AWS infrastructure code
- Terraform or CloudFormation
- systemd units
- nginx deployment config
- Docker and Compose assets
- deployment scripts

Must not contain:
- trading-domain logic

### `docs/`

Contains:
- architecture documents
- runbooks
- validation checklists
- rollout plans

### `tests/`

Contains:
- unit tests
- integration tests
- end-to-end tests

## Current-To-Target Migration Map

### Current `vinayak/api`

Move to:

```text
app/vinayak/api
```

### Current `vinayak/execution`

Move to:

```text
app/vinayak/execution
```

### Current `vinayak/observability`

Move to:

```text
app/vinayak/observability
```

### Current `vinayak/messaging`

Move to:

```text
app/vinayak/messaging
```

### Current `vinayak/analytics`

Move to:

```text
app/vinayak/analytics
```

### Current `vinayak/metrics`

Move to:

```text
app/vinayak/metrics
```

### Current `vinayak/notifications`

Move to:

```text
app/vinayak/notifications
```

### Current `vinayak/tests`

Merge into:

```text
tests/unit
tests/integration
tests/e2e
```

### Current `deploy/`

Move to:

```text
infra/
```

Suggested subfolders:

```text
infra/aws
infra/nginx
infra/systemd
infra/docker
infra/scripts
```

### Current Nginx and EC2 deployment files

Move under:

```text
web/nginx
infra/nginx
infra/systemd
infra/aws
```

depending on whether they are runtime-facing or deployment-facing.

### Current `docs/`

Keep under:

```text
docs/
```

But organize further:

```text
docs/architecture
docs/runbooks
docs/aws
docs/validation
```

## Proposed Directory Tree

```text
F:/Trading
  web/
    nginx/
      default.conf
      vinayak.conf
    static/
    templates/
    README.md

  app/
    vinayak/
      api/
      analytics/
      core/
      db/
      execution/
      messaging/
      metrics/
      notifications/
      observability/
      validation/
      workers/
      __init__.py
    requirements.txt
    alembic.ini
    README.md

  infra/
    aws/
    docker/
    nginx/
    scripts/
    systemd/
    README.md

  docs/
    architecture/
    aws/
    runbooks/
    validation/

  tests/
    unit/
    integration/
    e2e/

  data/
  snapshots/
```

## Migration Phases

### Phase 1: Documentation-First Separation

Do first:

1. define target structure
2. define ownership boundaries
3. define import strategy

No production behavior changes yet.

### Phase 2: Infra Extraction

Move:
- deployment scripts
- nginx assets
- systemd files
- AWS assets

This is usually the safest first physical move.

### Phase 3: App Namespace Consolidation

Move Vinayak application modules into:

```text
app/vinayak/
```

This creates a clear application boundary without changing logical domains.

### Phase 4: Test Reorganization

Move tests into:

```text
tests/unit
tests/integration
tests/e2e
```

This improves confidence and release discipline.

### Phase 5: Web Extraction

Move web-facing assets into:

```text
web/
```

This completes the code-level 3-tier reflection.

## Naming And Ownership Rules

Use these rules:

1. `web/` owns presentation and proxy assets only
2. `app/` owns business logic and runtime logic only
3. `infra/` owns environment and deployment assets only
4. `docs/` owns architecture and runbook material only
5. `tests/` mirrors behavior verification, not deployment concerns

If a file mixes concerns, split it before moving it.

## Final Recommendation

If Vinayak should look and behave like a serious 3-tier platform, the repository must reflect that separation clearly.

The correct target is:

- separate `web/`
- separate `app/`
- separate `infra/`
- structured `docs/`
- structured `tests/`

This does not just improve appearance.
It improves maintainability, ownership, deployment discipline, and long-term production safety.
