# Repository Ownership

This repository is organized around a practical 3-tier separation. New files should be placed according to responsibility, not convenience.

## `app/`

Application-tier runtime code.

Put here:
- FastAPI routes and dependencies
- trading logic
- strategy engines
- execution and broker integrations
- observability runtime code
- data-access code and migrations

Do not put here:
- nginx configs
- EC2, Docker, or Kubernetes deployment files
- generated architecture books or exported PDFs
- top-level test suites

## `web/`

Web-tier delivery assets.

Put here:
- reverse-proxy configs
- web server configs
- static delivery concerns

Do not put here:
- trading logic
- broker credentials
- database code

## `infra/`

Infrastructure and deployment assets.

Put here:
- AWS deployment scripts
- Dockerfiles and compose files
- systemd units
- Kubernetes manifests
- CloudWatch setup
- shared environment templates

Do not put here:
- application business logic
- runtime Python packages

## `docs/`

Reference and non-runtime material.

Put here:
- architecture documentation
- runbooks
- migration plans
- printable books
- diagrams and generated exports

Do not put here:
- live runtime code
- active application assets required for imports

## `tests/`

Top-level verification surface.

Put here:
- unit tests
- integration tests
- end-to-end tests

Current split:
- `tests/unit/`: application-focused tests
- `tests/integration/`: broader legacy and cross-surface tests

Do not put here:
- generated `.pyc` files
- runtime package data

## Legacy Surfaces

- `src/` remains the legacy runtime and should not absorb new Vinayak app-tier features unless the work is intentionally for the monolith.
- `vinayak/` is a compatibility shim only. Do not add new modules there.

## Practical Rule

If a file could be removed without breaking Python imports or app startup, it probably belongs in `docs/` or `infra/`, not `app/`.
