# Package Layout

## Canonical runtime packages

- `app/vinayak/` is the primary application codebase.
- `vinayak/` at the repo root is only a compatibility shim that points Python imports to `app/vinayak/`.
- `src/` is the legacy operator/runtime surface that remains supported for older entrypoints and backtest paths.

## Import boundary rules

- Application code under `app/vinayak/` should prefer `vinayak.*` imports.
- Legacy code under `src/` should prefer `src.*` imports.
- If `src/` needs functionality that is implemented in `app/vinayak/`, expose it through a narrow compatibility facade inside `src/` instead of importing `vinayak.*` directly all over the tree.
- The current approved `src` facade is `src.observability`, which re-exports observability helpers from `vinayak.observability`.
- The current approved `app/vinayak` facade is `vinayak.legacy`, which wraps the small set of supported legacy `src.*` integrations used by the app layer.

## Decision

- Keep the root `vinayak/` shim for now so tests, scripts, and app imports remain stable during the legacy-surface cleanup.
- Do not treat the shim as a permanent architectural destination.
- The target end state is to flatten the package layout by making `app/vinayak/` the only real `vinayak` package and removing the root shim after the remaining legacy entrypoints and compatibility imports are retired.

## Why this exists

This repository currently ships two active code surfaces. Without a clear import rule, files start mixing `src.*` and `vinayak.*` imports arbitrarily, which creates package drift, brittle test setup, and confusion about which tree owns runtime behavior.

The goal is not a risky full reorg. The goal is to make package ownership explicit, keep cross-tree dependencies few and named, and reduce the legacy surface until flattening is low-risk.
