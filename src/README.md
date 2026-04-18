# Deprecated Legacy Runtime

`src/` is no longer the supported runtime surface for this repository.

Supported runtime:
- `py -3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000`

`src/` remains in the repo temporarily for migration, compatibility verification, and historical reference. New operator workflows, automation, deployment assets, and documentation should target `app/` only.
