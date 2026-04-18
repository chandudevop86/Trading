# Rollback

## Service rollback

1. Revert the repository checkout to the previous known-good commit.
2. Reinstall dependencies if the revision changed requirements.
3. Downgrade the database only if the release introduced a migration that must be reversed.
4. Restart `vinayak-api.service`.

## Example rollback flow

```bash
cd /opt/trading
git log --oneline -n 5
git checkout <previous-good-commit>
. .venv/bin/activate
python -m pip install -r app/vinayak/requirements.txt
python -m alembic -c app/vinayak/alembic.ini current
sudo systemctl restart vinayak-api.service
sudo systemctl status vinayak-api.service --no-pager
```

## Migration rollback

Only downgrade Alembic revisions when the target downgrade path has been reviewed for data safety.

Inspect current revision:
```bash
python -m alembic -c app/vinayak/alembic.ini current
```

Downgrade one step:
```bash
python -m alembic -c app/vinayak/alembic.ini downgrade -1
```

## Operational checks after rollback

- `/health/live`
- `/health/ready`
- admin login
- reviewed-trade approval flow
- paper execution flow
- recent logs for startup/config/auth failures
