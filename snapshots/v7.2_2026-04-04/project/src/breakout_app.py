from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.legacy_scope import fail_noncanonical_entrypoint


if __name__ == "__main__":
    fail_noncanonical_entrypoint("src/breakout_app.py")
