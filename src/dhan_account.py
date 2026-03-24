from __future__ import annotations

import argparse
import json

from src.dhan_auth import DhanAuthManager
from src.dhan_api import DhanClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Dhan HQ account data from the command line")
    parser.add_argument(
        "--resource",
        default="positions",
        choices=["positions", "order"],
        help="Which broker resource to fetch",
    )
    parser.add_argument("--order-id", default="", help="Required when --resource order")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    auth_config = DhanAuthManager.load_from_env()
    auth_status = DhanAuthManager.validate_startup(auth_config)
    if not auth_status.ok:
        raise SystemExit('; '.join(auth_status.issues))
    client = DhanClient.from_env()

    if args.resource == "positions":
        result = client.get_positions()
    else:
        if not args.order_id.strip():
            raise SystemExit("--order-id is required when --resource order")
        result = client.get_order_by_id(args.order_id)

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
