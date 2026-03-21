from __future__ import annotations

import argparse
from pathlib import Path

from src.execution_engine import reconcile_live_positions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Reconcile live trading log against broker net positions')
    parser.add_argument('--live-log', type=Path, default=Path('data/live_trading_logs_all.csv'), help='Path to the live trading log CSV')
    parser.add_argument('--broker', default='DHAN', help='Broker name for status messages')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = reconcile_live_positions(args.live_log, broker_name=args.broker)
    print(f'Position rows: {len(rows)} | Log: {args.live_log}')
    for row in rows[:10]:
        print(
            f"{row.get('symbol', '-')}: expected={row.get('expected_net_qty', 0)} "
            f"broker={row.get('broker_net_qty', 0)} delta={row.get('qty_delta', 0)} "
            f"match={row.get('position_match', '-') }"
        )


if __name__ == '__main__':
    main()
