from __future__ import annotations

import argparse
from pathlib import Path

from src.execution_engine import reconcile_live_trades


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Reconcile live trading log rows with broker order status')
    parser.add_argument('--live-log', type=Path, default=Path('data/live_trading_logs_all.csv'), help='Path to the live trading log CSV')
    parser.add_argument('--broker', default='DHAN', help='Broker name for status messages')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = reconcile_live_trades(args.live_log, broker_name=args.broker)
    print(f'Reconciled rows: {len(rows)} | Log: {args.live_log}')
    for row in rows[:10]:
        print(
            f"{row.get('strategy', '-')}: order={row.get('broker_order_id', '-') } "
            f"status={row.get('broker_status', '-') } execution={row.get('execution_status', '-') }"
        )


if __name__ == '__main__':
    main()
