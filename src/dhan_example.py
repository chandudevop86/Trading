from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.dhan_api import DhanClient, build_order_request_from_candidate, load_security_map


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview or place a Dhan HQ order from the command line")
    parser.add_argument("--symbol", required=True, help="Underlying symbol, for example NIFTY")
    parser.add_argument("--side", required=True, choices=["BUY", "SELL"], help="Order side")
    parser.add_argument("--quantity", required=True, type=int, help="Order quantity")
    parser.add_argument("--security-map", type=Path, default=Path("data/dhan_security_map.csv"), help="CSV map with Dhan security IDs")
    parser.add_argument("--option-strike", default="", help="Contract or strike label, for example 27MAR24500CE")
    parser.add_argument("--option-type", default="", help="Optional option type, for example CE or PE")
    parser.add_argument("--strike-price", type=float, default=0.0, help="Optional numeric strike price")
    parser.add_argument("--price", type=float, default=0.0, help="Limit price when using LIMIT order type")
    parser.add_argument("--trigger-price", type=float, default=0.0, help="Trigger price when using STOP_LOSS order type")
    parser.add_argument("--order-type", default="MARKET", choices=["MARKET", "LIMIT", "STOP_LOSS"], help="Broker order type")
    parser.add_argument("--product-type", default="INTRADAY", help="Dhan product type")
    parser.add_argument("--trade-tag", default="CLI", help="Short strategy/tag label used in broker tag")
    parser.add_argument("--place-live", action="store_true", help="Actually send the order to Dhan instead of previewing payload only")
    return parser.parse_args()


def build_candidate(args: argparse.Namespace) -> dict[str, object]:
    candidate: dict[str, object] = {
        "strategy": args.trade_tag,
        "symbol": args.symbol,
        "side": args.side,
        "quantity": args.quantity,
        "order_type": args.order_type,
        "product_type": args.product_type,
    }
    if args.option_strike:
        candidate["option_strike"] = args.option_strike
    if args.option_type:
        candidate["option_type"] = args.option_type
    if args.strike_price > 0:
        candidate["strike_price"] = args.strike_price
    if args.price > 0:
        candidate["price"] = args.price
    if args.trigger_price > 0:
        candidate["trigger_price"] = args.trigger_price
    return candidate


def main() -> None:
    args = parse_args()
    security_map = load_security_map(args.security_map)
    client_id = DhanClient.from_env().client_id if args.place_live else "PREVIEW_CLIENT"
    candidate = build_candidate(args)
    order_request = build_order_request_from_candidate(
        candidate,
        client_id=client_id,
        security_map=security_map,
    )
    payload = order_request.to_payload()

    print("Preview payload:")
    print(json.dumps(payload, indent=2, sort_keys=True))

    if not args.place_live:
        print("\nPreview only. Re-run with --place-live to send this order to Dhan.")
        return

    client = DhanClient.from_env()
    result = client.place_order(order_request)
    print("\nBroker response:")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
