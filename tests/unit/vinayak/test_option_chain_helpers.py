from vinayak.infrastructure.market_data.option_chain import build_metrics_map, extract_option_records, normalize_index_symbol


def test_normalize_index_symbol_maps_supported_indices() -> None:
    assert normalize_index_symbol("^NSEI") == "NIFTY"
    assert normalize_index_symbol("NIFTY BANK") == "BANKNIFTY"
    assert normalize_index_symbol("^MIDCPNIFTY") == "MIDCPNIFTY"


def test_extract_option_records_and_build_metrics_map() -> None:
    payload = {
        "records": {
            "data": [
                {
                    "strikePrice": 22500,
                    "expiryDate": "24-Apr-2026",
                    "underlyingValue": 22480.0,
                    "CE": {
                        "lastPrice": 120.5,
                        "openInterest": 12345,
                        "totalTradedVolume": 6789,
                        "impliedVolatility": 13.2,
                    },
                    "PE": {
                        "lastPrice": 98.4,
                        "openInterest": 22345,
                        "totalTradedVolume": 7789,
                        "impliedVolatility": 14.6,
                    },
                }
            ]
        }
    }

    records = extract_option_records(payload)
    metrics = build_metrics_map(records)

    assert len(records) == 2
    assert metrics[(22500, "CE")]["option_ltp"] == 120.5
    assert metrics[(22500, "PE")]["option_oi"] == 22345
    assert metrics[(22500, "PE")]["option_expiry"] == "24-Apr-2026"
