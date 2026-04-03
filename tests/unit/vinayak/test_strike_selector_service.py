from vinayak.api.services.strike_selector import attach_option_strikes, pick_option_strike


def test_pick_option_strike_supports_atm_and_otm() -> None:
    strike, option_type = pick_option_strike(spot_price=22341.0, side='BUY', step=50, moneyness='ATM', steps=0)
    assert strike == 22350
    assert option_type == 'CE'

    strike_otm, option_type_otm = pick_option_strike(spot_price=22341.0, side='SELL', step=50, moneyness='OTM', steps=1)
    assert strike_otm == 22300
    assert option_type_otm == 'PE'


def test_attach_option_strikes_annotates_actionable_rows() -> None:
    rows = attach_option_strikes([
        {'side': 'BUY', 'entry_price': 22341.0},
        {'side': 'SELL', 'entry_price': 22341.0},
        {'side': 'HOLD', 'entry_price': 22341.0},
    ], strike_step=50, moneyness='ATM', steps=0)

    assert rows[0]['option_strike'] == '22350CE'
    assert rows[1]['option_strike'] == '22350PE'
    assert 'option_strike' not in rows[2]
