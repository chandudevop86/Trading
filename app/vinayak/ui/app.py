from __future__ import annotations

import os
from decimal import Decimal

import streamlit as st

from vinayak.ui.api_client import VinayakApiClient


def _client() -> VinayakApiClient:
    return VinayakApiClient(base_url=os.getenv('VINAYAK_API_BASE_URL', 'http://127.0.0.1:8000'))


def _signal_form() -> dict[str, object]:
    with st.form('signal_run_form'):
        symbol = st.text_input('Symbol', value='NIFTY')
        timeframe = st.selectbox('Timeframe', ['1m', '5m', '15m', '30m', '1h', '1d'], index=1)
        lookback = st.number_input('Lookback', min_value=20, max_value=1000, value=200)
        strategy = st.selectbox('Strategy', ['BREAKOUT', 'DEMAND_SUPPLY', 'CONFIRMATION'])
        risk_per_trade = st.number_input('Risk % per trade', min_value=0.1, max_value=5.0, value=1.0)
        max_daily_loss = st.number_input('Max daily loss %', min_value=0.1, max_value=20.0, value=3.0)
        max_trades = st.number_input('Max trades/day', min_value=1, max_value=100, value=5)
        cooldown = st.number_input('Cooldown minutes', min_value=0, max_value=1440, value=15)
        submitted = st.form_submit_button('Run Strategy')
    return {
        'submitted': submitted,
        'payload': {
            'symbol': symbol,
            'timeframe': timeframe,
            'lookback': int(lookback),
            'strategy': strategy,
            'risk_per_trade_pct': str(Decimal(str(risk_per_trade))),
            'max_daily_loss_pct': str(Decimal(str(max_daily_loss))),
            'max_trades_per_day': int(max_trades),
            'cooldown_minutes': int(cooldown),
        },
    }


def main() -> None:
    st.set_page_config(page_title='Vinayak UI', layout='wide')
    st.title('Vinayak Operator Console')
    client = _client()

    tab_dashboard, tab_charts, tab_strategies, tab_settings, tab_admin = st.tabs(
        ['Dashboard', 'Charts', 'Strategies', 'Settings', 'Admin']
    )

    with tab_dashboard:
        st.subheader('Health')
        st.json(client.health())
        st.subheader('Dashboard Summary')
        st.json(client.dashboard_summary())

    with tab_charts:
        st.info('Charts stay API-driven. Add a dedicated candle endpoint consumer here in the next slice.')

    with tab_strategies:
        form = _signal_form()
        if form['submitted']:
            st.json(client.run_signals(form['payload']))

    with tab_settings:
        st.write('UI-only settings. No broker calls are made from Streamlit.')
        st.code(f"VINAYAK_API_BASE_URL={client.base_url}")

    with tab_admin:
        st.subheader('Validation')
        st.json(client.admin_validation())
        st.subheader('Execution')
        st.json(client.admin_execution())
        st.subheader('Audit Logs')
        st.json(client.admin_logs())


if __name__ == '__main__':
    main()
