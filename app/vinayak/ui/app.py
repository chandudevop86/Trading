from __future__ import annotations

import os
from decimal import Decimal

import streamlit as st

from vinayak.ui.api_client import VinayakApiClient


def _client() -> VinayakApiClient:
    return VinayakApiClient(base_url=os.getenv('VINAYAK_API_BASE_URL', 'http://127.0.0.1/api'))


def _execution_form() -> dict[str, object]:
    with st.form('execution_request_form'):
        signal_id = st.text_input('Signal ID')
        idempotency_key = st.text_input('Execution Idempotency Key')
        mode = st.selectbox('Mode', ['PAPER', 'LIVE'])
        account_id = st.text_input('Account ID', value='paper-default')
        strategy_name = st.text_input('Strategy Name', value='BREAKOUT')
        symbol = st.text_input('Execution Symbol', value='NIFTY')
        timeframe = st.selectbox('Execution Timeframe', ['1m', '5m', '15m', '30m', '1h', '1d'], index=1)
        side = st.selectbox('Side', ['BUY', 'SELL'])
        entry_price = st.number_input('Entry', min_value=0.01, value=100.0)
        stop_loss = st.number_input('Stop Loss', min_value=0.01, value=99.0)
        target_price = st.number_input('Target', min_value=0.01, value=102.0)
        quantity = st.number_input('Quantity', min_value=1.0, value=1.0)
        submitted = st.form_submit_button('Submit Execution Request')
    return {
        'submitted': submitted,
        'payload': {
            'idempotency_key': idempotency_key,
            'mode': mode,
            'account_id': account_id,
            'requested_at': '2026-01-01T09:20:00Z',
            'risk': {
                'risk_per_trade_pct': '1',
                'max_daily_loss_pct': '3',
                'max_trades_per_day': 5,
                'cooldown_minutes': 15,
                'allow_live_trading': mode == 'LIVE',
                'live_unlock_token_required': mode == 'LIVE',
            },
            'signal': {
                'signal_id': signal_id,
                'idempotency_key': idempotency_key,
                'strategy_name': strategy_name,
                'symbol': symbol,
                'timeframe': timeframe,
                'signal_type': 'ENTRY',
                'status': 'VALIDATED',
                'generated_at': '2026-01-01T09:15:00Z',
                'candle_timestamp': '2026-01-01T09:15:00Z',
                'side': side,
                'entry_price': str(Decimal(str(entry_price))),
                'stop_loss': str(Decimal(str(stop_loss))),
                'target_price': str(Decimal(str(target_price))),
                'quantity': str(Decimal(str(quantity))),
                'confidence': '0.8',
                'rationale': 'Streamlit UI execution request',
                'metadata': {'source': 'streamlit-ui'},
            },
        },
    }


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
    st.sidebar.header('Environment')
    st.sidebar.code(f"VINAYAK_API_BASE_URL={client.resolved_base_url}")
    show_admin = st.sidebar.toggle('Show Admin Views', value=False)
    if st.sidebar.button('Refresh API Snapshot'):
        st.session_state['vinayak_last_refresh'] = os.urandom(4).hex()

    tabs = ['Dashboard', 'Charts', 'Strategies', 'Settings']
    if show_admin:
        tabs.append('Admin')
    selected_tabs = st.tabs(tabs)

    with selected_tabs[0]:
        st.subheader('Health')
        st.json(client.health())
        st.subheader('Dashboard Summary')
        st.json(client.dashboard_summary())

    with selected_tabs[1]:
        st.info('Charts stay API-driven. Add a dedicated candle endpoint consumer here in the next slice.')

    with selected_tabs[2]:
        form = _signal_form()
        if form['submitted']:
            st.json(client.run_signals(form['payload']))
        execution_form = _execution_form()
        if execution_form['submitted']:
            st.json(client.request_execution(execution_form['payload']))

    with selected_tabs[3]:
        st.write('UI-only settings. No broker calls are made from Streamlit.')
        st.write('This app is intentionally stateless and delegates validation/execution to FastAPI.')

    if show_admin:
        with selected_tabs[4]:
            st.warning('Admin views consume protected API endpoints only.')
            st.subheader('Validation')
            st.json(client.admin_validation())
            st.subheader('Execution')
            st.json(client.admin_execution())
            st.subheader('Audit Logs')
            st.json(client.admin_logs())


if __name__ == '__main__':
    main()
