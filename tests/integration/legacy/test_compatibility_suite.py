import importlib
import inspect
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

sys.modules.setdefault('yfinance', types.SimpleNamespace())
sys.modules.setdefault('certifi', types.SimpleNamespace(where=lambda: ''))
sys.modules.setdefault(
    'streamlit',
    types.SimpleNamespace(
        set_page_config=lambda **kwargs: None,
        markdown=lambda *args, **kwargs: None,
        columns=lambda count: [],
        tabs=lambda labels: [],
        caption=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        success=lambda *args, **kwargs: None,
        info=lambda *args, **kwargs: None,
        dataframe=lambda *args, **kwargs: None,
        altair_chart=lambda *args, **kwargs: None,
        code=lambda *args, **kwargs: None,
        expander=lambda *args, **kwargs: None,
        session_state={},
    ),
)

from src.backtest_engine import BacktestConfig, BacktestValidationConfig, run_backtest
from src.breakout_bot import BreakoutConfig, _coerce_candles
from src.brokers.base import BrokerOrderRequest
from src.brokers.paper_broker import PaperBroker, PaperBrokerConfig
from src.demand_supply_bot import DemandSupplyConfig
from src.execution_engine import (
    build_execution_candidates,
    execute_paper_trades,
    summarize_execution_result,
)
from src.indicator_bot import IndicatorConfig
from src.preprocessing import REQUIRED_OHLCV_COLUMNS, prepare_trading_data
from src.strategy_service import StrategyContext, generate_strategy_rows
from src.trading_ui_service import build_request, initialize_ui_runtime
import src.Trading as trading_page


class _FakeColumn:
    def __init__(self, st):
        self._st = st

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def text_input(self, label, value=''):
        return self._st.text_input(label, value=value)

    def selectbox(self, label, options, index=0):
        return self._st.selectbox(label, options, index=index)

    def number_input(self, label, min_value=None, value=0.0, step=None):
        return self._st.number_input(label, min_value=min_value, value=value, step=step)

    def button(self, label, **kwargs):
        return self._st.button(label, **kwargs)

    def metric(self, *args, **kwargs):
        return self._st.metric(*args, **kwargs)

    def dataframe(self, data, **kwargs):
        return self._st.dataframe(data, **kwargs)

    def markdown(self, *args, **kwargs):
        return self._st.markdown(*args, **kwargs)

    def caption(self, *args, **kwargs):
        return self._st.caption(*args, **kwargs)

    def info(self, *args, **kwargs):
        return self._st.info(*args, **kwargs)

    def warning(self, *args, **kwargs):
        return self._st.warning(*args, **kwargs)

    def success(self, *args, **kwargs):
        return self._st.success(*args, **kwargs)

    def error(self, *args, **kwargs):
        return self._st.error(*args, **kwargs)

    def code(self, *args, **kwargs):
        return self._st.code(*args, **kwargs)

    def altair_chart(self, *args, **kwargs):
        return self._st.altair_chart(*args, **kwargs)


class _FakeStreamlit:
    def __init__(self, *, run_clicked=False, backtest_clicked=False):
        self.session_state = {}
        self._run_clicked = run_clicked
        self._backtest_clicked = backtest_clicked
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.successes: list[str] = []
        self.infos: list[str] = []
        self.captions: list[str] = []
        self.dataframes: list[object] = []
        self.codes: list[str] = []

    def set_page_config(self, **kwargs):
        return None

    def markdown(self, *args, **kwargs):
        return None

    def caption(self, message, *args, **kwargs):
        self.captions.append(str(message))

    def columns(self, count):
        return [_FakeColumn(self) for _ in range(count)]

    def tabs(self, labels):
        return [_FakeColumn(self) for _ in labels]

    def expander(self, label, **kwargs):
        return _FakeColumn(self)

    def text_input(self, label, value=''):
        return value

    def selectbox(self, label, options, index=0):
        return options[index]

    def number_input(self, label, min_value=None, value=0.0, step=None):
        return value

    def button(self, label, **kwargs):
        if label == 'Start Paper':
            return self._run_clicked
        if label == 'Run Backtest':
            return self._backtest_clicked
        return False

    def metric(self, *args, **kwargs):
        return None

    def error(self, message):
        self.errors.append(str(message))

    def warning(self, message):
        self.warnings.append(str(message))

    def success(self, message):
        self.successes.append(str(message))

    def info(self, message):
        self.infos.append(str(message))

    def dataframe(self, data, **kwargs):
        self.dataframes.append(data)

    def altair_chart(self, *args, **kwargs):
        return None

    def code(self, value, *args, **kwargs):
        self.codes.append(str(value))


def _build_raw_market_rows(total_rows: int = 40) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    base_time = pd.Timestamp('2026-03-10 09:15:00')
    for idx in range(total_rows):
        open_price = round(100.0 + idx * 0.2, 4)
        close_price = round(open_price + 0.05, 4)
        rows.append(
            {
                'Date': (base_time + pd.Timedelta(minutes=5 * idx)).strftime('%Y-%m-%d'),
                'Time': (base_time + pd.Timedelta(minutes=5 * idx)).strftime('%H:%M:%S'),
                'Open': open_price,
                'High': round(close_price + 0.15, 4),
                'Low': round(open_price - 0.15, 4),
                'Adj Close': close_price,
                'Vol': 1000 + idx,
            }
        )

    for entry_idx in (6, 14, 22):
        entry_close = float(rows[entry_idx]['Adj Close'])
        rows[entry_idx + 1]['High'] = round(entry_close + 1.0, 4)
        rows[entry_idx + 1]['Low'] = round(entry_close - 0.1, 4)
    return rows


def _compatibility_strategy(df, capital: float, risk_pct: float, rr_ratio: float = 2.0, config=None):
    del capital, risk_pct, rr_ratio, config
    trades: list[dict[str, object]] = []
    for trade_no, entry_idx in enumerate((6, 14, 22), start=1):
        row = df.iloc[entry_idx]
        entry = float(row['close'])
        trades.append(
            {
                'timestamp': row['timestamp'],
                'side': 'BUY',
                'entry': entry,
                'stop_loss': round(entry - 0.3, 4),
                'target': round(entry + 0.6, 4),
                'strategy': 'COMPATIBILITY_TEST',
                'reason': 'retest_vwap_zone',
                'score': 7.5,
                'quantity': 1,
                'trade_no': trade_no,
            }
        )
    return trades


class TestTradingCompatibilitySuite(unittest.TestCase):
    def test_import_smoke_for_core_operator_modules(self):
        module_names = [
            'src.preprocessing',
            'src.strategy_service',
            'src.execution_engine',
            'src.backtest_engine',
            'src.trading_ui_service',
            'src.Trading',
            'src.brokers.paper_broker',
        ]
        for module_name in module_names:
            with self.subTest(layer=module_name):
                module = importlib.import_module(module_name)
                self.assertIsNotNone(module)

    def test_strategy_signature_contracts_remain_compatible(self):
        cases = [
            (trading_page.generate_breakout_trades, BreakoutConfig),
            (trading_page.generate_demand_supply_trades, DemandSupplyConfig),
            (trading_page.generate_indicator_rows, None),
        ]
        for func, config_type in cases:
            with self.subTest(strategy=func.__name__):
                params = list(inspect.signature(func).parameters.values())
                self.assertGreaterEqual(len(params), 1)
                if config_type is not None:
                    self.assertGreaterEqual(len(params), 5)
                    self.assertEqual(params[1].name, 'capital')
                    self.assertEqual(params[2].name, 'risk_pct')
                    self.assertEqual(params[3].name, 'rr_ratio')
                    self.assertEqual(params[4].name, 'config')
                    self.assertIn(config_type.__name__, str(params[4].annotation))

    def test_end_to_end_operator_pipeline_from_raw_data_to_files(self):
        raw_rows = _build_raw_market_rows()

        with self.subTest(layer='preprocessing'):
            prepared = prepare_trading_data(raw_rows)
            self.assertEqual(list(prepared.columns), REQUIRED_OHLCV_COLUMNS)
            self.assertEqual(len(prepared), len(raw_rows))
            self.assertTrue(prepared['timestamp'].is_monotonic_increasing)

        with self.subTest(layer='strategy'):
            context = StrategyContext(
                strategy='Demand Supply (Retest)',
                candles=prepared,
                candle_rows=_coerce_candles(prepared),
                capital=100000.0,
                risk_pct=1.0,
                rr_ratio=2.0,
                trailing_sl_pct=0.0,
                symbol='NIFTY',
                mode='Balanced',
            )
            strategy_rows = generate_strategy_rows(context, demand_supply_generator=_compatibility_strategy)
            self.assertEqual(len(strategy_rows), 3)
            for row in strategy_rows:
                self.assertTrue({'timestamp', 'entry_time', 'signal_time', 'strategy', 'source_strategy', 'side', 'entry', 'entry_price', 'stop_loss', 'target', 'target_price', 'quantity', 'reason', 'score', 'contract_version'}.issubset(row.keys()))
                self.assertEqual(row['source_strategy'], 'Demand Supply (Retest)')
                self.assertEqual(row['symbol'], 'NIFTY')

        with TemporaryDirectory() as td:
            temp_dir = Path(td)

            with self.subTest(layer='execution'):
                candidates = build_execution_candidates('Demand Supply (Retest)', strategy_rows, 'NIFTY')
                self.assertEqual(len(candidates), 3)
                self.assertTrue({'strategy', 'symbol', 'signal_time', 'side', 'price', 'quantity', 'reason'}.issubset(candidates[0].keys()))
                self.assertEqual(candidates[0]['side'], 'BUY')
                paper_output = temp_dir / 'paper_trades.csv'
                execution_result = execute_paper_trades(candidates, paper_output, deduplicate=True, max_trades_per_day=3)
                execution_summary = summarize_execution_result(execution_result, deduplicate_enabled=True, execution_type='PAPER')
                self.assertEqual(execution_result.executed_count, 3)
                self.assertEqual(execution_result.error_count, 0)
                self.assertEqual(execution_summary['execution_type'], 'PAPER')
                self.assertEqual(execution_summary['duplicate_trade_count'], 0)
                self.assertTrue(paper_output.exists())
                executed_frame = pd.read_csv(paper_output)
                self.assertTrue((executed_frame['execution_type'] == 'PAPER').all())

            with self.subTest(layer='backtest'):
                trades_output = temp_dir / 'backtest_trades.csv'
                summary_output = temp_dir / 'backtest_summary.csv'
                validation_output = temp_dir / 'validation_summary.csv'
                summary = run_backtest(
                    prepared,
                    _compatibility_strategy,
                    BacktestConfig(
                        capital=100000.0,
                        risk_pct=0.01,
                        rr_ratio=2.0,
                        trades_output=trades_output,
                        summary_output=summary_output,
                        validation_output=validation_output,
                        strategy_name='COMPATIBILITY_TEST',
                        validation=BacktestValidationConfig(
                            min_trades=1,
                            target_trades=2,
                            max_trades=10,
                            min_profit_factor=0.1,
                            min_expectancy_per_trade=-1.0,
                            min_win_rate=0.0,
                            min_avg_rr=0.0,
                            max_drawdown_pct=100.0,
                            require_positive_expectancy=False,
                            max_expectancy_stability_gap_ratio=99.0,
                            require_second_half_positive_expectancy=False,
                            require_drawdown_proof=False,
                        ),
                    ),
                )
                self.assertEqual(summary['total_trades'], 3)
                self.assertTrue(trades_output.exists())
                self.assertTrue(summary_output.exists())
                self.assertTrue(validation_output.exists())
                self.assertGreater(trades_output.stat().st_size, 0)
                self.assertGreater(summary_output.stat().st_size, 0)
                self.assertGreater(validation_output.stat().st_size, 0)

            with self.subTest(layer='ui-file-init'):
                output_paths = [
                    temp_dir / 'data' / 'trades.csv',
                    temp_dir / 'data' / 'backtest_trades.csv',
                    temp_dir / 'data' / 'validation_summary.csv',
                ]
                log_paths = [
                    temp_dir / 'logs' / 'execution.log',
                    temp_dir / 'logs' / 'rejections.log',
                ]
                initialize_ui_runtime(output_paths, log_paths)
                for path in output_paths + log_paths:
                    self.assertTrue(path.exists(), f'missing runtime artifact: {path}')

            with self.subTest(layer='paper-request-safety'):
                request = build_request(
                    strategy='Demand Supply (Retest)',
                    symbol='NIFTY',
                    timeframe='5m',
                    capital=100000.0,
                    risk_pct=1.0,
                    rr_ratio=2.0,
                    mode='Balanced',
                    broker_choice='Paper',
                    run_clicked=True,
                    backtest_clicked=False,
                )
                self.assertEqual(request.broker_choice, 'Paper')
                self.assertTrue(request.run_requested)
                self.assertFalse(request.backtest_requested)

    def test_duplicate_prevention_and_schema_consistency(self):
        duplicate_rows = [
            {
                'strategy': 'BREAKOUT',
                'symbol': 'NIFTY',
                'signal_time': '2026-03-10 10:00:00',
                'timestamp': '2026-03-10 10:00:00',
                'side': 'BUY',
                'entry': 101.0,
                'entry_price': 101.0,
                'price': 101.0,
                'stop_loss': 100.5,
                'target_price': 102.0,
                'quantity': 65,
                'reason': 'duplicate_test',
                'score': 6.0,
            },
            {
                'strategy': 'BREAKOUT',
                'symbol': 'NIFTY',
                'signal_time': '2026-03-10 10:00:00',
                'timestamp': '2026-03-10 10:00:00',
                'side': 'BUY',
                'entry': 101.0,
                'entry_price': 101.0,
                'price': 101.0,
                'stop_loss': 100.5,
                'target_price': 102.0,
                'quantity': 65,
                'reason': 'duplicate_test',
                'score': 6.0,
            },
        ]
        with TemporaryDirectory() as td:
            output_path = Path(td) / 'paper_duplicates.csv'
            result = execute_paper_trades(duplicate_rows, output_path, deduplicate=True)

        self.assertEqual(result.executed_count, 1)
        self.assertEqual(result.skipped_count, 1)
        self.assertIn(result.skipped_rows[0]['rejection_reason'], {'DUPLICATE_BATCH_TRADE', 'DUPLICATE_SIGNAL_KEY', 'DUPLICATE_ACTIVE_TRADE', 'DUPLICATE_SIGNAL_COOLDOWN'})
        self.assertTrue({'trade_id', 'trade_key', 'duplicate_signal_key', 'signal_instance_key'}.issubset(result.executed_rows[0].keys()))

    def test_paper_broker_stays_local_and_safe(self):
        with TemporaryDirectory() as td:
            orders_path = Path(td) / 'order_history.csv'
            broker = PaperBroker(PaperBrokerConfig(orders_path=orders_path))
            result = broker.place_order(
                BrokerOrderRequest(
                    trade_id='compat-trade-1',
                    strategy='COMPATIBILITY_TEST',
                    symbol='NIFTY',
                    side='BUY',
                    quantity=65,
                    order_type='MARKET',
                    product_type='INTRADAY',
                    validity='DAY',
                    price=101.25,
                    execution_type='PAPER',
                )
            )
            orders = broker.get_orders()
            self.assertFalse(broker.live)
            self.assertTrue(result.accepted)
            self.assertEqual(result.broker_name, 'PAPER')
            self.assertTrue(orders_path.exists())
            self.assertEqual(len(orders), 1)
            self.assertEqual(orders[0]['broker_name'], 'PAPER')
            self.assertEqual(orders[0]['execution_type'], 'PAPER')

    def test_streamlit_page_starts_without_runtime_errors(self):
        fake_st = _FakeStreamlit(run_clicked=False, backtest_clicked=False)
        with patch.object(trading_page, 'st', fake_st):
            with patch.object(trading_page, '_ensure_output_files'):
                with patch.object(trading_page, '_minimal_theme'):
                    with patch.object(trading_page, '_render_summary_cards'):
                        with patch.object(trading_page, '_render_operator_panels'):
                            trading_page.main()

        self.assertEqual(fake_st.errors, [])
        self.assertGreaterEqual(len(fake_st.dataframes), 3)
        self.assertTrue(any('Latest actionable setups' in caption for caption in fake_st.captions))


if __name__ == '__main__':
    unittest.main(verbosity=2)







