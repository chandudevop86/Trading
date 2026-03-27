 = Join-Path (Get-Location) 'src\backtest_engine.py'
 = [System.IO.File]::ReadAllText()

 = .IndexOf('def _simulate_trade_exit(')
 = .IndexOf('def _equity_curve_rows', )
if ( -lt 0 -or  -lt 0) { throw 'simulate function boundaries not found' }
 = @'
def _finalize_trade_exit(
    trade: _TradeLifecycle,
    *,
    exit_price: float,
    exit_time: pd.Timestamp,
    exit_reason: str,
    cfg: BacktestConfig,
) -> _TradeLifecycle:
    trade.exit_price = float(exit_price)
    trade.exit_time = pd.Timestamp(exit_time)
    trade.exit_reason = str(exit_reason)
    trade.status = 'closed'
    if trade.side == 'BUY':
        trade.gross_pnl = (trade.exit_price - trade.entry_price) * trade.quantity
    else:
        trade.gross_pnl = (trade.entry_price - trade.exit_price) * trade.quantity
    trade.trading_cost = _cost_for_trade(trade.entry_price, trade.exit_price, trade.quantity, cfg)
    trade.pnl = trade.gross_pnl - trade.trading_cost
    risk_per_unit = abs(trade.entry_price - trade.stop_loss)
    if risk_per_unit <= 0:
        trade.rr_achieved = 0.0
    else:
        rr = abs(trade.exit_price - trade.entry_price) / risk_per_unit
        if trade.pnl < 0:
            rr *= -1.0
        trade.rr_achieved = rr
    return trade


def _simulate_trade_exit(prepared: pd.DataFrame, trade: _TradeLifecycle, *, close_open_positions_at_end: bool, cfg: BacktestConfig) -> _TradeLifecycle:
    future = prepared[prepared['timestamp'] > trade.entry_time].copy()
    if future.empty:
        if close_open_positions_at_end:
            current_row = prepared[prepared['timestamp'] == trade.entry_time].tail(1)
            if not current_row.empty:
                last_row = current_row.iloc[-1]
                return _finalize_trade_exit(
                    trade,
                    exit_price=float(last_row['close']),
                    exit_time=pd.Timestamp(last_row['timestamp']),
                    exit_reason='END_OF_DATA',
                    cfg=cfg,
                )
        trade.status = 'invalid'
        trade.exit_reason = 'NO_FUTURE_DATA'
        return trade

    for row in future.itertuples(index=False):
        candle_time = pd.Timestamp(row.timestamp)
        if trade.side == 'BUY':
            if float(row.low) <= trade.stop_loss:
                return _finalize_trade_exit(trade, exit_price=trade.stop_loss, exit_time=candle_time, exit_reason='STOP_LOSS', cfg=cfg)
            if float(row.high) >= trade.target_price:
                return _finalize_trade_exit(trade, exit_price=trade.target_price, exit_time=candle_time, exit_reason='TARGET', cfg=cfg)
        else:
            if float(row.high) >= trade.stop_loss:
                return _finalize_trade_exit(trade, exit_price=trade.stop_loss, exit_time=candle_time, exit_reason='STOP_LOSS', cfg=cfg)
            if float(row.low) <= trade.target_price:
                return _finalize_trade_exit(trade, exit_price=trade.target_price, exit_time=candle_time, exit_reason='TARGET', cfg=cfg)

    if close_open_positions_at_end:
        last_row = future.iloc[-1]
        return _finalize_trade_exit(
            trade,
            exit_price=float(last_row['close']),
            exit_time=pd.Timestamp(last_row['timestamp']),
            exit_reason='END_OF_DATA',
            cfg=cfg,
        )

    trade.status = 'open'
    trade.exit_reason = 'OPEN_AT_END'
    return trade


'@
 = .Substring(0, ) +  + .Substring()
 = .Replace("    avg_r_winners = sum(_safe_float(trade.get('rr_achieved')) for trade in trades if _safe_float(trade.get('pnl')) > 0) / len(winning_trades) if winning_trades else 0.0
    avg_r_losers = sum(_safe_float(trade.get('rr_achieved')) for trade in trades if _safe_float(trade.get('pnl')) < 0) / len(losing_trades) if losing_trades else 0.0
    expectancy_r = ((wins / total_trades) * avg_r_winners) - ((losses / total_trades) * avg_r_losers) if total_trades else 0.0
", "    expectancy_r = sum(_safe_float(trade.get('rr_achieved')) for trade in trades) / total_trades if total_trades else 0.0
")
[System.IO.File]::WriteAllText(, )

 = Join-Path (Get-Location) 'tests\test_backtest_engine.py'
 = [System.IO.File]::ReadAllText()
 = @'
def _single_trade_strategy(entry_idx: int = 0):
    def _strategy(df, capital: float, risk_pct: float, rr_ratio: float, config=None):
        del capital, risk_pct, rr_ratio, config
        row = df.iloc[entry_idx]
        entry = float(row['close'])
        return [
            {
                'timestamp': row['timestamp'],
                'side': 'BUY',
                'entry': entry,
                'stop_loss': round(entry - 0.6, 4),
                'target': round(entry + 0.8, 4),
                'strategy': 'LIFECYCLE_TEST',
                'reason': 'single_trade',
                'score': 5.0,
                'quantity': 1,
            }
        ]
    return _strategy


'@
 = @'
def _single_trade_strategy(entry_idx: int = 0):
    def _strategy(df, capital: float, risk_pct: float, rr_ratio: float, config=None):
        del capital, risk_pct, rr_ratio, config
        row = df.iloc[entry_idx]
        entry = float(row['close'])
        return [
            {
                'timestamp': row['timestamp'],
                'side': 'BUY',
                'entry': entry,
                'stop_loss': round(entry - 0.6, 4),
                'target': round(entry + 0.8, 4),
                'strategy': 'LIFECYCLE_TEST',
                'reason': 'single_trade',
                'score': 5.0,
                'quantity': 1,
            }
        ]
    return _strategy


def _build_metric_validation_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {'timestamp': '2026-03-01 09:15:00', 'open': 100.0, 'high': 100.2, 'low': 99.8, 'close': 100.0, 'volume': 1000},
            {'timestamp': '2026-03-01 09:16:00', 'open': 100.0, 'high': 100.9, 'low': 99.9, 'close': 100.7, 'volume': 1001},
            {'timestamp': '2026-03-01 09:17:00', 'open': 101.0, 'high': 101.2, 'low': 100.8, 'close': 101.0, 'volume': 1002},
            {'timestamp': '2026-03-01 09:18:00', 'open': 101.0, 'high': 101.1, 'low': 100.3, 'close': 100.5, 'volume': 1003},
            {'timestamp': '2026-03-01 09:19:00', 'open': 102.0, 'high': 102.2, 'low': 101.8, 'close': 102.0, 'volume': 1004},
            {'timestamp': '2026-03-01 09:20:00', 'open': 102.0, 'high': 102.3, 'low': 101.8, 'close': 102.2, 'volume': 1005},
            {'timestamp': '2026-03-01 09:21:00', 'open': 102.2, 'high': 102.35, 'low': 101.9, 'close': 102.3, 'volume': 1006},
        ]
    )


def _metric_validation_strategy(df, capital: float, risk_pct: float, rr_ratio: float, config=None):
    del capital, risk_pct, rr_ratio, config
    trades: list[dict[str, object]] = []
    for idx in [0, 2, 4]:
        row = df.iloc[idx]
        entry = float(row['close'])
        trades.append(
            {
                'timestamp': row['timestamp'],
                'side': 'BUY',
                'entry': entry,
                'stop_loss': round(entry - 0.6, 4),
                'target': round(entry + 0.8, 4),
                'strategy': 'METRIC_VALIDATION',
                'reason': f'metric_trade_{idx}',
                'score': 7.0,
                'quantity': 10,
            }
        )
    return trades


'@
if (-not .Contains()) { throw 'test anchor not found' }
 = .Replace(, )
 = @'
    def test_summarize_trade_log_blocks_deployment_when_sample_too_small(self):
'@
 = @'
    def test_run_backtest_computes_expectancy_profit_factor_and_drawdown_from_closed_trades(self):
        df = _build_metric_validation_frame()
        with TemporaryDirectory() as td:
            trades_output = Path(td) / 'trades.csv'
            summary = run_backtest(
                df,
                _metric_validation_strategy,
                BacktestConfig(
                    capital=100.0,
                    risk_pct=0.01,
                    rr_ratio=2.0,
                    trades_output=trades_output,
                    summary_output=Path(td) / 'summary.csv',
                    validation_output=Path(td) / 'validation.csv',
                    strategy_name='METRIC_VALIDATION',
                    close_open_positions_at_end=True,
                ),
            )
            rows = pd.read_csv(trades_output).to_dict(orient='records')

        self.assertEqual(summary['closed_trades'], 3)
        self.assertAlmostEqual(float(summary['total_pnl']), 5.0, places=2)
        self.assertAlmostEqual(float(summary['expectancy_per_trade']), 1.67, places=2)
        self.assertAlmostEqual(float(summary['expectancy_r']), 0.28, places=2)
        self.assertAlmostEqual(float(summary['profit_factor']), 1.83, places=2)
        self.assertAlmostEqual(float(summary['max_drawdown']), 6.0, places=2)
        self.assertAlmostEqual(float(summary['max_drawdown_pct']), 5.56, places=2)
        self.assertEqual([row['exit_reason'] for row in rows], ['TARGET', 'STOP_LOSS', 'END_OF_DATA'])

    def test_summarize_trade_log_blocks_deployment_when_sample_too_small(self):
'@
if (-not .Contains()) { throw 'test method anchor not found' }
 = .Replace(, )
[System.IO.File]::WriteAllText(, )
