from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src import auto_backtest
from src.csv_io import read_csv_rows
from src.execution_engine import live_trading_unlock_status
from src.telegram_notifier import build_trade_summary, send_telegram_document, send_telegram_message


def _escape_pdf_text(text: str) -> str:
    return (
        text.replace('\\', r'\\\\')
        .replace('(', r'\\(')
        .replace(')', r'\\)')
        .replace('\r', ' ')
        .replace('\n', ' ')
    )


def write_text_pdf(path: Path, title: str, lines: list[str]) -> None:
    # Minimal PDF generator (no external dependencies).
    y = 770
    content_lines: list[str] = [
        'BT',
        '/F1 12 Tf',
        f'72 {y} Td',
        f'({_escape_pdf_text(title)}) Tj',
    ]
    y -= 20
    for line in lines:
        if y < 60:
            break
        content_lines.append(f'0 -14 Td ({_escape_pdf_text(line)}) Tj')
        y -= 14
    content_lines.append('ET')
    stream = '\n'.join(content_lines).encode('latin-1', errors='replace')

    # Build PDF objects.
    objects: list[bytes] = []
    objects.append(b'1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n')
    objects.append(b'2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n')
    objects.append(
        b'3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] '
        b'/Resources<< /Font<< /F1 4 0 R >> >> /Contents 5 0 R >>endobj\n'
    )
    objects.append(b'4 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n')
    objects.append(
        b'5 0 obj<< /Length ' + str(len(stream)).encode() + b' >>stream\n' + stream + b'\nendstream\nendobj\n'
    )

    # Write with xref.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as f:
        f.write(b'%PDF-1.4\n')
        xref: list[int] = [0]
        for obj in objects:
            xref.append(f.tell())
            f.write(obj)
        xref_start = f.tell()
        f.write(b'xref\n0 ' + str(len(xref)).encode() + b'\n')
        f.write(b'0000000000 65535 f \n')
        for off in xref[1:]:
            f.write(f'{off:010d} 00000 n \n'.encode())
        f.write(b'trailer<< /Size ' + str(len(xref)).encode() + b' /Root 1 0 R >>\n')
        f.write(b'startxref\n' + str(xref_start).encode() + b'\n%%EOF\n')


def write_html_report(path: Path, title: str, summary_rows: list[dict[str, Any]], extra_lines: list[str]) -> None:
    rows_html = ''.join(
        f"<tr><td>{r.get('strategy','')}</td><td>{r.get('trades','')}</td><td>{r.get('wins','')}</td><td>{r.get('losses','')}</td><td>{r.get('win_rate_pct','')}</td><td>{r.get('total_pnl','')}</td></tr>"
        for r in summary_rows
    )
    extras = ''.join(f"<li>{line}</li>" for line in extra_lines)
    html = f"""<!doctype html>
<html><head><meta charset='utf-8'/><title>{title}</title>
<style>body{{font-family:Arial,sans-serif;margin:24px}} table{{border-collapse:collapse}} td,th{{border:1px solid #ddd;padding:6px 10px}}</style>
</head><body>
<h1>{title}</h1>
<h2>Backtest Summary</h2>
<table>
<tr><th>Strategy</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Win %</th><th>Total PnL</th></tr>
{rows_html}
</table>
<h2>Notes</h2>
<ul>{extras}</ul>
</body></html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding='utf-8')


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Auto-run: fetch data, backtest, execute, report, telegram')
    p.add_argument('--symbol', default='^NSEI')
    p.add_argument('--interval', default='5m')
    p.add_argument('--period', default='1d')
    p.add_argument('--capital', type=float, default=100000.0)
    p.add_argument('--risk-pct', type=float, default=1.0)
    p.add_argument('--rr-ratio', type=float, default=2.0)
    p.add_argument('--trailing-sl-pct', type=float, default=1.0)
    p.add_argument('--execution-symbol', default='NIFTY')
    p.add_argument('--execution-type', default='PAPER', choices=['PAPER', 'LIVE', 'NONE'])
    p.add_argument('--paper-log-output', type=Path, default=Path('data/paper_trading_logs_all.csv'))
    p.add_argument('--live-log-output', type=Path, default=Path('data/live_trading_logs_all.csv'))
    p.add_argument('--live-broker', default='DHAN', choices=['DHAN', 'NONE'])
    p.add_argument('--security-map', type=Path, default=Path('data/dhan_security_map.csv'))
    p.add_argument('--min-paper-days', type=int, default=30)
    p.add_argument('--report-dir', type=Path, default=Path('reports'))
    p.add_argument('--send-telegram', action='store_true')
    p.add_argument('--send-telegram-pdf', action='store_true')
    p.add_argument('--telegram-token', default='')
    p.add_argument('--telegram-chat-id', default='')
    return p.parse_args()


def main() -> None:
    args = parse_args()

    requested_execution = str(args.execution_type or 'PAPER').strip().upper()
    execution_type = requested_execution
    execution_note = ''

    if requested_execution == 'LIVE':
        execution_note = 'Live execution enabled for auto-run when Dhan credentials and security map are configured.'

    backtest_args = argparse.Namespace(
        symbol=args.symbol,
        interval=args.interval,
        period=args.period,
        capital=args.capital,
        risk_pct=float(args.risk_pct) / 100.0,
        rr_ratio=args.rr_ratio,
        trailing_sl_pct=float(args.trailing_sl_pct) / 100.0,
        pivot_window=2,
        entry_cutoff='11:30',
        execution_symbol=args.execution_symbol,
        data_output=Path('data/live_ohlcv.csv'),
        summary_output=Path('data/backtest_results_all.csv'),
        summary_history_output=Path('data/backtest_results_history.csv'),
        paper_log_output=args.paper_log_output,
        execution_type=execution_type,
        live_log_output=args.live_log_output,
        live_broker=args.live_broker,
        security_map=args.security_map,
    )

    out = auto_backtest.run(backtest_args)

    run_at = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    title = f"Intratrade Auto Run - {out['timeframe']}"

    executed_log_path = Path(str(out.get('executed_log_path') or args.paper_log_output))
    recent: list[dict[str, Any]] = []
    if executed_log_path.exists():
        recent = list(read_csv_rows(executed_log_path))[-20:]

    signal_summary = build_trade_summary(recent) if recent else 'No recent rows in execution log.'

    summary_lines = [
        f"{r.get('strategy')}: trades={r.get('trades')} pnl={r.get('total_pnl')} win%={r.get('win_rate_pct')}"
        for r in (out.get('summary_rows') or [])
    ]

    extra_lines = [
        f"Run at: {run_at}",
        f"Data points: {out.get('data_points')}",
        f"Data range: {out.get('data_start')} → {out.get('data_end')}",
        f"Execution requested: {requested_execution}",
        f"Execution used: {out.get('execution_type')}",
        f"Executed rows: {out.get('executed_rows_count')}",
        f"Log: {out.get('executed_log_path')}",
    ]

    if execution_note:
        extra_lines.append(execution_note)

    extra_lines.append(signal_summary.replace('\n', ' | '))
    extra_lines.extend(summary_lines)

    ts = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    html_path = args.report_dir / f"auto_run_{ts}.html"
    pdf_path = args.report_dir / f"auto_run_{ts}.pdf"

    write_html_report(html_path, title=title, summary_rows=out['summary_rows'], extra_lines=extra_lines)
    write_text_pdf(pdf_path, title=title, lines=extra_lines)

    if args.send_telegram or args.send_telegram_pdf:
        token = (args.telegram_token or '').strip()
        chat = (args.telegram_chat_id or '').strip()
        if not token or not chat:
            raise SystemExit('Telegram token/chat id required when --send-telegram or --send-telegram-pdf is set')

        if args.send_telegram:
            msg = title + "\n\n" + "\n".join(extra_lines[:20])
            send_telegram_message(token, chat, msg)

        if args.send_telegram_pdf:
            send_telegram_document(token, chat, str(pdf_path), caption=title)

    print(f"Wrote report: {html_path}")
    print(f"Wrote report: {pdf_path}")


if __name__ == '__main__':
    main()

