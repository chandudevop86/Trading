from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, Callable

from sqlalchemy.orm import Session

from vinayak.api.services.dashboard_summary import DashboardSummaryService
from vinayak.auth.service import UserAuthService
from vinayak.cache.redis_client import RedisCache


DEFAULT_PAPER_LOG_PATH = Path('app/vinayak/data/paper_trading_logs_all.csv')
DEFAULT_REPORTS_DIR = Path('app/vinayak/data/reports')
DEFAULT_LOGS = {
    'app_log': Path('logs/app.log'),
    'execution_log': Path('logs/execution.log'),
    'rejections_log': Path('logs/rejections.log'),
    'errors_log': Path('logs/errors.log'),
}
_FILE_CACHE_TTL_SECONDS = 2.0
_FILE_CACHE: dict[str, dict[str, Any]] = {}


class RoleViewService:
    def __init__(self, session: Session | None = None) -> None:
        self.session = session
        self.cache = RedisCache.from_env()

    def build_admin_dashboard(self) -> dict[str, Any]:
        summary = DashboardSummaryService(self.session).build_summary() if self.session is not None else {}
        latest_analysis = self.load_latest_analysis()
        latest_signal = self.build_user_signal()
        admin_debug = self.build_admin_debug(latest_analysis)
        return {
            'summary': summary,
            'latest_signal': latest_signal,
            'admin_debug': admin_debug,
            'latest_analysis': latest_analysis,
        }

    def build_user_home(self) -> dict[str, Any]:
        latest_signal = self.build_user_signal()
        history = self.load_trade_history(limit=25)
        return {
            'latest_signal': latest_signal,
            'history_count': len(history),
            'last_trade_time': str(history[0].get('executed_at_utc', history[0].get('signal_time', '-'))) if history else '-',
        }

    def build_user_trade_history(self) -> dict[str, Any]:
        history = self.load_trade_history(limit=50)
        return {
            'history': history,
            'total': len(history),
        }

    def build_validation_page(self) -> dict[str, Any]:
        latest_analysis = self.load_latest_analysis()
        validation_summary = dict(latest_analysis.get('validation_summary', {}) or {})
        signals = list(latest_analysis.get('signals', []) or [])
        history = self.load_trade_history(limit=1)
        last_signal_time = '-'
        if signals:
            last_signal = dict(signals[-1])
            last_signal_time = str(last_signal.get('timestamp', last_signal.get('signal_time', '-')) or '-')
        elif history:
            last_signal_time = str(history[0].get('signal_time', history[0].get('executed_at_utc', '-')) or '-')
        has_analysis_data = bool(validation_summary) or bool(signals)
        empty_state = None
        if not has_analysis_data:
            empty_state = {
                'title': 'No analysis run yet',
                'message': 'Run a fresh analysis from the workspace to populate validation metrics, rejection reasons, and readiness details.',
                'last_analysis_time': str(latest_analysis.get('generated_at', '-')) if latest_analysis else '-',
                'last_signal_count': int(latest_analysis.get('signal_count', 0) or 0) if latest_analysis else 0,
                'last_signal_time': last_signal_time,
                'why_not_ready': 'System status stays NOT_READY until at least one analysis cycle produces signals and validation output.',
            }
            validation_summary = {
                'system_status': 'NOT_READY',
                'reason': 'NO_ANALYSIS_RUN_YET',
            }
        return {
            'latest_signal': self.build_user_signal(),
            'admin_debug': self.build_admin_debug(latest_analysis),
            'validation_summary': validation_summary,
            'empty_state': empty_state,
        }

    def build_execution_page(self) -> dict[str, Any]:
        history = self.load_trade_history(limit=50)
        paper_summary = dict(self.load_latest_analysis().get('execution_summary', {}) or {})
        return {
            'history': history,
            'paper_summary': paper_summary,
            'latest_signal': self.build_user_signal(),
        }

    def build_logs_page(self) -> dict[str, Any]:
        return {'logs': self.load_logs()}

    def build_settings_page(self) -> dict[str, Any]:
        users = UserAuthService(self.session).list_users() if self.session is not None else []
        return {
            'settings': {
                'paper_log_path': str(DEFAULT_PAPER_LOG_PATH),
                'reports_dir': str(DEFAULT_REPORTS_DIR),
                'cache_configured': self.cache.is_configured(),
                'role_model': {
                    'admin_pages': ['Dashboard', 'Validation', 'Execution', 'Logs', 'Settings'],
                    'user_pages': ['Home', 'Live Signal', 'Trade History'],
                },
                'users': users,
            }
        }

    def build_user_signal(self) -> dict[str, Any]:
        analysis = self.load_latest_analysis()
        signal = dict(analysis.get('signals', [{}])[-1] if analysis.get('signals') else {})
        history = self.load_trade_history(limit=1)
        if not signal and history:
            signal = dict(history[0])
        side = str(signal.get('side', '') or '').upper()
        status = side if side in {'BUY', 'SELL'} else 'NO TRADE'
        entry_price = self._safe_float(signal.get('entry_price', signal.get('entry', signal.get('price', 0.0))))
        stop_loss = self._safe_float(signal.get('stop_loss', signal.get('sl', 0.0)))
        target_price = self._safe_float(signal.get('target_price', signal.get('target', 0.0)))
        rr_ratio = self._derive_rr(entry_price, stop_loss, target_price)
        confidence = round(self._safe_float(signal.get('validation_score', signal.get('score', 0.0))), 2)
        last_updated = str(signal.get('timestamp', signal.get('signal_time', analysis.get('generated_at', '-'))) or '-')
        message = str(signal.get('reason', signal.get('notes', 'No live trade signal available.')) or 'No live trade signal available.')
        return {
            'symbol': str(signal.get('symbol', analysis.get('symbol', '^NSEI')) or '^NSEI'),
            'status': status,
            'entry_price': round(entry_price, 2),
            'stop_loss': round(stop_loss, 2),
            'target_price': round(target_price, 2),
            'rr_ratio': rr_ratio,
            'confidence': confidence,
            'last_updated': last_updated,
            'message': message,
        }

    def build_admin_debug(self, analysis: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(analysis or self.load_latest_analysis())
        signals = list(payload.get('signals', []) or [])
        validation_summary = dict(payload.get('validation_summary', {}) or {})
        rejection_reasons = dict(validation_summary.get('top_rejection_reasons', {}) or {})
        latest_errors = list(validation_summary.get('warnings', []) or []) + list(validation_summary.get('pass_fail_reasons', []) or [])
        accepted = 0
        rejected = 0
        for row in signals:
            validation_status = str(row.get('validation_status', '') or '').upper()
            if validation_status == 'PASS':
                accepted += 1
            elif validation_status == 'FAIL':
                rejected += 1
        if accepted == 0 and rejected == 0:
            accepted = int(payload.get('signal_count', 0) or len(signals))
        return {
            'zones_detected': int(payload.get('signal_count', len(signals)) or len(signals)),
            'accepted_zones': accepted,
            'rejected_zones': rejected,
            'rejection_reasons': rejection_reasons,
            'validation_checks': validation_summary,
            'latest_errors': latest_errors[:10],
        }

    def load_trade_history(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._read_csv_rows(DEFAULT_PAPER_LOG_PATH)
        if not rows:
            return []
        sort_key = None
        for column in ('executed_at_utc', 'exit_time', 'entry_time', 'signal_time', 'timestamp'):
            if column in rows[0]:
                sort_key = column
                break
        if sort_key:
            rows = sorted(rows, key=lambda row: str(row.get(sort_key, '') or ''), reverse=True)
        return rows[:max(int(limit), 1)]

    def load_logs(self, limit: int = 50) -> dict[str, str]:
        return {name: self._tail_text(path, limit=limit) for name, path in DEFAULT_LOGS.items()}

    def load_latest_analysis(self) -> dict[str, Any]:
        cached = self.cache.get_json('vinayak:artifact:latest_live_analysis') if self.cache.is_configured() else None
        if isinstance(cached, dict) and cached:
            return cached
        report_files = sorted(DEFAULT_REPORTS_DIR.glob('*live_analysis_result.json'), key=lambda path: path.stat().st_mtime, reverse=True)
        latest_path = report_files[0] if report_files else None
        if latest_path is None:
            return {}
        payload = self._read_json_file(latest_path)
        return payload if isinstance(payload, dict) else {}

    def _cache_key(self, path: Path, kind: str) -> str:
        return f'{kind}:{path.as_posix()}'

    def _read_cached_file(self, path: Path, *, kind: str, loader: Callable[[Path], Any], empty_value: Any) -> Any:
        if not path.exists() or path.stat().st_size == 0:
            return empty_value
        stat = path.stat()
        key = self._cache_key(path, kind)
        now = time.monotonic()
        cached = _FILE_CACHE.get(key)
        signature = (stat.st_mtime_ns, stat.st_size)
        if cached and cached.get('signature') == signature and (now - float(cached.get('loaded_at', 0.0))) <= _FILE_CACHE_TTL_SECONDS:
            return cached.get('value', empty_value)
        try:
            value = loader(path)
        except Exception:
            value = empty_value
        _FILE_CACHE[key] = {
            'signature': signature,
            'loaded_at': now,
            'value': value,
        }
        return value

    def _read_csv_rows(self, path: Path) -> list[dict[str, Any]]:
        def loader(target: Path) -> list[dict[str, Any]]:
            with target.open('r', encoding='utf-8', newline='') as handle:
                return [dict(row) for row in csv.DictReader(handle) if row]

        return self._read_cached_file(path, kind='csv', loader=loader, empty_value=[])

    def _read_json_file(self, path: Path) -> Any:
        def loader(target: Path) -> Any:
            return json.loads(target.read_text(encoding='utf-8'))

        return self._read_cached_file(path, kind='json', loader=loader, empty_value={})

    def _tail_text(self, path: Path, limit: int = 50) -> str:
        if not path.exists() or path.stat().st_size == 0:
            return 'No log entries yet.'

        def loader(target: Path) -> str:
            rows = target.read_text(encoding='utf-8', errors='replace').splitlines()
            return '\n'.join(rows[-max(int(limit), 1):]) if rows else 'No log entries yet.'

        return self._read_cached_file(path, kind=f'log:{max(int(limit), 1)}', loader=loader, empty_value='No log entries yet.')

    def _safe_float(self, value: Any) -> float:
        try:
            if value is None or str(value).strip() == '':
                return 0.0
            return float(value)
        except Exception:
            return 0.0

    def _derive_rr(self, entry: float, stop: float, target: float) -> float:
        if entry <= 0 or stop <= 0 or target <= 0 or entry == stop:
            return 0.0
        risk = abs(entry - stop)
        reward = abs(target - entry)
        return round((reward / risk), 2) if risk > 0 else 0.0


__all__ = ['RoleViewService']
