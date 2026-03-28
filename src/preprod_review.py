from __future__ import annotations

import argparse
import io
import json
import unittest
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


def _escape_pdf_text(text: str) -> str:
    return (
        text.replace('\\', r'\\\\')
        .replace('(', r'\(')
        .replace(')', r'\)')
        .replace('\r', ' ')
        .replace('\n', ' ')
    )


def write_text_pdf(path: Path, title: str, lines: list[str]) -> None:
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

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as handle:
        handle.write(b'%PDF-1.4\n')
        xref: list[int] = [0]
        for obj in objects:
            xref.append(handle.tell())
            handle.write(obj)
        xref_start = handle.tell()
        handle.write(b'xref\n0 ' + str(len(xref)).encode() + b'\n')
        handle.write(b'0000000000 65535 f \n')
        for offset in xref[1:]:
            handle.write(f'{offset:010d} 00000 n \n'.encode())
        handle.write(b'trailer<< /Size ' + str(len(xref)).encode() + b' /Root 1 0 R >>\n')
        handle.write(b'startxref\n' + str(xref_start).encode() + b'\n%%EOF\n')


@dataclass(slots=True)
class SuiteConfig:
    area: str
    pattern: str
    description: str


@dataclass(slots=True)
class SuiteResult:
    area: str
    pattern: str
    description: str
    status: str
    tests_run: int
    failures: int
    errors: int
    output: str


SUITES: list[SuiteConfig] = [
    SuiteConfig(
        area='Functional',
        pattern='test_compatibility_suite.py',
        description='Imports, preprocessing, end-to-end operator flow, and paper-broker safety.',
    ),
    SuiteConfig(
        area='Functional',
        pattern='test_trade_validation_service.py',
        description='Shared trade metrics, timestamp parsing, OHLCV normalization, and paper readiness.',
    ),
    SuiteConfig(
        area='Functional',
        pattern='test_execution_engine.py',
        description='Execution safety, duplicate prevention, daily limits, and live compatibility guards.',
    ),
    SuiteConfig(
        area='UAT',
        pattern='test_trading_runtime_service.py',
        description='Operator run and backtest actions through the runtime service.',
    ),
    SuiteConfig(
        area='UAT/UI',
        pattern='test_trading_page.py',
        description='Page-level operator flows, state cleanup, and safe failure rendering.',
    ),
    SuiteConfig(
        area='UI',
        pattern='test_trading_ui_service.py',
        description='UI runtime initialization, request building, and UI logging support.',
    ),
]


def _run_suite(tests_dir: Path, config: SuiteConfig, *, verbosity: int) -> SuiteResult:
    loader = unittest.TestLoader()
    suite = loader.discover(str(tests_dir), pattern=config.pattern)
    stream = io.StringIO()
    runner = unittest.TextTestRunner(stream=stream, verbosity=verbosity)
    result = runner.run(suite)
    failures = len(result.failures)
    errors = len(result.errors)
    status = 'PASS' if failures == 0 and errors == 0 else 'FAIL'
    return SuiteResult(
        area=config.area,
        pattern=config.pattern,
        description=config.description,
        status=status,
        tests_run=int(result.testsRun),
        failures=failures,
        errors=errors,
        output=stream.getvalue().strip(),
    )


def _overall_status(results: list[SuiteResult]) -> str:
    return 'PASS' if all(result.status == 'PASS' for result in results) else 'FAIL'


def _suggestions(results: list[SuiteResult]) -> list[str]:
    failing = [result for result in results if result.status != 'PASS']
    if not failing:
        return [
            'Add data/test_reports/latest_test_summary.json for quick CI-friendly health checks.',
            'Add one plain-English operator readiness file for paper-trading status.',
            'Show latest verification date and latest readiness result on the dashboard.',
            'Keep one rolling PDF review file such as data/reports/preprod_review_latest.pdf.',
        ]
    suggestions: list[str] = []
    for result in failing:
        suggestions.append(f'Review {result.pattern} and fix the failing checks in the {result.area} layer.')
    return suggestions


def _report_lines(results: list[SuiteResult], *, generated_at: str) -> list[str]:
    lines: list[str] = [
        f'Review generated: {generated_at}',
        '',
        f'Overall result: {_overall_status(results)}',
        'This review checks the current paper-trading surface only. It is not approval for live trading.',
        '',
        'Suite results:',
    ]
    for result in results:
        lines.append(
            f'{result.area}: {result.status} | {result.pattern} | tests={result.tests_run} | '
            f'failures={result.failures} | errors={result.errors}'
        )
        lines.append(f'Notes: {result.description}')
    lines.extend(['', 'Missing output suggestions:'])
    for item in _suggestions(results):
        lines.append(f'- {item}')
    failing = [result for result in results if result.status != 'PASS']
    if failing:
        lines.extend(['', 'Failing suite details:'])
        for result in failing:
            lines.append(f'[{result.pattern}]')
            lines.extend(result.output.splitlines()[:40])
    return lines


def _json_summary(results: list[SuiteResult], *, generated_at: str) -> dict[str, object]:
    return {
        'generated_at': generated_at,
        'overall_status': _overall_status(results),
        'suites': [asdict(result) for result in results],
        'missing_output_suggestions': _suggestions(results),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run a focused pre-production paper-trading review.')
    parser.add_argument('--report-dir', type=Path, default=Path('data/reports'))
    parser.add_argument('--tests-dir', type=Path, default=Path('tests'))
    parser.add_argument('--basename', default='preprod_review')
    parser.add_argument('--verbosity', type=int, default=2)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tests_dir = args.tests_dir
    results = [_run_suite(tests_dir, suite, verbosity=int(args.verbosity)) for suite in SUITES]

    report_dir = args.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)
    date_suffix = datetime.now().strftime('%Y-%m-%d')
    base = f'{args.basename}_{date_suffix}'
    txt_path = report_dir / f'{base}.txt'
    pdf_path = report_dir / f'{base}.pdf'
    json_path = report_dir / f'{base}.json'

    lines = _report_lines(results, generated_at=generated_at)
    txt_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    write_text_pdf(pdf_path, title='Pre-Production Paper-Trading Review', lines=lines)
    json_path.write_text(json.dumps(_json_summary(results, generated_at=generated_at), indent=2), encoding='utf-8')

    print(f'Overall: {_overall_status(results)}')
    for result in results:
        print(
            f'{result.area}: {result.status} | {result.pattern} | '
            f'tests={result.tests_run} failures={result.failures} errors={result.errors}'
        )
    print(f'Text report: {txt_path}')
    print(f'PDF report: {pdf_path}')
    print(f'JSON report: {json_path}')
    return 0 if _overall_status(results) == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
