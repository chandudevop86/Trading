from __future__ import annotations

import importlib


def test_canonical_vinayak_modules_import_without_src() -> None:
    module_names = [
        'vinayak.domain.models',
        'vinayak.market_data.normalization',
        'vinayak.execution.events',
        'vinayak.execution.guard',
        'vinayak.execution.service',
        'vinayak.api.routes.production',
        'vinayak.ui.app',
    ]

    for module_name in module_names:
        module = importlib.import_module(module_name)
        assert module is not None
