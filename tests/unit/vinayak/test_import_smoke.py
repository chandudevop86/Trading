import importlib
import inspect
from pathlib import Path

from vinayak.api.routes import executions as executions_route
from vinayak.execution import gateway as execution_gateway


def test_execution_service_import_smoke() -> None:
    module = importlib.import_module('vinayak.execution.facade')

    assert hasattr(module, 'ExecutionFacade')



def test_reviewed_trade_service_import_smoke() -> None:
    module = importlib.import_module('vinayak.execution.reviewed_trade_service')

    assert hasattr(module, 'ReviewedTradeService')
    assert hasattr(module, 'ReviewedTradeCreateCommand')
    assert hasattr(module, 'ReviewedTradeStatusUpdateCommand')



def test_messaging_events_import_smoke() -> None:
    module = importlib.import_module('vinayak.messaging.events')

    assert module.EVENT_TRADE_EXECUTED == 'trade.executed'
    assert module.EVENT_TRADE_EXECUTE_REQUESTED == 'trade.execute.requested'
    assert module.EVENT_REVIEWED_TRADE_CREATED == 'trade.reviewed'
    assert module.EVENT_REVIEWED_TRADE_STATUS_UPDATED == 'reviewed_trade.status.updated'
    assert module.EVENT_NOTIFICATION_REQUESTED == 'notification.requested'



def test_repo_has_no_stale_execution_event_imports() -> None:
    roots = [Path(r'F:/Trading/app'), Path(r'F:/Trading/src'), Path(r'F:/Trading/tests')]
    offenders: list[str] = []

    for root in roots:
        for path in root.rglob('*.py'):
            text = path.read_text(encoding='utf-8')
            stale_path = 'vinayak.' + 'execution.events'
            if stale_path in text and path.name != 'test_import_smoke.py':
                offenders.append(str(path))

    assert offenders == []



def test_execution_route_uses_execution_facade_boundary() -> None:
    source = inspect.getsource(executions_route.create_execution)

    assert '_execution_facade(db)' in source
    assert '.create_execution(' in source



def test_workspace_gateway_uses_execution_facade_execute_request() -> None:
    source = inspect.getsource(execution_gateway.execute_workspace_candidates)

    assert 'execution_facade = build_execution_facade(db_session)' in source
    assert 'execution_facade.execute_request(execution_request)' in source

