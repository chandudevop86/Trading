import importlib


def test_execution_service_import_smoke() -> None:
    module = importlib.import_module('vinayak.execution.service')

    assert hasattr(module, 'ExecutionService')
    assert hasattr(module, 'ExecutionCreateCommand')



def test_reviewed_trade_service_import_smoke() -> None:
    module = importlib.import_module('vinayak.execution.reviewed_trade_service')

    assert hasattr(module, 'ReviewedTradeService')
    assert hasattr(module, 'ReviewedTradeCreateCommand')
    assert hasattr(module, 'ReviewedTradeStatusUpdateCommand')



def test_messaging_events_import_smoke() -> None:
    module = importlib.import_module('vinayak.messaging.events')

    assert module.EVENT_REVIEWED_TRADE_CREATED == 'trade.reviewed'
    assert module.EVENT_REVIEWED_TRADE_STATUS_UPDATED == 'reviewed_trade.status.updated'
    assert module.EVENT_NOTIFICATION_REQUESTED == 'notification.requested'
