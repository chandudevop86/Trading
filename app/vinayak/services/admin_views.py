from __future__ import annotations

from typing import Protocol

from vinayak.domain.models import AuditEvent


class AuditReader(Protocol):
    def list_events(self) -> list[AuditEvent]:
        ...


class AdminViewService:
    def __init__(self, audit_reader: AuditReader | None = None) -> None:
        self.audit_reader = audit_reader

    def validation_view(self) -> dict[str, object]:
        events = self.audit_reader.list_events() if self.audit_reader is not None else []
        failures = [event for event in events if event.event_type.value == 'SIGNAL_VALIDATION_FAILED']
        return {
            'validation_failures': len(failures),
            'recent_failures': [event.model_dump(mode='json') for event in failures[-20:]],
        }

    def execution_view(self) -> dict[str, object]:
        events = self.audit_reader.list_events() if self.audit_reader is not None else []
        execution_events = [event for event in events if event.event_type.value in {'EXECUTION_REQUESTED', 'EXECUTION_REJECTED', 'EXECUTION_COMPLETED'}]
        return {
            'execution_events': len(execution_events),
            'recent_events': [event.model_dump(mode='json') for event in execution_events[-20:]],
        }

    def logs_view(self) -> dict[str, object]:
        events = self.audit_reader.list_events() if self.audit_reader is not None else []
        return {'audit_events': [event.model_dump(mode='json') for event in events[-50:]]}
