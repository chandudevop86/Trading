from __future__ import annotations

from abc import ABC, abstractmethod

from patch_validator.models import Finding, RuleContext


class Rule(ABC):
    @abstractmethod
    def evaluate(self, context: RuleContext) -> list[Finding]:
        raise NotImplementedError
