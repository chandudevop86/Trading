from __future__ import annotations


class SymbolMapper:
    def __init__(self, aliases: dict[str, str] | None = None) -> None:
        self.aliases = {str(key).upper(): str(value).upper() for key, value in (aliases or {}).items()}

    def normalize(self, symbol: str) -> str:
        cleaned = str(symbol or '').strip().upper()
        return self.aliases.get(cleaned, cleaned)
