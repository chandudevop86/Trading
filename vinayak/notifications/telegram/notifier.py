class TelegramNotifier:
    def __init__(self, token: str | None, chat_id: str | None) -> None:
        self.token = token
        self.chat_id = chat_id

    def is_ready(self) -> bool:
        return bool(self.token and self.chat_id)
