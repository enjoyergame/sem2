"""Запуск движка и доставка его событий прикладному коду."""

import asyncio

from p2pshare.config import Config
from p2pshare.core.connection_manager import ConnectionManager
from p2pshare.core.event_pump import pump_events
from p2pshare.engine import create_engine


class AppCore:
    def __init__(self, config: Config, *, fake: bool | None = None) -> None:
        self.config = config
        self.engine = create_engine(config, fake=fake)
        self.connections = ConnectionManager(self.engine)
        self.port = 0
        self._stopping = asyncio.Event()
        self._pump: asyncio.Task | None = None

    async def __aenter__(self):
        self.port = self.engine.start(self.config.tcp_port)
        self._pump = asyncio.create_task(
            pump_events(self.engine, self.connections.handle_event, self._stopping)
        )
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self._stopping.set()
        try:
            await self._pump
        finally:
            await asyncio.to_thread(self.engine.stop)