"""Запуск движка и доставка его событий прикладному коду."""

import asyncio
from pathlib import Path

from p2pshare.config import Config
from p2pshare.core.catalog import Catalog
from p2pshare.core.connection_manager import ConnectionManager
from p2pshare.core.control_protocol import ControlProtocol
from p2pshare.core.download_task import DownloadTask, Progress
from p2pshare.core.event_pump import pump_events
from p2pshare.engine import P2PError, create_engine
from p2pshare.engine.events import Event
from p2pshare.storage.paths import download_path


class AppCore:
    def __init__(self, config: Config, *, fake: bool | None = None) -> None:
        self.config = config
        self.engine = create_engine(config, fake=fake)
        self.connections = ConnectionManager(self.engine)
        self.catalog = Catalog(self.engine)
        self.control = ControlProtocol(self.engine, self.catalog)
        self._download: DownloadTask | None = None
        self.port = 0
        self._stopping = asyncio.Event()
        self._pump: asyncio.Task | None = None

    async def __aenter__(self):
        upload = self.config.upload_limit_bps
        download = self.config.download_limit_bps
        if not (0 <= upload < 2**63 and 0 <= download < 2**63):
            raise ValueError("Лимиты скорости должны быть неотрицательными числами байт/с")
        self.engine.set_rate_limit(upload, download)
        self.port = self.engine.start(self.config.tcp_port)
        self._pump = asyncio.create_task(
            pump_events(self.engine, self.handle_event, self._stopping)
        )
        return self

    async def download(
        self, host: str, port: int, file_id: bytes, progress: Progress | None = None
    ) -> Path:
        connection = await self.connections.connect(host, port)
        manifest = await self.control.get_manifest(connection.conn_id, file_id)
        target = download_path(self.config.download_dir, manifest.name)
        if self._download is not None:
            raise P2PError("Загрузка уже выполняется")
        if self.catalog.get(file_id) is not None:
            raise P2PError("Этот файл уже раздаётся в текущем экземпляре")

        task = DownloadTask(self.engine, connection, manifest)
        self._download = task
        try:
            return await task.run(target, progress)
        finally:
            self._download = None

    def handle_event(self, event: Event) -> None:
        self.connections.handle_event(event)
        self.control.handle_event(event)
        if self._download is not None:
            self._download.handle_event(event)

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self._stopping.set()
        try:
            await self._pump
        finally:
            await asyncio.to_thread(self.engine.stop)