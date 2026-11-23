"""Скачивание файла по чанкам с проверкой SHA-256."""

import asyncio
import os
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from tempfile import mkstemp

from p2pshare.core.connection_manager import Connection
from p2pshare.core.piece_picker import SequentialPicker
from p2pshare.core.verifier import Verifier
from p2pshare.engine import P2PError
from p2pshare.engine.events import Event, EventType
from p2pshare.storage.bitfield import Bitfield
from p2pshare.storage.manifest import Manifest

Progress = Callable[[int, int], None]


async def finish_on_cancel(operation):
    """Дождаться операции с .part перед закрытием файла и удалением."""
    task = asyncio.create_task(operation)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await asyncio.gather(task, return_exceptions=True)
        raise


class DownloadTask:
    def __init__(self, engine, connection: Connection, manifest: Manifest) -> None:
        self.engine = engine
        self.connection = connection
        self.manifest = manifest
        self.short_id = manifest.short_id
        self.events: asyncio.Queue[Event] = asyncio.Queue()
        self.pending: set[int] = set()

    def handle_event(self, event: Event) -> None:
        if event.conn_id != self.connection.conn_id:
            return
        if event.type == EventType.PEER_DISCONNECTED or (
            event.file_id == self.short_id
            and event.type in (EventType.CHUNK_RECEIVED, EventType.CHUNK_FAILED)
            and event.chunk_index in self.pending
        ):
            self.events.put_nowait(event)

    async def run(self, target: Path, progress: Progress | None = None) -> Path:
        if os.path.lexists(target):
            raise FileExistsError(f"Файл уже существует: {target}")

        fd, temp_name = mkstemp(prefix=".p2pshare-", suffix=".part", dir=target.parent)
        os.close(fd)
        part = Path(temp_name)
        verifier = Verifier()
        have = Bitfield(self.manifest.chunk_count)
        opened = False

        try:
            self.engine.open_download(
                self.short_id, part, self.manifest.size, self.manifest.chunk_size, have.to_bytes()
            )
            opened = True
            await self._receive(part, have, verifier, progress)
            if not await finish_on_cancel(verifier.verify_file(part, self.manifest)):
                raise P2PError("Итоговый SHA-256 файла не совпал")

            await finish_on_cancel(asyncio.to_thread(self.engine.flush, self.short_id))
            self.engine.close_file(self.short_id)
            opened = False

            # Обе операции атомарно отклоняют уже существующее итоговое имя.
            if os.name == "nt":
                part.rename(target)
            else:
                os.link(part, target)
            return target
        finally:
            verifier.close()
            if opened:
                if self.pending:
                    with suppress(P2PError):
                        self.engine.cancel_chunks(
                            self.connection.conn_id, self.short_id, list(self.pending)
                        )
                self.engine.close_file(self.short_id)
            part.unlink(missing_ok=True)

    async def _receive(
        self, part: Path, have: Bitfield, verifier: Verifier, progress: Progress | None
    ) -> None:
        picker = SequentialPicker()
        remote_have = Bitfield.full(self.manifest.chunk_count)
        done = 0
        if progress is not None:
            progress(done, self.manifest.size)

        while not have.is_complete():
            indices = picker.pick(self.connection.peer_id.hex(), remote_have, have, set(), 6)
            self.pending.update(indices)
            await finish_on_cancel(
                asyncio.to_thread(
                    self.engine.request_chunks, self.connection.conn_id, self.short_id, indices
                )
            )

            while self.pending:
                try:
                    event = await asyncio.wait_for(self.events.get(), timeout=40)
                except TimeoutError as exc:
                    raise P2PError("Пир перестал присылать чанки") from exc
                if event.type == EventType.PEER_DISCONNECTED:
                    raise P2PError("Соединение прервалось во время скачивания")

                index = event.chunk_index
                if index not in self.pending:
                    continue
                self.pending.remove(index)
                if event.type == EventType.CHUNK_FAILED:
                    raise P2PError(f"Не удалось скачать чанк {index}, код {event.code}")
                if not await finish_on_cancel(verifier.verify_chunk(part, self.manifest, index)):
                    raise P2PError(f"SHA-256 чанка {index} не совпал")

                have.set(index)
                done += self.manifest.chunk_length(index)

            if progress is not None:
                progress(done, self.manifest.size)