"""Скачивание файла по чанкам с проверкой SHA-256."""

import asyncio
import os
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path

from p2pshare.core.connection_manager import Connection
from p2pshare.core.piece_picker import SequentialPicker
from p2pshare.core.verifier import Verifier
from p2pshare.engine import P2PError
from p2pshare.engine.events import Event, EventType
from p2pshare.storage.bitfield import Bitfield
from p2pshare.storage.manifest import Manifest
from p2pshare.storage.paths import download_lock
from p2pshare.storage.state_store import StateStore

Progress = Callable[[int, int], None]


async def finish_on_cancel(operation):
    """Дождаться фоновой операции перед закрытием .part."""
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
        work_dir = target.parent / ".p2pshare"
        with download_lock(work_dir):
            if os.path.lexists(target):
                raise FileExistsError(f"Файл уже существует: {target}")
            store = StateStore(work_dir / f"{self.manifest.file_id_hex}.part")
            verifier = Verifier()
            try:
                return await self._run(target, store, verifier, progress)
            finally:
                verifier.close()

    async def _run(
        self, target: Path, store: StateStore, verifier: Verifier, progress: Progress | None
    ) -> Path:
        part = store.part_path
        if part.is_symlink() or (part.exists() and not part.is_file()):
            raise ValueError(f"Некорректный временный файл: {part}")

        have = await self._restore(store, verifier)
        self.engine.open_download(
            self.short_id, part, self.manifest.size, self.manifest.chunk_size, have.to_bytes()
        )
        try:
            await self._receive(store, have, verifier, progress)
            if not await finish_on_cancel(verifier.verify_file(part, self.manifest)):
                raise P2PError("Итоговый SHA-256 файла не совпал")
        except BaseException:
            if self.pending:
                with suppress(P2PError):
                    self.engine.cancel_chunks(
                        self.connection.conn_id, self.short_id, list(self.pending)
                    )
            # Если диск недоступен, следующий запуск перепроверит .part.
            with suppress(OSError, P2PError):
                await self._checkpoint(store, have)
            raise
        else:
            await finish_on_cancel(asyncio.to_thread(self.engine.flush, self.short_id))
        finally:
            self.engine.close_file(self.short_id)

        # Обе операции атомарно отклоняют уже существующее итоговое имя.
        if os.name == "nt":
            part.rename(target)
        else:
            os.link(part, target)
            part.unlink()
        store.delete()
        return target

    async def _restore(self, store: StateStore, verifier: Verifier) -> Bitfield:
        if not store.part_path.exists():
            return Bitfield(self.manifest.chunk_count)

        state = await asyncio.to_thread(store.load)
        if (
            state is not None
            and state.file_id == self.manifest.file_id
            and state.part_size == self.manifest.size
            and store.matches_part(state)
        ):
            return state.have
        return await finish_on_cancel(verifier.recheck(store.part_path, self.manifest))

    async def _checkpoint(self, store: StateStore, have: Bitfield) -> None:
        def save() -> None:
            self.engine.flush(self.short_id)
            store.save(self.manifest, have, [self.connection.peer_id.hex()])

        await finish_on_cancel(asyncio.to_thread(save))

    async def _receive(
        self, store: StateStore, have: Bitfield, verifier: Verifier, progress: Progress | None
    ) -> None:
        picker = SequentialPicker()
        remote_have = Bitfield.full(self.manifest.chunk_count)
        done = sum(self.manifest.chunk_length(index) for index in have.indices())
        verified_since_save = 0
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
                if not await finish_on_cancel(
                    verifier.verify_chunk(store.part_path, self.manifest, index)
                ):
                    raise P2PError(f"SHA-256 чанка {index} не совпал")

                have.set(index)
                verified_since_save += 1
                done += self.manifest.chunk_length(index)

            if verified_since_save >= 30:
                await self._checkpoint(store, have)
                verified_since_save = 0
            if progress is not None:
                progress(done, self.manifest.size)