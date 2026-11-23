"""Проверка содержимого: SHA-256 чанков, перепроверка ``.part`` и итоговый хэш файла.

Блокирующие функции (``check_chunk``, ``recheck_part``, ``hash_file``) работают в
пуле потоков через ``Verifier``; ``hashlib`` отпускает GIL на больших буферах, так что
интерфейс не подвисает. Читать ``.part`` параллельно с C++ ядром безопасно: ядро
открывает файл с общим доступом на чтение, запись и переименование.
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from p2pshare.storage.bitfield import Bitfield
from p2pshare.storage.manifest import Manifest

ProgressCallback = Callable[[int, int], None]  # (чанков проверено, всего чанков)

_READ_BLOCK = 4 * 1024 * 1024  # итоговый хэш считаем блоками, файл целиком в память не грузим


class RecheckCancelled(Exception):
    """Перепроверка прервана (пауза или выход из программы)."""


# ------------------------------------------------------- блокирующие функции


def read_chunk(part_path: str | Path, manifest: Manifest, index: int) -> bytes | None:
    """Прочитать чанк ``index`` из ``.part``; ``None`` — файл короче, чем нужно."""
    length = manifest.chunk_length(index)
    with open(part_path, "rb") as f:
        f.seek(manifest.chunk_offset(index))
        data = f.read(length)
    return data if len(data) == length else None


def check_chunk(part_path: str | Path, manifest: Manifest, index: int) -> bool:
    """Совпадает ли SHA-256 чанка в ``.part`` с манифестом."""
    try:
        data = read_chunk(part_path, manifest, index)
    except OSError:
        return False
    if data is None:
        return False
    return hashlib.sha256(data).digest() == manifest.chunk_sha256[index]


def recheck_part(
    part_path: str | Path,
    manifest: Manifest,
    progress: ProgressCallback | None = None,
    cancel: threading.Event | None = None,
) -> Bitfield:
    """Перехэшировать все чанки ``.part`` и восстановить bitfield.

    Медленно (читает весь файл), но докачка не теряется, если состояние пропало.
    """
    have = Bitfield(manifest.chunk_count)
    total = manifest.chunk_count
    try:
        f = open(part_path, "rb")
    except OSError:
        return have  # файла нет — ничего не скачано
    with f:
        for index in range(total):
            if cancel is not None and cancel.is_set():
                raise RecheckCancelled
            length = manifest.chunk_length(index)
            f.seek(manifest.chunk_offset(index))
            data = f.read(length)
            if len(data) == length and hashlib.sha256(data).digest() == manifest.chunk_sha256[index]:
                have.set(index)
            if progress is not None:
                progress(index + 1, total)
    return have


def hash_file(path: str | Path, cancel: threading.Event | None = None) -> bytes:
    """SHA-256 всего файла блоками."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(_READ_BLOCK):
            if cancel is not None and cancel.is_set():
                raise RecheckCancelled
            h.update(block)
    return h.digest()


def verify_file(part_path: str | Path, manifest: Manifest) -> bool:
    """Итоговая проверка: размер и SHA-256 всего файла совпадают с манифестом."""
    try:
        if Path(part_path).stat().st_size != manifest.size:
            return False
        return hash_file(part_path) == manifest.file_sha256
    except OSError:
        return False


# ------------------------------------------------------------- async-обёртка


class Verifier:
    """Выполняет проверки в пуле потоков, не блокируя event loop."""

    def __init__(self, max_workers: int = 2) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="verify")

    async def verify_chunk(self, part_path: str | Path, manifest: Manifest, index: int) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, check_chunk, part_path, manifest, index)

    async def recheck(
        self,
        part_path: str | Path,
        manifest: Manifest,
        progress: ProgressCallback | None = None,
        cancel: threading.Event | None = None,
    ) -> Bitfield:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._pool, recheck_part, part_path, manifest, progress, cancel
        )

    async def verify_file(self, part_path: str | Path, manifest: Manifest) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, verify_file, part_path, manifest)

    def close(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)