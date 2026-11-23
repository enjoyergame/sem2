"""Файлы, которые текущий экземпляр отдаёт пирам."""

import asyncio
from pathlib import Path

from p2pshare.storage.manifest import Manifest, build_manifest


class Catalog:
    def __init__(self, engine) -> None:
        self._engine = engine
        self._files: dict[bytes, Manifest] = {}

    async def share(self, path: Path) -> Manifest:
        path = path.resolve()
        if not path.is_file():
            raise ValueError(f"Файл не найден: {path}")

        manifest = await asyncio.to_thread(build_manifest, path)
        file_id = manifest.file_id
        self._engine.share_file(file_id[:16], path, manifest.size, manifest.chunk_size)
        self._files[file_id] = manifest
        return manifest

    def get(self, file_id: bytes) -> Manifest | None:
        return self._files.get(file_id)

    def items(self) -> list[dict]:
        return [
            {"file_id": file_id.hex(), "name": manifest.name, "size": manifest.size}
            for file_id, manifest in self._files.items()
        ]