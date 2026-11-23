"""Состояние загрузки для докачки: ``<имя>.p2pstate.json`` рядом с ``<имя>.part``.

В файле лежат манифест, bitfield проверенных чанков и «отпечаток» ``.part``
(размер и время изменения). По отпечатку при старте понятно, можно ли доверять
bitfield; если нет — нужен recheck (перехэшировать ``.part``).

Порядок записи, который обязан соблюдать вызывающий код (бит никогда не попадает
на диск раньше данных)::

    чанк записан → SHA-256 совпал → have.set(i) → engine.flush(file_id) → store.save(...)

Запись атомарная: во временный файл, ``fsync``, затем ``os.replace``.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from p2pshare.storage.bitfield import Bitfield
from p2pshare.storage.manifest import Manifest, ManifestError

STATE_VERSION = 1
PART_SUFFIX = ".part"
STATE_SUFFIX = ".p2pstate.json"
MAX_PEERS = 64


def state_path_for(part_path: str | Path) -> Path:
    """``film.mkv.part`` -> ``film.mkv.p2pstate.json`` (в той же папке)."""
    part_path = Path(part_path)
    name = part_path.name
    if name.endswith(PART_SUFFIX):
        name = name[: -len(PART_SUFFIX)]
    return part_path.with_name(name + STATE_SUFFIX)


@dataclass
class DownloadState:
    manifest: Manifest
    have: Bitfield
    part_size: int
    part_mtime_ns: int
    peers: list[str] = field(default_factory=list)  # peer_id в hex
    updated_at: str = ""

    @property
    def file_id(self) -> bytes:
        return self.manifest.file_id


class StateStore:
    def __init__(self, part_path: str | Path) -> None:
        self.part_path = Path(part_path)
        self.path = state_path_for(self.part_path)
        self._tmp_path = self.path.with_name(self.path.name + ".tmp")

    # ------------------------------------------------------------------ запись

    def save(
        self,
        manifest: Manifest,
        have: Bitfield,
        peers: list[str] | tuple[str, ...] = (),
    ) -> None:
        """Атомарно сохранить состояние. ``flush`` данных в движке — на вызывающем."""
        if len(have) != manifest.chunk_count:
            raise ValueError("bitfield size does not match the manifest")
        st = self.part_path.stat()
        data = {
            "version": STATE_VERSION,
            "file_id": manifest.file_id_hex,
            "manifest": manifest.to_dict(),
            "have": base64.b64encode(have.to_bytes()).decode("ascii"),
            "part_size": st.st_size,
            "part_mtime_ns": st.st_mtime_ns,
            "peers": list(peers)[:MAX_PEERS],
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        text = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        with open(self._tmp_path, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(self._tmp_path, self.path)

    def delete(self) -> None:
        """Удалить состояние (после успешного завершения загрузки)."""
        for p in (self.path, self._tmp_path):
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    # ------------------------------------------------------------------ чтение

    def load(self) -> DownloadState | None:
        """Прочитать состояние. ``None`` — файла нет или он повреждён (нужен recheck)."""
        try:
            obj = json.loads(self.path.read_text(encoding="utf-8"))
            return _parse_state(obj)
        except (OSError, ValueError, KeyError, TypeError, ManifestError, binascii.Error):
            return None

    def matches_part(self, state: DownloadState) -> bool:
        """Совпадает ли ``.part`` с отпечатком из состояния (иначе — recheck)."""
        try:
            st = self.part_path.stat()
        except OSError:
            return False
        return st.st_size == state.part_size and st.st_mtime_ns == state.part_mtime_ns


# ---------------------------------------------------------------- разбор файла


def _parse_state(obj: Any) -> DownloadState:
    if not isinstance(obj, dict) or obj.get("version") != STATE_VERSION:
        raise ValueError("unsupported state file")
    manifest = Manifest.from_dict(obj["manifest"])
    if obj["file_id"] != manifest.file_id_hex:
        raise ValueError("file_id does not match the manifest")

    have = Bitfield.from_bytes(base64.b64decode(obj["have"], validate=True), manifest.chunk_count)

    part_size = _non_negative_int(obj["part_size"])
    part_mtime_ns = _non_negative_int(obj["part_mtime_ns"])

    raw_peers = obj.get("peers", [])
    if not isinstance(raw_peers, list) or not all(isinstance(p, str) for p in raw_peers):
        raise ValueError("bad peers list")
    updated_at = obj.get("updated_at", "")
    if not isinstance(updated_at, str):
        raise ValueError("bad updated_at")

    return DownloadState(
        manifest=manifest,
        have=have,
        part_size=part_size,
        part_mtime_ns=part_mtime_ns,
        peers=raw_peers[:MAX_PEERS],
        updated_at=updated_at,
    )


def _non_negative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("expected a non-negative integer")
    return value