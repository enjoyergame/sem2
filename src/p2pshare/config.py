"""Настройки приложения: постоянный ``peer_id``, папки, порты.

``peer_id`` (16 байт, UUID4) создаётся при первом запуске и хранится в
``<data_dir>/config.json``. Пира узнают по нему, а не по IP, поэтому он не должен
меняться между запусками. Чтобы запустить два экземпляра на одном ПК, нужны разные
``--data-dir`` (и разные ``--port``): тогда у каждого свой ``peer_id``.
"""

from __future__ import annotations

import json
import os
import socket
import uuid
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_TCP_PORT = 47010
DEFAULT_DISCOVERY_PORT = 47011
CONFIG_FILENAME = "config.json"


def default_data_dir() -> Path:
    """``%APPDATA%\\p2pshare`` на Windows, ``~/.config/p2pshare`` в остальных случаях."""
    base = os.environ.get("APPDATA")
    root = Path(base) if base else Path.home() / ".config"
    return root / "p2pshare"


@dataclass(slots=True)
class Config:
    data_dir: Path
    peer_id: bytes  # 16 байт
    name: str
    tcp_port: int = DEFAULT_TCP_PORT
    discovery_port: int = DEFAULT_DISCOVERY_PORT
    download_dir: Path = field(default=Path())
    upload_limit_bps: int = 0  # 0 = без лимита
    download_limit_bps: int = 0

    @property
    def peer_id_hex(self) -> str:
        return self.peer_id.hex()

    @property
    def config_path(self) -> Path:
        return self.data_dir / CONFIG_FILENAME

    @classmethod
    def load(
        cls,
        data_dir: str | Path | None = None,
        *,
        port: int | None = None,
        name: str | None = None,
        download_dir: str | Path | None = None,
    ) -> Config:
        """Прочитать конфиг из ``data_dir`` (или создать при первом запуске)."""
        data_dir = Path(data_dir) if data_dir is not None else default_data_dir()
        data_dir.mkdir(parents=True, exist_ok=True)

        stored = _read_stored(data_dir / CONFIG_FILENAME)
        peer_id = _parse_peer_id(stored.get("peer_id"))
        created = peer_id is None
        if peer_id is None:
            peer_id = uuid.uuid4().bytes

        cfg = cls(
            data_dir=data_dir,
            peer_id=peer_id,
            name=name or _str_or(stored.get("name")) or socket.gethostname(),
            tcp_port=port if port is not None else _int_or(stored.get("tcp_port"), DEFAULT_TCP_PORT),
            discovery_port=_int_or(stored.get("discovery_port"), DEFAULT_DISCOVERY_PORT),
            download_dir=Path(download_dir)
            if download_dir is not None
            else Path(_str_or(stored.get("download_dir")) or data_dir / "downloads"),
            upload_limit_bps=_int_or(stored.get("upload_limit_bps"), 0),
            download_limit_bps=_int_or(stored.get("download_limit_bps"), 0),
        )
        cfg.download_dir.mkdir(parents=True, exist_ok=True)
        if created:
            cfg.save()  # peer_id должен пережить перезапуск
        return cfg

    def save(self) -> None:
        """Атомарная запись: во временный файл, затем ``os.replace``."""
        data = {
            "peer_id": self.peer_id_hex,
            "name": self.name,
            "tcp_port": self.tcp_port,
            "discovery_port": self.discovery_port,
            "download_dir": str(self.download_dir),
            "upload_limit_bps": self.upload_limit_bps,
            "download_limit_bps": self.download_limit_bps,
        }
        self.data_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.config_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.config_path)


# ---------------------------------------------------------------- вспомогательное


def _read_stored(path: Path) -> dict:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return obj if isinstance(obj, dict) else {}


def _parse_peer_id(value: object) -> bytes | None:
    if not isinstance(value, str):
        return None
    try:
        raw = bytes.fromhex(value)
    except ValueError:
        return None
    return raw if len(raw) == 16 else None


def _int_or(value: object, default: int) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def _str_or(value: object) -> str | None:
    return value if isinstance(value, str) and value else None