"""Манифест файла: размер чанка, SHA-256 чанков и всего файла, ``file_id``.

``file_id`` = SHA-256 от канонического JSON поля ``info`` (без имени файла), поэтому
он идентифицирует *содержимое*: один и тот же файл у разных пиров даёт один ``file_id``.
В заголовке кадра протокола передаются первые 16 байт (``short_id``).

Манифест приходит от чужого пира, поэтому ``Manifest.from_dict`` всё проверяет и
бросает ``ManifestError`` на любые странности. Санитайзинг имени для записи на диск
делается отдельно в ``storage/paths.py``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MANIFEST_VERSION = 1

MIN_CHUNK_SIZE = 256 * 1024  # 256 КиБ
MAX_CHUNK_SIZE = 8 * 1024 * 1024  # 8 МиБ — потолок для автоподбора
HARD_MAX_CHUNK_SIZE = 16 * 1024 * 1024  # = MAX_PAYLOAD протокола; больше принимать нельзя
TARGET_CHUNK_COUNT = 4096

MAX_FILE_SIZE = 1 << 40  # 1 ТиБ — защита от заведомо нелепых манифестов
MAX_CHUNKS = 1_000_000  # защита памяти: 1 млн хэшей ≈ 32 МБ
MAX_NAME_LENGTH = 255

SHORT_ID_LENGTH = 16  # байт file_id в заголовке кадра

_HEX64 = re.compile(r"^[0-9a-f]{64}$")

ProgressCallback = Callable[[int, int], None]  # (байт обработано, всего байт)


class ManifestError(ValueError):
    """Манифест некорректен или не совпадает с файлом."""


# ---------------------------------------------------------------- размер чанка


def _next_pow2(n: int) -> int:
    return 1 if n <= 1 else 1 << (n - 1).bit_length()


def choose_chunk_size(file_size: int) -> int:
    """``max(256 КиБ, next_pow2(size / 4096))``, но не больше 8 МиБ."""
    if file_size < 0:
        raise ValueError("file_size must be >= 0")
    per_chunk = -(-file_size // TARGET_CHUNK_COUNT)  # потолочное деление
    return min(MAX_CHUNK_SIZE, max(MIN_CHUNK_SIZE, _next_pow2(per_chunk)))


def chunk_count_for(size: int, chunk_size: int) -> int:
    return -(-size // chunk_size)


# -------------------------------------------------------------------- манифест


@dataclass(frozen=True, slots=True)
class Manifest:
    name: str
    size: int
    chunk_size: int
    chunk_sha256: tuple[bytes, ...]
    file_sha256: bytes

    # --- производные величины

    @property
    def chunk_count(self) -> int:
        return len(self.chunk_sha256)

    def chunk_length(self, index: int) -> int:
        """Длина чанка с номером ``index`` (последний может быть короче)."""
        if not 0 <= index < self.chunk_count:
            raise IndexError(f"chunk index {index} out of range 0..{self.chunk_count - 1}")
        if index == self.chunk_count - 1:
            return self.size - index * self.chunk_size
        return self.chunk_size

    def chunk_offset(self, index: int) -> int:
        if not 0 <= index < self.chunk_count:
            raise IndexError(f"chunk index {index} out of range 0..{self.chunk_count - 1}")
        return index * self.chunk_size

    # --- идентификатор содержимого

    def info_dict(self) -> dict[str, Any]:
        return {
            "size": self.size,
            "chunk_size": self.chunk_size,
            "chunk_sha256": [h.hex() for h in self.chunk_sha256],
            "file_sha256": self.file_sha256.hex(),
        }

    def canonical_info(self) -> bytes:
        """Канонический JSON поля ``info``: ключи по алфавиту, без пробелов, ASCII."""
        return json.dumps(
            self.info_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")

    @property
    def file_id(self) -> bytes:
        return hashlib.sha256(self.canonical_info()).digest()

    @property
    def file_id_hex(self) -> str:
        return self.file_id.hex()

    @property
    def short_id(self) -> bytes:
        """Первые 16 байт ``file_id`` — то, что идёт в заголовке кадра."""
        return self.file_id[:SHORT_ID_LENGTH]

    # --- сериализация

    def to_dict(self) -> dict[str, Any]:
        return {"version": MANIFEST_VERSION, "name": self.name, "info": self.info_dict()}

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )

    @classmethod
    def from_json(cls, data: bytes | str, *, expected_file_id: bytes | None = None) -> Manifest:
        try:
            obj = json.loads(data)
        except (ValueError, UnicodeDecodeError) as exc:
            raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
        return cls.from_dict(obj, expected_file_id=expected_file_id)

    @classmethod
    def from_dict(cls, obj: Any, *, expected_file_id: bytes | None = None) -> Manifest:
        """Разобрать и проверить манифест, полученный от пира.

        ``expected_file_id`` — полный (32 байта) или короткий (16 байт) ``file_id``,
        под которым мы манифест запрашивали; при несовпадении — ``ManifestError``.
        """
        if not isinstance(obj, dict):
            raise ManifestError("manifest must be an object")
        if obj.get("version") != MANIFEST_VERSION or isinstance(obj.get("version"), bool):
            raise ManifestError(f"unsupported manifest version: {obj.get('version')!r}")

        name = obj.get("name")
        if not isinstance(name, str) or not name or len(name) > MAX_NAME_LENGTH or "\0" in name:
            raise ManifestError("bad file name")

        info = obj.get("info")
        if not isinstance(info, dict):
            raise ManifestError("missing 'info'")

        size = _get_int(info, "size")
        chunk_size = _get_int(info, "chunk_size")
        if not 0 <= size <= MAX_FILE_SIZE:
            raise ManifestError(f"bad size: {size}")
        if not 1 <= chunk_size <= HARD_MAX_CHUNK_SIZE:
            raise ManifestError(f"bad chunk_size: {chunk_size}")

        expected_chunks = chunk_count_for(size, chunk_size)
        if expected_chunks > MAX_CHUNKS:
            raise ManifestError(f"too many chunks: {expected_chunks}")

        raw_hashes = info.get("chunk_sha256")
        if not isinstance(raw_hashes, list):
            raise ManifestError("chunk_sha256 must be a list")
        if len(raw_hashes) != expected_chunks:
            raise ManifestError(
                f"chunk_sha256 has {len(raw_hashes)} items, expected {expected_chunks}"
            )
        chunk_hashes = tuple(_parse_hash(h) for h in raw_hashes)
        file_sha = _parse_hash(info.get("file_sha256"))

        manifest = cls(
            name=name,
            size=size,
            chunk_size=chunk_size,
            chunk_sha256=chunk_hashes,
            file_sha256=file_sha,
        )

        if expected_file_id is not None:
            actual = manifest.file_id
            if len(expected_file_id) not in (SHORT_ID_LENGTH, len(actual)) or not actual.startswith(
                expected_file_id
            ):
                raise ManifestError("file_id mismatch: manifest does not match the request")
        return manifest


def _get_int(info: dict[str, Any], key: str) -> int:
    value = info.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ManifestError(f"'{key}' must be an integer")
    return value


def _parse_hash(value: Any) -> bytes:
    if not isinstance(value, str) or not _HEX64.match(value):
        raise ManifestError("hash must be 64 lowercase hex characters")
    return bytes.fromhex(value)


# ------------------------------------------------------------------- построение


def build_manifest(
    path: str | Path,
    *,
    chunk_size: int | None = None,
    name: str | None = None,
    progress: ProgressCallback | None = None,
) -> Manifest:
    """Прочитать файл один раз и посчитать SHA-256 каждого чанка и всего файла.

    Функция блокирующая; из asyncio вызывать через ``asyncio.to_thread``.
    ``hashlib`` отпускает GIL на больших буферах, так что UI не подвисает.
    """
    path = Path(path)
    size = path.stat().st_size
    if size > MAX_FILE_SIZE:
        raise ManifestError(f"file is too large: {size} bytes")
    if chunk_size is None:
        chunk_size = choose_chunk_size(size)
    if not 1 <= chunk_size <= HARD_MAX_CHUNK_SIZE:
        raise ValueError(f"chunk_size must be in 1..{HARD_MAX_CHUNK_SIZE}")

    file_hash = hashlib.sha256()
    chunk_hashes: list[bytes] = []
    done = 0
    with path.open("rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            chunk_hashes.append(hashlib.sha256(data).digest())
            file_hash.update(data)
            done += len(data)
            if progress is not None:
                progress(done, size)

    if done != size:
        raise ManifestError(f"file changed while hashing: expected {size} bytes, read {done}")

    return Manifest(
        name=name if name is not None else path.name,
        size=size,
        chunk_size=chunk_size,
        chunk_sha256=tuple(chunk_hashes),
        file_sha256=file_hash.digest(),
    )


# ---------------------------------------------------------------------- проверка


def verify_chunk(manifest: Manifest, index: int, data: bytes | bytearray | memoryview) -> bool:
    """Совпадает ли SHA-256 данных с эталоном чанка ``index`` (и длина тоже)."""
    if len(data) != manifest.chunk_length(index):
        return False
    return hashlib.sha256(data).digest() == manifest.chunk_sha256[index]