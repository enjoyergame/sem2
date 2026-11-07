"""FakeEngine — реализация API C++ ядра целиком в памяти (без сокетов).

«Сеть» — это реестр ``порт -> FakeEngine``. Чанки копируются прямо из файла одного
движка в ``.part`` другого, события те же, что у настоящего ядра. Благодаря этому
Python-часть разрабатывается и тестируется, не дожидаясь C++.

Отличия от настоящего ядра (намеренные упрощения):
  * всё выполняется синхронно в потоке вызывающего; ``max_inflight`` и лимиты скорости
    только сохраняются, но не применяются;
  * ``PROGRESS`` отправляется один раз после каждого ``request_chunks``;
  * ``fault_drop_after_bytes`` и ``fault_corrupt_chunk`` действуют на стороне ОТДАЮЩЕГО
    и срабатывают один раз (нужно уточнить у напарника, как сделано в C++).
"""

from __future__ import annotations

import itertools
import os
import threading
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from p2pshare.engine import codes
from p2pshare.engine.errors import P2PError
from p2pshare.engine.events import Event, EventType
from p2pshare.storage.bitfield import Bitfield

FILE_ID_LEN = 16


@dataclass
class EngineConfig:
    """Зеркало ``_p2pcore.EngineConfig``."""

    peer_id: bytes = bytes(16)
    name: str = "fake"
    max_inflight: int = 6
    keepalive_ms: int = 5000
    idle_timeout_ms: int = 15000
    fault_drop_after_bytes: int = 0  # 0 = выкл.
    fault_corrupt_chunk: int = -1  # -1 = выкл.


@dataclass
class _FileEntry:
    path: Path
    size: int
    chunk_size: int
    have: Bitfield
    writable: bool = False

    @property
    def chunk_count(self) -> int:
        return -(-self.size // self.chunk_size)

    def chunk_length(self, index: int) -> int:
        return min(self.chunk_size, self.size - index * self.chunk_size)


@dataclass
class _Conn:
    remote: FakeEngine
    remote_id: int


@dataclass
class _Counters:
    bytes_sent: int = 0
    bytes_received: int = 0
    per_file_sent: dict[bytes, int] = field(default_factory=dict)
    per_file_received: dict[bytes, int] = field(default_factory=dict)


class FakeEngine:
    _registry: ClassVar[dict[int, FakeEngine]] = {}
    _registry_lock: ClassVar[threading.Lock] = threading.Lock()
    _next_port: ClassVar[int] = 40000

    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()
        self._lock = threading.RLock()
        self._cond = threading.Condition()
        self._events: deque[Event] = deque()
        self._files: dict[bytes, _FileEntry] = {}
        self._conns: dict[int, _Conn] = {}
        self._conn_ids = itertools.count(1)
        self._port: int | None = None
        self._running = False
        self._counters = _Counters()
        self._drop_after = self.config.fault_drop_after_bytes
        self._corrupt_chunk = self.config.fault_corrupt_chunk
        self._upload_bps = 0
        self._download_bps = 0

    # ------------------------------------------------------------ жизненный цикл

    def start(self, port: int = 0) -> int:
        with self._lock:
            if self._running:
                raise P2PError("engine already started")
            with FakeEngine._registry_lock:
                if port == 0:
                    port = FakeEngine._next_port
                    while port in FakeEngine._registry:
                        port += 1
                    FakeEngine._next_port = port + 1
                elif port in FakeEngine._registry:
                    raise P2PError(f"port {port} is already in use")
                FakeEngine._registry[port] = self
            self._port = port
            self._running = True
            return port

    def stop(self) -> None:
        """Идемпотентно: закрывает все соединения и снимает порт."""
        with self._lock:
            if not self._running:
                return
            conn_ids = list(self._conns)
        for cid in conn_ids:
            self._close_conn(cid, codes.DISCONNECT_LOCAL, codes.DISCONNECT_REMOTE)
        with self._lock:
            self._running = False
            with FakeEngine._registry_lock:
                FakeEngine._registry.pop(self._port, None)
        with self._cond:
            self._cond.notify_all()

    # ------------------------------------------------------------------ соединения

    def connect(self, host: str, port: int, timeout_ms: int = 5000) -> int:
        self._require_running()
        with FakeEngine._registry_lock:
            target = FakeEngine._registry.get(port)
        if target is None or target is self or not target._running:
            raise P2PError(f"connection refused: {host}:{port}")
        local_id = next(self._conn_ids)
        remote_id = next(target._conn_ids)
        with self._lock:
            self._conns[local_id] = _Conn(remote=target, remote_id=remote_id)
        with target._lock:
            target._conns[remote_id] = _Conn(remote=self, remote_id=local_id)
        self._emit(self._hello_event(local_id, target, incoming=False))
        target._emit(self._hello_event(remote_id, self, incoming=True))
        return local_id

    def disconnect(self, conn_id: int) -> None:
        self._conn(conn_id)
        self._close_conn(conn_id, codes.DISCONNECT_LOCAL, codes.DISCONNECT_REMOTE)

    def ping(self, conn_id: int) -> None:
        self._conn(conn_id)
        self._emit(Event(EventType.PONG, conn_id=conn_id, value=0.1))

    def send_control(self, conn_id: int, payload: bytes) -> None:
        conn = self._conn(conn_id)
        conn.remote._emit(Event(EventType.CONTROL, conn_id=conn.remote_id, data=bytes(payload)))

    # ------------------------------------------------------------------ файлы

    def share_file(self, file_id: bytes, path: str | os.PathLike[str], size: int,
                   chunk_size: int) -> None:
        self._check_file_args(file_id, size, chunk_size)
        path = Path(path)
        if not path.is_file():
            raise P2PError(f"file not found: {path}")
        with self._lock:
            entry = _FileEntry(path, size, chunk_size, Bitfield(0))
            entry.have = Bitfield.full(entry.chunk_count)
            self._files[bytes(file_id)] = entry

    def open_download(self, file_id: bytes, part_path: str | os.PathLike[str], size: int,
                      chunk_size: int, have: bytes) -> None:
        self._check_file_args(file_id, size, chunk_size)
        part_path = Path(part_path)
        entry = _FileEntry(part_path, size, chunk_size, Bitfield(0), writable=True)
        try:
            entry.have = Bitfield.from_bytes(have, entry.chunk_count)
        except ValueError as exc:
            raise P2PError(f"bad bitfield: {exc}") from exc
        try:
            part_path.parent.mkdir(parents=True, exist_ok=True)
            with open(part_path, "r+b" if part_path.exists() else "w+b") as f:
                f.truncate(size)  # предразмещение
        except OSError as exc:
            raise P2PError(f"cannot open {part_path}: {exc}") from exc
        with self._lock:
            self._files[bytes(file_id)] = entry

    def set_have(self, file_id: bytes, chunk_index: int) -> None:
        with self._lock:
            entry = self._file(file_id)
            if not 0 <= chunk_index < entry.chunk_count:
                raise P2PError(f"chunk index out of range: {chunk_index}")
            entry.have.set(chunk_index)

    def send_bitfield(self, conn_id: int, file_id: bytes) -> None:
        conn = self._conn(conn_id)
        with self._lock:
            data = self._file(file_id).have.to_bytes()
        conn.remote._emit(
            Event(EventType.BITFIELD, conn_id=conn.remote_id, file_id=bytes(file_id), data=data)
        )

    def request_chunks(self, conn_id: int, file_id: bytes, indices: list[int]) -> None:
        conn = self._conn(conn_id)
        file_id = bytes(file_id)
        with self._lock:
            entry = self._file(file_id)
            if not entry.writable:
                raise P2PError("file is not open for download")
        indices = list(indices)

        for pos, idx in enumerate(indices):
            data, error = conn.remote._serve_chunk(conn.remote_id, file_id, idx)
            if error == codes.FAIL_DISCONNECT:
                # обрыв: все оставшиеся запросы проваливаются, затем закрываем соединение
                for rest in indices[pos:]:
                    self._fail(conn_id, file_id, rest, codes.FAIL_DISCONNECT)
                self._close_conn(conn_id, codes.DISCONNECT_FAULT, codes.DISCONNECT_FAULT)
                break
            if error:
                self._fail(conn_id, file_id, idx, error)
                continue
            with open(entry.path, "r+b") as f:
                f.seek(idx * entry.chunk_size)
                f.write(data)
            with self._lock:
                c = self._counters
                c.bytes_received += len(data)
                c.per_file_received[file_id] = c.per_file_received.get(file_id, 0) + len(data)
            self._emit(Event(EventType.CHUNK_RECEIVED, conn_id=conn_id, file_id=file_id,
                             chunk_index=idx))

        self._emit_progress(file_id, codes.DIR_DOWNLOAD)
        conn.remote._emit_progress(file_id, codes.DIR_UPLOAD)

    def cancel_chunks(self, conn_id: int, file_id: bytes, indices: list[int]) -> None:
        self._conn(conn_id)  # всё выполняется синхронно, отменять нечего

    def flush(self, file_id: bytes) -> None:
        with self._lock:
            self._file(file_id)  # файл закрывается после каждой записи — данные уже в ОС

    def close_file(self, file_id: bytes) -> None:
        with self._lock:
            self._files.pop(bytes(file_id), None)

    def set_rate_limit(self, upload_bps: int, download_bps: int) -> None:
        self._upload_bps, self._download_bps = upload_bps, download_bps

    # ------------------------------------------------------------------ события

    def poll_events(self, max_events: int = 512) -> list[Event]:
        with self._cond:
            return self._pop(max_events)

    def wait_events(self, timeout_ms: int = 100, max_events: int = 512) -> list[Event]:
        with self._cond:
            self._cond.wait_for(lambda: self._events or not self._running, timeout_ms / 1000)
            return self._pop(max_events)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "connections": len(self._conns),
                "files": len(self._files),
                "bytes_sent": self._counters.bytes_sent,
                "bytes_received": self._counters.bytes_received,
            }

    # ------------------------------------------------------------ внутреннее

    def _pop(self, max_events: int) -> list[Event]:
        out = []
        while self._events and len(out) < max_events:
            out.append(self._events.popleft())
        return out

    def _emit(self, ev: Event) -> None:
        with self._cond:
            self._events.append(ev)
            self._cond.notify_all()

    def _emit_progress(self, file_id: bytes, direction: int) -> None:
        with self._lock:
            c = self._counters
            per_file = c.per_file_received if direction == codes.DIR_DOWNLOAD else c.per_file_sent
            done = per_file.get(file_id, 0)
        self._emit(Event(EventType.PROGRESS, file_id=file_id, bytes_done=done,
                         value=float(direction)))

    @staticmethod
    def _hello_event(conn_id: int, other: FakeEngine, incoming: bool) -> Event:
        return Event(
            EventType.PEER_CONNECTED,
            conn_id=conn_id,
            data=other.config.peer_id + other.config.name.encode("utf-8"),
            code=1 if incoming else 0,
        )

    def _fail(self, conn_id: int, file_id: bytes, index: int, code: int) -> None:
        self._emit(Event(EventType.CHUNK_FAILED, conn_id=conn_id, file_id=file_id,
                         chunk_index=index, code=code))

    def _require_running(self) -> None:
        if not self._running:
            raise P2PError("engine is not running")

    def _conn(self, conn_id: int) -> _Conn:
        self._require_running()
        with self._lock:
            conn = self._conns.get(conn_id)
        if conn is None:
            raise P2PError(f"unknown connection: {conn_id}")
        return conn

    def _file(self, file_id: bytes) -> _FileEntry:
        entry = self._files.get(bytes(file_id))
        if entry is None:
            raise P2PError("unknown file_id")
        return entry

    @staticmethod
    def _check_file_args(file_id: bytes, size: int, chunk_size: int) -> None:
        if len(file_id) != FILE_ID_LEN:
            raise P2PError(f"file_id must be {FILE_ID_LEN} bytes")
        if size < 0 or chunk_size <= 0:
            raise P2PError("bad size / chunk_size")

    def _close_conn(self, conn_id: int, local_code: int, remote_code: int) -> None:
        with self._lock:
            conn = self._conns.pop(conn_id, None)
        if conn is None:
            return
        self._emit(Event(EventType.PEER_DISCONNECTED, conn_id=conn_id, code=local_code))
        conn.remote._on_remote_closed(conn.remote_id, remote_code)

    def _on_remote_closed(self, conn_id: int, code: int) -> None:
        with self._lock:
            conn = self._conns.pop(conn_id, None)
        if conn is not None:
            self._emit(Event(EventType.PEER_DISCONNECTED, conn_id=conn_id, code=code))

    def _serve_chunk(self, conn_id: int, file_id: bytes, index: int) -> tuple[bytes | None, int]:
        """Отдающая сторона: вернуть (данные, 0) или (None, код_отказа)."""
        with self._lock:
            if conn_id not in self._conns:
                return None, codes.FAIL_DISCONNECT
            entry = self._files.get(file_id)
            if entry is None:
                return None, codes.FAIL_UNKNOWN_FILE
            if not 0 <= index < entry.chunk_count or not entry.have.get(index):
                return None, codes.FAIL_NOT_HAVE
            length = entry.chunk_length(index)
            offset = index * entry.chunk_size
            path = entry.path
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read(length)
        with self._lock:
            c = self._counters
            if self._drop_after and c.bytes_sent + len(data) > self._drop_after:
                self._drop_after = 0  # срабатывает один раз
                return None, codes.FAIL_DISCONNECT
            if index == self._corrupt_chunk and data:
                self._corrupt_chunk = -1  # портим один раз, чтобы перекачка удалась
                data = bytes([data[0] ^ 0xFF]) + data[1:]
            c.bytes_sent += len(data)
            c.per_file_sent[file_id] = c.per_file_sent.get(file_id, 0) + len(data)
        return data, 0