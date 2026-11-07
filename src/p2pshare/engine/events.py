"""Python-зеркало событий C++ ядра (см. ``_p2pcore.pyi``).

Значения ``EventType`` обязаны совпадать с C++ — это общий контракт с напарником.
``FakeEngine`` и ядро на C++ отдают одинаковые ``Event``, поэтому остальной код
не знает, с каким движком работает.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class EventType(IntEnum):
    PEER_CONNECTED = 1  # conn_id, data = peer_id(16) + name, code = 1 если входящее
    PEER_DISCONNECTED = 2  # conn_id, code = причина, message
    PONG = 3  # conn_id, value = RTT в мс
    CONTROL = 4  # conn_id, data = JSON-байты
    BITFIELD = 5  # conn_id, file_id, data = bitfield
    HAVE = 6  # conn_id, file_id, chunk_index (курсовая)
    CHUNK_RECEIVED = 7  # conn_id, file_id, chunk_index (CRC ok, лежит на диске)
    CHUNK_FAILED = 8  # conn_id, file_id, chunk_index, code (CRC, REJECT, обрыв, таймаут)
    PROGRESS = 9  # file_id, bytes_done (накопительно), value = направление
    LOG = 10  # code = уровень, message
    ERROR = 11  # code, message


@dataclass(slots=True)
class Event:
    type: EventType
    conn_id: int = 0
    file_id: bytes = b""  # 16 байт или b""
    chunk_index: int = 0
    bytes_done: int = 0
    value: float = 0.0
    code: int = 0
    data: bytes = b""
    message: str = ""