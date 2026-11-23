"""Проверка установленного модуля p2pshare._p2pcore: собран, импортируется и передаёт файл.

Запуск после ``pip install .`` (не из корня репозитория, чтобы импортировался установленный
пакет, а не src/):

    python cpp/tests/smoke_module.py

Это не тест Python-части (те — в tests/, у pytest), а проверка упаковки: что колесо содержит
рабочее C++ ядро. Его же запускает CI после pip install.
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

# Корень репозитория и src/ убираем из пути: проверяем именно установленный пакет.
_here = Path(__file__).resolve()
_repo = _here.parents[2]
sys.path[:] = [
    p for p in sys.path if Path(p or ".").resolve() not in (_repo, _repo / "src", _here.parent)
]

from p2pshare import _p2pcore as core

CHUNK = 64 * 1024
FILE_ID = b"\x07" * 16


def wait_for(engine: core.Engine, pred, timeout: float = 10.0) -> list:
    """Копить события, пока pred(события) не станет истинным."""
    events: list = []
    deadline = time.monotonic() + timeout
    while not pred(events):
        if time.monotonic() > deadline:
            raise AssertionError(f"timeout; events: {events}")
        events.extend(engine.wait_events(50))
    return events


def config(letter: str, name: str) -> core.EngineConfig:
    cfg = core.EngineConfig()
    cfg.peer_id = letter.encode() * 16
    cfg.name = name
    return cfg


def main() -> int:
    print("module:", core.__file__)
    assert issubclass(core.P2PError, RuntimeError)
    assert int(core.EventType.CHUNK_RECEIVED) == 7

    with tempfile.TemporaryDirectory(prefix="p2p_smoke_") as tmp:
        tmpdir = Path(tmp)
        data = os.urandom(5 * CHUNK + 123)
        src = tmpdir / "исходный.bin"  # кириллица в пути — как в реальном использовании
        src.write_bytes(data)
        part = tmpdir / "загрузка.part"
        chunks = (len(data) + CHUNK - 1) // CHUNK

        with (
            core.Engine(config("S", "seeder")) as seeder,
            core.Engine(config("L", "Ноутбук")) as leecher,
        ):
            port = seeder.start(0)
            leecher.start(0)

            # синхронная ошибка — P2PError, а не падение процесса
            try:
                leecher.connect("127.0.0.1", 1, 500)
                raise AssertionError("connect to a closed port did not raise")
            except core.P2PError as exc:
                print("expected P2PError:", exc)

            seeder.share_file(FILE_ID, str(src), len(data), CHUNK)
            leecher.open_download(FILE_ID, str(part), len(data), CHUNK, bytes((chunks + 7) // 8))
            conn = leecher.connect("127.0.0.1", port)

            leecher.ping(conn)
            wait_for(leecher, lambda ev: any(e.type == core.EventType.PONG for e in ev))

            leecher.request_chunks(conn, FILE_ID, list(range(chunks)))
            events = wait_for(
                leecher,
                lambda ev: sum(e.type == core.EventType.CHUNK_RECEIVED for e in ev) == chunks,
            )
            failed = [e for e in events if e.type == core.EventType.CHUNK_FAILED]
            assert not failed, failed
            leecher.close_file(FILE_ID)
            assert part.read_bytes() == data, "downloaded bytes differ"

            leecher.set_rate_limit(0, 100_000)
            assert leecher.stats()["download_limit"] == 100_000
            print("stats:", leecher.stats())

    print("OK: _p2pcore works")
    return 0


if __name__ == "__main__":
    sys.exit(main())
