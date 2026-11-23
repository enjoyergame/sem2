import asyncio
import os
import threading

import pytest

from p2pshare.core.verifier import (
    RecheckCancelled,
    Verifier,
    check_chunk,
    hash_file,
    recheck_part,
    verify_file,
)
from p2pshare.storage.manifest import build_manifest

CHUNK = 1000


@pytest.fixture
def sample(tmp_path):
    """Файл на 4500 байт (5 чанков, последний короче) и его манифест."""
    data = os.urandom(4500)
    src = tmp_path / "src.bin"
    src.write_bytes(data)
    return data, build_manifest(src, chunk_size=CHUNK), tmp_path


def test_check_chunk_ok_and_corrupted(sample):
    data, m, tmp = sample
    part = tmp / "a.part"
    part.write_bytes(data)
    assert all(check_chunk(part, m, i) for i in range(m.chunk_count))

    broken = bytearray(data)
    broken[2 * CHUNK + 5] ^= 0x01
    part.write_bytes(bytes(broken))
    assert [check_chunk(part, m, i) for i in range(5)] == [True, True, False, True, True]


def test_check_chunk_short_or_missing_file(sample):
    data, m, tmp = sample
    part = tmp / "a.part"
    part.write_bytes(data[:2500])  # файл обрезан посреди третьего чанка
    assert [check_chunk(part, m, i) for i in range(5)] == [True, True, False, False, False]
    assert not check_chunk(tmp / "нет.part", m, 0)


def test_recheck_restores_bitfield(sample):
    data, m, tmp = sample
    part = tmp / "a.part"
    # чанки 0 и 3 на месте, остальное — нули (как предразмещённый .part)
    content = bytearray(len(data))
    content[0:CHUNK] = data[0:CHUNK]
    content[3 * CHUNK : 4 * CHUNK] = data[3 * CHUNK : 4 * CHUNK]
    part.write_bytes(bytes(content))

    calls = []
    have = recheck_part(part, m, progress=lambda done, total: calls.append((done, total)))
    assert list(have.indices()) == [0, 3]
    assert calls[-1] == (5, 5) and len(calls) == 5


def test_recheck_last_short_chunk(sample):
    data, m, tmp = sample
    part = tmp / "a.part"
    part.write_bytes(data)
    assert recheck_part(part, m).is_complete()


def test_recheck_missing_part_gives_empty_bitfield(sample):
    _, m, tmp = sample
    assert recheck_part(tmp / "нет.part", m).count() == 0


def test_recheck_can_be_cancelled(sample):
    data, m, tmp = sample
    part = tmp / "a.part"
    part.write_bytes(data)
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(RecheckCancelled):
        recheck_part(part, m, cancel=cancel)


def test_hash_and_verify_file(sample):
    data, m, tmp = sample
    part = tmp / "a.part"
    part.write_bytes(data)
    assert hash_file(part) == m.file_sha256
    assert verify_file(part, m)

    part.write_bytes(data[:-1])  # неверный размер
    assert not verify_file(part, m)
    part.write_bytes(data[:-1] + bytes([data[-1] ^ 0xFF]))  # размер тот же, байт другой
    assert not verify_file(part, m)
    assert not verify_file(tmp / "нет.part", m)


async def test_async_verifier(sample):
    data, m, tmp = sample
    part = tmp / "a.part"
    part.write_bytes(data)
    v = Verifier()
    try:
        assert await v.verify_chunk(part, m, 4)
        assert await v.verify_file(part, m)
        have = await v.recheck(part, m)
        assert have.is_complete()

        part.write_bytes(b"\0" * len(data))
        assert not await v.verify_chunk(part, m, 0)
        assert (await v.recheck(part, m)).count() == 0
        assert not await v.verify_file(part, m)
    finally:
        v.close()


async def test_verifier_runs_in_parallel_with_event_loop(sample):
    data, m, tmp = sample
    part = tmp / "a.part"
    part.write_bytes(data)
    v = Verifier()
    try:
        results = await asyncio.gather(*(v.verify_chunk(part, m, i) for i in range(5)))
        assert results == [True] * 5
    finally:
        v.close()