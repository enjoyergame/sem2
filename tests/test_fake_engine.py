import os
import time
from contextlib import contextmanager

import pytest

from p2pshare.engine import codes
from p2pshare.engine.errors import P2PError
from p2pshare.engine.events import EventType
from p2pshare.engine.fake_engine import EngineConfig, FakeEngine
from p2pshare.storage.bitfield import Bitfield
from p2pshare.storage.manifest import build_manifest, verify_chunk

CHUNK = 1000


@contextmanager
def pair(seeder_cfg=None):
    """Два запущенных движка: seeder (отдаёт) и leecher (качает)."""
    seeder = FakeEngine(seeder_cfg or EngineConfig(peer_id=b"S" * 16, name="seeder"))
    leecher = FakeEngine(EngineConfig(peer_id=b"L" * 16, name="Ноутбук"))
    seeder_port = seeder.start()
    leecher.start()
    try:
        yield seeder, leecher, seeder_port
    finally:
        leecher.stop()
        seeder.stop()


def types(events):
    return [e.type for e in events]


def setup_transfer(tmp_path, seeder, leecher, size=4500):
    """Раздача файла на seeder и открытая загрузка на leecher. Возвращает (data, manifest, part)."""
    data = os.urandom(size)
    src = tmp_path / "src.bin"
    src.write_bytes(data)
    m = build_manifest(src, chunk_size=CHUNK)
    part = tmp_path / "dst.part"
    seeder.share_file(m.short_id, src, m.size, m.chunk_size)
    leecher.open_download(m.short_id, part, m.size, m.chunk_size, Bitfield(m.chunk_count).to_bytes())
    return data, m, part


# ------------------------------------------------------------------ соединения


def test_connect_emits_peer_connected_on_both_sides():
    with pair() as (seeder, leecher, port):
        conn = leecher.connect("127.0.0.1", port)
        (out,) = leecher.poll_events()
        (inc,) = seeder.poll_events()
        assert out.type == inc.type == EventType.PEER_CONNECTED
        assert out.conn_id == conn and out.code == 0
        assert out.data == b"S" * 16 + b"seeder"
        assert inc.code == 1
        assert inc.data == b"L" * 16 + "Ноутбук".encode()


def test_connect_to_unknown_port_raises():
    with pair() as (_, leecher, _port):
        with pytest.raises(P2PError):
            leecher.connect("127.0.0.1", 1)


def test_cannot_use_engine_before_start():
    eng = FakeEngine()
    with pytest.raises(P2PError):
        eng.connect("127.0.0.1", 1)


def test_port_in_use():
    a, b = FakeEngine(), FakeEngine()
    port = a.start()
    try:
        with pytest.raises(P2PError):
            b.start(port)
    finally:
        a.stop()


def test_control_and_ping():
    with pair() as (seeder, leecher, port):
        conn = leecher.connect("127.0.0.1", port)
        leecher.poll_events()
        (inc,) = seeder.poll_events()
        leecher.send_control(conn, b'{"t":"list"}')
        leecher.ping(conn)
        (ctl,) = seeder.poll_events()
        assert ctl.type == EventType.CONTROL and ctl.data == b'{"t":"list"}'
        assert ctl.conn_id == inc.conn_id
        (pong,) = leecher.poll_events()
        assert pong.type == EventType.PONG and pong.value >= 0


def test_disconnect_notifies_both_sides():
    with pair() as (seeder, leecher, port):
        conn = leecher.connect("127.0.0.1", port)
        leecher.poll_events()
        seeder.poll_events()
        leecher.disconnect(conn)
        (mine,) = leecher.poll_events()
        (theirs,) = seeder.poll_events()
        assert mine.type == theirs.type == EventType.PEER_DISCONNECTED
        assert mine.code == codes.DISCONNECT_LOCAL
        assert theirs.code == codes.DISCONNECT_REMOTE
        with pytest.raises(P2PError):
            leecher.ping(conn)


def test_stop_is_idempotent_and_closes_connections():
    seeder = FakeEngine()
    leecher = FakeEngine()
    port = seeder.start()
    leecher.start()
    leecher.connect("127.0.0.1", port)
    leecher.poll_events()
    seeder.stop()
    seeder.stop()  # повторный вызов не падает
    assert EventType.PEER_DISCONNECTED in types(leecher.poll_events())
    leecher.stop()


def test_wait_events_times_out_and_wakes_up_on_stop():
    eng = FakeEngine()
    eng.start()
    t0 = time.monotonic()
    assert eng.wait_events(timeout_ms=50) == []
    assert time.monotonic() - t0 < 1
    eng.stop()
    t0 = time.monotonic()
    assert eng.wait_events(timeout_ms=5000) == []  # после stop не блокируется
    assert time.monotonic() - t0 < 1


# ------------------------------------------------------------------- передача


def test_full_download(tmp_path):
    with pair() as (seeder, leecher, port):
        data, m, part = setup_transfer(tmp_path, seeder, leecher)
        conn = leecher.connect("127.0.0.1", port)
        leecher.poll_events()

        leecher.request_chunks(conn, m.short_id, list(range(m.chunk_count)))
        events = leecher.poll_events()

        received = [e.chunk_index for e in events if e.type == EventType.CHUNK_RECEIVED]
        assert received == [0, 1, 2, 3, 4]
        assert part.read_bytes() == data
        for i in range(m.chunk_count):
            off = m.chunk_offset(i)
            assert verify_chunk(m, i, part.read_bytes()[off : off + m.chunk_length(i)])

        (progress,) = [e for e in events if e.type == EventType.PROGRESS]
        assert progress.bytes_done == len(data) and progress.value == codes.DIR_DOWNLOAD
        assert leecher.stats()["bytes_received"] == len(data)
        assert seeder.stats()["bytes_sent"] == len(data)


def test_bitfield_exchange(tmp_path):
    with pair() as (seeder, leecher, port):
        _, m, _ = setup_transfer(tmp_path, seeder, leecher)
        conn = leecher.connect("127.0.0.1", port)
        (inc,) = seeder.poll_events()  # PEER_CONNECTED со стороны seeder
        seeder.send_bitfield(inc.conn_id, m.short_id)
        (ev,) = [e for e in leecher.poll_events() if e.type == EventType.BITFIELD]
        assert Bitfield.from_bytes(ev.data, m.chunk_count).is_complete()
        assert ev.file_id == m.short_id and ev.conn_id == conn


def test_not_have_and_unknown_file(tmp_path):
    with pair() as (seeder, leecher, port):
        data, m, part = setup_transfer(tmp_path, seeder, leecher)
        # у seeder второй файл, но ни одного проверенного чанка
        other = b"\x01" * 16
        seeder.open_download(other, tmp_path / "x.part", 3000, CHUNK, Bitfield(3).to_bytes())
        leecher.open_download(other, tmp_path / "y.part", 3000, CHUNK, Bitfield(3).to_bytes())
        conn = leecher.connect("127.0.0.1", port)
        leecher.poll_events()

        leecher.request_chunks(conn, other, [0])
        leecher.request_chunks(conn, m.short_id, [99])
        unknown = b"\x02" * 16
        leecher.open_download(unknown, tmp_path / "z.part", 1000, CHUNK, Bitfield(1).to_bytes())
        leecher.request_chunks(conn, unknown, [0])

        failed = [e for e in leecher.poll_events() if e.type == EventType.CHUNK_FAILED]
        assert [e.code for e in failed] == [
            codes.FAIL_NOT_HAVE,
            codes.FAIL_NOT_HAVE,
            codes.FAIL_UNKNOWN_FILE,
        ]


def test_set_have_makes_downloaded_chunk_servable(tmp_path):
    with pair() as (seeder, leecher, port):
        data, m, part = setup_transfer(tmp_path, seeder, leecher)
        conn = leecher.connect("127.0.0.1", port)
        leecher.request_chunks(conn, m.short_id, [0])
        leecher.set_have(m.short_id, 0)  # после проверки SHA-256 в Python
        leecher.send_bitfield(conn, m.short_id)
        (bf,) = [e for e in seeder.poll_events() if e.type == EventType.BITFIELD]
        assert list(Bitfield.from_bytes(bf.data, m.chunk_count).indices()) == [0]


def test_open_download_rejects_wrong_bitfield_length(tmp_path):
    eng = FakeEngine()
    eng.start()
    try:
        with pytest.raises(P2PError):
            eng.open_download(b"\x00" * 16, tmp_path / "a.part", 5000, CHUNK, b"\x00\x00\x00")
    finally:
        eng.stop()


# --------------------------------------------------------------------- ошибки


def test_fault_corrupt_chunk_detected_by_sha256_and_fixed_on_retry(tmp_path):
    cfg = EngineConfig(peer_id=b"S" * 16, name="seeder", fault_corrupt_chunk=2)
    with pair(cfg) as (seeder, leecher, port):
        data, m, part = setup_transfer(tmp_path, seeder, leecher)
        conn = leecher.connect("127.0.0.1", port)
        leecher.request_chunks(conn, m.short_id, [2])
        bad = part.read_bytes()[m.chunk_offset(2) : m.chunk_offset(2) + CHUNK]
        assert not verify_chunk(m, 2, bad)  # SHA-256 ловит порчу

        leecher.request_chunks(conn, m.short_id, [2])  # повтор
        good = part.read_bytes()[m.chunk_offset(2) : m.chunk_offset(2) + CHUNK]
        assert verify_chunk(m, 2, good)


def test_fault_drop_after_bytes_then_resume(tmp_path):
    cfg = EngineConfig(peer_id=b"S" * 16, name="seeder", fault_drop_after_bytes=2500)
    with pair(cfg) as (seeder, leecher, port):
        data, m, part = setup_transfer(tmp_path, seeder, leecher)
        conn = leecher.connect("127.0.0.1", port)
        leecher.poll_events()
        seeder.poll_events()

        leecher.request_chunks(conn, m.short_id, list(range(m.chunk_count)))
        events = leecher.poll_events()
        received = [e.chunk_index for e in events if e.type == EventType.CHUNK_RECEIVED]
        failed = [e.chunk_index for e in events if e.type == EventType.CHUNK_FAILED]
        assert received == [0, 1]
        assert failed == [2, 3, 4]
        assert all(e.code == codes.FAIL_DISCONNECT for e in events if e.type == EventType.CHUNK_FAILED)
        disc = [e for e in events if e.type == EventType.PEER_DISCONNECTED]
        assert disc and disc[0].code == codes.DISCONNECT_FAULT
        seeder_disc = [e for e in seeder.poll_events() if e.type == EventType.PEER_DISCONNECTED]
        assert [e.code for e in seeder_disc] == [codes.DISCONNECT_FAULT]

        # докачка: новое соединение, просим только недостающее
        for i in received:
            leecher.set_have(m.short_id, i)
        conn2 = leecher.connect("127.0.0.1", port)
        need = [i for i in range(m.chunk_count) if i not in received]
        leecher.request_chunks(conn2, m.short_id, need)
        assert part.read_bytes() == data