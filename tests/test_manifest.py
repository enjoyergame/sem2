import hashlib
import json
import os

import pytest

from p2pshare.storage.manifest import (
    HARD_MAX_CHUNK_SIZE,
    MAX_CHUNK_SIZE,
    MIN_CHUNK_SIZE,
    Manifest,
    ManifestError,
    build_manifest,
    choose_chunk_size,
    verify_chunk,
)

MiB = 1024 * 1024
GiB = 1024 * MiB


def make_file(tmp_path, data: bytes, name: str = "a.bin"):
    p = tmp_path / name
    p.write_bytes(data)
    return p


# ------------------------------------------------------------- размер чанка


def test_choose_chunk_size_small_files_use_minimum():
    assert choose_chunk_size(0) == MIN_CHUNK_SIZE
    assert choose_chunk_size(1) == MIN_CHUNK_SIZE
    assert choose_chunk_size(500 * MiB) == MIN_CHUNK_SIZE


def test_choose_chunk_size_scales_and_is_capped():
    assert choose_chunk_size(4 * GiB) == 1 * MiB  # 4 ГиБ / 4096 = 1 МиБ
    assert choose_chunk_size(8 * GiB) == 2 * MiB
    assert choose_chunk_size(1000 * GiB) == MAX_CHUNK_SIZE


def test_choose_chunk_size_is_power_of_two():
    for size in (1, 10**6, 10**9, 5 * 10**10, 10**12):
        cs = choose_chunk_size(size)
        assert cs & (cs - 1) == 0


# ----------------------------------------------------------------- построение


def test_build_known_vector(tmp_path):
    """Эталон: если здесь что-то поменялось — сломается совместимость с напарником."""
    p = make_file(tmp_path, b"hello world")
    m = build_manifest(p, chunk_size=4)

    assert m.size == 11
    assert m.chunk_count == 3
    assert m.file_sha256 == hashlib.sha256(b"hello world").digest()
    assert m.chunk_sha256[0] == hashlib.sha256(b"hell").digest()
    assert m.chunk_sha256[2] == hashlib.sha256(b"rld").digest()  # последний короче

    assert m.canonical_info().decode() == (
        '{"chunk_sha256":['
        '"0ebdc3317b75839f643387d783535adc360ca01f33c75f7c1e7373adcd675c0b",'
        '"8af8e662a3b251ef02d2ed85745535086af77aa1ac63a4f5f800f501c8e5d093",'
        '"85c76dcae2299d2235c793fe3425bd88376f5c4721cfddc1c9a9c452c20c5d33"],'
        '"chunk_size":4,'
        '"file_sha256":"b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",'
        '"size":11}'
    )
    assert m.file_id_hex == "40aaae37a55e465cdd6638a6c4423d5d9e485733a3e89ff050876ddb5efc40b6"
    assert m.short_id == m.file_id[:16]
    assert len(m.short_id) == 16


def test_chunk_geometry(tmp_path):
    m = build_manifest(make_file(tmp_path, b"x" * 10), chunk_size=4)
    assert [m.chunk_length(i) for i in range(3)] == [4, 4, 2]
    assert [m.chunk_offset(i) for i in range(3)] == [0, 4, 8]
    with pytest.raises(IndexError):
        m.chunk_length(3)
    with pytest.raises(IndexError):
        m.chunk_offset(-1)


def test_exact_multiple_of_chunk_size(tmp_path):
    m = build_manifest(make_file(tmp_path, b"x" * 8), chunk_size=4)
    assert m.chunk_count == 2
    assert m.chunk_length(1) == 4


def test_empty_file(tmp_path):
    m = build_manifest(make_file(tmp_path, b""))
    assert m.size == 0
    assert m.chunk_count == 0
    assert m.file_sha256 == hashlib.sha256(b"").digest()
    # пустой манифест должен переживать круг сериализации
    assert Manifest.from_json(m.to_json()) == m


def test_default_chunk_size_is_chosen_automatically(tmp_path):
    m = build_manifest(make_file(tmp_path, b"x" * 1000))
    assert m.chunk_size == MIN_CHUNK_SIZE
    assert m.chunk_count == 1


def test_name_defaults_to_basename_and_can_be_overridden(tmp_path):
    p = make_file(tmp_path, b"abc", "оригинал.txt")
    assert build_manifest(p).name == "оригинал.txt"
    assert build_manifest(p, name="other.txt").name == "other.txt"


def test_progress_callback(tmp_path):
    p = make_file(tmp_path, b"x" * 10)
    calls = []
    build_manifest(p, chunk_size=4, progress=lambda done, total: calls.append((done, total)))
    assert calls == [(4, 10), (8, 10), (10, 10)]


def test_invalid_chunk_size_rejected(tmp_path):
    p = make_file(tmp_path, b"abc")
    with pytest.raises(ValueError):
        build_manifest(p, chunk_size=0)
    with pytest.raises(ValueError):
        build_manifest(p, chunk_size=HARD_MAX_CHUNK_SIZE + 1)


def test_multi_megabyte_file_with_default_settings(tmp_path):
    data = os.urandom(3 * MiB + 123)
    m = build_manifest(make_file(tmp_path, data))
    assert m.file_sha256 == hashlib.sha256(data).digest()
    assert m.chunk_count == 13  # 256 КиБ-чанки: 12 полных + хвост
    for i in range(m.chunk_count):
        off = m.chunk_offset(i)
        assert verify_chunk(m, i, data[off : off + m.chunk_length(i)])


# ------------------------------------------------------------------- file_id


def test_file_id_ignores_name(tmp_path):
    data = b"same content" * 100
    a = build_manifest(make_file(tmp_path, data, "one.bin"), chunk_size=64)
    b = build_manifest(make_file(tmp_path, data, "two.bin"), chunk_size=64)
    assert a.name != b.name
    assert a.file_id == b.file_id


def test_file_id_depends_on_content_and_chunk_size(tmp_path):
    base = build_manifest(make_file(tmp_path, b"a" * 100, "1"), chunk_size=64)
    other_content = build_manifest(make_file(tmp_path, b"b" * 100, "2"), chunk_size=64)
    other_chunks = build_manifest(make_file(tmp_path, b"a" * 100, "3"), chunk_size=32)
    assert base.file_id != other_content.file_id
    assert base.file_id != other_chunks.file_id


# ---------------------------------------------------------------- сериализация


def test_json_roundtrip_with_cyrillic_name(tmp_path):
    m = build_manifest(make_file(tmp_path, "данные".encode() * 50, "фильм №1.mkv"), chunk_size=64)
    restored = Manifest.from_json(m.to_json(), expected_file_id=m.file_id)
    assert restored == m
    assert restored.name == "фильм №1.mkv"
    assert restored.file_id == m.file_id


def test_expected_file_id_full_and_short(tmp_path):
    m = build_manifest(make_file(tmp_path, b"abc" * 10), chunk_size=8)
    Manifest.from_dict(m.to_dict(), expected_file_id=m.file_id)
    Manifest.from_dict(m.to_dict(), expected_file_id=m.short_id)
    with pytest.raises(ManifestError):
        Manifest.from_dict(m.to_dict(), expected_file_id=b"\x00" * 32)
    with pytest.raises(ManifestError):
        Manifest.from_dict(m.to_dict(), expected_file_id=b"\x00" * 16)
    with pytest.raises(ManifestError):  # неподходящая длина
        Manifest.from_dict(m.to_dict(), expected_file_id=m.file_id[:5])


# ------------------------------------------- недоверенный манифест от пира


@pytest.fixture
def good(tmp_path):
    return build_manifest(make_file(tmp_path, b"x" * 10), chunk_size=4).to_dict()


def mutate(good, **info_changes):
    obj = json.loads(json.dumps(good))
    obj["info"].update(info_changes)
    return obj


@pytest.mark.parametrize(
    "bad",
    [
        None,
        [],
        "text",
        42,
        {},
        {"version": 1},
        {"version": 1, "name": "a"},
        {"version": 1, "name": "a", "info": []},
    ],
)
def test_rejects_malformed_structure(bad):
    with pytest.raises(ManifestError):
        Manifest.from_dict(bad)


def test_rejects_bad_json():
    with pytest.raises(ManifestError):
        Manifest.from_json(b"{not json")
    with pytest.raises(ManifestError):
        Manifest.from_json(b"\xff\xfe")


@pytest.mark.parametrize("version", [0, 2, "1", True, None])
def test_rejects_bad_version(good, version):
    good["version"] = version
    with pytest.raises(ManifestError):
        Manifest.from_dict(good)


@pytest.mark.parametrize("name", ["", None, 5, "a\0b", "x" * 256])
def test_rejects_bad_name(good, name):
    good["name"] = name
    with pytest.raises(ManifestError):
        Manifest.from_dict(good)


@pytest.mark.parametrize(
    "changes",
    [
        {"size": -1},
        {"size": True},
        {"size": 10.5},
        {"size": "10"},
        {"size": 1 << 41},
        {"chunk_size": 0},
        {"chunk_size": -4},
        {"chunk_size": HARD_MAX_CHUNK_SIZE + 1},
        {"chunk_size": False},
        {"size": 13},  # число чанков не сходится с хэшами
        {"chunk_size": 2},
        {"chunk_sha256": "oops"},
        {"chunk_sha256": []},
        {"file_sha256": "abc"},
        {"file_sha256": "G" * 64},
        {"file_sha256": "A" * 64},  # только нижний регистр
        {"file_sha256": None},
    ],
)
def test_rejects_bad_info_fields(good, changes):
    with pytest.raises(ManifestError):
        Manifest.from_dict(mutate(good, **changes))


def test_rejects_bad_chunk_hash_entries(good):
    for bad_item in ("zz", 5, None, "a" * 63):
        obj = mutate(good)
        obj["info"]["chunk_sha256"][1] = bad_item
        with pytest.raises(ManifestError):
            Manifest.from_dict(obj)


def test_rejects_absurd_chunk_count():
    obj = {
        "version": 1,
        "name": "bomb",
        "info": {
            "size": 1 << 40,
            "chunk_size": 1,  # 10^12 хэшей — должны отбить до выделения памяти
            "chunk_sha256": [],
            "file_sha256": "0" * 64,
        },
    }
    with pytest.raises(ManifestError, match="too many chunks"):
        Manifest.from_dict(obj)


# ----------------------------------------------------------------- verify_chunk


def test_verify_chunk(tmp_path):
    m = build_manifest(make_file(tmp_path, b"hello world"), chunk_size=4)
    assert verify_chunk(m, 0, b"hell")
    assert verify_chunk(m, 2, b"rld")
    assert verify_chunk(m, 0, memoryview(bytearray(b"hell")))
    assert not verify_chunk(m, 0, b"hellX")  # другая длина
    assert not verify_chunk(m, 0, b"hel")
    assert not verify_chunk(m, 0, b"HELL")  # другое содержимое
    assert not verify_chunk(m, 2, b"rl!")
    with pytest.raises(IndexError):
        verify_chunk(m, 3, b"")


def test_verify_chunk_detects_single_bit_flip(tmp_path):
    data = os.urandom(5000)
    m = build_manifest(make_file(tmp_path, data), chunk_size=1024)
    corrupted = bytearray(data[:1024])
    corrupted[500] ^= 0x01
    assert verify_chunk(m, 0, data[:1024])
    assert not verify_chunk(m, 0, bytes(corrupted))