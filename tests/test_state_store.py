import base64
import json
import os
import re

import pytest

from p2pshare.storage.bitfield import Bitfield
from p2pshare.storage.manifest import build_manifest
from p2pshare.storage.state_store import StateStore, state_path_for


@pytest.fixture
def setup(tmp_path):
    """Манифест на 5 чанков, пустой .part нужного размера и StateStore."""
    src = tmp_path / "src.bin"
    src.write_bytes(os.urandom(4500))
    manifest = build_manifest(src, chunk_size=1000)
    part = tmp_path / "film.mkv.part"
    part.write_bytes(b"\0" * manifest.size)
    return manifest, part, StateStore(part)


def read_json(store):
    return json.loads(store.path.read_text(encoding="utf-8"))


def write_json(store, obj):
    store.path.write_text(json.dumps(obj), encoding="utf-8")


# ------------------------------------------------------------------- имена


def test_state_path_naming(tmp_path):
    assert state_path_for(tmp_path / "film.mkv.part") == tmp_path / "film.mkv.p2pstate.json"
    assert state_path_for(tmp_path / "фильм №1.mkv.part").name == "фильм №1.mkv.p2pstate.json"
    assert state_path_for(tmp_path / "noext").name == "noext.p2pstate.json"


# ------------------------------------------------------------- сохранение


def test_save_and_load_roundtrip(setup):
    manifest, part, store = setup
    have = Bitfield(manifest.chunk_count)
    have.set(0)
    have.set(3)
    store.save(manifest, have, peers=["ab" * 16, "cd" * 16])

    state = store.load()
    assert state is not None
    assert state.manifest == manifest
    assert state.file_id == manifest.file_id
    assert state.have == have and list(state.have.indices()) == [0, 3]
    assert state.peers == ["ab" * 16, "cd" * 16]
    assert state.part_size == manifest.size
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", state.updated_at)


def test_loaded_state_is_independent_copy(setup):
    manifest, part, store = setup
    have = Bitfield(manifest.chunk_count)
    store.save(manifest, have)
    have.set(2)  # изменение после save не должно попасть в файл
    assert not store.load().have.get(2)


def test_save_overwrites_and_leaves_no_tmp(setup):
    manifest, part, store = setup
    have = Bitfield(manifest.chunk_count)
    store.save(manifest, have)
    have.set(1)
    store.save(manifest, have)
    assert list(store.load().have.indices()) == [1]
    assert not store._tmp_path.exists()


def test_save_rejects_wrong_bitfield_size(setup):
    manifest, part, store = setup
    with pytest.raises(ValueError):
        store.save(manifest, Bitfield(manifest.chunk_count + 1))


# ------------------------------------------------------------------ загрузка


def test_load_missing_returns_none(setup):
    _, _, store = setup
    assert store.load() is None


def test_load_garbage_returns_none(setup):
    manifest, part, store = setup
    store.path.write_text("{ не json", encoding="utf-8")
    assert store.load() is None
    store.path.write_text("[1, 2, 3]", encoding="utf-8")
    assert store.load() is None
    store.path.write_bytes(b"\xff\xfe\x00")
    assert store.load() is None


def _saved(setup):
    manifest, part, store = setup
    store.save(manifest, Bitfield(manifest.chunk_count))
    return store, read_json(store)


def test_load_rejects_wrong_version(setup):
    store, obj = _saved(setup)
    obj["version"] = 2
    write_json(store, obj)
    assert store.load() is None


def test_load_rejects_file_id_mismatch(setup):
    store, obj = _saved(setup)
    obj["file_id"] = "0" * 64
    write_json(store, obj)
    assert store.load() is None


def test_load_rejects_tampered_manifest(setup):
    store, obj = _saved(setup)
    obj["manifest"]["info"]["size"] = 1  # число чанков перестаёт сходиться с хэшами
    write_json(store, obj)
    assert store.load() is None


def test_load_rejects_bad_bitfield(setup):
    store, obj = _saved(setup)
    obj["have"] = base64.b64encode(b"\x00\x00\x00").decode()  # неверная длина
    write_json(store, obj)
    assert store.load() is None

    obj["have"] = "!!! не base64 !!!"
    write_json(store, obj)
    assert store.load() is None

    obj["have"] = base64.b64encode(b"\xff").decode()  # ненулевые хвостовые биты (5 чанков)
    write_json(store, obj)
    assert store.load() is None


@pytest.mark.parametrize(
    "field, value",
    [
        ("part_size", -1),
        ("part_size", True),
        ("part_mtime_ns", "x"),
        ("peers", "abc"),
        ("peers", [1, 2]),
        ("updated_at", 5),
    ],
)
def test_load_rejects_bad_fields(setup, field, value):
    store, obj = _saved(setup)
    obj[field] = value
    write_json(store, obj)
    assert store.load() is None


# ------------------------------------------------------------- отпечаток .part


def test_matches_part_after_save(setup):
    manifest, part, store = setup
    store.save(manifest, Bitfield(manifest.chunk_count))
    assert store.matches_part(store.load())


def test_matches_part_detects_size_and_mtime_changes(setup):
    manifest, part, store = setup
    store.save(manifest, Bitfield(manifest.chunk_count))
    state = store.load()

    st = part.stat()
    os.utime(part, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
    assert not store.matches_part(state)  # время изменилось

    os.utime(part, ns=(st.st_atime_ns, st.st_mtime_ns))
    assert store.matches_part(state)  # вернули — снова совпало

    with open(part, "ab") as f:
        f.write(b"x")
    assert not store.matches_part(state)  # размер изменился


def test_matches_part_false_when_part_missing(setup):
    manifest, part, store = setup
    store.save(manifest, Bitfield(manifest.chunk_count))
    state = store.load()
    part.unlink()
    assert not store.matches_part(state)


# --------------------------------------------------------------------- удаление


def test_delete(setup):
    manifest, part, store = setup
    store.save(manifest, Bitfield(manifest.chunk_count))
    store.delete()
    assert not store.path.exists()
    store.delete()  # повторно — не падает