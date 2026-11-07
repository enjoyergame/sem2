import json

from p2pshare.config import (
    DEFAULT_DISCOVERY_PORT,
    DEFAULT_TCP_PORT,
    Config,
)
from p2pshare.engine.events import Event, EventType


def test_first_run_creates_config_and_peer_id(tmp_path):
    cfg = Config.load(tmp_path / "data")
    assert len(cfg.peer_id) == 16
    assert cfg.tcp_port == DEFAULT_TCP_PORT
    assert cfg.discovery_port == DEFAULT_DISCOVERY_PORT
    assert cfg.config_path.exists()
    assert cfg.download_dir.is_dir()
    assert cfg.name  # имя компьютера по умолчанию


def test_peer_id_is_stable_between_runs(tmp_path):
    first = Config.load(tmp_path)
    second = Config.load(tmp_path)
    assert first.peer_id == second.peer_id


def test_different_data_dirs_give_different_peer_ids(tmp_path):
    a = Config.load(tmp_path / "a")
    b = Config.load(tmp_path / "b")
    assert a.peer_id != b.peer_id


def test_cli_overrides(tmp_path):
    cfg = Config.load(tmp_path, port=50000, name="Ноутбук Артёма")
    assert cfg.tcp_port == 50000
    assert cfg.name == "Ноутбук Артёма"


def test_save_and_reload_roundtrip(tmp_path):
    cfg = Config.load(tmp_path, name="Alice")
    cfg.upload_limit_bps = 1_000_000
    cfg.save()
    again = Config.load(tmp_path)
    assert again.name == "Alice"
    assert again.upload_limit_bps == 1_000_000
    assert again.peer_id == cfg.peer_id


def test_corrupted_config_is_replaced_not_crashing(tmp_path):
    (tmp_path / "config.json").write_text("{ это не json", encoding="utf-8")
    cfg = Config.load(tmp_path)
    assert len(cfg.peer_id) == 16
    assert json.loads((tmp_path / "config.json").read_text(encoding="utf-8"))["peer_id"]


def test_bad_peer_id_in_file_is_regenerated(tmp_path):
    (tmp_path / "config.json").write_text(json.dumps({"peer_id": "abcd"}), encoding="utf-8")
    cfg = Config.load(tmp_path)
    assert len(cfg.peer_id) == 16


def test_no_tmp_file_left_behind(tmp_path):
    Config.load(tmp_path)
    assert not (tmp_path / "config.json.tmp").exists()


def test_event_types_match_contract():
    assert EventType.PEER_CONNECTED == 1
    assert EventType.CHUNK_RECEIVED == 7
    assert EventType.PROGRESS == 9
    assert EventType.ERROR == 11
    assert len(EventType) == 11


def test_event_defaults():
    ev = Event(EventType.PONG, conn_id=3, value=1.5)
    assert ev.file_id == b"" and ev.data == b"" and ev.message == ""