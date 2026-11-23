import pytest

from p2pshare.config import Config
from p2pshare.core.app_core import AppCore


@pytest.mark.parametrize("fake", [True, False], ids=["fake", "native"])
async def test_connect_and_ping(tmp_path, fake):
    if not fake:
        pytest.importorskip("p2pshare._p2pcore")

    receiver = Config.load(tmp_path / "receiver", port=0, name="Receiver")
    sender = Config.load(tmp_path / "sender", port=0, name="Sender")

    async with AppCore(receiver, fake=fake) as target, AppCore(sender, fake=fake) as client:
        connection = await client.connections.connect("127.0.0.1", target.port)

        assert connection.peer_id == receiver.peer_id
        assert connection.name == "Receiver"
        assert await client.connections.ping(connection) >= 0




@pytest.mark.parametrize("fake", [True, False], ids=["fake", "native"])
async def test_list_files_and_get_manifest(tmp_path, fake):
    if not fake:
        pytest.importorskip("p2pshare._p2pcore")

    source = tmp_path / "пример.txt"
    source.write_text("проверка", encoding="utf-8")
    seeder = Config.load(tmp_path / "seeder", port=0, name="Seeder")
    receiver = Config.load(tmp_path / "receiver", port=0, name="Receiver")

    async with AppCore(seeder, fake=fake) as server, AppCore(receiver, fake=fake) as client:
        connection = await client.connections.connect("127.0.0.1", server.port)
        conn_id = connection.conn_id
        assert await client.control.list_files(conn_id) == []

        shared = await server.catalog.share(source)
        files = await client.control.list_files(conn_id)
        assert files == [
            {"file_id": shared.file_id_hex, "name": source.name, "size": source.stat().st_size}
        ]
        assert await client.control.get_manifest(conn_id, shared.file_id) == shared

        with pytest.raises(RuntimeError, match="unknown_file"):
            await client.control.get_manifest(conn_id, bytes(32))


async def test_native_engine_uses_configured_rate_limits(tmp_path):
    pytest.importorskip("p2pshare._p2pcore")
    config = Config.load(tmp_path / "peer", port=0)
    config.upload_limit_bps = 524288
    config.download_limit_bps = 1048576

    async with AppCore(config, fake=False) as app:
        stats = app.engine.stats()
        assert stats["upload_limit"] == 524288
        assert stats["download_limit"] == 1048576