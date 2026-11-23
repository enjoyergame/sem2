import pytest

from p2pshare.config import Config
from p2pshare.core.app_core import AppCore
from p2pshare.storage.paths import download_path
from p2pshare.engine import P2PError
from p2pshare.storage.state_store import StateStore


@pytest.mark.parametrize("fake", [True, False], ids=["fake", "native"])
@pytest.mark.parametrize("data", [b"", bytes(range(256)) * 8193], ids=["empty", "multi_chunk"])
async def test_download_and_keep_existing_file(tmp_path, fake, data):
    if not fake:
        pytest.importorskip("p2pshare._p2pcore")

    source = tmp_path / "пример.bin"
    source.write_bytes(data)
    sender = Config.load(tmp_path / "sender", port=0, name="Sender")
    receiver = Config.load(tmp_path / "receiver", port=0, name="Receiver")

    async with AppCore(sender, fake=fake) as server, AppCore(receiver, fake=fake) as client:
        manifest = await server.catalog.share(source)
        progress = []
        target = await client.download(
            "127.0.0.1",
            server.port,
            manifest.file_id,
            lambda done, total: progress.append((done, total)),
        )

        assert target.name == source.name
        assert target.read_bytes() == data
        assert progress[0] == (0, len(data))
        assert progress[-1] == (len(data), len(data))
        assert list(receiver.download_dir.glob(".p2pshare/*.part")) == []

        target.write_bytes(b"local copy")
        with pytest.raises(FileExistsError):
            await client.download("127.0.0.1", server.port, manifest.file_id)
        assert target.read_bytes() == b"local copy"


@pytest.mark.parametrize("name", ["../outside", r"..\outside", "C:outside", "NUL.txt", "x."])
def test_download_rejects_unsafe_names(tmp_path, name):
    with pytest.raises(ValueError, match="Недопустимое имя"):
        download_path(tmp_path, name)


@pytest.mark.parametrize("fake", [True, False], ids=["fake", "native"])
async def test_download_rejects_changed_source(tmp_path, fake):
    if not fake:
        pytest.importorskip("p2pshare._p2pcore")

    source = tmp_path / "source.bin"
    source.write_bytes(b"original")
    sender = Config.load(tmp_path / "sender", port=0)
    receiver = Config.load(tmp_path / "receiver", port=0)

    async with AppCore(sender, fake=fake) as server, AppCore(receiver, fake=fake) as client:
        manifest = await server.catalog.share(source)
        source.write_bytes(b"modified")
        with pytest.raises(RuntimeError, match="SHA-256 чанка"):
            await client.download("127.0.0.1", server.port, manifest.file_id)

    assert not (receiver.download_dir / source.name).exists()
    (part,) = receiver.download_dir.glob(".p2pshare/*.part")
    state = StateStore(part).load()
    assert state is not None
    assert state.have.count() == 0


@pytest.mark.parametrize("fake", [True, False], ids=["fake", "native"])
@pytest.mark.parametrize("remove_state", [False, True], ids=["saved_state", "recheck"])
async def test_resume_after_disconnect(tmp_path, fake, remove_state):
    if not fake:
        pytest.importorskip("p2pshare._p2pcore")

    source = tmp_path / "source.bin"
    data = bytes(range(256)) * 8193
    source.write_bytes(data)
    sender = Config.load(tmp_path / "sender", port=0)
    receiver = Config.load(tmp_path / "receiver", port=0)
    first_progress = []

    async with AppCore(sender, fake=fake) as server:
        manifest = await server.catalog.share(source)

        def disconnect_after_first_batch(done, total):
            first_progress.append(done)
            if 0 < done < total:
                (conn_id,) = server.connections.connections
                server.engine.disconnect(conn_id)

        async with AppCore(receiver, fake=fake) as client:
            with pytest.raises(P2PError):
                await client.download(
                    "127.0.0.1", server.port, manifest.file_id, disconnect_after_first_batch
                )

        completed = first_progress[-1]
        assert 0 < completed < len(data)
        (part,) = receiver.download_dir.glob(".p2pshare/*.part")
        store = StateStore(part)
        state = store.load()
        assert state is not None
        assert state.have.count() == 6
        if remove_state:
            store.path.unlink()

        resumed = []
        async with AppCore(receiver, fake=fake) as client:
            target = await client.download(
                "127.0.0.1",
                server.port,
                manifest.file_id,
                lambda done, total: resumed.append(done),
            )
            assert client.engine.stats()["bytes_received"] == len(data) - completed

    assert resumed[0] == completed
    assert target.read_bytes() == data
    assert not part.exists()
    assert not store.path.exists()