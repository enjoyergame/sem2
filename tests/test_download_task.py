import pytest

from p2pshare.config import Config
from p2pshare.core.app_core import AppCore
from p2pshare.storage.paths import download_path


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
        assert list(receiver.download_dir.glob("*.part")) == []

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

    assert list(receiver.download_dir.iterdir()) == []