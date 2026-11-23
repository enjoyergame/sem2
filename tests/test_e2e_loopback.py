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