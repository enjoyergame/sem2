"""Проверка файлового обмена через интерфейс Textual."""

import asyncio

import pytest

from p2pshare.config import Config
from p2pshare.core.app_core import AppCore
from p2pshare.discovery.peer_table import PeerTable
from p2pshare.discovery.udp import UdpDiscovery


async def test_tui_lists_and_downloads_file(tmp_path):
    pytest.importorskip("textual")
    from textual.widgets import Button, DataTable, Input, ProgressBar

    from p2pshare.ui.tui.app import P2PApp

    source = tmp_path / "пример.txt"
    source.write_bytes(b"hello")
    sender = Config.load(tmp_path / "sender", port=0)
    receiver = Config.load(tmp_path / "receiver", port=0)

    async with AppCore(sender, fake=True) as server, AppCore(receiver, fake=True) as client:
        manifest = await server.catalog.share(source)
        peers = PeerTable()
        peers.update(sender.peer_id, "Sender", "127.0.0.1", server.port, 1)
        discovery = UdpDiscovery(
            peers, receiver.peer_id, receiver.name, client.port, 47011, lambda: 0
        )
        app = P2PApp(client, peers, discovery)

        async with app.run_test(size=(100, 40)) as pilot:
            app.query_one("#peers", DataTable).focus()
            await pilot.press("enter")
            assert app.query_one("#address", Input).value == f"127.0.0.1:{server.port}"

            app.query_one("#files", Button).press()
            await pilot.pause()
            await asyncio.wait_for(app._job, 5)

            table = app.query_one("#remote", DataTable)
            assert table.row_count == 1
            table.focus()
            await pilot.press("enter")
            assert app.query_one("#file-id", Input).value == manifest.file_id_hex

            app.query_one("#download", Button).press()
            await pilot.pause()
            await asyncio.wait_for(app._job, 5)

            assert app.query_one(ProgressBar).progress == 100
            assert (receiver.download_dir / source.name).read_bytes() == b"hello"