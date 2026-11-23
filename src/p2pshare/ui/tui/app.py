"""Терминальный интерфейс P2P Share."""

import asyncio
from argparse import ArgumentTypeError
from pathlib import Path

from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import Button, DataTable, Footer, Header, Input, Label, ProgressBar, Static

from p2pshare.core.app_core import AppCore
from p2pshare.core.control_protocol import parse_file_id
from p2pshare.discovery.peer_table import PeerTable
from p2pshare.discovery.udp import UdpDiscovery
from p2pshare.ui.cli import parse_address


class P2PApp(App):
    TITLE = "P2P Share"
    BINDINGS = (("ctrl+q", "quit", "Выход"),)
    CSS = """
    VerticalScroll { padding: 0 2; }
    Label { margin-top: 1; text-style: bold; }
    DataTable { height: 5; }
    Horizontal { height: auto; }
    Horizontal Input { width: 1fr; }
    Button { margin-left: 1; }
    #status { padding: 0 2; }
    """

    def __init__(self, core: AppCore, peers: PeerTable, discovery: UdpDiscovery) -> None:
        super().__init__()
        self.core = core
        self.peers = peers
        self.discovery = discovery
        self._peer_rows = {}
        self._job: asyncio.Task | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with VerticalScroll():
            yield Label("Пиры в локальной сети")
            yield DataTable(id="peers", cursor_type="row")
            with Horizontal():
                yield Input(placeholder="IP:порт пира", id="address")
                yield Button("Файлы пира", id="files")
            yield DataTable(id="remote", cursor_type="row")
            with Horizontal():
                yield Input(placeholder="ID файла", id="file-id")
                yield Button("Скачать / продолжить", id="download", variant="primary")
                yield Button("Пауза", id="pause", disabled=True)
            yield ProgressBar(total=100, show_eta=False)
            yield Label("Мои раздачи")
            yield Static("Раздач пока нет.", id="shares")
            with Horizontal():
                yield Input(placeholder="Путь к файлу", id="path")
                yield Button("Раздать", id="share")
        yield Static(id="status")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#peers", DataTable).add_columns("Имя", "Адрес", "Раздач")
        self.query_one("#remote", DataTable).add_columns("Файл", "Байт", "ID")
        self.query_one("#status", Static).update(f"Мой TCP-порт: {self.core.port}")
        self.refresh_peers()
        self.set_interval(1, self.refresh_peers)

    def refresh_peers(self) -> None:
        rows = {f"{p.host}:{p.port}": (p.name, p.shares) for p in self.peers.snapshot()}
        if rows == self._peer_rows:
            return
        self._peer_rows = rows
        table = self.query_one("#peers", DataTable)
        table.clear()
        for address, (name, shares) in rows.items():
            table.add_row(Text(name), address, str(shares), key=address)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        field = "#address" if event.data_table.id == "peers" else "#file-id"
        self.query_one(field, Input).value = event.row_key.value or ""

    def on_button_pressed(self, event: Button.Pressed) -> None:
        action = event.button.id
        if action == "pause":
            if self._job is not None:
                self._job.cancel()
        elif self._job is None or self._job.done():
            self._job = asyncio.create_task(self._run_action(action))

    async def stop_job(self) -> None:
        if self._job is not None and not self._job.done():
            self._job.cancel()
            await asyncio.gather(self._job, return_exceptions=True)

    async def _run_action(self, action: str) -> None:
        status = self.query_one("#status", Static)
        pause = self.query_one("#pause", Button)
        buttons = [self.query_one(f"#{name}", Button) for name in ("files", "share", "download")]
        for button in buttons:
            button.disabled = True
        pause.disabled = action != "download"
        status.update("Выполняется…")

        try:
            if action == "share":
                value = self.query_one("#path", Input).value.strip().strip('"')
                if not value:
                    raise ValueError("Укажи путь к файлу.")
                await self.core.catalog.share(Path(value).expanduser())
                items = self.core.catalog.items()
                self.query_one("#shares", Static).update(
                    Text("\n".join(f"{item['name']} · {item['size']} байт" for item in items))
                )
                self.discovery.announce()
                status.update("Раздача запущена.")
            else:
                host, port = parse_address(self.query_one("#address", Input).value.strip())
                if action == "files":
                    count = await self._list_files(host, port)
                    status.update("Выбери файл в таблице." if count else "У пира нет раздач.")
                else:
                    target = await self._download(host, port)
                    status.update(Text(f"Сохранено: {target}"))
        except asyncio.CancelledError:
            status.update("Загрузка приостановлена. Нажми «Скачать / продолжить».")
            raise
        except (ArgumentTypeError, OSError, RuntimeError, ValueError) as exc:
            status.update(Text(f"Ошибка: {exc}"))
        finally:
            pause.disabled = True
            for button in buttons:
                button.disabled = False

    async def _list_files(self, host: str, port: int) -> int:
        table = self.query_one("#remote", DataTable)
        table.clear()
        self.query_one("#file-id", Input).value = ""
        connection = await self.core.connections.connect(host, port)
        items = await self.core.control.list_files(connection.conn_id)
        for item in items:
            file_id = item["file_id"]
            if file_id not in table.rows:
                table.add_row(Text(item["name"]), str(item["size"]), file_id, key=file_id)
        return table.row_count

    async def _download(self, host: str, port: int) -> Path:
        file_id = parse_file_id(self.query_one("#file-id", Input).value.strip())
        bar = self.query_one(ProgressBar)
        bar.update(progress=0)

        def progress(done: int, total: int) -> None:
            bar.update(progress=100 * done / total if total else 100)

        return await self.core.download(host, port, file_id, progress)