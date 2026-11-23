"""Команды поиска пиров и проверки соединения."""

import argparse
import asyncio
from pathlib import Path

from p2pshare.config import Config
from p2pshare.core.app_core import AppCore
from p2pshare.discovery.peer_table import PeerTable
from p2pshare.discovery.udp import UdpDiscovery


async def find_peers(args: argparse.Namespace) -> None:
    config = Config.load(args.data_dir, port=args.port, name=args.name)
    async with AppCore(config) as app:
        peers = PeerTable()
        discovery = UdpDiscovery(
            peers, config.peer_id, config.name, app.port, config.discovery_port, lambda: 0
        )
        try:
            await discovery.start()
            await asyncio.sleep(args.wait)
            found = peers.snapshot()
            if not found:
                print("Пиры не найдены.")
            for peer in found:
                print(f"{peer.name}  {peer.host}:{peer.port}  раздач: {peer.shares}")
        finally:
            await discovery.stop()


def parse_address(value: str) -> tuple[str, int]:
    host, sep, port_text = value.rpartition(":")
    try:
        port = int(port_text)
    except ValueError:
        port = 0
    if not sep or not host or not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("Используй адрес вида 127.0.0.1:47010")
    return host, port


async def ping_peer(args: argparse.Namespace) -> None:
    config = Config.load(args.data_dir, port=args.port, name=args.name)
    async with AppCore(config) as app:
        connection = await app.connections.connect(*args.address)
        rtt = await app.connections.ping(connection)
        host, port = args.address
        name = "".join(char for char in connection.name if char.isprintable())
        print(f"{name} ({host}:{port})  RTT: {rtt:.2f} мс")


async def share_file(args: argparse.Namespace) -> None:
    config = Config.load(args.data_dir, port=args.port, name=args.name)
    async with AppCore(config) as app:
        manifest = await app.catalog.share(args.path)
        print(f"Раздаю {manifest.name} на TCP-порту {app.port}", flush=True)
        print(f"ID файла: {manifest.file_id_hex}", flush=True)
        await asyncio.Event().wait()


async def list_peer_files(args: argparse.Namespace) -> None:
    config = Config.load(args.data_dir, port=args.port, name=args.name)
    async with AppCore(config) as app:
        connection = await app.connections.connect(*args.address)
        items = await app.control.list_files(connection.conn_id)
        if not items:
            print("У пира нет раздач.")
        for item in items:
            print(f"{item['file_id']}  {item['name']!r}  {item['size']} байт")



def main() -> int:
    parser = argparse.ArgumentParser(prog="p2pshare")
    commands = parser.add_subparsers(dest="command", required=True)

    peers = commands.add_parser("peers", help="Поиск пиров в локальной сети")
    peers.add_argument("--wait", type=float, default=4.0)
    peers.set_defaults(run=find_peers)

    ping = commands.add_parser("ping", help="Проверить соединение с пиром")
    ping.add_argument("address", type=parse_address, metavar="HOST:PORT")
    ping.set_defaults(run=ping_peer)


    share = commands.add_parser("share", help="Добавить файл в раздачу")
    share.add_argument("path", type=Path)
    share.set_defaults(run=share_file)

    files = commands.add_parser("files", help="Получить список раздач пира")
    files.add_argument("address", type=parse_address, metavar="HOST:PORT")
    files.set_defaults(run=list_peer_files)

    for command in (peers, ping, share, files):
        command.add_argument("--data-dir", type=Path)
        command.add_argument("--port", type=int, default=0 if command in (ping, files) else None)
        command.add_argument("--name")

    args = parser.parse_args()
    try:
        asyncio.run(args.run(args))
    except KeyboardInterrupt:
        return 130
    except (OSError, RuntimeError, ValueError) as exc:
        parser.exit(1, f"Ошибка: {exc}\n")
    return 0