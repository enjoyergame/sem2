"""Команда поиска пиров в локальной сети."""

import argparse
import asyncio
from pathlib import Path

from p2pshare.config import Config
from p2pshare.discovery.peer_table import PeerTable
from p2pshare.discovery.udp import UdpDiscovery
from p2pshare.engine import create_engine


async def find_peers(args: argparse.Namespace) -> None:
    config = Config.load(args.data_dir, port=args.port, name=args.name)
    engine = create_engine(config)
    port = await asyncio.to_thread(engine.start, config.tcp_port)
    peers = PeerTable()
    discovery = UdpDiscovery(
        peers, config.peer_id, config.name, port, config.discovery_port, lambda: 0
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
        await asyncio.to_thread(engine.stop)


def main() -> int:
    parser = argparse.ArgumentParser(prog="p2pshare")
    commands = parser.add_subparsers(dest="command", required=True)
    peers = commands.add_parser("peers", help="Поиск пиров в локальной сети")
    peers.add_argument("--data-dir", type=Path)
    peers.add_argument("--port", type=int)
    peers.add_argument("--name")
    peers.add_argument("--wait", type=float, default=4.0)
    args = parser.parse_args()

    try:
        asyncio.run(find_peers(args))
    except KeyboardInterrupt:
        return 130
    except (OSError, RuntimeError, ValueError) as exc:
        parser.exit(1, f"Ошибка: {exc}\n")
    return 0