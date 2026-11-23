"""Обнаружение пиров через UDP multicast и broadcast в локальной сети."""

import asyncio
import json
import logging
import socket
from collections.abc import Callable

from p2pshare.discovery.peer_table import PeerTable

GROUP = "239.255.42.11"
MAGIC = "P2PF-DISC"
MAX_PACKET = 2048
logger = logging.getLogger(__name__)


class UdpDiscovery(asyncio.DatagramProtocol):
    def __init__(
        self,
        peers: PeerTable,
        peer_id: bytes,
        name: str,
        tcp_port: int,
        port: int,
        share_count: Callable[[], int],
    ) -> None:
        self.peers = peers
        self.peer_id = peer_id
        self.name = name
        self.tcp_port = tcp_port
        self.port = port
        self.share_count = share_count
        self.transport: asyncio.DatagramTransport | None = None
        self._announcements: asyncio.Task | None = None

    async def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.bind(("", self.port))
            self.port = sock.getsockname()[1]
            self._join_multicast(sock)
            await asyncio.get_running_loop().create_datagram_endpoint(lambda: self, sock=sock)
        except BaseException:
            sock.close()
            raise

        self._send("query")
        self.announce()
        self._announcements = asyncio.create_task(self._periodic())

    @staticmethod
    def _join_multicast(sock: socket.socket) -> None:
        group = socket.inet_aton(GROUP)
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            try:
                sock.setsockopt(
                    socket.IPPROTO_IP,
                    socket.IP_ADD_MEMBERSHIP,
                    group + socket.inet_aton("0.0.0.0"),
                )
            except OSError:
                loopback = socket.inet_aton("127.0.0.1")
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, loopback)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group + loopback)
        except OSError as exc:
            logger.debug("Не удалось подключить multicast: %s", exc)

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    async def stop(self) -> None:
        if self._announcements is not None:
            self._announcements.cancel()
            await asyncio.gather(self._announcements, return_exceptions=True)
            self._announcements = None

        if self.transport is not None:
            self.transport.close()
            self.transport = None

    async def _periodic(self) -> None:
        while True:
            await asyncio.sleep(3.0)
            self.announce()
            self.peers.snapshot()

    def announce(self) -> None:
        self._send("announce")

    def _send(self, kind: str, host: str | None = None) -> None:
        if self.transport is None:
            return

        payload = json.dumps(
            {
                "magic": MAGIC,
                "v": 1,
                "type": kind,
                "peer_id": self.peer_id.hex(),
                "name": self.name[:255],
                "tcp_port": self.tcp_port,
                "shares": self.share_count(),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()

        for address in (host,) if host else (GROUP, "255.255.255.255"):
            self.transport.sendto(payload, (address, self.port))

    def error_received(self, exc: Exception) -> None:
        logger.debug("Ошибка UDP discovery: %s", exc)

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if len(data) > MAX_PACKET:
            return

        try:
            msg = json.loads(data)
            if (
                not isinstance(msg, dict)
                or msg.get("magic") != MAGIC
                or type(msg.get("v")) is not int
                or msg["v"] != 1
            ):
                return
            peer_id = bytes.fromhex(msg.get("peer_id", ""))
        except (ValueError, TypeError, RecursionError):
            return

        if len(peer_id) != 16 or peer_id == self.peer_id:
            return
        kind = msg.get("type")
        if kind not in ("announce", "query"):
            return

        name, port, shares = msg.get("name"), msg.get("tcp_port"), msg.get("shares")
        if (
            isinstance(name, str)
            and 0 < len(name) <= 255
            and name.isprintable()
            and type(port) is int
            and 1 <= port <= 65535
            and type(shares) is int
            and shares >= 0
        ):
            self.peers.update(peer_id, name, addr[0], port, shares)
            if kind == "query":
                self._send("announce", addr[0])