"""Ручное подключение и измерение RTT по событиям движка."""

import asyncio
from dataclasses import dataclass

from p2pshare.engine import P2PError
from p2pshare.engine.events import Event, EventType


@dataclass(slots=True)
class Connection:
    conn_id: int
    peer_id: bytes
    name: str


class ConnectionManager:
    def __init__(self, engine) -> None:
        self.engine = engine
        self.connections: dict[int, Connection] = {}
        self._connected = asyncio.Event()
        self._pings: dict[int, asyncio.Future[float | None]] = {}

    async def connect(self, host: str, port: int) -> Connection:
        conn_id = await asyncio.to_thread(self.engine.connect, host, port)
        try:
            async with asyncio.timeout(3):
                while conn_id not in self.connections:
                    await self._connected.wait()
        except TimeoutError as exc:
            raise P2PError("Соединение закрылось до обработки HELLO") from exc
        return self.connections[conn_id]

    async def ping(self, connection: Connection) -> float:
        conn_id = connection.conn_id
        if conn_id in self._pings:
            raise P2PError("Для этого соединения уже выполняется ping")

        reply = asyncio.get_running_loop().create_future()
        self._pings[conn_id] = reply
        try:
            await asyncio.to_thread(self.engine.ping, conn_id)
            try:
                rtt = await asyncio.wait_for(reply, timeout=5)
            except TimeoutError as exc:
                raise P2PError("Пир не ответил на ping") from exc
            if rtt is None:
                raise P2PError("Соединение закрылось во время ping")
            return rtt
        finally:
            self._pings.pop(conn_id, None)
            reply.cancel()

    def handle_event(self, event: Event) -> None:
        if event.type == EventType.PEER_CONNECTED:
            self.connections[event.conn_id] = Connection(
                event.conn_id, event.data[:16], event.data[16:].decode(errors="replace")
            )
            self._connected.set()
            self._connected.clear()
        elif event.type == EventType.PEER_DISCONNECTED:
            self.connections.pop(event.conn_id, None)
            self._resolve_ping(event.conn_id, None)
        elif event.type == EventType.PONG:
            self._resolve_ping(event.conn_id, event.value)

    def _resolve_ping(self, conn_id: int, rtt: float | None) -> None:
        reply = self._pings.get(conn_id)
        if reply is not None and not reply.done():
            reply.set_result(rtt)