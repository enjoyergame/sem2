"""Таблица пиров, обнаруженных в локальной сети."""

from dataclasses import dataclass
from time import monotonic


@dataclass(frozen=True, slots=True)
class PeerInfo:
    peer_id: bytes
    name: str
    host: str
    port: int
    shares: int
    last_seen: float


class PeerTable:
    def __init__(self, ttl: float = 10.0) -> None:
        self.ttl = ttl
        self._peers: dict[bytes, PeerInfo] = {}

    def update(self, peer_id: bytes, name: str, host: str, port: int, shares: int) -> None:
        self._peers[peer_id] = PeerInfo(peer_id, name, host, port, shares, monotonic())

    def snapshot(self) -> list[PeerInfo]:
        now = monotonic()
        for peer_id, peer in list(self._peers.items()):
            if now - peer.last_seen >= self.ttl:
                del self._peers[peer_id]
        return sorted(self._peers.values(), key=lambda peer: (peer.name.casefold(), peer.peer_id))