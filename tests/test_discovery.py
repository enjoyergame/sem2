import json

from p2pshare.discovery.peer_table import PeerTable
from p2pshare.discovery.udp import UdpDiscovery


def test_discovery_ignores_self_and_invalid_packets():
    table = PeerTable()
    discovery = UdpDiscovery(table, b"a" * 16, "Alice", 47010, 47011, lambda: 0)
    address = ("192.168.1.8", 47011)
    packet = {
        "magic": "P2PF-DISC",
        "v": 1,
        "type": "announce",
        "peer_id": (b"a" * 16).hex(),
        "name": "Alice",
        "tcp_port": 47010,
        "shares": 0,
    }

    discovery.datagram_received(b"{invalid", address)
    discovery.datagram_received(json.dumps(packet).encode(), address)
    packet.update(peer_id=(b"b" * 16).hex(), name="Боб", tcp_port=70000)
    discovery.datagram_received(json.dumps(packet).encode(), address)
    assert table.snapshot() == []

    packet.update(tcp_port=47012, shares=2)
    discovery.datagram_received(json.dumps(packet).encode(), address)
    (peer,) = table.snapshot()
    assert (peer.name, peer.host, peer.port, peer.shares) == (
        "Боб", "192.168.1.8", 47012, 2
    )


def test_peer_expires_after_ttl(monkeypatch):
    now = 100.0
    monkeypatch.setattr("p2pshare.discovery.peer_table.monotonic", lambda: now)
    table = PeerTable()
    table.update(b"b" * 16, "Bob", "192.168.1.8", 47012, 0)

    now += 9.0
    assert len(table.snapshot()) == 1

    now += 1.0
    assert table.snapshot() == []