"""Выбор движка: настоящее C++ ядро (``_p2pcore``) или ``FakeEngine``.

``P2P_FAKE_ENGINE=1`` принудительно включает FakeEngine (разработка без C++).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from p2pshare.engine.errors import P2PError

if TYPE_CHECKING:
    from p2pshare.config import Config


def create_engine(config: Config, *, fake: bool | None = None):
    """Создать движок для приложения. ``fake=None`` — решает переменная окружения."""
    if fake is None:
        fake = os.environ.get("P2P_FAKE_ENGINE") == "1"

    if fake:
        from p2pshare.engine.fake_engine import EngineConfig, FakeEngine

        return FakeEngine(EngineConfig(peer_id=config.peer_id, name=config.name))

    try:
        from p2pshare import _p2pcore
    except ImportError as exc:
        raise RuntimeError(
            "C++ ядро (_p2pcore) не собрано. Для разработки без него задайте "
            "переменную окружения P2P_FAKE_ENGINE=1"
        ) from exc
    engine_config = _p2pcore.EngineConfig()
    engine_config.peer_id = config.peer_id
    engine_config.name = config.name
    return _p2pcore.Engine(engine_config)


__all__ = ["P2PError", "create_engine"]