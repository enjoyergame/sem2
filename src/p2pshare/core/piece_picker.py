"""Выбор чанков для запроса: какие и у кого.

C++ ядро ничего не решает за нас: оно просто скачивает то, что попросили. Стратегия
живёт здесь, за интерфейсом ``PiecePicker``, поэтому в курсовой достаточно подставить
``RarestFirstPicker`` без изменений в остальном коде.
"""

from __future__ import annotations

from typing import Protocol

from p2pshare.storage.bitfield import Bitfield


class PiecePicker(Protocol):
    def pick(
        self,
        peer_id: str,
        peer_have: Bitfield,
        my_have: Bitfield,
        in_flight: set[int],
        limit: int,
    ) -> list[int]:
        """До ``limit`` номеров чанков, которые стоит запросить у пира ``peer_id``."""
        ...


class SequentialPicker:
    """Семестровая стратегия: по порядку, то, что есть у пира, нет у нас и не в полёте."""

    def pick(
        self,
        peer_id: str,
        peer_have: Bitfield,
        my_have: Bitfield,
        in_flight: set[int],
        limit: int,
    ) -> list[int]:
        if len(peer_have) != len(my_have):
            raise ValueError("bitfields have different sizes")
        picked: list[int] = []
        if limit <= 0:
            return picked
        for index in range(len(my_have)):
            if peer_have.get(index) and not my_have.get(index) and index not in in_flight:
                picked.append(index)
                if len(picked) >= limit:
                    break
        return picked