"""Передаёт события движка в цикл asyncio."""

import asyncio
from collections.abc import Callable

from p2pshare.engine.events import Event


async def pump_events(engine, handle: Callable[[Event], None], stopping: asyncio.Event) -> None:
    while not stopping.is_set():
        events = await asyncio.to_thread(engine.wait_events, 100, 512)
        for event in events:
            handle(event)