"""Запросы каталога и манифеста поверх событий CONTROL."""

import asyncio
import json
from contextlib import suppress
from itertools import count

from p2pshare.core.catalog import Catalog
from p2pshare.engine import P2PError
from p2pshare.engine.events import Event, EventType
from p2pshare.storage.manifest import Manifest

MAX_CONTROL_SIZE = 16 * 1024 * 1024


def parse_file_id(value: object) -> bytes:
    if not isinstance(value, str) or len(value) != 64:
        raise ValueError("Ожидается file_id из 64 шестнадцатеричных символов")
    file_id = bytes.fromhex(value)
    if len(file_id) != 32:
        raise ValueError("Некорректный file_id")
    return file_id


class ControlProtocol:
    def __init__(self, engine, catalog: Catalog) -> None:
        self._engine = engine
        self._catalog = catalog
        self._ids = count(1)
        self._pending: dict[tuple[int, int], asyncio.Future[dict]] = {}

    async def list_files(self, conn_id: int) -> list[dict]:
        response = await self._request(conn_id, {"t": "list"}, "files")
        items = response.get("items")
        if not isinstance(items, list):
            raise P2PError("Пир прислал некорректный список файлов")

        for item in items:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("name"), str)
                or type(item.get("size")) is not int
                or item["size"] < 0
            ):
                raise P2PError("Пир прислал некорректную запись о файле")
            parse_file_id(item.get("file_id"))

        return items

    async def get_manifest(self, conn_id: int, file_id: bytes) -> Manifest:
        response = await self._request(
            conn_id, {"t": "get_manifest", "file_id": file_id.hex()}, "manifest"
        )
        return await asyncio.to_thread(
            Manifest.from_dict, response.get("manifest"), expected_file_id=file_id
        )

    async def _request(self, conn_id: int, message: dict, expected: str) -> dict:
        request_id = next(self._ids)
        key = (conn_id, request_id)
        reply = asyncio.get_running_loop().create_future()
        self._pending[key] = reply

        try:
            self._send(conn_id, dict(message, id=request_id))
            try:
                response = await asyncio.wait_for(reply, timeout=5)
            except TimeoutError as exc:
                raise P2PError("Пир не ответил на запрос метаданных") from exc

            if response.get("t") == "error":
                raise P2PError(f"Запрос метаданных не выполнен: {response.get('code')!r}")
            if response.get("t") != expected:
                raise P2PError("Пир прислал ответ неожиданного типа")
            return response
        finally:
            self._pending.pop(key, None)
            reply.cancel()

    def _send(self, conn_id: int, message: dict) -> None:
        data = json.dumps(message, separators=(",", ":")).encode("ascii")
        if len(data) > MAX_CONTROL_SIZE:
            raise P2PError("Метаданные превышают лимит кадра")
        self._engine.send_control(conn_id, data)

    def handle_event(self, event: Event) -> None:
        if event.type == EventType.PEER_DISCONNECTED:
            for (conn_id, _), reply in self._pending.items():
                if conn_id == event.conn_id and not reply.done():
                    reply.set_result({"t": "error", "code": "disconnected"})
            return

        if event.type != EventType.CONTROL or len(event.data) > MAX_CONTROL_SIZE:
            return

        try:
            message = json.loads(event.data)
        except (ValueError, RecursionError):
            return
        if not isinstance(message, dict) or type(message.get("id")) is not int:
            return

        request_id = message["id"]
        kind = message.get("t")
        if kind == "list":
            response = {"t": "files", "items": self._catalog.items()}
        elif kind == "get_manifest":
            try:
                manifest = self._catalog.get(parse_file_id(message.get("file_id")))
            except ValueError:
                manifest = None
            response = (
                {"t": "manifest", "manifest": manifest.to_dict()}
                if manifest is not None
                else {"t": "error", "code": "unknown_file"}
            )
        elif kind in ("files", "manifest", "error"):
            reply = self._pending.get((event.conn_id, request_id))
            if reply is not None and not reply.done():
                reply.set_result(message)
            return
        else:
            return

        response["id"] = request_id
        try:
            self._send(event.conn_id, response)
        except P2PError:
            with suppress(P2PError):
                self._send(event.conn_id, {"t": "error", "id": request_id, "code": "send_failed"})