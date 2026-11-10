# API C++ ядра (`p2pshare._p2pcore`)

Контракт №2 между C++ и Python. Модуль повторяет API `FakeEngine`
(`src/p2pshare/engine/fake_engine.py`), поэтому остальной Python-код не знает, с каким движком
работает. Код: `cpp/include/p2pcore/engine.hpp`, привязки — `cpp/bindings/module.cpp`.

## Сборка модуля без pip

Для проверки ядра из Python до перехода `pyproject.toml` на scikit-build-core:

```
cmake -S . -B build/py -G Ninja -DP2P_BUILD_PYTHON=ON -DCMAKE_BUILD_TYPE=Release ^
      -DPython_EXECUTABLE=venv/Scripts/python.exe -Dpybind11_DIR=<вывод python -m pybind11 --cmakedir>
cmake --build build/py
```

Получится `build/py/_p2pcore.cp3XX-win_amd64.pyd`. На MinGW он собирается со статическим
рантаймом C++ и зависит только от системных DLL и `python3XX.dll`.

C++-тесты: `cmake -S . -B build/tests -G Ninja -DP2P_BUILD_TESTS=ON`, затем
`cmake --build build/tests` и `ctest --test-dir build/tests --output-on-failure`.

## Классы

`EngineConfig()` — поля: `peer_id: bytes` (ровно 16, иначе `ValueError`), `name: str`,
`max_inflight` (6), `keepalive_ms` (5000), `idle_timeout_ms` (15000),
`request_timeout_ms` (30000; 0 — выкл.) — таймаут ответа на `REQUEST`, см. ниже,
`fault_drop_after_bytes` (0 — выкл.), `fault_corrupt_chunk` (-1 — выкл.),
`fault_stall_chunk` (-1 — выкл.) — отдающий один раз не отвечает на `REQUEST` чанка N
(для проверки таймаута запроса).

`Engine(config)` — методы как у `FakeEngine`. Все методы, которые могут ждать, отпускают GIL.
Синхронные ошибки — `P2PError` (наследник `RuntimeError`, тот же класс подхватывает
`p2pshare.engine.errors`). Поддерживает `with Engine(cfg) as e:` (`stop()` при выходе).

| Метод | Ошибки `P2PError` |
| --- | --- |
| `start(port=0) -> int` | уже запущен; порт занят |
| `stop()` | — (идемпотентно) |
| `connect(host, port, timeout_ms=5000) -> conn_id` | не запущен; отказ; таймаут; подключение к себе |
| `disconnect(conn_id)`, `ping(conn_id)`, `send_control(conn_id, payload)` | не запущен; неизвестное соединение |
| `share_file(file_id, path, size, chunk_size)` | `file_id` не 16 байт; файла нет; `chunk_size` = 0 |
| `open_download(file_id, part_path, size, chunk_size, have)` | неверная длина/хвост `have`; не удалось открыть |
| `set_have(file_id, i)` | неизвестный файл; индекс вне файла |
| `send_bitfield(conn_id, file_id)` | неизвестное соединение или файл |
| `request_chunks(conn_id, file_id, indices)` | неизвестный файл; файл не открыт через `open_download` |
| `cancel_chunks(conn_id, file_id, indices)` | неизвестное соединение |
| `flush(file_id)` | неизвестный файл; ошибка диска |
| `close_file(file_id)` | — (неизвестный файл молча игнорируется) |
| `set_rate_limit(up_bps, down_bps)` | — |
| `poll_events(max=512)`, `wait_events(timeout_ms=100, max=512)` | — |
| `stats() -> dict` | — (`connections`, `files`, `bytes_sent`, `bytes_received`, `upload_limit`, `download_limit`) |

Пути принимают `str` и `os.PathLike`, кириллица работает.

## События

`Event`: `type`, `conn_id`, `file_id` (`bytes`, 16 или `b""`), `chunk_index`, `bytes_done`,
`value`, `code`, `data` (`bytes`), `message` (`str`). `type` — `_p2pcore.EventType`,
настоящий `enum.IntEnum`: сравнивается с `p2pshare.engine.events.EventType` и с `int`.

| Тип | Поля |
| --- | --- |
| 1 `PEER_CONNECTED` | `conn_id`; `data` = peer_id(16) + имя (UTF-8); `code` = 1 входящее, 0 исходящее |
| 2 `PEER_DISCONNECTED` | `conn_id`; `code` — причина (ниже); `message` — текст |
| 3 `PONG` | `conn_id`; `value` = RTT в мс |
| 4 `CONTROL` | `conn_id`; `data` = JSON-байты |
| 5 `BITFIELD` | `conn_id`, `file_id`; `data` = карта (длину проверяет Python) |
| 6 `HAVE` | `conn_id`, `file_id`, `chunk_index` |
| 7 `CHUNK_RECEIVED` | `conn_id`, `file_id`, `chunk_index` — CRC сошёлся, данные в `.part` |
| 8 `CHUNK_FAILED` | `conn_id`, `file_id`, `chunk_index`, `code` — причина (ниже) |
| 9 `PROGRESS` | `file_id`; `bytes_done` — сколько байт нагрузки передано по файлу (накопительно); `value` — направление |
| 10 `LOG` | `code` — уровень как в `logging` (10/20/30/40); `message` |
| 11 `ERROR` | `code`, `message` (сейчас не используется) |

Коды совпадают с `src/p2pshare/engine/codes.py`:

- `PEER_DISCONNECTED.code`: 0 `DISCONNECT_LOCAL`, 1 `DISCONNECT_REMOTE` (пришёл `BYE`),
  2 `DISCONNECT_FAULT` (обрыв, таймаут, нарушение протокола);
- `CHUNK_FAILED.code`: 1 `UNKNOWN_FILE`, 2 `NOT_HAVE`, 3 `BUSY`, 4 `CRC`, 5 `DISCONNECT`,
  6 `TIMEOUT` — на `REQUEST` не ответили за `request_timeout_ms`, **7 `DISK`** — не удалось записать в `.part`
  (**нового кода нет в `codes.py`**: Python должен считать незнакомый код обычным провалом);
- `PROGRESS.value`: 0 `DIR_DOWNLOAD`, 1 `DIR_UPLOAD`.

## Чем настоящий движок отличается от FakeEngine

Python-код, который работает с обоими движками, должен учитывать:

1. **События приходят асинхронно.** После `request_chunks`, `send_control`, `ping`, `disconnect`
   события собеседника и результаты появляются не сразу — их нужно ждать через
   `wait_events`. Синхронно (до возврата из метода) приходит только `PEER_CONNECTED`
   своей стороны после `connect` и `PEER_DISCONNECTED(LOCAL)` после `disconnect`/`stop`.
2. **`PROGRESS` — не чаще 10 раз в секунду** на файл и направление, а не одно событие на
   `request_chunks`. Последнее событие несёт полный счётчик.
3. **`PONG` приходит и сам по себе**: keepalive-`PING` раз в `keepalive_ms` даёт `PONG` с RTT.
4. **Провалы при разрыве** (`CHUNK_FAILED`, код 5) приходят раньше `PEER_DISCONNECTED`.
5. **`cancel_chunks` событий не даёт**; повторный запрос того же чанка, пока он «в полёте», игнорируется.
6. **`set_rate_limit(upload_bps, download_bps)` действительно ограничивает скорость** (FakeEngine
   значения только запоминает). Лимиты общие для движка: все соединения делят один лимит отдачи
   и один лимит приёма; считаются только данные чанков. 0 — без ограничения; значения меньше
   1024 байт/с поднимаются до 1024. Новый лимит действует сразу, в том числе на идущие передачи.
   После простоя до 256 КиБ уходят без ожидания (запас token bucket), дальше — ровно с заданной
   скоростью. При низкой скорости данные идут мелкими порциями (около 1/8 секундного объёма,
   не меньше 512 байт), поэтому таймауты тишины и запроса не срабатывают. Текущие лимиты —
   в `stats()`: `upload_limit`, `download_limit` (у FakeEngine этих ключей нет).
7. **Таймаут запроса.** Если пир жив (отвечает на `PING`), но за `request_timeout_ms` не прислал
   ни байта ответа на `REQUEST`, приходят: `CHUNK_FAILED` с кодом 6 `TIMEOUT` для зависших
   запросов, `CHUNK_FAILED` с кодом 5 `DISCONNECT` для остальных запросов этого соединения,
   затем `PEER_DISCONNECTED` (`FAULT`, `message` = `"request timeout"`). Соединение закрыто:
   чанки нужно вернуть в очередь и запросить заново (у другого пира или после переподключения).
   В FakeEngine полей `request_timeout_ms` и `fault_stall_chunk` нет.
7. **Подключение к самому себе** отклоняется с `P2PError` и без событий.
8. **После `close_file`** файл закрыт сразу: на Windows `.part` можно переименовать.
