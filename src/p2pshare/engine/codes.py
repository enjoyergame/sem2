"""Числовые коды в событиях (поле ``Event.code`` и ``Event.value``).

Значения согласованы с C++ ядром (см. ``docs/API.md``).
"""

# PEER_DISCONNECTED.code
DISCONNECT_LOCAL = 0  # закрыли мы
DISCONNECT_REMOTE = 1  # закрыл собеседник
DISCONNECT_FAULT = 2  # обрыв / таймаут keepalive

# CHUNK_FAILED.code (первые три совпадают с RejectReason из protocol.hpp)
FAIL_UNKNOWN_FILE = 1
FAIL_NOT_HAVE = 2
FAIL_BUSY = 3
FAIL_CRC = 4
FAIL_DISCONNECT = 5
FAIL_TIMEOUT = 6
FAIL_DISK = 7  # не удалось записать в .part (есть только у настоящего ядра)

# PROGRESS.value — направление передачи
DIR_DOWNLOAD = 0
DIR_UPLOAD = 1