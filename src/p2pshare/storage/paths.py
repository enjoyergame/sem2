"""Проверка имён файлов из манифестов."""

import os
from contextlib import contextmanager
from pathlib import Path

_INVALID = '<>:"/\\|?*'
_RESERVED = {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"} | {
    f"{prefix}{digit}" for prefix in ("COM", "LPT") for digit in "123456789¹²³"
}


def download_path(directory: Path, name: str) -> Path:
    base_name = name.partition(".")[0].rstrip().upper()
    if (
        not name
        or name.casefold() == ".p2pshare"
        or name.endswith((" ", "."))
        or any(char in _INVALID or not char.isprintable() for char in name)
        or base_name in _RESERVED
    ):
        raise ValueError(f"Недопустимое имя файла: {name!r}")
    return directory / name


@contextmanager
def download_lock(directory: Path):
    # ОС освобождает блокировку при закрытии файла или завершении процесса.
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink():
        raise ValueError(f"Служебная папка не должна быть ссылкой: {directory}")

    with (directory / "download.lock").open("a+b") as lock:
        lock.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise OSError("Не удалось заблокировать папку загрузок") from exc
        yield