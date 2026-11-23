"""Проверка имён файлов из манифестов."""

from pathlib import Path

_INVALID = '<>:"/\\|?*'
_RESERVED = {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"} | {
    f"{prefix}{digit}" for prefix in ("COM", "LPT") for digit in "123456789¹²³"
}


def download_path(directory: Path, name: str) -> Path:
    base_name = name.partition(".")[0].rstrip().upper()
    if (
        not name
        or name.endswith((" ", "."))
        or any(char in _INVALID or not char.isprintable() for char in name)
        or base_name in _RESERVED
    ):
        raise ValueError(f"Недопустимое имя файла: {name!r}")
    return directory / name