"""Общее исключение движка: тот же класс, что и в C++ модуле, если он собран."""

try:
    from p2pshare._p2pcore import P2PError
except ImportError:  # C++ ядро ещё не собрано — работаем на FakeEngine

    class P2PError(RuntimeError):  # type: ignore[no-redef]
        """Синхронная ошибка движка (неверный аргумент, не удалось подключиться)."""


__all__ = ["P2PError"]