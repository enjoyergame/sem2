"""Битовая карта чанков: какие чанки файла у нас уже есть (и проверены).

Формат совпадает с ``BITFIELD`` протокола: MSB-first, ``ceil(n / 8)`` байт,
хвостовые (лишние) биты последнего байта равны 0.
"""

from __future__ import annotations

from collections.abc import Iterator


class Bitfield:
    __slots__ = ("_data", "_size")

    def __init__(self, size: int, data: bytes | bytearray | None = None) -> None:
        if size < 0:
            raise ValueError("size must be >= 0")
        self._size = size
        nbytes = (size + 7) // 8
        if data is None:
            self._data = bytearray(nbytes)
        else:
            if len(data) != nbytes:
                raise ValueError(f"expected {nbytes} bytes for {size} bits, got {len(data)}")
            self._data = bytearray(data)
            if size % 8 and self._data and self._data[-1] & (0xFF >> (size % 8)):
                raise ValueError("trailing bits must be zero")

    # --- конструкторы

    @classmethod
    def full(cls, size: int) -> Bitfield:
        bf = cls(size)
        for i in range(size):
            bf.set(i)
        return bf

    @classmethod
    def from_bytes(cls, data: bytes | bytearray, size: int) -> Bitfield:
        return cls(size, data)

    # --- доступ

    def __len__(self) -> int:
        return self._size

    def _check(self, index: int) -> None:
        if not 0 <= index < self._size:
            raise IndexError(f"bit {index} out of range 0..{self._size - 1}")

    def get(self, index: int) -> bool:
        self._check(index)
        return bool(self._data[index >> 3] & (0x80 >> (index & 7)))

    __getitem__ = get

    def set(self, index: int) -> None:
        self._check(index)
        self._data[index >> 3] |= 0x80 >> (index & 7)

    def clear(self, index: int) -> None:
        self._check(index)
        self._data[index >> 3] &= ~(0x80 >> (index & 7)) & 0xFF

    def count(self) -> int:
        return int.from_bytes(self._data, "big").bit_count()

    def is_complete(self) -> bool:
        return self.count() == self._size

    def indices(self) -> Iterator[int]:
        """Номера установленных битов по возрастанию."""
        return (i for i in range(self._size) if self.get(i))

    def missing(self) -> list[int]:
        """Номера неустановленных битов."""
        return [i for i in range(self._size) if not self.get(i)]

    def needed_from(self, peer_have: Bitfield) -> list[int]:
        """Чанки, которые есть у пира, но нет у нас: ``peer_have & ~self``."""
        if len(peer_have) != self._size:
            raise ValueError("bitfields have different sizes")
        return [i for i in range(self._size) if peer_have.get(i) and not self.get(i)]

    def to_bytes(self) -> bytes:
        return bytes(self._data)

    def copy(self) -> Bitfield:
        return Bitfield(self._size, self._data)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Bitfield):
            return NotImplemented
        return self._size == other._size and self._data == other._data

    def __repr__(self) -> str:
        return f"Bitfield({self.count()}/{self._size})"