import pytest

from p2pshare.storage.bitfield import Bitfield


def test_msb_first_layout():
    bf = Bitfield(10)
    bf.set(0)
    bf.set(9)
    assert bf.to_bytes() == b"\x80\x40"


def test_full_has_zero_tail_bits():
    assert Bitfield.full(10).to_bytes() == b"\xff\xc0"
    assert Bitfield.full(8).to_bytes() == b"\xff"
    assert Bitfield.full(0).to_bytes() == b""


def test_get_set_clear_count():
    bf = Bitfield(20)
    assert bf.count() == 0 and not bf.is_complete()
    for i in (0, 7, 8, 19):
        bf.set(i)
    assert [bf.get(i) for i in (0, 1, 7, 8, 19)] == [True, False, True, True, True]
    assert bf.count() == 4
    bf.clear(7)
    assert not bf[7] and bf.count() == 3
    assert list(bf.indices()) == [0, 8, 19]


def test_complete():
    bf = Bitfield(3)
    for i in range(3):
        bf.set(i)
    assert bf.is_complete()
    assert Bitfield(0).is_complete()


def test_missing_and_needed_from():
    mine = Bitfield(6)
    mine.set(0)
    mine.set(1)
    theirs = Bitfield(6)
    for i in (1, 2, 5):
        theirs.set(i)
    assert mine.missing() == [2, 3, 4, 5]
    assert mine.needed_from(theirs) == [2, 5]
    with pytest.raises(ValueError):
        mine.needed_from(Bitfield(7))


def test_roundtrip_bytes():
    bf = Bitfield(13)
    for i in (1, 4, 12):
        bf.set(i)
    assert Bitfield.from_bytes(bf.to_bytes(), 13) == bf


def test_from_bytes_validation():
    with pytest.raises(ValueError):
        Bitfield.from_bytes(b"\x00", 10)  # неверная длина
    with pytest.raises(ValueError):
        Bitfield.from_bytes(b"\xff\xff", 10)  # ненулевые хвостовые биты
    Bitfield.from_bytes(b"\xff\xc0", 10)


def test_out_of_range():
    bf = Bitfield(4)
    for bad in (-1, 4):
        with pytest.raises(IndexError):
            bf.get(bad)
        with pytest.raises(IndexError):
            bf.set(bad)


def test_copy_is_independent():
    a = Bitfield(4)
    b = a.copy()
    b.set(1)
    assert not a.get(1)