import pytest

from p2pshare.core.piece_picker import SequentialPicker
from p2pshare.storage.bitfield import Bitfield


def bf(size, *bits):
    b = Bitfield(size)
    for i in bits:
        b.set(i)
    return b


def test_picks_in_order_what_peer_has_and_we_lack():
    picker = SequentialPicker()
    mine = bf(8, 0, 1)
    theirs = bf(8, 1, 2, 3, 5)
    assert picker.pick("p", theirs, mine, set(), 10) == [2, 3, 5]


def test_respects_limit():
    picker = SequentialPicker()
    assert picker.pick("p", Bitfield.full(10), Bitfield(10), set(), 3) == [0, 1, 2]


def test_skips_chunks_in_flight():
    picker = SequentialPicker()
    result = picker.pick("p", Bitfield.full(6), Bitfield(6), {0, 1, 3}, 10)
    assert result == [2, 4, 5]


def test_nothing_to_pick():
    picker = SequentialPicker()
    assert picker.pick("p", Bitfield(5), Bitfield(5), set(), 4) == []  # у пира ничего нет
    assert picker.pick("p", Bitfield.full(5), Bitfield.full(5), set(), 4) == []  # всё уже есть
    assert picker.pick("p", Bitfield.full(5), Bitfield(5), set(), 0) == []  # лимит 0


def test_size_mismatch_is_an_error():
    with pytest.raises(ValueError):
        SequentialPicker().pick("p", Bitfield(4), Bitfield(5), set(), 1)


def test_does_not_modify_inputs():
    mine, theirs, flight = bf(4, 0), Bitfield.full(4), {1}
    SequentialPicker().pick("p", theirs, mine, flight, 4)
    assert list(mine.indices()) == [0] and flight == {1}