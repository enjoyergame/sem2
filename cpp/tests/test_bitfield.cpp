#include <doctest/doctest.h>

#include <algorithm>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "p2pcore/bitfield.hpp"
#include "p2pcore/crc32c.hpp"

using namespace p2p;

using Bytes = std::vector<std::uint8_t>;

TEST_CASE("bitfield: MSB-first layout like storage/bitfield.py") {
    Bitfield bf(10);
    CHECK(bf.bytes().size() == 2);
    bf.set(0);
    bf.set(9);
    CHECK(bf.bytes() == Bytes{0x80, 0x40});
    CHECK(bf.get(0));
    CHECK_FALSE(bf.get(1));
    CHECK(bf.get(9));
    CHECK(bf.count() == 2);
    bf.clear(0);
    CHECK(bf.bytes() == Bytes{0x00, 0x40});
}

TEST_CASE("bitfield: full sets exactly n bits and keeps the tail zero") {
    CHECK(Bitfield::full(0).bytes().empty());
    CHECK(Bitfield::full(8).bytes() == Bytes{0xFF});
    CHECK(Bitfield::full(5).bytes() == Bytes{0xF8});
    const Bitfield bf = Bitfield::full(13);
    CHECK(bf.bytes() == Bytes{0xFF, 0xF8});
    CHECK(bf.is_complete());
    CHECK(bf.count() == 13);
}

TEST_CASE("bitfield: out-of-range index is ignored, not UB") {
    Bitfield bf(3);
    bf.set(3);
    bf.set(1000);
    CHECK(bf.count() == 0);
    CHECK_FALSE(bf.get(3));
    CHECK_FALSE(Bitfield{}.get(0));
}

TEST_CASE("bitfield: from_bytes validates length and trailing bits") {
    std::string err;
    CHECK(Bitfield::from_bytes(Bytes{0xE0}, 3, err).has_value());
    CHECK_FALSE(Bitfield::from_bytes(Bytes{0xE0, 0x00}, 3, err).has_value());
    CHECK(err.find("expected 1 bytes") != std::string::npos);
    CHECK_FALSE(Bitfield::from_bytes(Bytes{0xF0}, 3, err).has_value());
    CHECK(err == "trailing bits must be zero");
    CHECK(Bitfield::from_bytes(Bytes{}, 0).has_value());
    // как в test_open_download_rejects_wrong_bitfield_length: 5 чанков -> 1 байт, а прислали 3
    CHECK_FALSE(Bitfield::from_bytes(Bytes{0, 0, 0}, 5).has_value());

    const auto bf = Bitfield::from_bytes(Bytes{0xA0}, 3);
    REQUIRE(bf.has_value());
    CHECK(bf->get(0));
    CHECK_FALSE(bf->get(1));
    CHECK(bf->get(2));
}

// ---------------------------------------------------------------------- CRC32C

namespace {
Bytes ascii(const char* s) { return Bytes(s, s + std::char_traits<char>::length(s)); }
}  // namespace

TEST_CASE("crc32c: known vectors") {
    CHECK(crc32c(Bytes{}) == 0x00000000u);
    CHECK(crc32c(ascii("123456789")) == 0xE3069283u);  // RFC 3720 B.4 / iSCSI check value
    CHECK(crc32c(ascii("a")) == 0xC1D04330u);
    CHECK(crc32c(Bytes(32, 0x00)) == 0x8A9136AAu);  // RFC 3720 B.4: 32 нулевых байта
    CHECK(crc32c(Bytes(32, 0xFF)) == 0x62A8AB43u);  // RFC 3720 B.4: 32 байта 0xFF
}

TEST_CASE("crc32c: incremental update equals one-shot") {
    Bytes data(100000);
    for (std::size_t i = 0; i < data.size(); ++i) data[i] = static_cast<std::uint8_t>(i * 31 + 7);
    Crc32c crc;
    for (std::size_t off = 0; off < data.size(); off += 65536) {
        const std::size_t n = std::min<std::size_t>(65536, data.size() - off);
        crc.update(std::span(data).subspan(off, n));
    }
    CHECK(crc.value() == crc32c(data));
    crc.reset();
    CHECK(crc.value() == 0u);
}
