#include <doctest/doctest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "p2pcore/protocol.hpp"

using namespace p2p;

namespace {

using Buf = std::array<std::uint8_t, kHeaderSize>;

FrameHeader sample_header() {
    FrameHeader h;
    h.type = MsgType::Chunk;
    h.flags = 0x0102;
    for (std::size_t i = 0; i < h.file_id.size(); ++i) h.file_id[i] = static_cast<std::uint8_t>(0xA0 + i);
    h.chunk_index = 0x01020304;
    h.payload_len = 0x00100000;
    h.payload_crc = 0xDEADBEEF;
    h.seq = 0x0A0B0C0D;
    return h;
}

Buf encode(const FrameHeader& h) {
    Buf b{};
    encode_header(h, b);
    return b;
}

}  // namespace

TEST_CASE("header: size constant") { CHECK(kHeaderSize == 40); }

TEST_CASE("header: exact bytes are big-endian at documented offsets") {
    const Buf b = encode(sample_header());
    // magic "P2PF"
    CHECK(b[0] == 0x50);
    CHECK(b[1] == 0x32);
    CHECK(b[2] == 0x50);
    CHECK(b[3] == 0x46);
    CHECK(b[4] == kProtocolVersion);
    CHECK(b[5] == 0x31);  // CHUNK
    CHECK(b[6] == 0x01);
    CHECK(b[7] == 0x02);
    for (std::size_t i = 0; i < 16; ++i) CHECK(b[8 + i] == 0xA0 + i);
    const std::array<std::uint8_t, 16> tail = {0x01, 0x02, 0x03, 0x04, 0x00, 0x10, 0x00, 0x00,
                                               0xDE, 0xAD, 0xBE, 0xEF, 0x0A, 0x0B, 0x0C, 0x0D};
    CHECK(std::equal(tail.begin(), tail.end(), b.begin() + 24));
}

TEST_CASE("header: encode overwrites every byte of a reused buffer") {
    Buf b;
    b.fill(0xAA);
    encode_header(FrameHeader{}, b);
    // заголовок по умолчанию: PING, нулевой file_id, chunk_index = 0xFFFFFFFF
    const Buf expected = {0x50, 0x32, 0x50, 0x46, 0x01, 0x02, 0x00, 0x00, 0, 0, 0, 0, 0, 0,
                          0,    0,    0,    0,    0,    0,    0,    0,    0, 0, 0xFF, 0xFF, 0xFF, 0xFF,
                          0,    0,    0,    0,    0,    0,    0,    0,    0, 0, 0, 0};
    CHECK(b == expected);
}

TEST_CASE("header: round trip") {
    const FrameHeader h = sample_header();
    const Buf b = encode(h);
    FrameHeader out;
    REQUIRE(decode_header(b, out) == DecodeError::None);
    CHECK(out == h);
}

TEST_CASE("header: extreme values survive (no sign/promotion bugs)") {
    FrameHeader h;
    h.type = MsgType::Cancel;
    h.flags = 0xFFFF;
    h.file_id.fill(0xFF);
    h.chunk_index = kNoChunk;
    h.payload_len = kMaxPayload;
    h.payload_crc = 0xFFFFFFFF;
    h.seq = 0xFFFFFFFF;
    FrameHeader out;
    REQUIRE(decode_header(encode(h), out) == DecodeError::None);
    CHECK(out == h);
}

TEST_CASE("header: every known type round-trips, unknown ones are rejected") {
    for (int t = 0; t < 256; ++t) {
        Buf b = encode(FrameHeader{});
        b[5] = static_cast<std::uint8_t>(t);
        FrameHeader out;
        const DecodeError e = decode_header(b, out);
        if (is_known_type(static_cast<std::uint8_t>(t))) {
            CHECK(e == DecodeError::None);
            CHECK(std::to_underlying(out.type) == t);
        } else {
            CHECK(e == DecodeError::UnknownType);
        }
    }
    CHECK(is_known_type(0x01));
    CHECK(is_known_type(0x33));
    CHECK_FALSE(is_known_type(0x00));
    CHECK_FALSE(is_known_type(0x04));
    CHECK_FALSE(is_known_type(0xFF));
}

TEST_CASE("header: decode errors") {
    FrameHeader out;

    SUBCASE("bad magic") {
        Buf b = encode(sample_header());
        b[0] = 0x00;
        CHECK(decode_header(b, out) == DecodeError::BadMagic);
    }
    SUBCASE("bad version") {
        FrameHeader h = sample_header();
        h.version = 2;
        CHECK(decode_header(encode(h), out) == DecodeError::BadVersion);
    }
    SUBCASE("payload limit is inclusive") {
        FrameHeader h = sample_header();
        h.payload_len = kMaxPayload;
        CHECK(decode_header(encode(h), out) == DecodeError::None);
        h.payload_len = kMaxPayload + 1;
        CHECK(decode_header(encode(h), out) == DecodeError::PayloadTooLarge);
    }
    SUBCASE("check order: magic before version before length before type") {
        FrameHeader h = sample_header();
        h.version = 9;
        h.payload_len = 0xFFFFFFFF;
        Buf b = encode(h);
        b[5] = 0xEE;
        b[0] = 0;
        CHECK(decode_header(b, out) == DecodeError::BadMagic);
        b[0] = 0x50;
        CHECK(decode_header(b, out) == DecodeError::BadVersion);
        b[4] = kProtocolVersion;
        CHECK(decode_header(b, out) == DecodeError::PayloadTooLarge);
        b[28] = b[29] = b[30] = b[31] = 0;
        CHECK(decode_header(b, out) == DecodeError::UnknownType);
    }
}

TEST_CASE("header: output is untouched on error") {
    const FrameHeader before = sample_header();
    FrameHeader h = before;
    Buf b = encode(FrameHeader{});
    b[1] = 0;
    REQUIRE(decode_header(b, h) == DecodeError::BadMagic);
    CHECK(h == before);
}

TEST_CASE("header: flags are not validated on receive") {
    FrameHeader h = sample_header();
    h.flags = 0x8001;
    FrameHeader out;
    REQUIRE(decode_header(encode(h), out) == DecodeError::None);
    CHECK(out.flags == 0x8001);
}

// ------------------------------------------------------------------ нагрузки

TEST_CASE("hello: round trip with cyrillic and empty names") {
    HelloPayload p;
    p.peer_id.fill(0x4C);
    p.listen_port = 47010;
    p.caps = 0x80000001;
    for (const char* name : {"Ноутбук", ""}) {
        p.name = name;
        const auto bytes = encode_hello(p);
        CHECK(bytes.size() == kHelloFixedSize + p.name.size());
        HelloPayload out;
        REQUIRE(decode_hello(bytes, out));
        CHECK(out == p);
    }
}

TEST_CASE("hello: exact layout") {
    HelloPayload p;
    p.peer_id.fill(0x11);
    p.listen_port = 0x1234;
    p.caps = 0x0A0B0C0D;
    p.name = "ab";
    const std::vector<std::uint8_t> bytes = encode_hello(p);
    REQUIRE(bytes.size() == 24);
    CHECK(bytes[16] == 0x12);
    CHECK(bytes[17] == 0x34);
    CHECK(bytes[18] == 0x0A);
    CHECK(bytes[21] == 0x0D);
    CHECK(bytes[22] == 'a');
    CHECK(bytes[23] == 'b');
}

TEST_CASE("hello: wrong sizes are rejected and output is untouched") {
    HelloPayload out;
    out.name = "keep";
    std::vector<std::uint8_t> short_payload(kHelloFixedSize - 1, 0);
    CHECK_FALSE(decode_hello(short_payload, out));
    std::vector<std::uint8_t> long_payload(kHelloFixedSize + kMaxNameSize + 1, 'x');
    CHECK_FALSE(decode_hello(long_payload, out));
    CHECK(out.name == "keep");
}

TEST_CASE("hello: long name is truncated on a UTF-8 boundary") {
    HelloPayload p;
    p.name.clear();
    for (int i = 0; i < 200; ++i) p.name += "я";  // 2 байта на символ, 400 байт
    const auto bytes = encode_hello(p);
    HelloPayload out;
    REQUIRE(decode_hello(bytes, out));
    CHECK(out.name.size() == 254);  // 255 разрезало бы символ
    CHECK(out.name == p.name.substr(0, 254));
}

TEST_CASE("truncate_utf8") {
    CHECK(truncate_utf8("abc", 10) == "abc");
    CHECK(truncate_utf8("abc", 2) == "ab");
    CHECK(truncate_utf8("ж", 1).empty());       // 2-байтовый символ не влезает
    CHECK(truncate_utf8("a€", 3) == "a");        // € = 3 байта
    CHECK(truncate_utf8("a€b", 4) == "a€");
}

TEST_CASE("reason (REJECT/BYE): exactly two bytes") {
    const auto bytes = encode_reason(0xBEEF);
    REQUIRE(bytes.size() == 2);
    CHECK(bytes[0] == 0xBE);
    CHECK(bytes[1] == 0xEF);
    std::uint16_t r = 0;
    REQUIRE(decode_reason(bytes, r));
    CHECK(r == 0xBEEF);
    std::uint16_t untouched = 7;
    CHECK_FALSE(decode_reason(std::vector<std::uint8_t>{1}, untouched));
    CHECK_FALSE(decode_reason(std::vector<std::uint8_t>{1, 2, 3}, untouched));
    CHECK(untouched == 7);
}

TEST_CASE("ping: u64 with the high bit set") {
    const std::uint64_t ts = 0x8000000000000001ull;
    const auto bytes = encode_ping(ts);
    REQUIRE(bytes.size() == 8);
    CHECK(bytes[0] == 0x80);
    CHECK(bytes[7] == 0x01);
    std::uint64_t out = 0;
    REQUIRE(decode_ping(bytes, out));
    CHECK(out == ts);
    CHECK_FALSE(decode_ping(std::vector<std::uint8_t>(7, 0), out));
}

TEST_CASE("reason codes match the protocol") {
    CHECK(std::to_underlying(RejectReason::UnknownFile) == 1);
    CHECK(std::to_underlying(RejectReason::NotHave) == 2);
    CHECK(std::to_underlying(RejectReason::Busy) == 3);
    CHECK(std::to_underlying(ByeReason::Normal) == 1);
    CHECK(std::to_underlying(ByeReason::ProtocolError) == 2);
    CHECK(std::to_underlying(ByeReason::Incompatible) == 3);
    CHECK(std::to_underlying(ByeReason::Duplicate) == 4);
}
