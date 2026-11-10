// Golden-векторы: эталонные кадры из tests/vectors/frames.json, общие с Python.
#include <doctest/doctest.h>

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <set>
#include <span>
#include <string>
#include <vector>

#include "golden_vectors.hpp"
#include "p2pcore/bitfield.hpp"
#include "p2pcore/crc32c.hpp"
#include "p2pcore/protocol.hpp"

using namespace p2p;

namespace {

std::string to_hex(std::span<const std::uint8_t> data) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    for (const std::uint8_t b : data) {
        out += kDigits[b >> 4];
        out += kDigits[b & 0x0F];
    }
    return out;
}

const golden::Frame& frame(const std::vector<golden::Frame>& all, const std::string& name) {
    const auto it = std::ranges::find_if(all, [&](const golden::Frame& f) { return f.name == name; });
    REQUIRE_MESSAGE(it != all.end(), "no golden frame named " << name);
    return *it;
}

// Файл целиком, без \r: git на Windows при checkout может заменить \n на \r\n.
std::string read_without_cr(const char* path) {
    std::ifstream in(path, std::ios::binary);
    REQUIRE_MESSAGE(in.good(), "cannot open " << path);
    std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::erase(text, '\r');
    return text;
}

// Номер первой отличающейся строки (с 1) — чтобы сразу видеть, что разошлось.
std::size_t first_different_line(const std::string& a, const std::string& b) {
    const auto [ia, ib] = std::ranges::mismatch(a, b);
    return 1 + static_cast<std::size_t>(std::count(a.begin(), ia, '\n'));
}

}  // namespace

TEST_CASE("vectors: frames.json in the repository matches what the codec produces now") {
    const std::string expected = golden::build_json();
    const std::string actual = read_without_cr(P2P_VECTORS_PATH);
    const bool same = actual == expected;
    CHECK_MESSAGE(same, "tests/vectors/frames.json differs from the codec output, first difference at line "
                            << first_different_line(actual, expected)
                            << ". If the protocol change is intended: run p2pcore_gen_vectors, review the git diff, "
                               "tell the Python side. Otherwise the codec is broken.");
}

TEST_CASE("vectors: names are unique") {
    std::set<std::string> names;
    for (const auto& f : golden::frames()) CHECK_MESSAGE(names.insert(f.name).second, f.name);
    for (const auto& c : golden::header_checks()) CHECK_MESSAGE(names.insert(c.name).second, c.name);
    for (const auto& b : golden::bad_payloads()) CHECK_MESSAGE(names.insert(b.name).second, b.name);
}

// Байты, выписанные вручную по таблицам PROTOCOL.md — без участия кодека.
// Остальные векторы строит сам кодек, а эти два — независимая точка опоры.
TEST_CASE("vectors: hand-written frames from PROTOCOL.md") {
    const auto all = golden::frames();
    // magic, version=1, type=0x30 REQUEST, flags=0 | file_id a0..af | chunk=7, len=0, crc=0, seq=6
    CHECK(to_hex(golden::frame_bytes(frame(all, "request"))) ==
          "5032504601300000"
          "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
          "00000007000000000000000000000006");
    // 10 бит, установлены 0,1,2,9: 1110 0000 | 0100 0000
    CHECK(to_hex(frame(all, "bitfield").payload) == "e040");
    // HELLO: peer_id | порт 0x1234 | caps 0x01020304 | пустое имя
    CHECK(to_hex(frame(all, "hello_empty_name").payload) == "101112131415161718191a1b1c1d1e1f123401020304");
}

TEST_CASE("vectors: every frame decodes back to the fields it was built from") {
    for (const auto& f : golden::frames()) {
        CAPTURE(f.name);
        const std::vector<std::uint8_t> bytes = golden::frame_bytes(f);
        REQUIRE(bytes.size() == kHeaderSize + f.payload.size());

        FrameHeader h;
        REQUIRE(decode_header(std::span<const std::uint8_t, kHeaderSize>(bytes.data(), kHeaderSize), h) ==
                DecodeError::None);
        CHECK(h == f.header);

        const std::span<const std::uint8_t> payload = std::span(bytes).subspan(kHeaderSize);
        CHECK(h.payload_len == payload.size());
        CHECK(h.payload_crc == crc32c(payload));
        if (payload.empty()) CHECK(h.payload_crc == 0);

        // Нагрузка: декодер принимает её, а повторное кодирование даёт те же байты.
        switch (h.type) {
            case MsgType::Hello: {
                HelloPayload p;
                REQUIRE(decode_hello(payload, p));
                CHECK(encode_hello(p) == f.payload);
                break;
            }
            case MsgType::Ping:
            case MsgType::Pong: {
                std::uint64_t ts = 0;
                REQUIRE(decode_ping(payload, ts));
                CHECK(encode_ping(ts) == f.payload);
                break;
            }
            case MsgType::Bye:
            case MsgType::Reject: {
                std::uint16_t reason = 0;
                REQUIRE(decode_reason(payload, reason));
                CHECK(encode_reason(reason) == f.payload);
                break;
            }
            case MsgType::Bitfield: {
                const auto bits = Bitfield::from_bytes(payload, 10);
                REQUIRE(bits.has_value());
                CHECK(bits->count() == 4);
                CHECK(bits->get(0));
                CHECK(bits->get(9));
                CHECK_FALSE(bits->get(3));
                break;
            }
            case MsgType::Have:
            case MsgType::Request:
            case MsgType::Cancel:
                CHECK(payload.empty());
                break;
            case MsgType::Control:
            case MsgType::Chunk:
                CHECK_FALSE(payload.empty());  // непрозрачные байты, кодека нет
                break;
        }
    }
}

TEST_CASE("vectors: decoded values of selected frames") {
    const auto all = golden::frames();

    HelloPayload p;
    REQUIRE(decode_hello(frame(all, "hello").payload, p));
    CHECK(p.listen_port == 50505);
    CHECK(p.caps == 0);
    CHECK(p.name.size() == 21);  // "Ноутбук Ани": 10 букв по 2 байта + пробел
    CHECK(p.peer_id[0] == 0x10);
    CHECK(p.peer_id[15] == 0x1F);

    std::uint64_t ts = 0;
    REQUIRE(decode_ping(frame(all, "ping").payload, ts));
    CHECK(ts == 0x0102030405060708ULL);
    CHECK(frame(all, "pong").payload == frame(all, "ping").payload);

    std::uint16_t reason = 0;
    REQUIRE(decode_reason(frame(all, "reject_busy").payload, reason));
    CHECK(reason == 3);
    REQUIRE(decode_reason(frame(all, "bye_normal").payload, reason));
    CHECK(reason == 1);

    CHECK(frame(all, "request_flags_ignored").header.flags == 0x8001);
}

TEST_CASE("vectors: header checks give the documented result") {
    for (const auto& c : golden::header_checks()) {
        CAPTURE(c.name);
        FrameHeader h;
        CHECK(golden::result_name(decode_header(c.bytes, h)) == golden::result_name(c.expected));
    }
}

TEST_CASE("vectors: payloads of wrong length are rejected") {
    for (const auto& b : golden::bad_payloads()) {
        CAPTURE(b.name);
        switch (b.type) {
            case MsgType::Hello: {
                HelloPayload p;
                CHECK_FALSE(decode_hello(b.payload, p));
                break;
            }
            case MsgType::Ping:
            case MsgType::Pong: {
                std::uint64_t ts = 0;
                CHECK_FALSE(decode_ping(b.payload, ts));
                break;
            }
            case MsgType::Bye:
            case MsgType::Reject: {
                std::uint16_t reason = 0;
                CHECK_FALSE(decode_reason(b.payload, reason));
                break;
            }
            default:
                FAIL("no length rule for this message type");
        }
    }
}
