#include "golden_vectors.hpp"

#include <algorithm>
#include <cstddef>
#include <span>
#include <string_view>
#include <utility>

#include "p2pcore/bitfield.hpp"
#include "p2pcore/crc32c.hpp"

namespace p2p::golden {
namespace {

// ---------------------------------------------------------------- текст JSON

std::string hex(std::span<const std::uint8_t> data) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    out.reserve(data.size() * 2);
    for (const std::uint8_t b : data) {
        out += kDigits[b >> 4];
        out += kDigits[b & 0x0F];
    }
    return out;
}

// Строка JSON в кавычках. Не-ASCII байты (UTF-8) пишутся как есть: файл в UTF-8.
std::string quoted(std::string_view s) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out = "\"";
    for (const char c : s) {
        const auto u = static_cast<unsigned char>(c);
        if (c == '"' || c == '\\') {
            out += '\\';
            out += c;
        } else if (u < 0x20) {
            out += "\\u00";
            out += kDigits[u >> 4];
            out += kDigits[u & 0x0F];
        } else {
            out += c;
        }
    }
    out += '"';
    return out;
}

// u8"..." даёт char8_t; ядро хранит UTF-8 в обычном std::string.
std::string utf8(std::u8string_view s) { return std::string(s.begin(), s.end()); }

std::string num(std::uint64_t v) { return std::to_string(v); }

std::string header_json(const FrameHeader& h, std::string_view indent) {
    const std::string in(indent);
    std::string out = "{\n";
    out += in + "  \"version\": " + num(h.version) + ",\n";
    out += in + "  \"type\": " + num(std::to_underlying(h.type)) + ",\n";
    out += in + "  \"type_name\": " + quoted(to_string(h.type)) + ",\n";
    out += in + "  \"flags\": " + num(h.flags) + ",\n";
    out += in + "  \"file_id\": " + quoted(hex(h.file_id)) + ",\n";
    out += in + "  \"chunk_index\": " + num(h.chunk_index) + ",\n";
    out += in + "  \"payload_len\": " + num(h.payload_len) + ",\n";
    out += in + "  \"payload_crc32c\": " + num(h.payload_crc) + ",\n";
    out += in + "  \"seq\": " + num(h.seq) + "\n";
    out += in + "}";
    return out;
}

// ---------------------------------------------------------------- сборка кадров

constexpr FileId kFile = {0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7,
                          0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF};
constexpr PeerId kPeer = {0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
                          0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F};

// Длину и CRC нагрузки считаем так же, как Connection::queue_frame.
Frame make(std::string name, std::string note, FrameHeader h, std::vector<std::uint8_t> payload,
           std::string fields = "null") {
    h.payload_len = static_cast<std::uint32_t>(payload.size());
    h.payload_crc = crc32c(payload);
    return Frame{std::move(name), std::move(note), h, std::move(payload), std::move(fields)};
}

// Сессионное сообщение: file_id нулевой, chunk_index = 0xFFFFFFFF.
FrameHeader session(MsgType type, std::uint32_t seq) {
    FrameHeader h;
    h.type = type;
    h.seq = seq;
    return h;
}

// Сообщение о файле kFile.
FrameHeader about_file(MsgType type, std::uint32_t chunk_index, std::uint32_t seq) {
    FrameHeader h;
    h.type = type;
    h.file_id = kFile;
    h.chunk_index = chunk_index;
    h.seq = seq;
    return h;
}

Frame hello(std::string name, std::string note, const HelloPayload& p, std::uint32_t seq) {
    std::string fields = "{\"peer_id\": " + quoted(hex(p.peer_id)) + ", \"listen_port\": " + num(p.listen_port) +
                         ", \"caps\": " + num(p.caps) + ", \"name\": " + quoted(p.name) + "}";
    return make(std::move(name), std::move(note), session(MsgType::Hello, seq), encode_hello(p), std::move(fields));
}

Frame reason(std::string name, std::string note, FrameHeader h, std::uint16_t code, std::string_view code_name) {
    std::string fields = "{\"reason\": " + num(code) + ", \"reason_name\": " + quoted(code_name) + "}";
    return make(std::move(name), std::move(note), h, encode_reason(code), std::move(fields));
}

std::array<std::uint8_t, kHeaderSize> encoded(const FrameHeader& h) {
    std::array<std::uint8_t, kHeaderSize> out{};
    encode_header(h, out);
    return out;
}

}  // namespace

std::vector<Frame> frames() {
    std::vector<Frame> v;

    v.push_back(hello("hello", "Имя — UTF-8 (кириллица, 2 байта на букву), без завершающего нуля.",
                      HelloPayload{kPeer, 50505, 0, utf8(u8"Ноутбук Ани")}, 0));
    v.push_back(hello("hello_empty_name", "Минимальная нагрузка HELLO: ровно 22 байта. caps проверяет порядок байт u32.",
                      HelloPayload{kPeer, 0x1234, 0x01020304, ""}, 0));

    const std::uint64_t ts = 0x0102030405060708ULL;
    const std::string ts_fields = "{\"timestamp_ns\": " + num(ts) + "}";
    v.push_back(make("ping", "Все 8 байт времени разные: видно порядок байт u64.", session(MsgType::Ping, 1),
                     encode_ping(ts), ts_fields));
    v.push_back(make("pong", "Нагрузка — эхо PING: те же 8 байт, поэтому и CRC тот же.", session(MsgType::Pong, 7),
                     encode_ping(ts), ts_fields));

    v.push_back(reason("bye_normal", "Штатное закрытие соединения.", session(MsgType::Bye, 42),
                       std::to_underlying(ByeReason::Normal), "NORMAL"));

    const std::string control_text = R"({"type":"list"})";
    v.push_back(make("control", "Нагрузка CONTROL для ядра — непрозрачные байты; смысл придаёт Python.",
                     session(MsgType::Control, 3), std::vector<std::uint8_t>(control_text.begin(), control_text.end()),
                     "{\"text\": " + quoted(control_text) + "}"));

    Bitfield bits(10);
    for (const std::size_t i : {0u, 1u, 2u, 9u}) bits.set(i);
    v.push_back(make("bitfield",
                     "10 чанков, есть 0, 1, 2 и 9. MSB-first: бит 0 — старший бит байта 0; 6 хвостовых бит равны 0.",
                     about_file(MsgType::Bitfield, kNoChunk, 4), bits.bytes(),
                     "{\"bit_count\": 10, \"set_bits\": [0, 1, 2, 9]}"));

    v.push_back(make("have", "Нагрузки нет: payload_len = 0 и payload_crc32c = 0.", about_file(MsgType::Have, 5, 5), {}));
    v.push_back(make("request", "Запрос чанка 7.", about_file(MsgType::Request, 7, 6), {}));

    FrameHeader flagged = about_file(MsgType::Request, 0x01020304, 8);
    flagged.flags = 0x8001;
    v.push_back(make("request_flags_ignored",
                     "flags не ноль: получатель обязан принять кадр и флаги проигнорировать.", flagged, {}));

    std::vector<std::uint8_t> data(32);
    for (std::size_t i = 0; i < data.size(); ++i) data[i] = static_cast<std::uint8_t>(i * 7 + 3);
    v.push_back(make("chunk", "Ответ на REQUEST чанка 7. Нагрузка — сами данные (здесь 32 байта: i*7+3).",
                     about_file(MsgType::Chunk, 7, 9), std::move(data)));

    v.push_back(reason("reject_busy", "Отказ вместо CHUNK: очередь отдачи переполнена.",
                       about_file(MsgType::Reject, 7, 10), std::to_underlying(RejectReason::Busy), "BUSY"));
    v.push_back(make("cancel", "Отмена запроса чанка 7.", about_file(MsgType::Cancel, 7, 11), {}));

    return v;
}

std::vector<HeaderCheck> header_checks() {
    std::vector<HeaderCheck> v;

    v.push_back({"default_ping_header", "Пример из PROTOCOL.md, раздел 5.", encoded(FrameHeader{}), DecodeError::None});

    FrameHeader h = about_file(MsgType::Request, 1, 2);
    h.flags = 0xFFFF;
    v.push_back({"flags_all_ones", "Любые флаги допустимы: получатель их игнорирует.", encoded(h), DecodeError::None});

    h = about_file(MsgType::Chunk, 0, 0);
    h.payload_len = kMaxPayload;
    v.push_back({"payload_len_max", "Ровно 16 МиБ — ещё допустимо.", encoded(h), DecodeError::None});

    h.payload_len = kMaxPayload + 1;
    v.push_back({"payload_too_large", "16 МиБ + 1 байт.", encoded(h), DecodeError::PayloadTooLarge});

    auto bytes = encoded(FrameHeader{});
    bytes[3] = 'X';  // "P2PX"
    v.push_back({"bad_magic", "Испорчен последний байт magic.", bytes, DecodeError::BadMagic});

    h = FrameHeader{};
    h.version = 2;
    v.push_back({"bad_version", "Версия 2.", encoded(h), DecodeError::BadVersion});

    h = FrameHeader{};
    h.type = static_cast<MsgType>(0x04);
    v.push_back({"unknown_type", "Типа 0x04 в таблице нет.", encoded(h), DecodeError::UnknownType});

    // Порядок проверок: magic -> version -> payload_len -> type.
    h.version = 2;
    v.push_back({"bad_version_before_unknown_type", "Неверны и версия, и тип: сообщается версия.", encoded(h),
                 DecodeError::BadVersion});

    h.version = kProtocolVersion;
    h.payload_len = kMaxPayload + 1;
    v.push_back({"too_large_before_unknown_type", "Неверны и длина, и тип: сообщается длина.", encoded(h),
                 DecodeError::PayloadTooLarge});

    h.version = 2;
    bytes = encoded(h);
    bytes[0] = 0x00;
    v.push_back({"bad_magic_first", "Неверно всё сразу: сообщается magic.", bytes, DecodeError::BadMagic});

    return v;
}

std::vector<BadPayload> bad_payloads() {
    return {
        {"hello_too_short", "21 байт: на байт меньше обязательной части (22).", MsgType::Hello,
         std::vector<std::uint8_t>(kHelloFixedSize - 1, 0x11)},
        {"hello_name_too_long", "22 + 256 байт: имя длиннее 255 байт.", MsgType::Hello,
         std::vector<std::uint8_t>(kHelloFixedSize + kMaxNameSize + 1, 0x41)},
        {"ping_too_short", "7 байт вместо 8.", MsgType::Ping, std::vector<std::uint8_t>(7, 0x22)},
        {"pong_too_long", "9 байт вместо 8.", MsgType::Pong, std::vector<std::uint8_t>(9, 0x33)},
        {"bye_empty", "0 байт вместо 2.", MsgType::Bye, {}},
        {"reject_too_long", "3 байта вместо 2.", MsgType::Reject, std::vector<std::uint8_t>(3, 0x44)},
    };
}

std::vector<std::uint8_t> frame_bytes(const Frame& f) {
    std::vector<std::uint8_t> out(kHeaderSize + f.payload.size());
    encode_header(f.header, std::span<std::uint8_t, kHeaderSize>(out.data(), kHeaderSize));
    std::ranges::copy(f.payload, out.begin() + static_cast<std::ptrdiff_t>(kHeaderSize));
    return out;
}

std::string result_name(DecodeError e) {
    switch (e) {
        case DecodeError::None: return "OK";
        case DecodeError::BadMagic: return "BAD_MAGIC";
        case DecodeError::BadVersion: return "BAD_VERSION";
        case DecodeError::PayloadTooLarge: return "PAYLOAD_TOO_LARGE";
        case DecodeError::UnknownType: return "UNKNOWN_TYPE";
    }
    return "?";
}

std::string build_json() {
    std::string out = "{\n";
    out += "  \"protocol_version\": " + num(kProtocolVersion) + ",\n";
    out += "  \"header_struct\": \">IBBH16sIIII\",\n";
    out += "  \"generated_by\": \"p2pcore_gen_vectors (cpp/tools/gen_vectors.cpp); не править вручную\",\n";

    out += "  \"frames\": [\n";
    const auto fr = frames();
    for (std::size_t i = 0; i < fr.size(); ++i) {
        const Frame& f = fr[i];
        out += "    {\n";
        out += "      \"name\": " + quoted(f.name) + ",\n";
        out += "      \"note\": " + quoted(f.note) + ",\n";
        out += "      \"header\": " + header_json(f.header, "      ") + ",\n";
        out += "      \"payload\": " + f.fields + ",\n";
        out += "      \"payload_hex\": " + quoted(hex(f.payload)) + ",\n";
        out += "      \"frame_hex\": " + quoted(hex(frame_bytes(f))) + "\n";
        out += i + 1 < fr.size() ? "    },\n" : "    }\n";
    }
    out += "  ],\n";

    out += "  \"header_checks\": [\n";
    const auto hc = header_checks();
    for (std::size_t i = 0; i < hc.size(); ++i) {
        const HeaderCheck& c = hc[i];
        out += "    {\n";
        out += "      \"name\": " + quoted(c.name) + ",\n";
        out += "      \"note\": " + quoted(c.note) + ",\n";
        out += "      \"header_hex\": " + quoted(hex(c.bytes)) + ",\n";
        out += "      \"result\": " + quoted(result_name(c.expected)) + "\n";
        out += i + 1 < hc.size() ? "    },\n" : "    }\n";
    }
    out += "  ],\n";

    out += "  \"bad_payloads\": [\n";
    const auto bp = bad_payloads();
    for (std::size_t i = 0; i < bp.size(); ++i) {
        const BadPayload& b = bp[i];
        out += "    {\n";
        out += "      \"name\": " + quoted(b.name) + ",\n";
        out += "      \"note\": " + quoted(b.note) + ",\n";
        out += "      \"type\": " + num(std::to_underlying(b.type)) + ",\n";
        out += "      \"type_name\": " + quoted(to_string(b.type)) + ",\n";
        out += "      \"payload_hex\": " + quoted(hex(b.payload)) + "\n";
        out += i + 1 < bp.size() ? "    },\n" : "    }\n";
    }
    out += "  ]\n";
    out += "}\n";
    return out;
}

}  // namespace p2p::golden
