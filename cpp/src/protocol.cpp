#include "p2pcore/protocol.hpp"

#include <algorithm>
#include <concepts>
#include <utility>

namespace p2p {
namespace {

// ---------------------------------------------------------------- big-endian
// Запись/чтение беззнакового числа старшим байтом вперёд. Размер span задан
// в типе, поэтому выйти за границы буфера нельзя: subspan<смещение, размер>()
// проверяется при компиляции.

template <std::unsigned_integral T>
void put_be(std::span<std::uint8_t, sizeof(T)> out, T v) noexcept {
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out[sizeof(T) - 1 - i] = static_cast<std::uint8_t>(v & 0xFFu);  // младший байт в конец
        v = static_cast<T>(v >> 8);
    }
}

template <std::unsigned_integral T>
T get_be(std::span<const std::uint8_t, sizeof(T)> in) noexcept {
    // Сдвигаем накопленное значение типа T, а не сам байт: выражение uint8_t << 24
    // считается в int, и для байта >= 0x80 это переполнение знакового (UB).
    T v = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        v = static_cast<T>((v << 8) | in[i]);
    }
    return v;
}

template <std::unsigned_integral T>
void append_be(std::vector<std::uint8_t>& out, T v) {
    std::array<std::uint8_t, sizeof(T)> tmp;
    put_be<T>(tmp, v);
    out.insert(out.end(), tmp.begin(), tmp.end());
}

// ------------------------------------------------------- смещения заголовка
// Единственный источник правды для обеих функций (таблица из PROTOCOL.md).
constexpr std::size_t kOffMagic = 0;
constexpr std::size_t kOffVersion = 4;
constexpr std::size_t kOffType = 5;
constexpr std::size_t kOffFlags = 6;
constexpr std::size_t kOffFileId = 8;
constexpr std::size_t kOffChunk = 24;
constexpr std::size_t kOffLen = 28;
constexpr std::size_t kOffCrc = 32;
constexpr std::size_t kOffSeq = 36;
static_assert(kOffSeq + 4 == kHeaderSize, "таблица смещений должна покрывать ровно 40 байт");

}  // namespace

// ------------------------------------------------------------------ заголовок

void encode_header(const FrameHeader& h, std::span<std::uint8_t, kHeaderSize> out) noexcept {
    put_be<std::uint32_t>(out.subspan<kOffMagic, 4>(), kMagic);
    out[kOffVersion] = h.version;
    out[kOffType] = std::to_underlying(h.type);
    put_be<std::uint16_t>(out.subspan<kOffFlags, 2>(), h.flags);
    std::ranges::copy(h.file_id, out.subspan<kOffFileId, kFileIdSize>().begin());
    put_be<std::uint32_t>(out.subspan<kOffChunk, 4>(), h.chunk_index);
    put_be<std::uint32_t>(out.subspan<kOffLen, 4>(), h.payload_len);
    put_be<std::uint32_t>(out.subspan<kOffCrc, 4>(), h.payload_crc);
    put_be<std::uint32_t>(out.subspan<kOffSeq, 4>(), h.seq);
}

DecodeError decode_header(std::span<const std::uint8_t, kHeaderSize> in, FrameHeader& h) noexcept {
    // 1. magic: если не совпал, остальные байты — вообще не наш протокол
    if (get_be<std::uint32_t>(in.subspan<kOffMagic, 4>()) != kMagic) return DecodeError::BadMagic;
    // 2. версия: в другой версии поля могут лежать иначе
    if (in[kOffVersion] != kProtocolVersion) return DecodeError::BadVersion;
    // 3. длина: защита от мусора, иначе Connection может ждать гигабайты
    const auto payload_len = get_be<std::uint32_t>(in.subspan<kOffLen, 4>());
    if (payload_len > kMaxPayload) return DecodeError::PayloadTooLarge;
    // 4. тип: длина уже проверена, поэтому в будущем незнакомый кадр можно будет пропустить
    if (!is_known_type(in[kOffType])) return DecodeError::UnknownType;

    FrameHeader tmp;  // заполняем копию и присваиваем в конце: при ошибке h не меняется
    tmp.version = in[kOffVersion];
    tmp.type = static_cast<MsgType>(in[kOffType]);
    tmp.flags = get_be<std::uint16_t>(in.subspan<kOffFlags, 2>());
    std::ranges::copy(in.subspan<kOffFileId, kFileIdSize>(), tmp.file_id.begin());
    tmp.chunk_index = get_be<std::uint32_t>(in.subspan<kOffChunk, 4>());
    tmp.payload_len = payload_len;
    tmp.payload_crc = get_be<std::uint32_t>(in.subspan<kOffCrc, 4>());
    tmp.seq = get_be<std::uint32_t>(in.subspan<kOffSeq, 4>());
    h = tmp;
    return DecodeError::None;
}

bool is_known_type(std::uint8_t type) noexcept {
    // Приведение байта к enum с фиксированным базовым типом законно, но может дать
    // значение без имени. switch по всем именам (и -Wswitch-enum) это отсекает.
    switch (static_cast<MsgType>(type)) {
        case MsgType::Hello:
        case MsgType::Ping:
        case MsgType::Pong:
        case MsgType::Bye:
        case MsgType::Control:
        case MsgType::Bitfield:
        case MsgType::Have:
        case MsgType::Request:
        case MsgType::Chunk:
        case MsgType::Reject:
        case MsgType::Cancel:
            return true;
    }
    return false;
}

std::string_view to_string(DecodeError e) noexcept {
    switch (e) {
        case DecodeError::None: return "ok";
        case DecodeError::BadMagic: return "bad magic";
        case DecodeError::BadVersion: return "unsupported protocol version";
        case DecodeError::PayloadTooLarge: return "payload too large";
        case DecodeError::UnknownType: return "unknown message type";
    }
    return "?";
}

std::string_view to_string(MsgType t) noexcept {
    switch (t) {
        case MsgType::Hello: return "HELLO";
        case MsgType::Ping: return "PING";
        case MsgType::Pong: return "PONG";
        case MsgType::Bye: return "BYE";
        case MsgType::Control: return "CONTROL";
        case MsgType::Bitfield: return "BITFIELD";
        case MsgType::Have: return "HAVE";
        case MsgType::Request: return "REQUEST";
        case MsgType::Chunk: return "CHUNK";
        case MsgType::Reject: return "REJECT";
        case MsgType::Cancel: return "CANCEL";
    }
    return "?";
}

// ------------------------------------------------------------------- нагрузки

std::string truncate_utf8(std::string_view s, std::size_t max_bytes) {
    if (s.size() <= max_bytes) return std::string(s);
    std::size_t n = max_bytes;
    // байты вида 10xxxxxx — продолжение символа; отступаем до начала символа
    while (n > 0 && (static_cast<unsigned char>(s[n]) & 0xC0u) == 0x80u) --n;
    return std::string(s.substr(0, n));
}

std::vector<std::uint8_t> encode_hello(const HelloPayload& p) {
    const std::string name = truncate_utf8(p.name, kMaxNameSize);
    std::vector<std::uint8_t> out;
    out.reserve(kHelloFixedSize + name.size());
    out.insert(out.end(), p.peer_id.begin(), p.peer_id.end());
    append_be<std::uint16_t>(out, p.listen_port);
    append_be<std::uint32_t>(out, p.caps);
    out.insert(out.end(), name.begin(), name.end());
    return out;
}

bool decode_hello(std::span<const std::uint8_t> in, HelloPayload& out) {
    if (in.size() < kHelloFixedSize || in.size() > kHelloFixedSize + kMaxNameSize) return false;
    HelloPayload tmp;
    std::ranges::copy(in.first<kPeerIdSize>(), tmp.peer_id.begin());
    tmp.listen_port = get_be<std::uint16_t>(in.subspan<kPeerIdSize, 2>());
    tmp.caps = get_be<std::uint32_t>(in.subspan<kPeerIdSize + 2, 4>());
    const auto name = in.subspan(kHelloFixedSize);
    tmp.name.assign(name.begin(), name.end());  // UTF-8 не проверяем: это просто байты
    out = std::move(tmp);
    return true;
}

std::vector<std::uint8_t> encode_reason(std::uint16_t reason) {
    std::vector<std::uint8_t> out;
    append_be<std::uint16_t>(out, reason);
    return out;
}

bool decode_reason(std::span<const std::uint8_t> in, std::uint16_t& out) noexcept {
    if (in.size() != 2) return false;
    out = get_be<std::uint16_t>(in.first<2>());
    return true;
}

std::vector<std::uint8_t> encode_ping(std::uint64_t timestamp_ns) {
    std::vector<std::uint8_t> out;
    append_be<std::uint64_t>(out, timestamp_ns);
    return out;
}

bool decode_ping(std::span<const std::uint8_t> in, std::uint64_t& out) noexcept {
    if (in.size() != 8) return false;
    out = get_be<std::uint64_t>(in.first<8>());
    return true;
}

}  // namespace p2p
