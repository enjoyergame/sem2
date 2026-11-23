#pragma once
// Формат кадра протокола P2P Share v1 (см. docs/PROTOCOL.md).
//
// Кадр = заголовок 40 байт + нагрузка payload_len байт. Все числа — big-endian.
// Сериализация побайтовая (сдвигами), без reinterpret_cast к упакованным структурам:
// так код не зависит ни от порядка байт процессора, ни от выравнивания.

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace p2p {

inline constexpr std::uint32_t kMagic = 0x50325046;  // "P2PF"
inline constexpr std::uint8_t kProtocolVersion = 1;
inline constexpr std::size_t kHeaderSize = 40;
inline constexpr std::uint32_t kNoChunk = 0xFFFFFFFF;
inline constexpr std::uint32_t kMaxPayload = 16u << 20;  // 16 МиБ
inline constexpr std::size_t kFileIdSize = 16;
inline constexpr std::size_t kPeerIdSize = 16;

using FileId = std::array<std::uint8_t, kFileIdSize>;
using PeerId = std::array<std::uint8_t, kPeerIdSize>;

enum class MsgType : std::uint8_t {
    Hello = 0x01,
    Ping = 0x02,
    Pong = 0x03,
    Bye = 0x0F,
    Control = 0x10,
    Bitfield = 0x20,
    Have = 0x21,
    Request = 0x30,
    Chunk = 0x31,
    Reject = 0x32,
    Cancel = 0x33,
};

struct FrameHeader {
    std::uint8_t version = kProtocolVersion;
    MsgType type = MsgType::Ping;
    std::uint16_t flags = 0;  // резерв: отправляем 0, при приёме игнорируем
    FileId file_id{};         // нули для сессионных сообщений
    std::uint32_t chunk_index = kNoChunk;
    std::uint32_t payload_len = 0;
    std::uint32_t payload_crc = 0;  // CRC32C нагрузки, 0 при пустой
    std::uint32_t seq = 0;

    bool operator==(const FrameHeader&) const = default;
};

enum class DecodeError { None, BadMagic, BadVersion, PayloadTooLarge, UnknownType };

// Причина в нагрузке REJECT (u16). Значения совпадают с CHUNK_FAILED.code 1..3.
enum class RejectReason : std::uint16_t {
    UnknownFile = 1,  // UNKNOWN_FILE: такого file_id нет
    NotHave = 2,      // NOT_HAVE: чанк ещё не скачан/не проверен или индекс вне файла
    Busy = 3,         // BUSY: очередь отправки переполнена, повторите позже
};

// Причина в нагрузке BYE (u16).
enum class ByeReason : std::uint16_t {
    Normal = 1,         // штатное закрытие (disconnect/stop)
    ProtocolError = 2,  // собеседник нарушил протокол
    Incompatible = 3,   // несовместимая версия / подключение к самому себе
    Duplicate = 4,      // дубль соединения (резерв, решает Python)
};

// Нагрузка HELLO: peer_id[16] + listen_port u16 + caps u32 + имя (UTF-8, до kMaxNameSize байт).
inline constexpr std::size_t kHelloFixedSize = kPeerIdSize + 2 + 4;  // 22
inline constexpr std::size_t kMaxNameSize = 255;

struct HelloPayload {
    PeerId peer_id{};
    std::uint16_t listen_port = 0;
    std::uint32_t caps = 0;
    std::string name;

    bool operator==(const HelloPayload&) const = default;
};

// Заголовок. encode_header ничего не проверяет (отправитель — наш код), decode_header
// проверяет всё, что можно проверить по одному заголовку, в порядке:
// magic -> version -> payload_len -> type. При ошибке h не меняется.
void encode_header(const FrameHeader& h, std::span<std::uint8_t, kHeaderSize> out) noexcept;
[[nodiscard]] DecodeError decode_header(std::span<const std::uint8_t, kHeaderSize> in,
                                        FrameHeader& h) noexcept;

[[nodiscard]] bool is_known_type(std::uint8_t type) noexcept;
[[nodiscard]] std::string_view to_string(DecodeError e) noexcept;
[[nodiscard]] std::string_view to_string(MsgType t) noexcept;

// Нагрузки. decode_* возвращают false, если длина или содержимое некорректны
// (это нарушение протокола), и тогда не меняют out.
// Имя длиннее kMaxNameSize обрезается по границе символа UTF-8.
[[nodiscard]] std::vector<std::uint8_t> encode_hello(const HelloPayload& p);
[[nodiscard]] bool decode_hello(std::span<const std::uint8_t> in, HelloPayload& out);

// REJECT и BYE: ровно 2 байта (u16).
[[nodiscard]] std::vector<std::uint8_t> encode_reason(std::uint16_t reason);
[[nodiscard]] bool decode_reason(std::span<const std::uint8_t> in, std::uint16_t& out) noexcept;

// PING и PONG: ровно 8 байт (u64, время отправителя в нс; PONG возвращает его эхом).
[[nodiscard]] std::vector<std::uint8_t> encode_ping(std::uint64_t timestamp_ns);
[[nodiscard]] bool decode_ping(std::span<const std::uint8_t> in, std::uint64_t& out) noexcept;

// Обрезать UTF-8 строку до max_bytes, не разрезая многобайтовый символ.
[[nodiscard]] std::string truncate_utf8(std::string_view s, std::size_t max_bytes);

}  // namespace p2p
