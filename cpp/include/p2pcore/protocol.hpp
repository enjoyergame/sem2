#pragma once

#include <array>
#include <cstdint>
#include <span>
#include <string>

namespace p2p {
inline constexpr std::uint32_t kMagic = 0x50325046; // "P2PF"
inline constexpr std::uint8_t kProtocolVersion = 1;
inline constexpr std::size_t kHeaderSize = 40;
inline constexpr std::uint32_t kNoChunk = 0xFFFFFFFF;
inline constexpr std::uint32_t kMaxPayload = 16u << 20;

using FileId = std::array<std::uint8_t, 16>;

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
  std::uint16_t flags = 0;
  FileId file_id{};
  std::uint32_t chunk_index = kNoChunk;
  std::uint32_t payload_len = 0;
  std::uint32_t payload_crc = 0;
  std::uint32_t seq = 0;
};

enum class DecodeError {
  None,
  BadMagic,
  BadVersion,
  PayloadTooLarge,
  UnknownType
};

enum class RejectReason : std::uint16_t {
    UnknownFile = 1, // UNKNOWN_FILE
    NotHave     = 2, // NOT_HAVE
    Busy        = 3, // BUSY
};

struct HelloPayload {
    std::array<std::uint8_t, 16> peer_id{};
    std::uint16_t listen_port = 0;
    std::uint32_t caps        = 0;
    std::string   name;
};

void encode_header(const FrameHeader &h,
                   std::span<std::uint8_t, kHeaderSize> out);
DecodeError decode_header(std::span<const std::uint8_t, kHeaderSize> in,
                          FrameHeader &h);
} // namespace p2p