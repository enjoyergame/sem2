#include "p2pcore/protocol.hpp"

namespace {
void writeU16BE(uint8_t *out, uint16_t v) {
  for (size_t i = 0; i < sizeof(uint16_t); ++i) {
    out[sizeof(uint16_t) - i - 1] = static_cast<uint8_t>(v & 0xFF);
    v >>= 8;
  }
}
void writeU32BE(uint8_t *out, uint32_t v) {
  for (size_t i = 0; i < sizeof(uint32_t); ++i) {
    out[sizeof(uint32_t) - i - 1] = static_cast<uint8_t>(v & 0xFF);
    v >>= 8;
  }
}
void writeU64BE(uint8_t *out, uint64_t v) {
  for (size_t i = 0; i < sizeof(uint64_t); ++i) {
    out[sizeof(uint64_t) - i - 1] = static_cast<uint8_t>(v & 0xFF);
    v >>= 8;
  }
}
uint16_t readU16BE(const uint8_t *in) {
  uint16_t result = 0;
  for (size_t i = 0; i < sizeof(uint16_t); ++i) {
    result <<= 8;
    result |= static_cast<uint8_t>(in[i]);
  }
  return result;
}

uint32_t readU32BE(const uint8_t *in) {
  uint32_t result = 0;
  for (size_t i = 0; i < sizeof(uint32_t); ++i) {
    result <<= 8;
    result |= static_cast<uint8_t>(in[i]);
  }
  return result;
}

uint64_t readU64BE(const uint8_t *in) {
  uint64_t result = 0;
  for (size_t i = 0; i < sizeof(uint64_t); ++i) {
    result <<= 8;
    result |= static_cast<uint8_t>(in[i]);
  }
  return result;
}
} // namespace

namespace p2p {
// Здесь позже будет реализация encode_header и decode_header

void encode_header(const FrameHeader &h,
                   std::span<std::uint8_t, kHeaderSize> out) {}

DecodeError decode_header(std::span<const std::uint8_t, kHeaderSize> in,
                          FrameHeader &h) {

  return DecodeError::None;
}
} // namespace p2p

