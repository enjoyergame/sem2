#include "p2pcore/bitfield.hpp"

#include <bit>

namespace p2p {
namespace {

constexpr std::uint8_t mask(std::size_t index) noexcept {
    return static_cast<std::uint8_t>(0x80u >> (index & 7u));  // MSB-first
}

}  // namespace

Bitfield::Bitfield(std::size_t size) : size_(size), data_(byte_size(size), 0) {}

Bitfield Bitfield::full(std::size_t size) {
    Bitfield bf(size);
    for (std::size_t i = 0; i < size / 8; ++i) bf.data_[i] = 0xFF;
    for (std::size_t i = size / 8 * 8; i < size; ++i) bf.set(i);
    return bf;
}

std::optional<Bitfield> Bitfield::from_bytes(std::span<const std::uint8_t> data, std::size_t size) {
    std::string ignored;
    return from_bytes(data, size, ignored);
}

std::optional<Bitfield> Bitfield::from_bytes(std::span<const std::uint8_t> data, std::size_t size,
                                             std::string& error) {
    const std::size_t nbytes = byte_size(size);
    if (data.size() != nbytes) {
        error = "expected " + std::to_string(nbytes) + " bytes for " + std::to_string(size) +
                " bits, got " + std::to_string(data.size());
        return std::nullopt;
    }
    if (size % 8 != 0 && (data.back() & (0xFFu >> (size % 8))) != 0) {
        error = "trailing bits must be zero";
        return std::nullopt;
    }
    Bitfield bf;
    bf.size_ = size;
    bf.data_.assign(data.begin(), data.end());
    return bf;
}

bool Bitfield::get(std::size_t index) const noexcept {
    return index < size_ && (data_[index >> 3] & mask(index)) != 0;
}

void Bitfield::set(std::size_t index) noexcept {
    if (index < size_) data_[index >> 3] = static_cast<std::uint8_t>(data_[index >> 3] | mask(index));
}

void Bitfield::clear(std::size_t index) noexcept {
    if (index < size_) data_[index >> 3] = static_cast<std::uint8_t>(data_[index >> 3] & ~mask(index));
}

std::size_t Bitfield::count() const noexcept {
    std::size_t n = 0;
    for (const std::uint8_t b : data_) n += static_cast<std::size_t>(std::popcount(b));
    return n;
}

}  // namespace p2p
