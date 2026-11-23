#include "p2pcore/crc32c.hpp"

#include <array>

namespace p2p {
namespace {

// Таблица на 256 значений считается при компиляции. Обработка по байту:
// ~0,5–1 ГБ/с, этого хватает с запасом (гигабитная сеть — ~115 МБ/с).
// Ускорение на будущее: slicing-by-8 или инструкция crc32 из SSE4.2.
constexpr std::array<std::uint32_t, 256> make_table() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1u) ? (c >> 1) ^ 0x82F63B78u : (c >> 1);
        table[i] = c;
    }
    return table;
}

constexpr auto kTable = make_table();

}  // namespace

void Crc32c::update(std::span<const std::uint8_t> data) noexcept {
    std::uint32_t c = state_;
    for (const std::uint8_t b : data) c = kTable[(c ^ b) & 0xFFu] ^ (c >> 8);
    state_ = c;
}

std::uint32_t crc32c(std::span<const std::uint8_t> data) noexcept {
    Crc32c crc;
    crc.update(data);
    return crc.value();
}

}  // namespace p2p
