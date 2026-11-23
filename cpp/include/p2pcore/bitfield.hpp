#pragma once
// Битовая карта чанков. Формат совпадает с BITFIELD протокола и с
// src/p2pshare/storage/bitfield.py: MSB-first (бит 0 — старший бит байта 0),
// ceil(n/8) байт, хвостовые биты последнего байта равны 0.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace p2p {

class Bitfield {
public:
    Bitfield() = default;
    explicit Bitfield(std::size_t size);  // все биты 0

    static Bitfield full(std::size_t size);
    // Разбор присланной карты: nullopt, если длина не ceil(size/8) или хвостовые биты не нули.
    static std::optional<Bitfield> from_bytes(std::span<const std::uint8_t> data, std::size_t size);
    // То же, но с текстом ошибки (для сообщений исключений).
    static std::optional<Bitfield> from_bytes(std::span<const std::uint8_t> data, std::size_t size,
                                              std::string& error);

    static constexpr std::size_t byte_size(std::size_t bits) noexcept { return (bits + 7) / 8; }

    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    // Индекс за пределами — false/no-op: удобно для чужих запросов (NOT_HAVE), без исключений.
    [[nodiscard]] bool get(std::size_t index) const noexcept;
    void set(std::size_t index) noexcept;
    void clear(std::size_t index) noexcept;
    [[nodiscard]] std::size_t count() const noexcept;
    [[nodiscard]] bool is_complete() const noexcept { return count() == size_; }

    [[nodiscard]] const std::vector<std::uint8_t>& bytes() const noexcept { return data_; }

    bool operator==(const Bitfield&) const = default;

private:
    std::size_t size_ = 0;
    std::vector<std::uint8_t> data_;
};

}  // namespace p2p
