#pragma once
// CRC32C (Castagnoli): полином 0x1EDC6F41, в отражённой форме 0x82F63B78,
// начальное значение и финальный XOR 0xFFFFFFFF. Контрольное значение:
// crc32c("123456789") == 0xE3069283 (RFC 3720, приложение B.4).
//
// Ловит ошибки работы с буферами в самом C++ (неверное смещение, не тот слайс);
// целостность содержимого файла проверяет Python по SHA-256.

#include <cstdint>
#include <span>

namespace p2p {

// Потоковый подсчёт: нагрузку CHUNK читаем слайсами по 64 КиБ, CRC обновляем по мере чтения.
class Crc32c {
public:
    void update(std::span<const std::uint8_t> data) noexcept;
    [[nodiscard]] std::uint32_t value() const noexcept { return ~state_; }
    void reset() noexcept { state_ = 0xFFFFFFFFu; }

private:
    std::uint32_t state_ = 0xFFFFFFFFu;
};

// CRC32C всего буфера за один вызов (0 для пустого).
[[nodiscard]] std::uint32_t crc32c(std::span<const std::uint8_t> data) noexcept;

}  // namespace p2p
