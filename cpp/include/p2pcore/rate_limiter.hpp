#pragma once
// Ограничение скорости: token bucket с «долгом».
//
// В ведре копятся токены (байты) со скоростью rate, но не больше burst. Чтобы передать n байт,
// их забирают из ведра сразу, даже если токенов не хватает: остаток уходит в минус (долг).
// take() возвращает, сколько подождать, пока долг не погасится, — и только потом передаём.
// Так средняя скорость равна rate, после простоя до burst байт уходит без ожидания, а
// ждать можно на asio::steady_timer — без опроса в цикле.
//
// Одно ведро на направление делят все соединения движка: тот, кто пришёл позже, ждёт и
// долг тех, кто взял раньше, — получается очередь по времени, и общий лимит соблюдается.
//
// Класс не знает про часы: время передаётся параметром. Поэтому тесты детерминированы.

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace p2p {

class TokenBucket {
public:
    using Clock = std::chrono::steady_clock;

    static constexpr std::uint64_t kDefaultBurst = 256 * 1024;
    // Ниже этого лимит не опускается: при меньшей скорости порции данных шли бы реже, чем раз
    // в несколько секунд, и соединение рвалось бы по таймауту тишины или запроса.
    static constexpr std::uint64_t kMinRate = 1024;
    // Самая маленькая порция данных (см. slice_size).
    static constexpr std::size_t kMinSlice = 512;

    explicit TokenBucket(std::uint64_t burst = kDefaultBurst) noexcept;

    // Байт в секунду; 0 — без ограничения. Значения меньше kMinRate поднимаются до kMinRate.
    void set_rate(std::uint64_t bytes_per_second, Clock::time_point now) noexcept;
    [[nodiscard]] std::uint64_t rate() const noexcept { return rate_; }
    [[nodiscard]] bool limited() const noexcept { return rate_ != 0; }

    // Забрать n байт. Возвращает, сколько ждать перед их передачей (0 — можно сразу).
    [[nodiscard]] std::chrono::nanoseconds take(std::uint64_t n, Clock::time_point now) noexcept;

    // Сколько байт передавать за раз (не больше max): без лимита — max, с лимитом — около
    // восьмой части секундного объёма. Чем ниже скорость, тем мельче порции и тем ровнее поток:
    // собеседник получает байты постоянно, а не пачкой раз в несколько секунд.
    [[nodiscard]] std::size_t slice_size(std::size_t max) const noexcept;

    // Токенов сейчас (отрицательное — долг). Для тестов.
    [[nodiscard]] double tokens(Clock::time_point now) noexcept;

private:
    void refill(Clock::time_point now) noexcept;

    std::uint64_t rate_ = 0;
    double burst_;
    double tokens_;
    Clock::time_point last_{};
};

}  // namespace p2p
