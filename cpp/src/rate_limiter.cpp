#include "p2pcore/rate_limiter.hpp"

#include <algorithm>
#include <cmath>

namespace p2p {

TokenBucket::TokenBucket(std::uint64_t burst) noexcept
    : burst_(static_cast<double>(burst)), tokens_(static_cast<double>(burst)) {}

void TokenBucket::refill(Clock::time_point now) noexcept {
    if (now <= last_) return;  // время не идёт назад: steady_clock, но вызовы могут прийти не по порядку
    const double elapsed = std::chrono::duration<double>(now - last_).count();
    last_ = now;
    tokens_ = std::min(burst_, tokens_ + elapsed * static_cast<double>(rate_));
}

void TokenBucket::set_rate(std::uint64_t bytes_per_second, Clock::time_point now) noexcept {
    if (bytes_per_second == 0) {
        // Без лимита ведро не тратится; при следующем включении начинаем с полного ведра.
        rate_ = 0;
        tokens_ = burst_;
        last_ = now;
        return;
    }
    // Токены, накопленные по старой скорости, сохраняем (пока лимита не было — ведро полное).
    if (rate_ != 0) {
        refill(now);
    } else {
        tokens_ = burst_;
    }
    last_ = std::max(last_, now);
    rate_ = std::max(bytes_per_second, kMinRate);
}

std::chrono::nanoseconds TokenBucket::take(std::uint64_t n, Clock::time_point now) noexcept {
    if (rate_ == 0) return std::chrono::nanoseconds::zero();
    refill(now);
    tokens_ -= static_cast<double>(n);
    if (tokens_ >= 0) return std::chrono::nanoseconds::zero();
    // Долг гасится со скоростью rate; округляем вверх, чтобы не прийти на наносекунду раньше.
    const double seconds = -tokens_ / static_cast<double>(rate_);
    return std::chrono::nanoseconds(static_cast<std::int64_t>(std::ceil(seconds * 1e9)));
}

std::size_t TokenBucket::slice_size(std::size_t max) const noexcept {
    if (rate_ == 0) return max;
    const std::uint64_t eighth = rate_ / 8;
    return static_cast<std::size_t>(std::clamp<std::uint64_t>(eighth, kMinSlice, std::max<std::size_t>(max, kMinSlice)));
}

double TokenBucket::tokens(Clock::time_point now) noexcept {
    if (rate_ != 0) refill(now);
    return tokens_;
}

}  // namespace p2p
