// TokenBucket на подставном времени: результаты не зависят от скорости машины.
#include <doctest/doctest.h>

#include <chrono>
#include <cstdint>

#include "p2pcore/rate_limiter.hpp"

using namespace p2p;
using namespace std::chrono_literals;

namespace {

using Clock = TokenBucket::Clock;
const Clock::time_point t0 = Clock::time_point{} + 1000s;  // произвольная точка отсчёта

constexpr std::uint64_t KiB = 1024;

}  // namespace

TEST_CASE("token bucket: unlimited by default never waits") {
    TokenBucket b;
    CHECK_FALSE(b.limited());
    for (int i = 0; i < 1000; ++i) CHECK(b.take(1u << 20, t0) == 0ns);
    CHECK(b.slice_size(64 * KiB) == 64 * KiB);
}

TEST_CASE("token bucket: burst goes through at once, then the debt makes us wait") {
    TokenBucket b(256 * KiB);
    b.set_rate(100 * KiB, t0);
    // 4 порции по 64 КиБ — ровно burst: ждать не нужно
    for (int i = 0; i < 4; ++i) CHECK(b.take(64 * KiB, t0) == 0ns);
    // пятая уходит в долг на 64 КиБ: при 100 КиБ/с это 0,64 с
    CHECK(b.take(64 * KiB, t0) == 640ms);
    // шестая, взятая тут же, ждёт ещё и долг пятой
    CHECK(b.take(64 * KiB, t0) == 1280ms);
}

TEST_CASE("token bucket: tokens refill with time, but never above burst") {
    TokenBucket b(256 * KiB);
    b.set_rate(128 * KiB, t0);
    CHECK(b.take(256 * KiB, t0) == 0ns);
    CHECK(b.tokens(t0) == doctest::Approx(0.0));
    CHECK(b.tokens(t0 + 1s) == doctest::Approx(128.0 * KiB));
    CHECK(b.tokens(t0 + 1h) == doctest::Approx(256.0 * KiB));  // долгий простой — не больше burst
    CHECK(b.take(256 * KiB, t0 + 1h) == 0ns);
    CHECK(b.take(128 * KiB, t0 + 1h) == 1s);
}

TEST_CASE("token bucket: average rate over a long transfer equals the limit") {
    // Передаём 10 МиБ порциями, каждый раз выжидая ровно столько, сколько велит take().
    TokenBucket b(256 * KiB);
    const std::uint64_t rate = 1000 * KiB;
    b.set_rate(rate, t0);
    Clock::time_point now = t0;
    const std::uint64_t total = 10 * 1024 * KiB;
    const std::size_t slice = b.slice_size(64 * KiB);
    for (std::uint64_t sent = 0; sent < total; sent += slice) now += b.take(slice, now);
    const double seconds = std::chrono::duration<double>(now - t0).count();
    // первые burst байт бесплатны, а перед каждой следующей порцией (и последней тоже) ждём:
    // (10 МиБ - 256 КиБ) / 1000 КиБ/с
    const double expected = static_cast<double>(total - 256 * KiB) / static_cast<double>(rate);
    CHECK(seconds == doctest::Approx(expected).epsilon(0.001));
}

TEST_CASE("token bucket: rate below the minimum is raised, zero switches the limit off") {
    TokenBucket b;
    b.set_rate(1, t0);
    CHECK(b.rate() == TokenBucket::kMinRate);
    b.set_rate(0, t0);
    CHECK_FALSE(b.limited());
    CHECK(b.take(1u << 30, t0) == 0ns);
}

TEST_CASE("token bucket: switching the limit off forgives the debt, switching on starts full") {
    TokenBucket b(256 * KiB);
    b.set_rate(100 * KiB, t0);
    (void)b.take(1024 * KiB, t0);  // большой долг
    CHECK(b.tokens(t0) < 0);
    b.set_rate(0, t0);
    b.set_rate(100 * KiB, t0);
    CHECK(b.tokens(t0) == doctest::Approx(256.0 * KiB));
}

TEST_CASE("token bucket: changing the rate keeps tokens earned at the old rate") {
    TokenBucket b(256 * KiB);
    b.set_rate(100 * KiB, t0);
    (void)b.take(256 * KiB, t0);
    b.set_rate(200 * KiB, t0 + 1s);  // за секунду по старой скорости накопилось 100 КиБ
    CHECK(b.tokens(t0 + 1s) == doctest::Approx(100.0 * KiB));
    CHECK(b.tokens(t0 + 1500ms) == doctest::Approx(200.0 * KiB));  // дальше — по новой
}

TEST_CASE("token bucket: time going backwards does not add tokens") {
    TokenBucket b(256 * KiB);
    b.set_rate(100 * KiB, t0 + 10s);
    (void)b.take(256 * KiB, t0 + 10s);
    CHECK(b.tokens(t0) == doctest::Approx(0.0));
    CHECK(b.take(100 * KiB, t0) == 1s);
}

TEST_CASE("token bucket: slice size follows the rate") {
    TokenBucket b;
    b.set_rate(8 * 1024 * KiB, t0);  // 8 МиБ/с: восьмая часть — 1 МиБ, упираемся в max
    CHECK(b.slice_size(64 * KiB) == 64 * KiB);
    b.set_rate(80 * KiB, t0);  // восьмая часть — 10 КиБ
    CHECK(b.slice_size(64 * KiB) == 10 * KiB);
    b.set_rate(1, t0);  // минимум скорости 1 КиБ/с, минимум порции 512 байт
    CHECK(b.slice_size(64 * KiB) == TokenBucket::kMinSlice);
}
