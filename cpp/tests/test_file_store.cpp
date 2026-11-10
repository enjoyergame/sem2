#include <doctest/doctest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "p2pcore/errors.hpp"
#include "p2pcore/event_queue.hpp"
#include "p2pcore/file_store.hpp"

using namespace p2p;
namespace fs = std::filesystem;

namespace {

// Временная папка с кириллицей в имени: проверяем, что пути не ломаются на Windows.
struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / fs::path(u8"p2pcore_тест") / std::to_string(rd());
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::remove(path.parent_path(), ec);  // удалится, только если пустая
    }
};

std::vector<std::uint8_t> pattern(std::size_t n, std::uint8_t seed) {
    std::vector<std::uint8_t> v(n);
    for (std::size_t i = 0; i < n; ++i) v[i] = static_cast<std::uint8_t>(seed + i * 13);
    return v;
}

}  // namespace

TEST_CASE("file_store: preallocate, write at offsets, read back") {
    TempDir dir;
    const fs::path p = dir.path / u8"фильм.mkv.part";
    const std::uint64_t size = (1u << 20) + 3;

    PositionalFile f = PositionalFile::open_write(p, size);
    REQUIRE(f.is_open());
    CHECK(f.size() == size);
    CHECK(fs::file_size(p) == size);

    const auto tail = pattern(3, 7);
    const auto head = pattern(1000, 1);
    f.write_at(1u << 20, tail);  // сначала конец: запись «вразброс», как при докачке
    f.write_at(0, head);
    f.flush();

    std::vector<std::uint8_t> buf(1000);
    CHECK(f.read_at(0, buf) == 1000);
    CHECK(buf == head);
    buf.assign(10, 0);
    CHECK(f.read_at(1u << 20, buf) == 3);  // короткое чтение только на конце файла
    CHECK(std::vector<std::uint8_t>(buf.begin(), buf.begin() + 3) == tail);
    CHECK(f.read_at(size + 100, buf) == 0);

    // в промежутке — нули
    std::vector<std::uint8_t> gap(16, 0xEE);
    CHECK(f.read_at(5000, gap) == 16);
    CHECK(gap == std::vector<std::uint8_t>(16, 0));
}

TEST_CASE("file_store: reopen keeps data, resize changes size") {
    TempDir dir;
    const fs::path p = dir.path / "a.part";
    {
        PositionalFile f = PositionalFile::open_write(p, 100);
        f.write_at(10, pattern(5, 3));
    }
    PositionalFile f = PositionalFile::open_write(p, 100);
    std::vector<std::uint8_t> buf(5);
    CHECK(f.read_at(10, buf) == 5);
    CHECK(buf == pattern(5, 3));

    PositionalFile r = PositionalFile::open_read(p);
    CHECK(r.size() == 100);
}

TEST_CASE("file_store: another reader can open the file while we hold it for writing") {
    TempDir dir;
    const fs::path p = dir.path / "shared.part";
    PositionalFile w = PositionalFile::open_write(p, 4);
    w.write_at(0, pattern(4, 9));
    std::ifstream in(p, std::ios::binary);  // так .part читает Python-верификатор
    REQUIRE(in.good());
    char c = 0;
    in.read(&c, 1);
    CHECK(static_cast<std::uint8_t>(c) == 9);
}

TEST_CASE("file_store: missing file raises P2PError with the path in UTF-8") {
    TempDir dir;
    const fs::path p = dir.path / u8"нет_такого.bin";
    try {
        (void)PositionalFile::open_read(p);
        FAIL("expected P2PError");
    } catch (const P2PError& e) {
        const std::string msg = e.what();
        CHECK(msg.find("cannot open") != std::string::npos);
        CHECK(msg.find(path_to_utf8(fs::path(u8"нет_такого.bin"))) != std::string::npos);
    }
}

TEST_CASE("file_store: move transfers ownership") {
    TempDir dir;
    PositionalFile a = PositionalFile::open_write(dir.path / "m.part", 1);
    PositionalFile b = std::move(a);
    CHECK_FALSE(a.is_open());  // NOLINT(bugprone-use-after-move)
    CHECK(b.is_open());
    b.close();
    CHECK_FALSE(b.is_open());
}

TEST_CASE("file_entry: chunk arithmetic, last chunk shorter") {
    FileEntry e;
    e.size = 4500;
    e.chunk_size = 1000;
    CHECK(e.chunk_count() == 5);
    CHECK(e.chunk_length(0) == 1000);
    CHECK(e.chunk_length(4) == 500);
    CHECK(e.chunk_length(5) == 0);
    CHECK(e.chunk_offset(4) == 4000);
    e.size = 0;
    CHECK(e.chunk_count() == 0);
    e.size = 3000;
    CHECK(e.chunk_count() == 3);
    CHECK(e.chunk_length(2) == 1000);
}

// ---------------------------------------------------------------- EventQueue

TEST_CASE("event_queue: poll respects max and order") {
    EventQueue q;
    q.open();
    for (int i = 0; i < 5; ++i) {
        Event ev;
        ev.code = i;
        q.push(ev);
    }
    auto first = q.poll(3);
    REQUIRE(first.size() == 3);
    CHECK(first[0].code == 0);
    CHECK(first[2].code == 2);
    CHECK(q.poll(10).size() == 2);
    CHECK(q.poll(10).empty());
}

TEST_CASE("event_queue: wait times out, wakes on push and does not block when closed") {
    using namespace std::chrono;
    EventQueue q;
    q.open();
    auto t0 = steady_clock::now();
    CHECK(q.wait(milliseconds(50), 10).empty());
    CHECK(steady_clock::now() - t0 >= milliseconds(40));

    std::thread producer([&] {
        std::this_thread::sleep_for(milliseconds(30));
        q.push(Event{});
    });
    t0 = steady_clock::now();
    CHECK(q.wait(milliseconds(5000), 10).size() == 1);
    CHECK(steady_clock::now() - t0 < milliseconds(2000));
    producer.join();

    std::thread closer([&] {
        std::this_thread::sleep_for(milliseconds(30));
        q.close();
    });
    t0 = steady_clock::now();
    CHECK(q.wait(milliseconds(5000), 10).empty());
    CHECK(steady_clock::now() - t0 < milliseconds(2000));
    closer.join();

    t0 = steady_clock::now();
    CHECK(q.wait(milliseconds(5000), 10).empty());  // закрыта — сразу возвращается
    CHECK(steady_clock::now() - t0 < milliseconds(500));
}
