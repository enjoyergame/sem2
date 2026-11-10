// Два настоящих Engine на 127.0.0.1 — те же сценарии, что в tests/test_fake_engine.py,
// но с ожиданием событий: настоящее ядро асинхронное.
#include <doctest/doctest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "p2pcore/bitfield.hpp"
#include "p2pcore/engine.hpp"

using namespace p2p;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr std::uint32_t kChunk = 1000;

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / fs::path(u8"p2pcore_петля") / std::to_string(rd());
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::remove(path.parent_path(), ec);
    }
};

EngineConfig config(char id, const std::string& name) {
    EngineConfig c;
    c.peer_id = std::string(16, id);
    c.name = name;
    return c;
}

// Копит события движка и ждёт нужного.
struct Events {
    Engine& engine;
    std::vector<Event> all;

    // Ждать, пока среди накопленных событий не найдётся подходящее; вернуть его.
    const Event* wait(const std::function<bool(const Event&)>& pred, std::chrono::milliseconds timeout = 5s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            for (const Event& e : all) {
                if (pred(e)) return &e;
            }
            if (std::chrono::steady_clock::now() > deadline) return nullptr;
            for (Event& e : engine.wait_events(20)) all.push_back(std::move(e));
        }
    }
    const Event* wait_type(EventType t, std::chrono::milliseconds timeout = 5s) {
        return wait([t](const Event& e) { return e.type == t; }, timeout);
    }
    // Ждать, пока событий типа t станет n.
    std::vector<Event> wait_count(EventType t, std::size_t n, std::chrono::milliseconds timeout = 5s) {
        wait([&](const Event&) { return count(t) >= n; }, timeout);
        std::vector<Event> out;
        for (const Event& e : all) {
            if (e.type == t) out.push_back(e);
        }
        return out;
    }
    std::size_t count(EventType t) const {
        return static_cast<std::size_t>(std::ranges::count_if(all, [t](const Event& e) { return e.type == t; }));
    }
    void collect_for(std::chrono::milliseconds d) {
        const auto deadline = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < deadline) {
            for (Event& e : engine.wait_events(10)) all.push_back(std::move(e));
        }
    }
    void clear() { all.clear(); }
};

std::string random_bytes(std::size_t n, unsigned seed) {
    std::mt19937 gen(seed);
    std::string s(n, '\0');
    for (char& c : s) c = static_cast<char>(gen() & 0xFF);
    return s;
}

void write_file(const fs::path& p, const std::string& data) {
    std::ofstream(p, std::ios::binary).write(data.data(), static_cast<std::streamsize>(data.size()));
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

std::string empty_bitfield(std::uint64_t size, std::uint32_t chunk) {
    const Bitfield bf(static_cast<std::size_t>((size + chunk - 1) / chunk));
    return std::string(bf.bytes().begin(), bf.bytes().end());
}

std::vector<std::uint32_t> range(std::uint32_t n) {
    std::vector<std::uint32_t> v(n);
    for (std::uint32_t i = 0; i < n; ++i) v[i] = i;
    return v;
}

// Пара движков: seeder отдаёт, leecher качает (как pair() в тестах Python).
struct Pair {
    Engine seeder;
    Engine leecher;
    std::uint16_t port;
    Events se{seeder, {}};
    Events le{leecher, {}};

    explicit Pair(EngineConfig seeder_cfg = config('S', "seeder"))
        : seeder(std::move(seeder_cfg)), leecher(config('L', "Ноутбук")), port(seeder.start()) {
        leecher.start();
    }

    // Раздать файл на seeder и открыть его загрузку на leecher.
    std::string setup_transfer(const TempDir& dir, const std::string& file_id, std::size_t size, fs::path& part) {
        const std::string data = random_bytes(size, 42);
        const fs::path src = dir.path / u8"исходный.bin";
        write_file(src, data);
        part = dir.path / u8"загрузка" / "dst.part";  // папки ещё нет — open_download создаст
        seeder.share_file(file_id, src, size, kChunk);
        leecher.open_download(file_id, part, size, kChunk, empty_bitfield(size, kChunk));
        return data;
    }

    ConnId connect() {
        const ConnId c = leecher.connect("127.0.0.1", port);
        REQUIRE(le.wait_type(EventType::PeerConnected));
        REQUIRE(se.wait_type(EventType::PeerConnected));
        return c;
    }
};

const std::string kFile = std::string(16, '\x07');

}  // namespace

// ================================================================ соединения

TEST_CASE("loopback: connect emits PEER_CONNECTED on both sides") {
    Pair p;
    const ConnId conn = p.leecher.connect("127.0.0.1", p.port);
    const Event* out = p.le.wait_type(EventType::PeerConnected);
    const Event* inc = p.se.wait_type(EventType::PeerConnected);
    REQUIRE(out);
    REQUIRE(inc);
    CHECK(out->conn_id == conn);
    CHECK(out->code == 0);
    CHECK(out->data == std::string(16, 'S') + "seeder");
    CHECK(inc->code == 1);
    CHECK(inc->data == std::string(16, 'L') + "Ноутбук");
    CHECK(p.leecher.stats()["connections"] == 1);
}

TEST_CASE("loopback: connect to a closed port and before start raise P2PError") {
    Pair p;
    CHECK_THROWS_AS(p.leecher.connect("127.0.0.1", 1, 2000), P2PError);
    Engine idle(config('I', "idle"));
    CHECK_THROWS_AS(idle.connect("127.0.0.1", p.port), P2PError);
}

TEST_CASE("loopback: port in use, double start") {
    Engine a(config('A', "a"));
    Engine b(config('B', "b"));
    const std::uint16_t port = a.start();
    CHECK(port != 0);
    CHECK_THROWS_AS(b.start(port), P2PError);
    CHECK_THROWS_AS(a.start(), P2PError);
    a.stop();
    CHECK(b.start(port) == port);  // после stop порт свободен
}

TEST_CASE("loopback: connecting to yourself is refused without events") {
    Engine e(config('E', "self"));
    const std::uint16_t port = e.start();
    CHECK_THROWS_AS(e.connect("127.0.0.1", port), P2PError);
    Events ev{e, {}};
    ev.collect_for(200ms);
    CHECK(ev.count(EventType::PeerConnected) == 0);
    CHECK(ev.count(EventType::PeerDisconnected) == 0);
}

TEST_CASE("loopback: control and ping") {
    Pair p;
    const ConnId conn = p.connect();
    const ConnId inc = p.se.wait_type(EventType::PeerConnected)->conn_id;
    p.leecher.send_control(conn, R"({"t":"list"})");
    p.leecher.ping(conn);
    const Event* ctl = p.se.wait_type(EventType::Control);
    REQUIRE(ctl);
    CHECK(ctl->data == R"({"t":"list"})");
    CHECK(ctl->conn_id == inc);
    const Event* pong = p.le.wait_type(EventType::Pong);
    REQUIRE(pong);
    CHECK(pong->value >= 0.0);
    CHECK(pong->conn_id == conn);
}

TEST_CASE("loopback: disconnect notifies both sides with LOCAL / REMOTE") {
    Pair p;
    const ConnId conn = p.connect();
    p.leecher.disconnect(conn);
    const Event* mine = p.le.wait_type(EventType::PeerDisconnected);
    const Event* theirs = p.se.wait_type(EventType::PeerDisconnected);
    REQUIRE(mine);
    REQUIRE(theirs);
    CHECK(mine->code == static_cast<int>(DisconnectCode::Local));
    CHECK(theirs->code == static_cast<int>(DisconnectCode::Remote));
    CHECK_THROWS_AS(p.leecher.ping(conn), P2PError);
}

TEST_CASE("loopback: stop is idempotent, closes connections, wakes wait_events") {
    Pair p;
    p.connect();
    p.seeder.stop();
    p.seeder.stop();
    const Event* d = p.le.wait_type(EventType::PeerDisconnected);
    REQUIRE(d);
    CHECK(d->code == static_cast<int>(DisconnectCode::Remote));

    const auto t0 = std::chrono::steady_clock::now();
    (void)p.seeder.wait_events(5000);  // после stop не блокируется
    CHECK(std::chrono::steady_clock::now() - t0 < 1s);
}

TEST_CASE("loopback: wait_events times out") {
    Engine e(config('W', "w"));
    e.start();
    const auto t0 = std::chrono::steady_clock::now();
    CHECK(e.wait_events(50).empty());
    const auto dt = std::chrono::steady_clock::now() - t0;
    CHECK(dt >= 40ms);
    CHECK(dt < 1s);
}

// =================================================================== передача

TEST_CASE("loopback: full download, cyrillic paths, progress and stats") {
    TempDir dir;
    Pair p;
    fs::path part;
    const std::string data = p.setup_transfer(dir, kFile, 4500, part);
    const ConnId conn = p.connect();

    p.leecher.request_chunks(conn, kFile, range(5));
    const auto received = p.le.wait_count(EventType::ChunkReceived, 5);
    std::vector<std::uint32_t> idx;
    for (const Event& e : received) idx.push_back(e.chunk_index);
    std::ranges::sort(idx);
    CHECK(idx == range(5));
    for (const Event& e : received) CHECK(e.file_id == kFile);
    CHECK(read_file(part) == data);

    const Event* prog = p.le.wait([&](const Event& e) {
        return e.type == EventType::Progress && e.bytes_done == data.size();
    });
    REQUIRE(prog);
    CHECK(prog->value == static_cast<double>(Direction::Download));
    CHECK(prog->file_id == kFile);
    CHECK(p.se.wait([&](const Event& e) {
        return e.type == EventType::Progress && e.bytes_done == data.size() &&
               e.value == static_cast<double>(Direction::Upload);
    }));
    CHECK(p.leecher.stats()["bytes_received"] == 4500);
    CHECK(p.seeder.stats()["bytes_sent"] == 4500);
    CHECK(p.le.count(EventType::ChunkFailed) == 0);

    // после close_file .part можно переименовать (Windows не даёт трогать открытый файл)
    p.leecher.flush(kFile);
    p.leecher.close_file(kFile);
    fs::rename(part, dir.path / "done.bin");
}

TEST_CASE("loopback: larger file with multi-slice chunks and pipelining") {
    TempDir dir;
    Pair p;
    const std::size_t size = 5 * 1024 * 1024 + 123;
    const std::uint32_t chunk = 256 * 1024;  // 4 слайса по 64 КиБ на чанк
    const std::string data = random_bytes(size, 7);
    const fs::path src = dir.path / "big.bin";
    write_file(src, data);
    const fs::path part = dir.path / "big.part";
    p.seeder.share_file(kFile, src, size, chunk);
    p.leecher.open_download(kFile, part, size, chunk, empty_bitfield(size, chunk));
    const ConnId conn = p.connect();
    const auto n = static_cast<std::uint32_t>((size + chunk - 1) / chunk);
    p.leecher.request_chunks(conn, kFile, range(n));
    CHECK(p.le.wait_count(EventType::ChunkReceived, n, 20s).size() == n);
    p.leecher.close_file(kFile);
    CHECK(read_file(part) == data);
}

TEST_CASE("loopback: bitfield exchange") {
    TempDir dir;
    Pair p;
    fs::path part;
    p.setup_transfer(dir, kFile, 4500, part);
    const ConnId conn = p.connect();
    const ConnId inc = p.se.wait_type(EventType::PeerConnected)->conn_id;
    p.seeder.send_bitfield(inc, kFile);
    const Event* ev = p.le.wait_type(EventType::Bitfield);
    REQUIRE(ev);
    CHECK(ev->file_id == kFile);
    CHECK(ev->conn_id == conn);
    const auto bf = Bitfield::from_bytes(std::span(reinterpret_cast<const std::uint8_t*>(ev->data.data()), ev->data.size()), 5);
    REQUIRE(bf);
    CHECK(bf->is_complete());
}

TEST_CASE("loopback: NOT_HAVE and UNKNOWN_FILE") {
    TempDir dir;
    Pair p;
    fs::path part;
    p.setup_transfer(dir, kFile, 4500, part);
    const std::string other(16, '\x01');
    p.seeder.open_download(other, dir.path / "x.part", 3000, kChunk, empty_bitfield(3000, kChunk));
    p.leecher.open_download(other, dir.path / "y.part", 3000, kChunk, empty_bitfield(3000, kChunk));
    const std::string unknown(16, '\x02');
    p.leecher.open_download(unknown, dir.path / "z.part", 1000, kChunk, empty_bitfield(1000, kChunk));
    const ConnId conn = p.connect();

    p.leecher.request_chunks(conn, other, {0});
    p.leecher.request_chunks(conn, kFile, {99});
    p.leecher.request_chunks(conn, unknown, {0});
    const auto failed = p.le.wait_count(EventType::ChunkFailed, 3);
    REQUIRE(failed.size() == 3);
    CHECK(failed[0].code == static_cast<int>(FailCode::NotHave));
    CHECK(failed[1].code == static_cast<int>(FailCode::NotHave));
    CHECK(failed[2].code == static_cast<int>(FailCode::UnknownFile));
}

TEST_CASE("loopback: set_have makes a downloaded chunk servable") {
    TempDir dir;
    Pair p;
    fs::path part;
    p.setup_transfer(dir, kFile, 4500, part);
    const ConnId conn = p.connect();
    p.leecher.request_chunks(conn, kFile, {0});
    REQUIRE(p.le.wait_type(EventType::ChunkReceived));
    p.leecher.set_have(kFile, 0);
    p.leecher.send_bitfield(conn, kFile);
    const Event* bf = p.se.wait_type(EventType::Bitfield);
    REQUIRE(bf);
    CHECK(bf->data == std::string(1, '\x80'));
    CHECK_THROWS_AS(p.leecher.set_have(kFile, 5), P2PError);
}

TEST_CASE("loopback: argument errors") {
    TempDir dir;
    Engine e(config('X', "x"));
    e.start();
    CHECK_THROWS_AS(e.open_download(std::string(16, '\0'), dir.path / "a.part", 5000, kChunk, "\0\0\0"), P2PError);
    CHECK_THROWS_AS(e.share_file(std::string(16, '\0'), dir.path / "missing.bin", 10, kChunk), P2PError);
    CHECK_THROWS_AS(e.share_file("short", dir.path / "missing.bin", 10, kChunk), P2PError);
    CHECK_THROWS_AS(e.set_have(std::string(16, '\x09'), 0), P2PError);
    CHECK_THROWS_AS(e.flush(std::string(16, '\x09')), P2PError);
    e.close_file(std::string(16, '\x09'));  // неизвестный файл — молча
    CHECK_THROWS_AS(e.ping(12345), P2PError);
    CHECK_THROWS_AS(Engine(EngineConfig{.peer_id = "short"}), P2PError);
}

TEST_CASE("loopback: request for a file not opened for download is refused") {
    TempDir dir;
    Pair p;
    fs::path part;
    p.setup_transfer(dir, kFile, 4500, part);
    const ConnId inc = (p.connect(), p.se.wait_type(EventType::PeerConnected)->conn_id);
    CHECK_THROWS_AS(p.seeder.request_chunks(inc, kFile, {0}), P2PError);  // у seeder это раздача
}

TEST_CASE("loopback: cancel_chunks frees the window and produces no events") {
    TempDir dir;
    EngineConfig cfg = config('S', "seeder");
    Pair p(cfg);
    fs::path part;
    const std::string data = p.setup_transfer(dir, kFile, 4500, part);
    const ConnId conn = p.connect();
    p.leecher.request_chunks(conn, kFile, range(5));
    p.leecher.cancel_chunks(conn, kFile, range(5));
    p.le.collect_for(300ms);
    // что-то могло успеть прийти до отмены, но провалов «из-за отмены» быть не должно
    CHECK(p.le.count(EventType::ChunkFailed) == 0);
    p.le.clear();
    p.leecher.request_chunks(conn, kFile, range(5));  // окно свободно — всё скачивается заново
    CHECK(p.le.wait_count(EventType::ChunkReceived, 5).size() == 5);
    p.leecher.close_file(kFile);
    CHECK(read_file(part) == data);
}

// ===================================================================== сбои

TEST_CASE("loopback: fault_corrupt_chunk passes CRC but data differs, retry is clean") {
    TempDir dir;
    EngineConfig cfg = config('S', "seeder");
    cfg.fault_corrupt_chunk = 2;
    Pair p(cfg);
    fs::path part;
    const std::string data = p.setup_transfer(dir, kFile, 4500, part);
    const ConnId conn = p.connect();

    p.leecher.request_chunks(conn, kFile, {2});
    REQUIRE(p.le.wait_type(EventType::ChunkReceived));  // CRC совпал: портили до подсчёта
    CHECK(p.le.count(EventType::ChunkFailed) == 0);
    p.leecher.flush(kFile);
    CHECK(read_file(part).substr(2000, 1000) != data.substr(2000, 1000));  // это поймает SHA-256 в Python

    p.le.clear();
    p.leecher.request_chunks(conn, kFile, {2});
    REQUIRE(p.le.wait_type(EventType::ChunkReceived));
    p.leecher.flush(kFile);
    CHECK(read_file(part).substr(2000, 1000) == data.substr(2000, 1000));
}

TEST_CASE("loopback: fault_drop_after_bytes, then resume on a new connection") {
    TempDir dir;
    EngineConfig cfg = config('S', "seeder");
    cfg.fault_drop_after_bytes = 2500;
    Pair p(cfg);
    fs::path part;
    const std::string data = p.setup_transfer(dir, kFile, 4500, part);
    const ConnId conn = p.connect();
    p.le.clear();
    p.se.clear();

    p.leecher.request_chunks(conn, kFile, range(5));
    const Event* disc = p.le.wait_type(EventType::PeerDisconnected);
    REQUIRE(disc);
    CHECK(disc->code == static_cast<int>(DisconnectCode::Fault));

    std::vector<std::uint32_t> received, failed;
    for (const Event& e : p.le.all) {
        if (e.type == EventType::ChunkReceived) received.push_back(e.chunk_index);
        if (e.type == EventType::ChunkFailed) {
            failed.push_back(e.chunk_index);
            CHECK(e.code == static_cast<int>(FailCode::Disconnect));
        }
    }
    CHECK(received == std::vector<std::uint32_t>{0, 1});
    CHECK(failed == std::vector<std::uint32_t>{2, 3, 4});
    const Event* sdisc = p.se.wait_type(EventType::PeerDisconnected);
    REQUIRE(sdisc);
    CHECK(sdisc->code == static_cast<int>(DisconnectCode::Fault));

    // докачка: новое соединение, просим только недостающее
    for (const std::uint32_t i : received) p.leecher.set_have(kFile, i);
    p.le.clear();
    const ConnId conn2 = p.leecher.connect("127.0.0.1", p.port);
    p.leecher.request_chunks(conn2, kFile, failed);
    CHECK(p.le.wait_count(EventType::ChunkReceived, 3).size() == 3);
    p.leecher.close_file(kFile);
    CHECK(read_file(part) == data);
}

TEST_CASE("loopback: keepalive timeout closes a silent connection") {
    // Пир, который подключился по TCP и молчит, отваливается по таймауту рукопожатия/тишины.
    EngineConfig cfg = config('K', "k");
    cfg.keepalive_ms = 50;
    cfg.idle_timeout_ms = 200;
    Engine a(cfg);
    Engine b(cfg);
    const std::uint16_t port = a.start();
    b.start();
    Events ea{a, {}};
    const ConnId c = b.connect("127.0.0.1", port);
    REQUIRE(ea.wait_type(EventType::PeerConnected));
    ea.collect_for(600ms);  // PING каждые 50 мс держит соединение живым дольше idle_timeout
    CHECK(ea.count(EventType::PeerDisconnected) == 0);
    CHECK(ea.count(EventType::Pong) > 0);
    b.disconnect(c);
}
