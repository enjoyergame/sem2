// p2pcore_bench: скорость передачи в зависимости от размера чанка и окна запросов (max_inflight).
// Таблица и CSV — для отчёта (ARCHITECTURE.md, этап 4: «Бенчмарки»).
//
//   p2pcore_bench loop  [опции]             оба движка в этом процессе, через 127.0.0.1
//   p2pcore_bench serve [--port P] [опции]  раздавать тестовый файл (на одном ноутбуке)
//   p2pcore_bench fetch HOST:PORT [опции]   качать его (на другом ноутбуке)
//
// Опции:
//   --size MiB          размер тестового файла (по умолчанию 256)
//   --chunks K,K,...    размеры чанка в КиБ (по умолчанию 64,256,1024,4096)
//   --windows W,W,...   значения max_inflight (по умолчанию 1,2,4,8,16)
//   --repeat N          повторов каждой комбинации, в таблице — медиана (по умолчанию 3)
//   --csv FILE          дописать все замеры в CSV
// У serve и fetch --size и --chunks должны совпадать: по ним обе стороны строят одинаковые file_id.
//
// Замер — от request_chunks до последнего CHUNK_RECEIVED; в него входят сеть, CRC32C и запись
// .part на диск (обычно в кэш ОС). На loopback сети нет: это предел самого ядра, а не LAN.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "p2pcore/bitfield.hpp"
#include "p2pcore/engine.hpp"
#include "p2pcore/protocol.hpp"

using namespace p2p;
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

namespace {

constexpr std::uint64_t kMiB = 1024 * 1024;

struct Options {
    std::string mode;
    std::string host;
    std::uint16_t port = 0;
    std::uint64_t size_mib = 256;
    std::vector<std::uint32_t> chunks_kib = {64, 256, 1024, 4096};
    std::vector<std::uint32_t> windows = {1, 2, 4, 8, 16};
    int repeat = 3;
    std::string csv;
};

[[noreturn]] void usage(const std::string& error = {}) {
    if (!error.empty()) std::fprintf(stderr, "error: %s\n\n", error.c_str());
    std::fprintf(stderr,
                 "usage:\n"
                 "  p2pcore_bench loop  [--size MiB] [--chunks 64,256,...] [--windows 1,2,...] [--repeat N] [--csv F]\n"
                 "  p2pcore_bench serve [--port P] [--size MiB] [--chunks ...]\n"
                 "  p2pcore_bench fetch HOST:PORT [--size MiB] [--chunks ...] [--windows ...] [--repeat N] [--csv F]\n");
    std::exit(2);
}

std::uint64_t parse_number(const std::string& s, const char* what) {
    try {
        std::size_t used = 0;
        const unsigned long long v = std::stoull(s, &used);
        if (used != s.size() || v == 0) throw std::invalid_argument(s);
        return v;
    } catch (const std::exception&) {
        usage(std::string("bad ") + what + ": " + s);
    }
}

std::vector<std::uint32_t> parse_list(const std::string& s, const char* what) {
    std::vector<std::uint32_t> out;
    std::stringstream in(s);
    for (std::string item; std::getline(in, item, ',');) out.push_back(static_cast<std::uint32_t>(parse_number(item, what)));
    if (out.empty()) usage(std::string("empty ") + what);
    return out;
}

Options parse(int argc, char** argv) {
    if (argc < 2) usage();
    Options o;
    o.mode = argv[1];
    if (o.mode != "loop" && o.mode != "serve" && o.mode != "fetch") usage("unknown mode " + o.mode);
    int i = 2;
    if (o.mode == "fetch") {
        if (argc < 3) usage("fetch needs HOST:PORT");
        const std::string target = argv[2];
        const auto colon = target.rfind(':');
        if (colon == std::string::npos) usage("fetch needs HOST:PORT");
        o.host = target.substr(0, colon);
        o.port = static_cast<std::uint16_t>(parse_number(target.substr(colon + 1), "port"));
        i = 3;
    }
    for (; i < argc; ++i) {
        const std::string arg = argv[i];
        if (i + 1 >= argc) usage("missing value for " + arg);
        const std::string value = argv[++i];
        if (arg == "--size") o.size_mib = parse_number(value, "size");
        else if (arg == "--chunks") o.chunks_kib = parse_list(value, "chunk size");
        else if (arg == "--windows") o.windows = parse_list(value, "window");
        else if (arg == "--repeat") o.repeat = static_cast<int>(parse_number(value, "repeat"));
        else if (arg == "--port") o.port = static_cast<std::uint16_t>(parse_number(value, "port"));
        else if (arg == "--csv") o.csv = value;
        else usage("unknown option " + arg);
    }
    for (const std::uint32_t c : o.chunks_kib) {
        if (static_cast<std::uint64_t>(c) * 1024 > kMaxPayload) usage("chunk size above 16 MiB: " + std::to_string(c));
    }
    return o;
}

// Одинаковый на обеих сторонах file_id для каждого размера чанка: "bench" + размер в КиБ.
std::string file_id_for(std::uint32_t chunk_kib) {
    std::string id = "bench" + std::to_string(chunk_kib);
    id.resize(kFileIdSize, '_');
    return id;
}

EngineConfig config(char id, std::uint32_t max_inflight) {
    EngineConfig c;
    c.peer_id = std::string(16, id);
    c.name = "p2pcore_bench";
    c.max_inflight = max_inflight;
    return c;
}

fs::path work_dir() {
    std::random_device rd;
    const fs::path dir = fs::temp_directory_path() / ("p2pcore_bench_" + std::to_string(rd()));
    fs::create_directories(dir);
    return dir;
}

// Тестовый файл из псевдослучайных байт (не нули: так честнее для любой будущей компрессии).
fs::path make_source(const fs::path& dir, std::uint64_t size) {
    const fs::path p = dir / "source.bin";
    std::ofstream out(p, std::ios::binary);
    std::mt19937_64 gen(12345);
    std::vector<std::uint64_t> block(kMiB / 8);
    for (std::uint64_t done = 0; done < size; done += kMiB) {
        for (auto& w : block) w = gen();
        const std::uint64_t n = std::min<std::uint64_t>(kMiB, size - done);
        out.write(reinterpret_cast<const char*>(block.data()), static_cast<std::streamsize>(n));
    }
    if (!out) throw std::runtime_error("cannot write " + p.string());
    return p;
}

void share_all(Engine& seeder, const fs::path& src, const Options& o) {
    for (const std::uint32_t c : o.chunks_kib) seeder.share_file(file_id_for(c), src, o.size_mib * kMiB, c * 1024);
}

// Одна загрузка файла целиком новым движком-получателем. Возвращает секунды.
double fetch_once(const std::string& host, std::uint16_t port, const fs::path& dir, std::uint64_t size,
                  std::uint32_t chunk, std::uint32_t window) {
    Engine leecher(config('F', window));
    leecher.start();
    const std::uint32_t count = static_cast<std::uint32_t>((size + chunk - 1) / chunk);
    const Bitfield empty(count);
    const fs::path part = dir / "download.part";
    std::error_code ec;
    fs::remove(part, ec);
    const std::string id = file_id_for(chunk / 1024);
    leecher.open_download(id, part, size, chunk, std::string(empty.bytes().begin(), empty.bytes().end()));
    const ConnId conn = leecher.connect(host, port, 10000);

    std::vector<std::uint32_t> all(count);
    std::iota(all.begin(), all.end(), 0u);
    const auto t0 = Clock::now();
    leecher.request_chunks(conn, id, all);
    std::uint32_t received = 0;
    while (received < count) {
        for (const Event& e : leecher.wait_events(100)) {
            if (e.type == EventType::ChunkReceived) ++received;
            if (e.type == EventType::ChunkFailed && e.code == static_cast<int>(FailCode::UnknownFile)) {
                throw std::runtime_error("the server has no file for chunk size " + std::to_string(chunk / 1024) +
                                         " KiB: --size and --chunks must be the same as on serve");
            }
            if (e.type == EventType::ChunkFailed || e.type == EventType::PeerDisconnected) {
                throw std::runtime_error("transfer failed (event " + std::to_string(static_cast<int>(e.type)) +
                                         ", code " + std::to_string(e.code) + ") " + e.message);
            }
        }
    }
    const double seconds = std::chrono::duration<double>(Clock::now() - t0).count();
    leecher.close_file(id);
    leecher.stop();
    fs::remove(part, ec);
    return seconds;
}

double median(std::vector<double> v) {
    std::ranges::sort(v);
    const std::size_t n = v.size();
    return n % 2 ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2;
}

// Прогнать все комбинации, напечатать таблицу МиБ/с (медиана повторов).
void run_matrix(const Options& o, const std::string& host, std::uint16_t port, const fs::path& dir) {
    const std::uint64_t size = o.size_mib * kMiB;
    std::ofstream csv;
    if (!o.csv.empty()) {
        const bool fresh = !fs::exists(o.csv);
        csv.open(o.csv, std::ios::app);
        if (!csv) throw std::runtime_error("cannot open " + o.csv);
        if (fresh) csv << "mode,size_mib,chunk_kib,max_inflight,run,seconds,mib_per_s\n";
    }

    std::printf("%s: %llu MiB, %d run(s) per cell, median MiB/s\n\n", o.mode.c_str(),
                static_cast<unsigned long long>(o.size_mib), o.repeat);
    std::printf("| chunk \\ max_inflight |");
    for (const std::uint32_t w : o.windows) std::printf(" %6u |", w);
    std::printf("\n|---|");
    for (std::size_t i = 0; i < o.windows.size(); ++i) std::printf("---:|");
    std::printf("\n");

    for (const std::uint32_t c : o.chunks_kib) {
        std::printf("| %6u KiB |", c);
        std::fflush(stdout);
        for (const std::uint32_t w : o.windows) {
            std::vector<double> speeds;
            for (int r = 1; r <= o.repeat; ++r) {
                const double s = fetch_once(host, port, dir, size, c * 1024, w);
                const double mib_s = static_cast<double>(o.size_mib) / s;
                speeds.push_back(mib_s);
                if (csv) csv << o.mode << ',' << o.size_mib << ',' << c << ',' << w << ',' << r << ',' << s << ',' << mib_s << '\n';
            }
            std::printf(" %6.0f |", median(speeds));
            std::fflush(stdout);
        }
        std::printf("\n");
    }
}

int run(const Options& o) {
    const fs::path dir = work_dir();
    struct Cleanup {
        fs::path dir;
        ~Cleanup() {
            std::error_code ec;
            fs::remove_all(dir, ec);
        }
    } cleanup{dir};

    if (o.mode == "fetch") {
        run_matrix(o, o.host, o.port, dir);
        return 0;
    }

    std::fprintf(stderr, "generating %llu MiB test file...\n", static_cast<unsigned long long>(o.size_mib));
    const fs::path src = make_source(dir, o.size_mib * kMiB);
    Engine seeder(config('S', 6));
    const std::uint16_t port = seeder.start(o.port);
    share_all(seeder, src, o);

    if (o.mode == "serve") {
        std::string chunks;
        for (const std::uint32_t c : o.chunks_kib) chunks += (chunks.empty() ? "" : ",") + std::to_string(c);
        std::printf("serving on port %u. On the other machine run:\n"
                    "  p2pcore_bench fetch <this-ip>:%u --size %llu --chunks %s\n"
                    "Press Enter to stop.\n",
                    port, port, static_cast<unsigned long long>(o.size_mib), chunks.c_str());
        std::fflush(stdout);
        std::string line;
        std::getline(std::cin, line);
        seeder.stop();
        return 0;
    }

    run_matrix(o, "127.0.0.1", port, dir);
    seeder.stop();
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const Options o = parse(argc, argv);
    try {
        return run(o);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "\nerror: %s\n", e.what());
        return 1;
    }
}
