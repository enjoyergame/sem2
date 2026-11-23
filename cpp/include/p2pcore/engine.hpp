#pragma once
// Engine — фасад C++ ядра, тот же API, что у FakeEngine (src/p2pshare/engine/fake_engine.py).
//
// Внутри — один io-поток Asio. Все публичные методы вызываются из потоков Python:
// они передают работу в io-поток (asio::post) и ждут результата. Поэтому состояние
// соединений и файлов трогает только io-поток и мьютексы нужны лишь очереди событий.
// Синхронные ошибки (неверный аргумент, нет файла, не удалось подключиться) —
// исключение P2PError; всё, что случилось позже в сети, — только событиями.
//
// Asio в этот заголовок не попадает (pimpl): модуль pybind11 собирается быстрее.

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "p2pcore/errors.hpp"
#include "p2pcore/event_queue.hpp"

namespace p2p {

struct EngineConfig {
    std::string peer_id = std::string(16, '\0');  // 16 байт, постоянный (uuid4 из конфига)
    std::string name = "p2pshare";                // UTF-8, в HELLO обрезается до 255 байт
    std::uint32_t max_inflight = 6;               // окно запросов REQUEST на соединение
    std::uint32_t keepalive_ms = 5000;            // PING раз в столько мс (0 — выкл.)
    std::uint32_t idle_timeout_ms = 15000;        // разрыв после стольких мс тишины (0 — выкл.)
    // Таймаут ответа на REQUEST (0 — выкл.): если за это время не пришло ни байта ответа ни на
    // запрос, ни на отправленные раньше него — CHUNK_FAILED(TIMEOUT) и разрыв соединения.
    // Нужен против пира, который жив (отвечает на PING), но на запросы не отвечает.
    std::uint32_t request_timeout_ms = 30000;
    std::uint64_t fault_drop_after_bytes = 0;     // 0 = выкл.; оборвать соединение, когда отдано > N байт
    std::int64_t fault_corrupt_chunk = -1;        // -1 = выкл.; испортить чанк N (один раз, до CRC)
    std::int64_t fault_stall_chunk = -1;          // -1 = выкл.; не ответить на REQUEST чанка N (один раз)
};

class Engine {
public:
    explicit Engine(EngineConfig config);
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // Начать принимать подключения. port = 0 — любой свободный. Возвращает фактический порт.
    std::uint16_t start(std::uint16_t port = 0);
    // Закрыть все соединения (BYE) и перестать принимать новые. Идемпотентно.
    void stop();

    // Подключиться и дождаться обмена HELLO. Возвращает conn_id.
    ConnId connect(const std::string& host, std::uint16_t port, int timeout_ms = 5000);
    void disconnect(ConnId conn_id);
    void ping(ConnId conn_id);
    void send_control(ConnId conn_id, const std::string& payload);

    void share_file(const std::string& file_id, const std::filesystem::path& path, std::uint64_t size,
                    std::uint32_t chunk_size);
    void open_download(const std::string& file_id, const std::filesystem::path& part_path, std::uint64_t size,
                       std::uint32_t chunk_size, const std::string& have);
    void set_have(const std::string& file_id, std::uint32_t chunk_index);
    void send_bitfield(ConnId conn_id, const std::string& file_id);
    void request_chunks(ConnId conn_id, const std::string& file_id, const std::vector<std::uint32_t>& indices);
    void cancel_chunks(ConnId conn_id, const std::string& file_id, const std::vector<std::uint32_t>& indices);
    void flush(const std::string& file_id);
    void close_file(const std::string& file_id);

    // Общие лимиты скорости данных чанков, байт/с; 0 — без ограничения. Действуют сразу, в том
    // числе на идущие передачи. Значения меньше 1 КиБ/с поднимаются до 1 КиБ/с.
    void set_rate_limit(std::uint64_t upload_bps, std::uint64_t download_bps);

    std::vector<Event> poll_events(std::size_t max_events = 512);
    std::vector<Event> wait_events(int timeout_ms = 100, std::size_t max_events = 512);
    std::map<std::string, std::int64_t> stats();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace p2p
