#pragma once
// События ядра и потокобезопасная очередь для них.
//
// io-поток C++ никогда не вызывает Python напрямую (см. ARCHITECTURE.md, 1.4): он кладёт
// события в очередь, а Python забирает их через poll_events()/wait_events(). Ожидание
// на condition_variable идёт в C++ с отпущенным GIL.
//
// Числовые значения совпадают с src/p2pshare/engine/events.py и codes.py — это контракт
// с Python-частью, менять только вместе.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

namespace p2p {

using ConnId = std::uint64_t;

enum class EventType : int {
    PeerConnected = 1,     // conn_id, data = peer_id(16) + name, code = 1 если входящее
    PeerDisconnected = 2,  // conn_id, code = DisconnectCode, message
    Pong = 3,              // conn_id, value = RTT в мс
    Control = 4,           // conn_id, data = JSON-байты
    Bitfield = 5,          // conn_id, file_id, data = bitfield
    Have = 6,              // conn_id, file_id, chunk_index
    ChunkReceived = 7,     // conn_id, file_id, chunk_index (CRC ok, данные на диске)
    ChunkFailed = 8,       // conn_id, file_id, chunk_index, code = FailCode
    Progress = 9,          // file_id, bytes_done (накопительно), value = Direction
    Log = 10,              // code = уровень (как в logging: 10/20/30/40), message
    Error = 11,            // code, message
};

// PEER_DISCONNECTED.code (codes.py: DISCONNECT_*)
enum class DisconnectCode : int {
    Local = 0,   // закрыли мы (disconnect/stop)
    Remote = 1,  // собеседник прислал BYE
    Fault = 2,   // обрыв, таймаут keepalive, нарушение протокола
};

// CHUNK_FAILED.code (codes.py: FAIL_*). 1..3 совпадают с RejectReason из протокола.
enum class FailCode : int {
    UnknownFile = 1,
    NotHave = 2,
    Busy = 3,
    Crc = 4,
    Disconnect = 5,
    Timeout = 6,
    Disk = 7,  // не удалось записать в .part (нет в codes.py — Python считает это общим провалом)
};

// PROGRESS.value (codes.py: DIR_*)
enum class Direction : int { Download = 0, Upload = 1 };

// Уровни LOG — как в модуле logging Python: logger.log(ev.code, ev.message).
enum class LogLevel : int { Debug = 10, Info = 20, Warning = 30, Error = 40 };

struct Event {
    EventType type = EventType::Log;
    ConnId conn_id = 0;
    std::string file_id;  // 16 байт или пусто
    std::uint32_t chunk_index = 0;
    std::uint64_t bytes_done = 0;
    double value = 0.0;
    int code = 0;
    std::string data;     // произвольные байты
    std::string message;  // UTF-8
};

class EventQueue {
public:
    void push(Event ev);

    // Забрать до max событий, не блокируясь.
    [[nodiscard]] std::vector<Event> poll(std::size_t max);

    // Ждать первое событие не дольше timeout и забрать до max событий.
    // Пока очередь закрыта (движок не запущен или остановлен), не блокируется вовсе:
    // так wait_events() сразу возвращается после stop() и Ctrl+C не зависает.
    [[nodiscard]] std::vector<Event> wait(std::chrono::milliseconds timeout, std::size_t max);

    void open();
    void close();  // будит всех ожидающих

    [[nodiscard]] std::size_t size() const;

private:
    std::vector<Event> take_locked(std::size_t max);

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<Event> queue_;
    bool open_ = false;
};

}  // namespace p2p
