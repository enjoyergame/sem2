#pragma once
// Connection — одно TCP-соединение с пиром: рукопожатие HELLO, чтение и запись кадров,
// отдача чанков по REQUEST и приём CHUNK прямо в файл, окно запросов, keepalive.
//
// Живёт только в io-потоке. Всё, что нужно от движка (файлы, события, конфиг, счётчики),
// получает через интерфейс ConnectionHost — так Connection не зависит от Engine.
//
// Память на соединение: заголовок + два буфера по 64 КиБ (приём и отдача) + небольшие
// кадры в очереди. Чанк целиком в памяти не держится: и отдаётся, и принимается слайсами.

#include <asio.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include "p2pcore/crc32c.hpp"
#include "p2pcore/event_queue.hpp"
#include "p2pcore/file_store.hpp"
#include "p2pcore/protocol.hpp"

namespace p2p {

struct EngineConfig;
class Connection;

inline constexpr std::size_t kSliceSize = 64 * 1024;  // через этот буфер идут данные чанка
inline constexpr std::size_t kMaxQueuedChunks = 64;   // больше чанков в очереди отдачи — REJECT(BUSY)

// Причина REJECT в ответ на CANCEL ещё не начатой отдачи. Нужна, чтобы на каждый REQUEST
// приходил ровно один ответ (CHUNK или REJECT) и запрашивающий знал, когда слот свободен.
inline constexpr std::uint16_t kRejectCancelled = 4;

class ConnectionHost {
public:
    virtual ~ConnectionHost() = default;

    [[nodiscard]] virtual const EngineConfig& config() const = 0;
    [[nodiscard]] virtual std::uint16_t listen_port() const = 0;
    virtual FileRegistry& files() = 0;
    virtual void emit(Event ev) = 0;

    // HELLO получен. false — закрыть соединение без событий (подключение к самому себе).
    virtual bool on_ready(Connection& c) = 0;
    // Соединение закрыто (вызывается ровно один раз). notify — нужно ли событие PEER_DISCONNECTED.
    virtual void on_closed(Connection& c, DisconnectCode code, const std::string& reason, bool notify) = 0;
    // Сокет действительно закрыт (после закрытия с BYE это происходит позже on_closed).
    virtual void on_socket_closed(Connection& c) = 0;

    virtual void count_transfer(const FileId& id, Direction dir, std::uint64_t bytes) = 0;
    // Внедрение сбоев (на стороне отдающего, срабатывают один раз).
    virtual bool take_drop_fault(std::uint64_t chunk_len) = 0;
    virtual bool take_corrupt_fault(std::uint32_t chunk_index) = 0;
    virtual bool take_stall_fault(std::uint32_t chunk_index) = 0;
};

class Connection : public std::enable_shared_from_this<Connection> {
public:
    Connection(asio::ip::tcp::socket socket, ConnId id, bool incoming, ConnectionHost& host);

    // Отправить HELLO, начать чтение, ждать HELLO собеседника не дольше handshake_timeout.
    void start(std::chrono::milliseconds handshake_timeout);

    [[nodiscard]] ConnId id() const noexcept { return id_; }
    [[nodiscard]] bool incoming() const noexcept { return incoming_; }
    [[nodiscard]] bool ready() const noexcept { return ready_ && !closed_; }
    [[nodiscard]] const HelloPayload& remote() const noexcept { return remote_; }

    void send_control(std::span<const std::uint8_t> payload);
    void send_ping();
    void send_bitfield(const FileEntry& entry);
    void request_chunks(const std::shared_ptr<FileEntry>& entry, std::span<const std::uint32_t> indices);
    void cancel_chunks(const FileId& id, std::span<const std::uint32_t> indices);

    // Штатное закрытие: BYE(NORMAL) собеседнику, здесь PEER_DISCONNECTED(LOCAL).
    void close_local();
    // Немедленное закрытие без события (например, по таймауту connect()).
    void abort(const std::string& reason);

private:
    struct ChunkJob {
        std::shared_ptr<FileEntry> entry;
        std::uint32_t index = 0;
    };
    using Outgoing = std::variant<std::vector<std::uint8_t>, ChunkJob>;

    struct Request {
        FileId id{};
        std::uint32_t index = 0;
        std::shared_ptr<FileEntry> entry;
        bool cancelled = false;  // отменён, ждём ответ (CHUNK или REJECT), чтобы освободить слот
        // С какого момента ждём ответ: время отправки REQUEST, затем — время последнего байта
        // ответа на этот запрос или на любой, отправленный раньше него (см. note_response).
        std::chrono::steady_clock::time_point waiting_since{};
    };

    // Abort — закрыть сокет сразу; SendBye — отправить BYE, закрыть свою половину (FIN) и
    // дочитать вход; HalfClose — то же без BYE (имитация обрыва: уже отправленное дойдёт).
    enum class CloseMode { Abort, SendBye, HalfClose };

    // --- чтение
    void read_header();
    void on_header(const asio::error_code& ec);
    void on_payload(const asio::error_code& ec);
    void dispatch(const FrameHeader& h, std::span<const std::uint8_t> payload);
    void on_hello(std::span<const std::uint8_t> payload);
    void on_request(const FrameHeader& h);
    void on_reject(const FrameHeader& h, std::span<const std::uint8_t> payload);
    void on_cancel(const FrameHeader& h);
    void begin_chunk(const FrameHeader& h);
    void read_chunk_slice();
    void on_chunk_slice(const asio::error_code& ec, std::size_t n);
    void end_chunk();
    void drain();

    // --- запись
    void queue_frame(FrameHeader h, std::span<const std::uint8_t> payload);
    void queue_reject(const FrameHeader& request, std::uint16_t reason);
    void write_next();
    void start_chunk_job(ChunkJob job);
    void write_chunk_slice();

    // --- запросы (мы качаем)
    void pump_requests();
    std::vector<Request>::iterator find_inflight(const FileId& id, std::uint32_t index);
    // Пришла часть ответа на запрос: он и все отправленные после него ждут не зря.
    void note_response(const FileId& id, std::uint32_t index);
    // Есть запрос, ждущий дольше request_timeout_ms: провалить его и разорвать соединение.
    bool check_request_timeout(std::chrono::steady_clock::time_point now);

    // --- жизненный цикл
    void schedule_tick();
    void on_tick();
    void protocol_error(const std::string& what);
    void on_io_error(const asio::error_code& ec);
    void finish(DisconnectCode code, const std::string& reason, CloseMode mode, bool notify,
                ByeReason bye = ByeReason::Normal);
    void close_socket() noexcept;
    void emit_chunk_event(EventType type, const FileId& id, std::uint32_t index, int code = 0);

    asio::ip::tcp::socket socket_;
    asio::steady_timer handshake_timer_;
    asio::steady_timer tick_timer_;
    asio::steady_timer close_timer_;
    ConnectionHost& host_;
    const ConnId id_;
    const bool incoming_;

    bool ready_ = false;                 // HELLO получен
    bool closed_ = false;                // finish() уже был: событий больше нет
    bool closing_gracefully_ = false;    // ждём, пока уйдёт BYE
    bool socket_closed_ = false;
    HelloPayload remote_;
    std::uint32_t next_seq_ = 1;

    // приём
    std::array<std::uint8_t, kHeaderSize> rx_header_buf_{};
    FrameHeader rx_header_;
    std::vector<std::uint8_t> rx_payload_;
    std::vector<std::uint8_t> rx_slice_;
    struct RxChunk {
        std::shared_ptr<FileEntry> entry;
        std::uint64_t offset = 0;
        std::uint32_t remaining = 0;
        Crc32c crc;
        bool disk_error = false;
    } rx_chunk_;

    // отдача
    std::deque<Outgoing> out_;
    std::size_t queued_chunks_ = 0;
    bool writing_ = false;
    std::vector<std::uint8_t> tx_frame_;
    std::vector<std::uint8_t> tx_slice_;
    struct TxChunk {
        ChunkJob job;
        std::array<std::uint8_t, kHeaderSize> header{};
        std::uint64_t offset = 0;
        std::uint32_t remaining = 0;
        bool corrupt = false;
        bool first_slice = true;
    } tx_chunk_;

    // запросы
    std::deque<Request> pending_;   // ещё не отправлены (окно занято)
    std::vector<Request> inflight_;  // отправлены, в порядке отправки

    std::chrono::steady_clock::time_point last_recv_{};
    std::chrono::steady_clock::time_point last_ping_{};
};

}  // namespace p2p
