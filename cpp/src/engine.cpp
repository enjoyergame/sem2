#include "p2pcore/engine.hpp"

#include <algorithm>
#include <future>
#include <optional>
#include <set>
#include <thread>
#include <type_traits>
#include <utility>

#include "p2pcore/connection.hpp"

namespace p2p {
namespace {

using namespace std::chrono_literals;

FileId to_file_id(const std::string& s) {
    if (s.size() != kFileIdSize) throw P2PError("file_id must be 16 bytes");
    FileId id{};
    std::copy(s.begin(), s.end(), id.begin());
    return id;
}

std::string bytes_of(const FileId& id) { return std::string(id.begin(), id.end()); }

void set_exclusive_bind(asio::ip::tcp::acceptor& acceptor) {
#ifdef _WIN32
    // На Windows SO_REUSEADDR позволяет занять порт, который уже слушает другой процесс,
    // поэтому его не включаем, а просим монопольное владение портом.
    BOOL on = TRUE;
    ::setsockopt(acceptor.native_handle(), SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&on),
                 sizeof(on));
#else
    // На Linux SO_REUSEADDR лишь разрешает перезапуск, пока старые соединения в TIME_WAIT.
    acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
#endif
}

}  // namespace

// Незавершённый connect(): разрешение имени -> TCP -> HELLO. Python-поток ждёт promise.
struct ConnectOp {
    explicit ConnectOp(asio::io_context& io) : resolver(io), socket(io), timer(io) {}
    asio::ip::tcp::resolver resolver;
    asio::ip::tcp::socket socket;
    asio::steady_timer timer;
    std::promise<ConnId> promise;
    std::string target;
    ConnId id = 0;
    std::chrono::steady_clock::time_point deadline;
    bool done = false;
};

struct Engine::Impl final : ConnectionHost {
    explicit Impl(EngineConfig cfg);
    ~Impl() override;

    // Выполнить f в io-потоке и дождаться результата (исключения пробрасываются вызывающему).
    template <class F>
    auto call(F&& f) -> std::invoke_result_t<F&> {
        std::packaged_task<std::invoke_result_t<F&>()> task(std::forward<F>(f));
        auto fut = task.get_future();
        asio::post(io_, [&task] { task(); });
        return fut.get();
    }

    // --- в io-потоке
    std::uint16_t do_start(std::uint16_t port);
    void do_stop();
    std::future<ConnId> begin_connect(const std::string& host, std::uint16_t port, int timeout_ms);
    void fail_connect(const std::shared_ptr<ConnectOp>& op, const std::string& message);
    void accept_next();
    void schedule_progress();
    void emit_progress();
    Connection& conn(ConnId id);
    FileEntry& file(const FileId& id);

    // --- ConnectionHost
    const EngineConfig& config() const override { return cfg_; }
    std::uint16_t listen_port() const override { return listen_port_; }
    FileRegistry& files() override { return files_; }
    void emit(Event ev) override { events_.push(std::move(ev)); }
    bool on_ready(Connection& c) override;
    void on_closed(Connection& c, DisconnectCode code, const std::string& reason, bool notify) override;
    void on_socket_closed(Connection& c) override;
    void count_transfer(const FileId& id, Direction dir, std::uint64_t bytes) override;
    bool take_drop_fault(std::uint64_t chunk_len) override;
    bool take_corrupt_fault(std::uint32_t chunk_index) override;

    // Порядок важен: всё, что держит сокеты и таймеры, объявлено после io_ и
    // уничтожается раньше него.
    EngineConfig cfg_;
    PeerId peer_id_{};
    asio::io_context io_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    EventQueue events_;  // общая для io-потока и Python
    std::thread thread_;

    // Дальше — только io-поток.
    std::optional<asio::ip::tcp::acceptor> acceptor_;
    asio::steady_timer progress_timer_;
    bool running_ = false;
    std::uint16_t listen_port_ = 0;
    ConnId next_id_ = 1;
    std::map<ConnId, std::shared_ptr<Connection>> conns_;
    std::set<std::shared_ptr<Connection>> lingering_;  // закрыты, но BYE ещё уходит
    std::map<ConnId, std::shared_ptr<ConnectOp>> connecting_;
    FileRegistry files_;

    struct Counters {
        std::uint64_t down = 0, up = 0;
        bool dirty_down = false, dirty_up = false;
    };
    std::map<FileId, Counters> counters_;
    std::uint64_t bytes_sent_ = 0;
    std::uint64_t bytes_received_ = 0;
    std::uint64_t drop_after_ = 0;
    std::int64_t corrupt_chunk_ = -1;
    std::uint64_t upload_bps_ = 0;
    std::uint64_t download_bps_ = 0;
};

// ====================================================================== Impl

Engine::Impl::Impl(EngineConfig cfg)
    : cfg_(std::move(cfg)), work_(asio::make_work_guard(io_)), progress_timer_(io_) {
    if (cfg_.peer_id.size() != kPeerIdSize) throw P2PError("peer_id must be 16 bytes");
    std::copy(cfg_.peer_id.begin(), cfg_.peer_id.end(), peer_id_.begin());
    drop_after_ = cfg_.fault_drop_after_bytes;
    corrupt_chunk_ = cfg_.fault_corrupt_chunk;
    thread_ = std::thread([this] { io_.run(); });
}

Engine::Impl::~Impl() {
    try {
        call([this] { do_stop(); });
        // Даём уйти BYE (не дольше ~1,5 с), чтобы собеседник получил DISCONNECT_REMOTE, а не FAULT.
        for (int i = 0; i < 150 && !call([this] { return lingering_.empty(); }); ++i) std::this_thread::sleep_for(10ms);
    } catch (...) {
    }
    work_.reset();
    io_.stop();
    if (thread_.joinable()) thread_.join();
}

std::uint16_t Engine::Impl::do_start(std::uint16_t port) {
    if (running_) throw P2PError("engine already started");
    asio::ip::tcp::acceptor acceptor(io_);
    const asio::ip::tcp::endpoint ep(asio::ip::tcp::v4(), port);
    asio::error_code ec;
    acceptor.open(ep.protocol(), ec);
    if (!ec) set_exclusive_bind(acceptor);
    if (!ec) acceptor.bind(ep, ec);
    if (ec == asio::error::address_in_use || ec == asio::error::access_denied) {
        throw P2PError("port " + std::to_string(port) + " is already in use");
    }
    if (!ec) acceptor.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) throw P2PError("cannot listen on port " + std::to_string(port) + ": " + error_text(ec));

    listen_port_ = acceptor.local_endpoint().port();
    acceptor_.emplace(std::move(acceptor));
    running_ = true;
    events_.open();
    accept_next();
    schedule_progress();
    return listen_port_;
}

void Engine::Impl::do_stop() {
    if (!running_) return;
    running_ = false;
    if (acceptor_) {
        asio::error_code ignored;
        acceptor_->close(ignored);
        acceptor_.reset();
    }
    progress_timer_.cancel();
    emit_progress();
    for (const auto& [id, op] : std::map(connecting_)) fail_connect(op, "engine stopped");
    for (const auto& [id, c] : std::map(conns_)) c->close_local();
    listen_port_ = 0;
    events_.close();  // будит wait_events(): после stop он больше не блокируется
}

void Engine::Impl::accept_next() {
    acceptor_->async_accept([this](const asio::error_code& ec, asio::ip::tcp::socket socket) {
        if (!running_ || !acceptor_) return;
        if (!ec) {
            const ConnId id = next_id_++;
            auto c = std::make_shared<Connection>(std::move(socket), id, true, *this);
            conns_[id] = c;
            c->start(10s);
        }
        accept_next();
    });
}

std::future<ConnId> Engine::Impl::begin_connect(const std::string& host, std::uint16_t port, int timeout_ms) {
    if (!running_) throw P2PError("engine is not running");
    auto op = std::make_shared<ConnectOp>(io_);
    op->target = host + ":" + std::to_string(port);
    op->id = next_id_++;
    op->deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max(timeout_ms, 1));
    connecting_[op->id] = op;
    auto fut = op->promise.get_future();

    op->timer.expires_at(op->deadline);
    op->timer.async_wait([this, op](const asio::error_code& ec) {
        if (!ec) fail_connect(op, "connection timed out: " + op->target);
    });

    // Слушаем только IPv4, поэтому и имя разрешаем в IPv4 ("localhost" не уйдёт на ::1).
    op->resolver.async_resolve(
        asio::ip::tcp::v4(), host, std::to_string(port),
        [this, op](const asio::error_code& ec, const asio::ip::tcp::resolver::results_type& results) {
            if (op->done) return;
            if (ec) return fail_connect(op, "cannot resolve " + op->target + ": " + error_text(ec));
            asio::async_connect(op->socket, results, [this, op](const asio::error_code& e, const auto&) {
                if (op->done) return;
                if (e) return fail_connect(op, "connection refused: " + op->target + " (" + error_text(e) + ")");
                auto c = std::make_shared<Connection>(std::move(op->socket), op->id, false, *this);
                conns_[op->id] = c;
                const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                    op->deadline - std::chrono::steady_clock::now());
                c->start(std::max(left, std::chrono::milliseconds(1)));
            });
        });
    return fut;
}

void Engine::Impl::fail_connect(const std::shared_ptr<ConnectOp>& op, const std::string& message) {
    if (op->done) return;
    op->done = true;
    connecting_.erase(op->id);
    op->timer.cancel();
    op->resolver.cancel();
    asio::error_code ignored;
    op->socket.close(ignored);
    if (const auto it = conns_.find(op->id); it != conns_.end()) it->second->abort(message);
    op->promise.set_exception(std::make_exception_ptr(P2PError(message)));
}

bool Engine::Impl::on_ready(Connection& c) {
    const HelloPayload& r = c.remote();
    // Подключились сами к себе (свой адрес в ручном вводе): тот же peer_id и тот же порт.
    if (r.peer_id == peer_id_ && listen_port_ != 0 && r.listen_port == listen_port_) return false;

    Event ev;
    ev.type = EventType::PeerConnected;
    ev.conn_id = c.id();
    ev.code = c.incoming() ? 1 : 0;
    ev.data.assign(r.peer_id.begin(), r.peer_id.end());
    ev.data += r.name;
    emit(std::move(ev));

    if (const auto it = connecting_.find(c.id()); it != connecting_.end()) {
        const auto op = it->second;
        connecting_.erase(it);
        op->done = true;
        op->timer.cancel();
        op->promise.set_value(c.id());
    }
    return true;
}

void Engine::Impl::on_closed(Connection& c, DisconnectCode code, const std::string& reason, bool notify) {
    if (const auto it = conns_.find(c.id()); it != conns_.end()) {
        lingering_.insert(it->second);
        conns_.erase(it);
    }
    if (notify) {
        Event ev;
        ev.type = EventType::PeerDisconnected;
        ev.conn_id = c.id();
        ev.code = static_cast<int>(code);
        ev.message = reason;
        emit(std::move(ev));
    }
    if (const auto it = connecting_.find(c.id()); it != connecting_.end()) {
        fail_connect(it->second, reason == "cannot connect to self" ? reason : "handshake failed: " + reason);
    }
}

void Engine::Impl::on_socket_closed(Connection& c) {
    std::erase_if(lingering_, [&](const std::shared_ptr<Connection>& p) { return p.get() == &c; });
}

void Engine::Impl::count_transfer(const FileId& id, Direction dir, std::uint64_t bytes) {
    Counters& c = counters_[id];
    if (dir == Direction::Download) {
        bytes_received_ += bytes;
        c.down += bytes;
        c.dirty_down = true;
    } else {
        bytes_sent_ += bytes;
        c.up += bytes;
        c.dirty_up = true;
    }
}

bool Engine::Impl::take_drop_fault(std::uint64_t chunk_len) {
    if (drop_after_ == 0 || bytes_sent_ + chunk_len <= drop_after_) return false;
    drop_after_ = 0;  // срабатывает один раз — чтобы докачка после обрыва прошла
    return true;
}

bool Engine::Impl::take_corrupt_fault(std::uint32_t chunk_index) {
    if (corrupt_chunk_ < 0 || static_cast<std::int64_t>(chunk_index) != corrupt_chunk_) return false;
    corrupt_chunk_ = -1;  // портим один раз, чтобы повторный запрос получил целый чанк
    return true;
}

// PROGRESS не на каждые 64 КиБ, а не чаще раза в 100 мс на файл и направление.
void Engine::Impl::schedule_progress() {
    progress_timer_.expires_after(100ms);
    progress_timer_.async_wait([this](const asio::error_code& ec) {
        if (ec || !running_) return;
        emit_progress();
        schedule_progress();
    });
}

void Engine::Impl::emit_progress() {
    for (auto& [id, c] : counters_) {
        for (const Direction dir : {Direction::Download, Direction::Upload}) {
            bool& dirty = dir == Direction::Download ? c.dirty_down : c.dirty_up;
            if (!dirty) continue;
            dirty = false;
            Event ev;
            ev.type = EventType::Progress;
            ev.file_id = bytes_of(id);
            ev.bytes_done = dir == Direction::Download ? c.down : c.up;
            ev.value = static_cast<double>(static_cast<int>(dir));
            emit(std::move(ev));
        }
    }
}

Connection& Engine::Impl::conn(ConnId id) {
    if (!running_) throw P2PError("engine is not running");
    const auto it = conns_.find(id);
    if (it == conns_.end() || !it->second->ready()) throw P2PError("unknown connection: " + std::to_string(id));
    return *it->second;
}

FileEntry& Engine::Impl::file(const FileId& id) {
    const auto it = files_.find(id);
    if (it == files_.end()) throw P2PError("unknown file_id");
    return *it->second;
}

// ==================================================================== Engine

namespace {

void check_file_args(std::uint64_t size, std::uint32_t chunk_size) {
    if (chunk_size == 0) throw P2PError("bad size / chunk_size");
    if ((size + chunk_size - 1) / chunk_size >= kNoChunk) throw P2PError("too many chunks for this chunk_size");
}

}  // namespace

Engine::Engine(EngineConfig config) : impl_(std::make_unique<Impl>(std::move(config))) {}

Engine::~Engine() = default;

std::uint16_t Engine::start(std::uint16_t port) {
    return impl_->call([&] { return impl_->do_start(port); });
}

void Engine::stop() {
    impl_->call([&] { impl_->do_stop(); });
}

ConnId Engine::connect(const std::string& host, std::uint16_t port, int timeout_ms) {
    std::future<ConnId> fut = impl_->call([&] { return impl_->begin_connect(host, port, timeout_ms); });
    return fut.get();  // ждём уже вне io-потока; таймер в io-потоке гарантирует ответ
}

void Engine::disconnect(ConnId conn_id) {
    impl_->call([&] { impl_->conn(conn_id).close_local(); });
}

void Engine::ping(ConnId conn_id) {
    impl_->call([&] { impl_->conn(conn_id).send_ping(); });
}

void Engine::send_control(ConnId conn_id, const std::string& payload) {
    if (payload.size() > kMaxPayload) throw P2PError("control payload too large");
    impl_->call([&] {
        impl_->conn(conn_id).send_control(
            std::span(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size()));
    });
}

void Engine::share_file(const std::string& file_id, const std::filesystem::path& path, std::uint64_t size,
                        std::uint32_t chunk_size) {
    const FileId id = to_file_id(file_id);
    check_file_args(size, chunk_size);
    std::error_code ec;
    if (!std::filesystem::is_regular_file(path, ec)) throw P2PError("file not found: " + path_to_utf8(path));
    auto entry = std::make_shared<FileEntry>();
    entry->id = id;
    entry->path = path;
    entry->size = size;
    entry->chunk_size = chunk_size;
    entry->have = Bitfield::full(entry->chunk_count());
    entry->file = PositionalFile::open_read(path);  // открываем вне io-потока: диск может быть медленным
    impl_->call([&] { impl_->files_[id] = std::move(entry); });
}

void Engine::open_download(const std::string& file_id, const std::filesystem::path& part_path, std::uint64_t size,
                           std::uint32_t chunk_size, const std::string& have) {
    const FileId id = to_file_id(file_id);
    check_file_args(size, chunk_size);
    auto entry = std::make_shared<FileEntry>();
    entry->id = id;
    entry->path = part_path;
    entry->size = size;
    entry->chunk_size = chunk_size;
    entry->writable = true;
    std::string error;
    auto bf = Bitfield::from_bytes(std::span(reinterpret_cast<const std::uint8_t*>(have.data()), have.size()),
                                   entry->chunk_count(), error);
    if (!bf) throw P2PError("bad bitfield: " + error);
    entry->have = std::move(*bf);
    if (part_path.has_parent_path()) {
        std::error_code ec;
        std::filesystem::create_directories(part_path.parent_path(), ec);
        if (ec) throw P2PError("cannot open " + path_to_utf8(part_path) + ": " + error_text(ec));
    }
    entry->file = PositionalFile::open_write(part_path, size);
    impl_->call([&] { impl_->files_[id] = std::move(entry); });
}

void Engine::set_have(const std::string& file_id, std::uint32_t chunk_index) {
    const FileId id = to_file_id(file_id);
    impl_->call([&] {
        FileEntry& e = impl_->file(id);
        if (chunk_index >= e.chunk_count()) throw P2PError("chunk index out of range: " + std::to_string(chunk_index));
        e.have.set(chunk_index);
    });
}

void Engine::send_bitfield(ConnId conn_id, const std::string& file_id) {
    const FileId id = to_file_id(file_id);
    impl_->call([&] {
        Connection& c = impl_->conn(conn_id);
        c.send_bitfield(impl_->file(id));
    });
}

void Engine::request_chunks(ConnId conn_id, const std::string& file_id, const std::vector<std::uint32_t>& indices) {
    const FileId id = to_file_id(file_id);
    impl_->call([&] {
        Connection& c = impl_->conn(conn_id);
        const auto it = impl_->files_.find(id);
        if (it == impl_->files_.end()) throw P2PError("unknown file_id");
        if (!it->second->writable) throw P2PError("file is not open for download");
        c.request_chunks(it->second, indices);
    });
}

void Engine::cancel_chunks(ConnId conn_id, const std::string& file_id, const std::vector<std::uint32_t>& indices) {
    const FileId id = to_file_id(file_id);
    impl_->call([&] { impl_->conn(conn_id).cancel_chunks(id, indices); });
}

void Engine::flush(const std::string& file_id) {
    const FileId id = to_file_id(file_id);
    impl_->call([&] {
        FileEntry& e = impl_->file(id);
        if (e.writable && e.file.is_open()) e.file.flush();
    });
}

void Engine::close_file(const std::string& file_id) {
    const FileId id = to_file_id(file_id);
    impl_->call([&] {
        const auto it = impl_->files_.find(id);
        if (it == impl_->files_.end()) return;
        // .part закрываем сразу: на Windows Python переименует его сразу после close_file.
        // Свою раздачу закроет последний владелец (соединение, которое как раз её отдаёт).
        if (it->second->writable) it->second->file.close();
        impl_->files_.erase(it);
    });
}

void Engine::set_rate_limit(std::uint64_t upload_bps, std::uint64_t download_bps) {
    impl_->call([&] {
        impl_->upload_bps_ = upload_bps;
        impl_->download_bps_ = download_bps;
    });
}

std::vector<Event> Engine::poll_events(std::size_t max_events) { return impl_->events_.poll(max_events); }

std::vector<Event> Engine::wait_events(int timeout_ms, std::size_t max_events) {
    return impl_->events_.wait(std::chrono::milliseconds(std::max(timeout_ms, 0)), max_events);
}

std::map<std::string, std::int64_t> Engine::stats() {
    return impl_->call([&] {
        std::int64_t ready = 0;
        for (const auto& [id, c] : impl_->conns_) ready += c->ready() ? 1 : 0;
        return std::map<std::string, std::int64_t>{
            {"connections", ready},
            {"files", static_cast<std::int64_t>(impl_->files_.size())},
            {"bytes_sent", static_cast<std::int64_t>(impl_->bytes_sent_)},
            {"bytes_received", static_cast<std::int64_t>(impl_->bytes_received_)},
        };
    });
}

}  // namespace p2p
