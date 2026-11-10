#include "p2pcore/connection.hpp"

#include <algorithm>
#include <utility>

#include "p2pcore/engine.hpp"
#include "p2pcore/errors.hpp"

namespace p2p {
namespace {

using Clock = std::chrono::steady_clock;

std::uint64_t now_ns() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

std::string file_id_bytes(const FileId& id) { return std::string(id.begin(), id.end()); }

// Код CHUNK_FAILED по причине REJECT: 1..3 совпадают, неизвестные считаем NOT_HAVE.
int fail_code_for_reject(std::uint16_t reason) {
    switch (reason) {
        case std::to_underlying(RejectReason::UnknownFile): return static_cast<int>(FailCode::UnknownFile);
        case std::to_underlying(RejectReason::Busy): return static_cast<int>(FailCode::Busy);
        default: return static_cast<int>(FailCode::NotHave);
    }
}

}  // namespace

Connection::Connection(asio::ip::tcp::socket socket, ConnId id, bool incoming, ConnectionHost& host)
    : socket_(std::move(socket)),
      handshake_timer_(socket_.get_executor()),
      tick_timer_(socket_.get_executor()),
      close_timer_(socket_.get_executor()),
      host_(host),
      id_(id),
      incoming_(incoming),
      rx_slice_(kSliceSize),
      tx_slice_(kSliceSize) {}

// ===================================================================== старт

void Connection::start(std::chrono::milliseconds handshake_timeout) {
    asio::error_code ignored;
    socket_.set_option(asio::ip::tcp::no_delay(true), ignored);  // мелкие кадры (PING, REQUEST) — без задержки

    last_recv_ = Clock::now();
    const EngineConfig& cfg = host_.config();
    HelloPayload hello;
    std::copy_n(cfg.peer_id.begin(), std::min(cfg.peer_id.size(), hello.peer_id.size()), hello.peer_id.begin());
    hello.listen_port = host_.listen_port();
    hello.name = cfg.name;
    FrameHeader h;
    h.type = MsgType::Hello;
    queue_frame(h, encode_hello(hello));

    read_header();

    handshake_timer_.expires_after(handshake_timeout);
    handshake_timer_.async_wait([self = shared_from_this()](const asio::error_code& ec) {
        if (!ec && !self->ready_ && !self->closed_) {
            self->finish(DisconnectCode::Fault, "handshake timeout", CloseMode::Abort, false);
        }
    });
}

// ================================================================= публичное

void Connection::send_control(std::span<const std::uint8_t> payload) {
    FrameHeader h;
    h.type = MsgType::Control;
    queue_frame(h, payload);
}

void Connection::send_ping() {
    last_ping_ = Clock::now();
    FrameHeader h;
    h.type = MsgType::Ping;
    queue_frame(h, encode_ping(now_ns()));
}

void Connection::send_bitfield(const FileEntry& entry) {
    FrameHeader h;
    h.type = MsgType::Bitfield;
    h.file_id = entry.id;
    queue_frame(h, entry.have.bytes());
}

void Connection::request_chunks(const std::shared_ptr<FileEntry>& entry, std::span<const std::uint32_t> indices) {
    for (const std::uint32_t index : indices) {
        const bool pending = std::ranges::any_of(
            pending_, [&](const Request& r) { return r.id == entry->id && r.index == index; });
        const auto it = find_inflight(entry->id, index);
        if (pending || (it != inflight_.end() && !it->cancelled)) continue;  // уже запрошен
        pending_.push_back(Request{entry->id, index, entry, false});
    }
    pump_requests();
}

void Connection::cancel_chunks(const FileId& id, std::span<const std::uint32_t> indices) {
    for (const std::uint32_t index : indices) {
        // ещё не отправленные просто забываем
        std::erase_if(pending_, [&](const Request& r) { return r.id == id && r.index == index; });
        // отправленные помечаем: ответ (CHUNK или REJECT) всё равно придёт и освободит слот
        const auto it = find_inflight(id, index);
        if (it != inflight_.end() && !it->cancelled) {
            it->cancelled = true;
            FrameHeader h;
            h.type = MsgType::Cancel;
            h.file_id = id;
            h.chunk_index = index;
            queue_frame(h, {});
        }
    }
}

void Connection::close_local() { finish(DisconnectCode::Local, "closed locally", CloseMode::SendBye, true); }

void Connection::abort(const std::string& reason) { finish(DisconnectCode::Fault, reason, CloseMode::Abort, false); }

// ==================================================================== чтение

void Connection::read_header() {
    asio::async_read(socket_, asio::buffer(rx_header_buf_),
                     [self = shared_from_this()](const asio::error_code& ec, std::size_t) { self->on_header(ec); });
}

void Connection::on_header(const asio::error_code& ec) {
    if (closed_) {
        if (closing_gracefully_) ec ? close_socket() : drain();
        return;
    }
    if (ec) return on_io_error(ec);
    last_recv_ = Clock::now();

    FrameHeader h;
    if (const DecodeError err = decode_header(rx_header_buf_, h); err != DecodeError::None) {
        return protocol_error(std::string(to_string(err)));
    }
    if (!ready_ && h.type != MsgType::Hello) return protocol_error("expected HELLO first");
    if (ready_ && h.type == MsgType::Hello) return protocol_error("unexpected second HELLO");

    rx_header_ = h;
    if (h.type == MsgType::Chunk) return begin_chunk(h);
    if (h.payload_len == 0) {
        if (h.payload_crc != 0) return protocol_error("frame CRC mismatch");
        dispatch(h, {});
        if (!closed_) read_header();
        return;
    }
    rx_payload_.resize(h.payload_len);
    asio::async_read(socket_, asio::buffer(rx_payload_),
                     [self = shared_from_this()](const asio::error_code& e, std::size_t) { self->on_payload(e); });
}

void Connection::on_payload(const asio::error_code& ec) {
    if (closed_) {
        if (closing_gracefully_) ec ? close_socket() : drain();
        return;
    }
    if (ec) return on_io_error(ec);
    last_recv_ = Clock::now();
    if (crc32c(rx_payload_) != rx_header_.payload_crc) return protocol_error("frame CRC mismatch");
    dispatch(rx_header_, rx_payload_);
    if (!closed_) read_header();
}

void Connection::dispatch(const FrameHeader& h, std::span<const std::uint8_t> payload) {
    switch (h.type) {
        case MsgType::Hello:
            return on_hello(payload);
        case MsgType::Ping: {
            std::uint64_t ts = 0;
            if (!decode_ping(payload, ts)) return protocol_error("bad PING");
            FrameHeader pong;
            pong.type = MsgType::Pong;
            return queue_frame(pong, payload);  // эхо того же времени
        }
        case MsgType::Pong: {
            std::uint64_t ts = 0;
            if (!decode_ping(payload, ts)) return protocol_error("bad PONG");
            const std::uint64_t now = now_ns();
            Event ev;
            ev.type = EventType::Pong;
            ev.conn_id = id_;
            ev.value = now >= ts ? static_cast<double>(now - ts) / 1e6 : 0.0;  // RTT в мс
            return host_.emit(std::move(ev));
        }
        case MsgType::Bye:
            return finish(DisconnectCode::Remote, "closed by peer", CloseMode::Abort, true);
        case MsgType::Control: {
            Event ev;
            ev.type = EventType::Control;
            ev.conn_id = id_;
            ev.data.assign(payload.begin(), payload.end());
            return host_.emit(std::move(ev));
        }
        case MsgType::Bitfield: {
            Event ev;
            ev.type = EventType::Bitfield;
            ev.conn_id = id_;
            ev.file_id = file_id_bytes(h.file_id);
            ev.data.assign(payload.begin(), payload.end());  // проверяет Python: он знает число чанков
            return host_.emit(std::move(ev));
        }
        case MsgType::Have:
            return emit_chunk_event(EventType::Have, h.file_id, h.chunk_index);
        case MsgType::Request:
            return on_request(h);
        case MsgType::Reject:
            return on_reject(h, payload);
        case MsgType::Cancel:
            return on_cancel(h);
        case MsgType::Chunk:
            return;  // обрабатывается потоково в begin_chunk
    }
}

void Connection::on_hello(std::span<const std::uint8_t> payload) {
    HelloPayload hello;
    if (!decode_hello(payload, hello)) return protocol_error("bad HELLO");
    remote_ = std::move(hello);
    ready_ = true;
    handshake_timer_.cancel();
    if (!host_.on_ready(*this)) {
        finish(DisconnectCode::Fault, "cannot connect to self", CloseMode::SendBye, false, ByeReason::Incompatible);
        return;
    }
    last_ping_ = Clock::now();
    schedule_tick();
    pump_requests();
}

// ------------------------------------------------- отдача: входящие REQUEST

void Connection::on_request(const FrameHeader& h) {
    const auto& files = host_.files();
    const auto it = files.find(h.file_id);
    if (it == files.end()) return queue_reject(h, std::to_underlying(RejectReason::UnknownFile));
    const auto& entry = it->second;
    if (!entry->file.is_open() || !entry->have.get(h.chunk_index) || entry->chunk_length(h.chunk_index) == 0) {
        return queue_reject(h, std::to_underlying(RejectReason::NotHave));
    }
    if (queued_chunks_ >= kMaxQueuedChunks) return queue_reject(h, std::to_underlying(RejectReason::Busy));
    out_.emplace_back(ChunkJob{entry, h.chunk_index});
    ++queued_chunks_;
    write_next();
}

void Connection::on_cancel(const FrameHeader& h) {
    const auto it = std::ranges::find_if(out_, [&](const Outgoing& o) {
        const auto* job = std::get_if<ChunkJob>(&o);
        return job != nullptr && job->entry->id == h.file_id && job->index == h.chunk_index;
    });
    if (it == out_.end()) return;  // уже отдаётся или отдан — CHUNK сам придёт к собеседнику
    out_.erase(it);
    --queued_chunks_;
    queue_reject(h, kRejectCancelled);
}

// ------------------------------------------------- приём: ответы на REQUEST

void Connection::on_reject(const FrameHeader& h, std::span<const std::uint8_t> payload) {
    std::uint16_t reason = 0;
    if (!decode_reason(payload, reason)) return protocol_error("bad REJECT");
    const auto it = find_inflight(h.file_id, h.chunk_index);
    if (it == inflight_.end()) return;  // ответ на то, чего мы уже не ждём
    const bool cancelled = it->cancelled;
    inflight_.erase(it);
    if (!cancelled) emit_chunk_event(EventType::ChunkFailed, h.file_id, h.chunk_index, fail_code_for_reject(reason));
    pump_requests();
}

void Connection::begin_chunk(const FrameHeader& h) {
    const auto it = find_inflight(h.file_id, h.chunk_index);
    if (it == inflight_.end()) return protocol_error("unsolicited CHUNK");
    if (h.payload_len != it->entry->chunk_length(h.chunk_index)) return protocol_error("CHUNK length mismatch");
    rx_chunk_ = RxChunk{};
    rx_chunk_.entry = it->entry;
    rx_chunk_.offset = it->entry->chunk_offset(h.chunk_index);
    rx_chunk_.remaining = h.payload_len;
    read_chunk_slice();
}

void Connection::read_chunk_slice() {
    if (rx_chunk_.remaining == 0) return end_chunk();
    const std::size_t n = std::min<std::size_t>(rx_chunk_.remaining, kSliceSize);
    asio::async_read(socket_, asio::buffer(rx_slice_.data(), n),
                     [self = shared_from_this(), n](const asio::error_code& ec, std::size_t) {
                         self->on_chunk_slice(ec, n);
                     });
}

void Connection::on_chunk_slice(const asio::error_code& ec, std::size_t n) {
    if (closed_) {
        if (closing_gracefully_) ec ? close_socket() : drain();
        return;
    }
    if (ec) return on_io_error(ec);
    last_recv_ = Clock::now();

    const std::span<const std::uint8_t> slice(rx_slice_.data(), n);
    rx_chunk_.crc.update(slice);
    FileEntry& entry = *rx_chunk_.entry;
    const auto it = find_inflight(rx_header_.file_id, rx_header_.chunk_index);
    const bool cancelled = it == inflight_.end() || it->cancelled;
    // Пишем прямо в .part по смещению; файл могли закрыть (close_file) — тогда просто дочитываем кадр.
    if (!cancelled && entry.writable && entry.file.is_open() && !rx_chunk_.disk_error) {
        try {
            entry.file.write_at(rx_chunk_.offset, slice);
        } catch (const P2PError& e) {
            rx_chunk_.disk_error = true;
            Event ev;
            ev.type = EventType::Log;
            ev.code = static_cast<int>(LogLevel::Error);
            ev.message = e.what();
            host_.emit(std::move(ev));
        }
    }
    host_.count_transfer(rx_header_.file_id, Direction::Download, n);
    rx_chunk_.offset += n;
    rx_chunk_.remaining -= static_cast<std::uint32_t>(n);
    read_chunk_slice();
}

void Connection::end_chunk() {
    const FrameHeader h = rx_header_;
    const auto it = find_inflight(h.file_id, h.chunk_index);
    const bool cancelled = it == inflight_.end() || it->cancelled;
    if (it != inflight_.end()) inflight_.erase(it);
    const bool file_open = rx_chunk_.entry->file.is_open();
    rx_chunk_.entry.reset();

    if (!cancelled && file_open) {
        if (rx_chunk_.crc.value() != h.payload_crc) {
            emit_chunk_event(EventType::ChunkFailed, h.file_id, h.chunk_index, static_cast<int>(FailCode::Crc));
        } else if (rx_chunk_.disk_error) {
            emit_chunk_event(EventType::ChunkFailed, h.file_id, h.chunk_index, static_cast<int>(FailCode::Disk));
        } else {
            emit_chunk_event(EventType::ChunkReceived, h.file_id, h.chunk_index);
        }
    }
    pump_requests();
    if (!closed_) read_header();
}

// Во время штатного закрытия дочитываем и выбрасываем всё, пока собеседник не закроет
// соединение: если закрыть сокет с непрочитанными данными, ОС пошлёт RST и собеседник
// может потерять наш BYE.
void Connection::drain() {
    socket_.async_read_some(asio::buffer(rx_slice_), [self = shared_from_this()](const asio::error_code& ec, std::size_t) {
        if (ec) return self->close_socket();
        self->drain();
    });
}

// ==================================================================== запись

void Connection::queue_frame(FrameHeader h, std::span<const std::uint8_t> payload) {
    h.payload_len = static_cast<std::uint32_t>(payload.size());
    h.payload_crc = crc32c(payload);
    h.seq = next_seq_++;
    std::vector<std::uint8_t> frame(kHeaderSize + payload.size());
    encode_header(h, std::span<std::uint8_t, kHeaderSize>(frame.data(), kHeaderSize));
    std::ranges::copy(payload, frame.begin() + static_cast<std::ptrdiff_t>(kHeaderSize));
    // Мелкие кадры ставим перед чанками, ждущими отдачи: PONG и REQUEST не стоят в очереди за мегабайтами.
    const auto first_job =
        std::ranges::find_if(out_, [](const Outgoing& o) { return std::holds_alternative<ChunkJob>(o); });
    out_.emplace(first_job, std::move(frame));
    write_next();
}

void Connection::queue_reject(const FrameHeader& request, std::uint16_t reason) {
    FrameHeader h;
    h.type = MsgType::Reject;
    h.file_id = request.file_id;
    h.chunk_index = request.chunk_index;
    queue_frame(h, encode_reason(reason));
}

void Connection::write_next() {
    if (writing_ || socket_closed_ || (closed_ && !closing_gracefully_)) return;
    if (out_.empty()) {
        if (closing_gracefully_) {
            // BYE ушёл: закрываем свою половину и ждём, пока собеседник закроет свою
            asio::error_code ignored;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_send, ignored);
        }
        return;
    }
    Outgoing item = std::move(out_.front());
    out_.pop_front();
    if (auto* frame = std::get_if<std::vector<std::uint8_t>>(&item)) {
        tx_frame_ = std::move(*frame);
        writing_ = true;
        asio::async_write(socket_, asio::buffer(tx_frame_),
                          [self = shared_from_this()](const asio::error_code& ec, std::size_t) {
                              self->writing_ = false;
                              if (ec) return self->on_io_error(ec);
                              self->write_next();
                          });
        return;
    }
    --queued_chunks_;
    start_chunk_job(std::get<ChunkJob>(std::move(item)));
}

// Отдача чанка в два прохода по 64 КиБ: первый считает CRC32C (он нужен в заголовке до
// данных), второй читает те же байты снова и отправляет. Повторное чтение попадает в кэш ОС,
// зато в памяти никогда нет чанка целиком.
void Connection::start_chunk_job(ChunkJob job) {
    FileEntry& entry = *job.entry;
    const std::uint32_t index = job.index;
    const std::uint32_t len = entry.chunk_length(index);
    FrameHeader request;
    request.file_id = entry.id;
    request.chunk_index = index;
    if (len == 0 || !entry.file.is_open()) return queue_reject(request, std::to_underlying(RejectReason::NotHave));

    if (host_.take_drop_fault(len)) {
        finish(DisconnectCode::Fault, "fault injection: connection dropped", CloseMode::Abort, true);
        return;
    }
    const bool corrupt = host_.take_corrupt_fault(index);

    Crc32c crc;
    try {
        std::uint64_t off = entry.chunk_offset(index);
        std::uint32_t left = len;
        bool first = true;
        while (left > 0) {
            const std::size_t n = std::min<std::size_t>(left, kSliceSize);
            const std::span<std::uint8_t> slice(tx_slice_.data(), n);
            if (entry.file.read_at(off, slice) != n) throw P2PError("file is shorter than announced");
            if (first && corrupt) slice[0] ^= 0xFF;  // порча до CRC: транспорт её не заметит, SHA-256 заметит
            first = false;
            crc.update(slice);
            off += n;
            left -= static_cast<std::uint32_t>(n);
        }
    } catch (const P2PError& e) {
        Event ev;
        ev.type = EventType::Log;
        ev.code = static_cast<int>(LogLevel::Warning);
        ev.message = std::string("cannot serve chunk: ") + e.what();
        host_.emit(std::move(ev));
        return queue_reject(request, std::to_underlying(RejectReason::NotHave));
    }

    FrameHeader h;
    h.type = MsgType::Chunk;
    h.file_id = entry.id;
    h.chunk_index = index;
    h.payload_len = len;
    h.payload_crc = crc.value();
    h.seq = next_seq_++;

    tx_chunk_ = TxChunk{};
    tx_chunk_.job = std::move(job);
    tx_chunk_.offset = entry.chunk_offset(index);
    tx_chunk_.remaining = len;
    tx_chunk_.corrupt = corrupt;
    encode_header(h, tx_chunk_.header);

    writing_ = true;
    asio::async_write(socket_, asio::buffer(tx_chunk_.header),
                      [self = shared_from_this()](const asio::error_code& ec, std::size_t) {
                          if (ec) {
                              self->writing_ = false;
                              return self->on_io_error(ec);
                          }
                          self->write_chunk_slice();
                      });
}

void Connection::write_chunk_slice() {
    TxChunk& t = tx_chunk_;
    if (t.remaining == 0) {
        t.job.entry.reset();
        writing_ = false;
        return write_next();
    }
    const std::size_t n = std::min<std::size_t>(t.remaining, kSliceSize);
    const std::span<std::uint8_t> slice(tx_slice_.data(), n);
    std::size_t got = 0;
    try {
        got = t.job.entry->file.read_at(t.offset, slice);
    } catch (const P2PError& e) {
        // Заголовок уже ушёл, оборвать кадр посередине нельзя — только закрыть соединение.
        writing_ = false;
        return finish(DisconnectCode::Fault, std::string("read error while sending: ") + e.what(), CloseMode::Abort,
                      true);
    }
    if (got != n) {
        writing_ = false;
        return finish(DisconnectCode::Fault, "file shrank while sending", CloseMode::Abort, true);
    }
    if (t.first_slice && t.corrupt) slice[0] ^= 0xFF;
    t.first_slice = false;

    asio::async_write(socket_, asio::buffer(tx_slice_.data(), n),
                      [self = shared_from_this(), n](const asio::error_code& ec, std::size_t) {
                          if (ec) {
                              self->writing_ = false;
                              return self->on_io_error(ec);
                          }
                          TxChunk& tc = self->tx_chunk_;
                          self->host_.count_transfer(tc.job.entry->id, Direction::Upload, n);
                          tc.offset += n;
                          tc.remaining -= static_cast<std::uint32_t>(n);
                          self->write_chunk_slice();
                      });
}

// ================================================================== запросы

void Connection::pump_requests() {
    if (!ready_ || closed_) return;
    const std::size_t window = std::max<std::uint32_t>(1, host_.config().max_inflight);
    for (auto it = pending_.begin(); it != pending_.end() && inflight_.size() < window;) {
        // тот же чанк ещё «в полёте» (отменён, ждём ответа) — подождём, иначе ответы перепутаются
        if (find_inflight(it->id, it->index) != inflight_.end()) {
            ++it;
            continue;
        }
        FrameHeader h;
        h.type = MsgType::Request;
        h.file_id = it->id;
        h.chunk_index = it->index;
        queue_frame(h, {});
        inflight_.push_back(std::move(*it));
        it = pending_.erase(it);
    }
}

std::vector<Connection::Request>::iterator Connection::find_inflight(const FileId& id, std::uint32_t index) {
    return std::ranges::find_if(inflight_, [&](const Request& r) { return r.id == id && r.index == index; });
}

// ======================================================== жизненный цикл

void Connection::schedule_tick() {
    const EngineConfig& cfg = host_.config();
    std::uint32_t base = 1000;
    if (cfg.keepalive_ms > 0) base = std::min(base, cfg.keepalive_ms);
    if (cfg.idle_timeout_ms > 0) base = std::min(base, cfg.idle_timeout_ms);
    const auto period = std::chrono::milliseconds(std::clamp<std::uint32_t>(base / 4, 10, 1000));
    tick_timer_.expires_after(period);
    tick_timer_.async_wait([self = shared_from_this()](const asio::error_code& ec) {
        if (!ec) self->on_tick();
    });
}

void Connection::on_tick() {
    if (closed_) return;
    const EngineConfig& cfg = host_.config();
    const auto now = Clock::now();
    if (cfg.idle_timeout_ms > 0 && now - last_recv_ >= std::chrono::milliseconds(cfg.idle_timeout_ms)) {
        return finish(DisconnectCode::Fault, "keepalive timeout", CloseMode::Abort, true);
    }
    if (cfg.keepalive_ms > 0 && now - last_ping_ >= std::chrono::milliseconds(cfg.keepalive_ms)) send_ping();
    schedule_tick();
}

void Connection::protocol_error(const std::string& what) {
    Event ev;
    ev.type = EventType::Log;
    ev.code = static_cast<int>(LogLevel::Warning);
    ev.conn_id = id_;
    ev.message = "protocol error: " + what;
    host_.emit(std::move(ev));
    finish(DisconnectCode::Fault, "protocol error: " + what, CloseMode::Abort, true);
}

void Connection::on_io_error(const asio::error_code& ec) {
    if (closed_) {
        if (closing_gracefully_) close_socket();
        return;
    }
    if (ec == asio::error::operation_aborted) return;
    const std::string reason = ec == asio::error::eof ? "connection closed by peer" : error_text(ec);
    finish(DisconnectCode::Fault, reason, CloseMode::Abort, true);
}

void Connection::finish(DisconnectCode code, const std::string& reason, CloseMode mode, bool notify, ByeReason bye) {
    if (closed_) return;
    closed_ = true;
    const bool was_ready = ready_;
    handshake_timer_.cancel();
    tick_timer_.cancel();

    // Всё, что мы ждали, провалилось: Python вернёт эти чанки в очередь и попросит у другого пира.
    if (was_ready) {
        for (const Request& r : inflight_) {
            if (!r.cancelled) emit_chunk_event(EventType::ChunkFailed, r.id, r.index, static_cast<int>(FailCode::Disconnect));
        }
        for (const Request& r : pending_) {
            emit_chunk_event(EventType::ChunkFailed, r.id, r.index, static_cast<int>(FailCode::Disconnect));
        }
    }
    inflight_.clear();
    pending_.clear();
    std::erase_if(out_, [](const Outgoing& o) { return std::holds_alternative<ChunkJob>(o); });
    queued_chunks_ = 0;

    host_.on_closed(*this, code, reason, was_ready && notify);

    if (mode == CloseMode::SendBye && !socket_closed_) {
        closing_gracefully_ = true;
        FrameHeader h;
        h.type = MsgType::Bye;
        queue_frame(h, encode_reason(std::to_underlying(bye)));
        close_timer_.expires_after(std::chrono::seconds(2));  // собеседник молчит — закрываем сами
        close_timer_.async_wait([self = shared_from_this()](const asio::error_code& ec) {
            if (!ec) self->close_socket();
        });
    } else {
        close_socket();
    }
}

void Connection::close_socket() noexcept {
    if (socket_closed_) return;
    socket_closed_ = true;
    asio::error_code ignored;
    socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
    socket_.close(ignored);
    handshake_timer_.cancel();
    tick_timer_.cancel();
    close_timer_.cancel();
    host_.on_socket_closed(*this);
}

void Connection::emit_chunk_event(EventType type, const FileId& id, std::uint32_t index, int code) {
    Event ev;
    ev.type = type;
    ev.conn_id = id_;
    ev.file_id = file_id_bytes(id);
    ev.chunk_index = index;
    ev.code = code;
    host_.emit(std::move(ev));
}

}  // namespace p2p
