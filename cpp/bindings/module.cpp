// Python-модуль p2pshare._p2pcore: тот же API, что у FakeEngine.
//
// Правила GIL (ARCHITECTURE.md, 1.4): каждый метод Engine, который может ждать
// (сеть, io-поток, condition_variable), отпускает GIL через call_guard. Аргументы
// конвертируются до отпускания, результат — после, уже под GIL, поэтому внутри
// C++ нет ни одного обращения к объектам Python.
#include <pybind11/native_enum.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl/filesystem.h>

#include <cstring>
#include <string>

#include "p2pcore/engine.hpp"

namespace py = pybind11;
using p2p::Engine;
using p2p::EngineConfig;
using p2p::Event;
using p2p::EventType;

namespace {

// Текст из C++ всегда UTF-8, но на всякий случай не даём кривому байту превратиться
// в UnicodeDecodeError вместо нормального сообщения.
py::str utf8(const std::string& s) {
    PyObject* o = PyUnicode_DecodeUTF8(s.data(), static_cast<Py_ssize_t>(s.size()), "replace");
    if (o == nullptr) throw py::error_already_set();
    return py::reinterpret_steal<py::str>(o);
}

using Release = py::call_guard<py::gil_scoped_release>;

}  // namespace

PYBIND11_MODULE(_p2pcore, m) {
    m.doc() = "P2P Share C++ core: network protocol and chunk transfer";

    // ------------------------------------------------------------ P2PError
    PYBIND11_CONSTINIT static py::gil_safe_call_once_and_store<py::object> p2p_error;
    p2p_error.call_once_and_store_result(
        [&] { return py::exception<p2p::P2PError>(m, "P2PError", PyExc_RuntimeError); });
    py::register_exception_translator([](std::exception_ptr p) {
        try {
            if (p) std::rethrow_exception(p);
        } catch (const p2p::P2PError& e) {
            const py::str msg = utf8(e.what());
            PyErr_SetObject(p2p_error.get_stored().ptr(), msg.ptr());
        }
    });

    // ----------------------------------------------------------- EventType
    // Настоящий enum.IntEnum: сравнивается с p2pshare.engine.events.EventType и с int.
    py::native_enum<EventType>(m, "EventType", "enum.IntEnum")
        .value("PEER_CONNECTED", EventType::PeerConnected)
        .value("PEER_DISCONNECTED", EventType::PeerDisconnected)
        .value("PONG", EventType::Pong)
        .value("CONTROL", EventType::Control)
        .value("BITFIELD", EventType::Bitfield)
        .value("HAVE", EventType::Have)
        .value("CHUNK_RECEIVED", EventType::ChunkReceived)
        .value("CHUNK_FAILED", EventType::ChunkFailed)
        .value("PROGRESS", EventType::Progress)
        .value("LOG", EventType::Log)
        .value("ERROR", EventType::Error)
        .finalize();

    // --------------------------------------------------------------- Event
    py::class_<Event>(m, "Event")
        .def_property_readonly("type", [](const Event& e) { return e.type; })
        .def_readonly("conn_id", &Event::conn_id)
        .def_property_readonly("file_id", [](const Event& e) { return py::bytes(e.file_id); })
        .def_readonly("chunk_index", &Event::chunk_index)
        .def_readonly("bytes_done", &Event::bytes_done)
        .def_readonly("value", &Event::value)
        .def_readonly("code", &Event::code)
        .def_property_readonly("data", [](const Event& e) { return py::bytes(e.data); })
        .def_property_readonly("message", [](const Event& e) { return utf8(e.message); })
        .def("__repr__", [](const Event& e) {
            return py::str("Event(type={}, conn_id={}, chunk_index={}, code={}, bytes_done={}, value={})")
                .format(py::cast(e.type), e.conn_id, e.chunk_index, e.code, e.bytes_done, e.value);
        });

    // -------------------------------------------------------- EngineConfig
    py::class_<EngineConfig>(m, "EngineConfig")
        .def(py::init<>())
        .def_property(
            "peer_id", [](const EngineConfig& c) { return py::bytes(c.peer_id); },
            [](EngineConfig& c, const py::bytes& v) {
                std::string s = v;
                if (s.size() != 16) throw py::value_error("peer_id must be 16 bytes");
                c.peer_id = std::move(s);
            })
        .def_readwrite("name", &EngineConfig::name)
        .def_readwrite("max_inflight", &EngineConfig::max_inflight)
        .def_readwrite("keepalive_ms", &EngineConfig::keepalive_ms)
        .def_readwrite("idle_timeout_ms", &EngineConfig::idle_timeout_ms)
        .def_readwrite("fault_drop_after_bytes", &EngineConfig::fault_drop_after_bytes)
        .def_readwrite("fault_corrupt_chunk", &EngineConfig::fault_corrupt_chunk);

    // -------------------------------------------------------------- Engine
    // file_id, have, payload — Python bytes -> std::string (двоичные данные целиком);
    // пути — str или os.PathLike -> std::filesystem::path (на Windows широкие, кириллица работает).
    py::class_<Engine>(m, "Engine")
        .def(py::init<EngineConfig>(), py::arg("config"))
        .def("start", &Engine::start, py::arg("port") = 0, Release())
        .def("stop", &Engine::stop, Release())
        .def("connect", &Engine::connect, py::arg("host"), py::arg("port"), py::arg("timeout_ms") = 5000, Release())
        .def("disconnect", &Engine::disconnect, py::arg("conn_id"), Release())
        .def("ping", &Engine::ping, py::arg("conn_id"), Release())
        .def("send_control", &Engine::send_control, py::arg("conn_id"), py::arg("payload"), Release())
        .def("share_file", &Engine::share_file, py::arg("file_id"), py::arg("path"), py::arg("size"),
             py::arg("chunk_size"), Release())
        .def("open_download", &Engine::open_download, py::arg("file_id"), py::arg("part_path"), py::arg("size"),
             py::arg("chunk_size"), py::arg("have"), Release())
        .def("set_have", &Engine::set_have, py::arg("file_id"), py::arg("chunk_index"), Release())
        .def("send_bitfield", &Engine::send_bitfield, py::arg("conn_id"), py::arg("file_id"), Release())
        .def("request_chunks", &Engine::request_chunks, py::arg("conn_id"), py::arg("file_id"), py::arg("indices"),
             Release())
        .def("cancel_chunks", &Engine::cancel_chunks, py::arg("conn_id"), py::arg("file_id"), py::arg("indices"),
             Release())
        .def("flush", &Engine::flush, py::arg("file_id"), Release())
        .def("close_file", &Engine::close_file, py::arg("file_id"), Release())
        .def("set_rate_limit", &Engine::set_rate_limit, py::arg("upload_bps"), py::arg("download_bps"), Release())
        .def("poll_events", &Engine::poll_events, py::arg("max_events") = 512, Release())
        .def("wait_events", &Engine::wait_events, py::arg("timeout_ms") = 100, py::arg("max_events") = 512, Release())
        .def("stats", &Engine::stats, Release())
        // with Engine(cfg) as e: ... — stop() при выходе
        .def("__enter__", [](Engine& e) -> Engine& { return e; }, py::return_value_policy::reference)
        .def("__exit__", [](Engine& e, const py::args&) { e.stop(); }, Release());
}
