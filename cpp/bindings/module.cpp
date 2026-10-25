#include <pybind11/pybind11.h>

namespace py = pybind11;

// Этот макрос создаёт Python-модуль с именем _p2pcore
PYBIND11_MODULE(_p2pcore, m) {
    m.doc() = "P2P Share C++ core module (Stub)";

    // Позже здесь появятся привязки классов Engine, EngineConfig и т.д.
}