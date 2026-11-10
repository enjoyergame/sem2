cmake_minimum_required(VERSION 3.20)
project(p2pcore LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include(FetchContent)

# --- standalone Asio (header-only, без Boost) ---
FetchContent_Declare(asio
    GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
    GIT_TAG        asio-1-30-2
    GIT_SHALLOW    TRUE)
FetchContent_MakeAvailable(asio)   # своего CMakeLists у asio нет — просто скачается
add_library(asio INTERFACE)
target_include_directories(asio SYSTEM INTERFACE ${asio_SOURCE_DIR}/asio/include)
target_compile_definitions(asio INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)
if(WIN32)
    target_compile_definitions(asio INTERFACE _WIN32_WINNT=0x0A00)  # Windows 10
    target_link_libraries(asio INTERFACE ws2_32 mswsock)
endif()

find_package(Threads REQUIRED)

# --- ядро: статическая библиотека, общая для модуля и тестов ---
set(P2PCORE_SOURCES
    cpp/src/errors.cpp
    cpp/src/protocol.cpp cpp/src/crc32c.cpp cpp/src/bitfield.cpp
    cpp/src/file_store.cpp cpp/src/rate_limiter.cpp cpp/src/event_queue.cpp
    cpp/src/connection.cpp cpp/src/engine.cpp)
if(WIN32)
    list(APPEND P2PCORE_SOURCES cpp/src/file_store_win32.cpp)
else()
    list(APPEND P2PCORE_SOURCES cpp/src/file_store_posix.cpp)
endif()

add_library(p2pcore_lib STATIC ${P2PCORE_SOURCES})
target_include_directories(p2pcore_lib PUBLIC cpp/include)
target_link_libraries(p2pcore_lib PUBLIC asio Threads::Threads)
if(MSVC)
    target_compile_options(p2pcore_lib PRIVATE /W4 /permissive- /utf-8)
else()
    target_compile_options(p2pcore_lib PRIVATE -Wall -Wextra -Wpedantic)
endif()

# --- Python-модуль (собирается при pip install через scikit-build-core) ---
if(SKBUILD)
    find_package(Python 3.11 REQUIRED COMPONENTS Interpreter Development.Module)
    find_package(pybind11 CONFIG REQUIRED)
    pybind11_add_module(_p2pcore MODULE cpp/bindings/module.cpp)
    target_link_libraries(_p2pcore PRIVATE p2pcore_lib)
    install(TARGETS _p2pcore LIBRARY DESTINATION p2pshare)
endif()

# --- C++-тесты: cmake -B build -DP2P_BUILD_TESTS=ON && cmake --build build && ctest --test-dir build ---
option(P2P_BUILD_TESTS "Build C++ unit tests" OFF)
if(P2P_BUILD_TESTS)
    FetchContent_Declare(doctest
        GIT_REPOSITORY https://github.com/doctest/doctest.git
        GIT_TAG        v2.4.12
        GIT_SHALLOW    TRUE)
    FetchContent_MakeAvailable(doctest)
    enable_testing()
    add_executable(p2pcore_tests
        cpp/tests/test_protocol.cpp cpp/tests/test_bitfield.cpp
        cpp/tests/test_file_store.cpp cpp/tests/test_rate_limiter.cpp
        cpp/tests/test_loopback.cpp)
    target_link_libraries(p2pcore_tests PRIVATE p2pcore_lib doctest::doctest_with_main)
    target_compile_definitions(p2pcore_tests PRIVATE
        P2P_VECTORS_PATH="${CMAKE_CURRENT_SOURCE_DIR}/tests/vectors/frames.json")
    add_test(NAME p2pcore_tests COMMAND p2pcore_tests)
endif()
