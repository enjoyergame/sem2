// PositionalFile для Windows: CreateFileW (широкий путь — кириллица работает),
// ReadFile/WriteFile с OVERLAPPED.Offset — чтение/запись по смещению без общего
// указателя файла.
#include "p2pcore/errors.hpp"
#include "p2pcore/file_store.hpp"

#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <winioctl.h>

#include <algorithm>
#include <string>

namespace p2p {
namespace {

HANDLE as_handle(std::intptr_t h) noexcept { return reinterpret_cast<HANDLE>(h); }

[[noreturn]] void fail(const char* what, const std::filesystem::path& p) {
    const std::error_code ec = last_os_error();
    throw P2PError(std::string(what) + " " + path_to_utf8(p) + ": " + error_text(ec));
}

OVERLAPPED at(std::uint64_t offset) noexcept {
    OVERLAPPED ov{};
    ov.Offset = static_cast<DWORD>(offset & 0xFFFFFFFFu);
    ov.OffsetHigh = static_cast<DWORD>(offset >> 32);
    return ov;
}

// Делим большие запросы: ReadFile/WriteFile принимают DWORD.
constexpr std::size_t kMaxIo = 1u << 30;

// Разрешаем другим читать, писать и переименовывать файл, пока он открыт у нас:
// Python-верификатор читает .part параллельно, а FILE_SHARE_DELETE не мешает os.replace.
constexpr DWORD kShareAll = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

}  // namespace

PositionalFile PositionalFile::open_read(const std::filesystem::path& p) {
    const HANDLE h = CreateFileW(p.c_str(), GENERIC_READ, kShareAll, nullptr, OPEN_EXISTING,
                                 FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE) fail("cannot open", p);
    return PositionalFile(reinterpret_cast<std::intptr_t>(h), p);
}

PositionalFile PositionalFile::open_write(const std::filesystem::path& p, std::uint64_t size) {
    const HANDLE h = CreateFileW(p.c_str(), GENERIC_READ | GENERIC_WRITE, kShareAll, nullptr,
                                 OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE) fail("cannot open", p);
    PositionalFile f(reinterpret_cast<std::intptr_t>(h), p);

    // Разрежённый файл: не на всех ФС поддерживается (FAT32) — тогда просто обычный.
    DWORD returned = 0;
    DeviceIoControl(h, FSCTL_SET_SPARSE, nullptr, 0, nullptr, 0, &returned, nullptr);

    FILE_END_OF_FILE_INFO eof{};
    eof.EndOfFile.QuadPart = static_cast<LONGLONG>(size);
    if (!SetFileInformationByHandle(h, FileEndOfFileInfo, &eof, sizeof(eof))) fail("cannot resize", p);
    return f;
}

std::size_t PositionalFile::read_at(std::uint64_t offset, std::span<std::uint8_t> buf) {
    std::size_t done = 0;
    while (done < buf.size()) {
        OVERLAPPED ov = at(offset + done);
        const auto want = static_cast<DWORD>(std::min(buf.size() - done, kMaxIo));
        DWORD got = 0;
        if (!ReadFile(as_handle(handle_), buf.data() + done, want, &got, &ov)) {
            if (GetLastError() == ERROR_HANDLE_EOF) break;
            fail("cannot read", path_);
        }
        if (got == 0) break;  // конец файла
        done += got;
    }
    return done;
}

void PositionalFile::write_at(std::uint64_t offset, std::span<const std::uint8_t> buf) {
    std::size_t done = 0;
    while (done < buf.size()) {
        OVERLAPPED ov = at(offset + done);
        const auto want = static_cast<DWORD>(std::min(buf.size() - done, kMaxIo));
        DWORD put = 0;
        if (!WriteFile(as_handle(handle_), buf.data() + done, want, &put, &ov)) fail("cannot write", path_);
        if (put == 0) fail("cannot write", path_);
        done += put;
    }
}

void PositionalFile::flush() {
    if (!FlushFileBuffers(as_handle(handle_))) fail("cannot flush", path_);
}

void PositionalFile::close() noexcept {
    if (handle_ != kInvalid) {
        CloseHandle(as_handle(handle_));
        handle_ = kInvalid;
    }
}

std::uint64_t PositionalFile::size() const {
    LARGE_INTEGER sz{};
    if (!GetFileSizeEx(as_handle(handle_), &sz)) fail("cannot stat", path_);
    return static_cast<std::uint64_t>(sz.QuadPart);
}

}  // namespace p2p
