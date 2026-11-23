// PositionalFile для Linux/macOS: open + pread/pwrite (чтение/запись по смещению
// без общего указателя файла), ftruncate для предразмещения (файл получается разрежённым).
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <string>

#include "p2pcore/errors.hpp"
#include "p2pcore/file_store.hpp"

namespace p2p {
namespace {

[[noreturn]] void fail(const char* what, const std::filesystem::path& p) {
    const std::error_code ec = last_os_error();
    throw P2PError(std::string(what) + " " + path_to_utf8(p) + ": " + error_text(ec));
}

int fd_of(std::intptr_t h) noexcept { return static_cast<int>(h); }

constexpr std::size_t kMaxIo = 1u << 30;

}  // namespace

PositionalFile PositionalFile::open_read(const std::filesystem::path& p) {
    int fd = -1;
    do {
        fd = ::open(p.c_str(), O_RDONLY | O_CLOEXEC);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) fail("cannot open", p);
    return PositionalFile(fd, p);
}

PositionalFile PositionalFile::open_write(const std::filesystem::path& p, std::uint64_t size) {
    int fd = -1;
    do {
        fd = ::open(p.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) fail("cannot open", p);
    PositionalFile f(fd, p);
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0) fail("cannot resize", p);
    return f;
}

std::size_t PositionalFile::read_at(std::uint64_t offset, std::span<std::uint8_t> buf) {
    std::size_t done = 0;
    while (done < buf.size()) {
        const std::size_t want = std::min(buf.size() - done, kMaxIo);
        const ssize_t got = ::pread(fd_of(handle_), buf.data() + done, want, static_cast<off_t>(offset + done));
        if (got < 0) {
            if (errno == EINTR) continue;
            fail("cannot read", path_);
        }
        if (got == 0) break;  // конец файла
        done += static_cast<std::size_t>(got);
    }
    return done;
}

void PositionalFile::write_at(std::uint64_t offset, std::span<const std::uint8_t> buf) {
    std::size_t done = 0;
    while (done < buf.size()) {
        const std::size_t want = std::min(buf.size() - done, kMaxIo);
        const ssize_t put = ::pwrite(fd_of(handle_), buf.data() + done, want, static_cast<off_t>(offset + done));
        if (put < 0) {
            if (errno == EINTR) continue;
            fail("cannot write", path_);
        }
        if (put == 0) fail("cannot write", path_);
        done += static_cast<std::size_t>(put);
    }
}

void PositionalFile::flush() {
    if (::fsync(fd_of(handle_)) != 0) fail("cannot flush", path_);
}

void PositionalFile::close() noexcept {
    if (handle_ != kInvalid) {
        ::close(fd_of(handle_));
        handle_ = kInvalid;
    }
}

std::uint64_t PositionalFile::size() const {
    struct stat st {};
    if (::fstat(fd_of(handle_), &st) != 0) fail("cannot stat", path_);
    return static_cast<std::uint64_t>(st.st_size);
}

}  // namespace p2p
