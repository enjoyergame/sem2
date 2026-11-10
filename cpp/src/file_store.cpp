// Общая часть PositionalFile; платформенные функции — в file_store_win32.cpp / file_store_posix.cpp.
#include "p2pcore/file_store.hpp"

#include <utility>

namespace p2p {

PositionalFile::PositionalFile(std::intptr_t handle, std::filesystem::path p) noexcept
    : handle_(handle), path_(std::move(p)) {}

PositionalFile::~PositionalFile() { close(); }

PositionalFile::PositionalFile(PositionalFile&& other) noexcept
    : handle_(std::exchange(other.handle_, kInvalid)), path_(std::move(other.path_)) {}

PositionalFile& PositionalFile::operator=(PositionalFile&& other) noexcept {
    if (this != &other) {
        close();
        handle_ = std::exchange(other.handle_, kInvalid);
        path_ = std::move(other.path_);
    }
    return *this;
}

}  // namespace p2p
