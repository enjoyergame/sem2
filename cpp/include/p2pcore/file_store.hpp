#pragma once
// Позиционный ввод-вывод и реестр файлов движка.
//
// Почему не mmap: на Windows ошибки диска при работе с отображением приходят как
// access violation, а не как код ошибки, и для потоковой передачи выигрыша нет.
// Почему не fstream: не умеет читать/писать по смещению без общего указателя файла
// и на Windows с std::string-путём ломается на кириллице. Здесь — pread/pwrite
// (POSIX) и ReadFile/WriteFile с OVERLAPPED (Win32), путь — std::filesystem::path.

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <span>

#include "p2pcore/bitfield.hpp"
#include "p2pcore/protocol.hpp"

namespace p2p {

// Файл, открытый для чтения/записи по смещению. Только перемещение, не копирование.
// Ошибки — исключение P2PError с текстом в UTF-8.
class PositionalFile {
public:
    PositionalFile() noexcept = default;
    ~PositionalFile();
    PositionalFile(PositionalFile&& other) noexcept;
    PositionalFile& operator=(PositionalFile&& other) noexcept;
    PositionalFile(const PositionalFile&) = delete;
    PositionalFile& operator=(const PositionalFile&) = delete;

    // Существующий файл только для чтения.
    static PositionalFile open_read(const std::filesystem::path& p);
    // Открыть или создать файл для записи и установить его размер (предразмещение).
    // На NTFS файл помечается разрежённым: запись далеко от начала не заставляет ОС
    // заранее занулять весь промежуток.
    static PositionalFile open_write(const std::filesystem::path& p, std::uint64_t size);

    // Прочитать до buf.size() байт; меньше — только если файл кончился.
    std::size_t read_at(std::uint64_t offset, std::span<std::uint8_t> buf);
    // Записать все байты или бросить исключение.
    void write_at(std::uint64_t offset, std::span<const std::uint8_t> buf);
    // Сбросить данные на диск (fsync / FlushFileBuffers): вызывается перед сохранением state.
    void flush();
    void close() noexcept;

    [[nodiscard]] bool is_open() const noexcept { return handle_ != kInvalid; }
    [[nodiscard]] std::uint64_t size() const;
    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    // HANDLE на Windows (INVALID_HANDLE_VALUE == -1) или дескриптор на POSIX (-1 == нет файла).
    static constexpr std::intptr_t kInvalid = -1;

    PositionalFile(std::intptr_t handle, std::filesystem::path p) noexcept;

    std::intptr_t handle_ = kInvalid;
    std::filesystem::path path_;
};

// Файл, известный движку: своя раздача (share_file) или загрузка (open_download).
// Живёт в shared_ptr: соединение, которое как раз отдаёт чанк, держит запись живой,
// даже если Python уже вызвал close_file.
struct FileEntry {
    FileId id{};
    std::filesystem::path path;
    std::uint64_t size = 0;
    std::uint32_t chunk_size = 0;
    Bitfield have;          // какие чанки можно отдавать (рабочая копия; правда — у Python)
    bool writable = false;  // открыт через open_download: в него пишутся принятые чанки
    PositionalFile file;

    [[nodiscard]] std::uint32_t chunk_count() const noexcept {
        return chunk_size == 0 ? 0 : static_cast<std::uint32_t>((size + chunk_size - 1) / chunk_size);
    }
    [[nodiscard]] std::uint64_t chunk_offset(std::uint32_t index) const noexcept {
        return static_cast<std::uint64_t>(index) * chunk_size;
    }
    // Длина чанка: последний может быть короче.
    [[nodiscard]] std::uint32_t chunk_length(std::uint32_t index) const noexcept {
        const std::uint64_t off = chunk_offset(index);
        if (index >= chunk_count() || off >= size) return 0;
        const std::uint64_t rest = size - off;
        return static_cast<std::uint32_t>(rest < chunk_size ? rest : chunk_size);
    }
};

using FileRegistry = std::map<FileId, std::shared_ptr<FileEntry>>;

}  // namespace p2p
