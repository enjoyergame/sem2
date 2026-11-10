#pragma once
// Общее исключение ядра и вспомогательные функции для текста ошибок.
//
// Все сообщения ядра — в UTF-8: pybind11 превращает what() в Python str, и текст
// в кодировке ANSI (например, системные сообщения Windows на русском в CP1251)
// превратился бы в UnicodeDecodeError вместо P2PError.

#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>

namespace p2p {

// Синхронная ошибка API (неверный аргумент, не удалось подключиться, нет файла).
// В Python — p2pshare._p2pcore.P2PError (наследник RuntimeError).
class P2PError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Текст ошибки в UTF-8. На Windows системные коды переводятся через FormatMessageW.
[[nodiscard]] std::string error_text(const std::error_code& ec);

// Последняя ошибка ОС (GetLastError / errno) как std::error_code.
[[nodiscard]] std::error_code last_os_error() noexcept;

// Путь в UTF-8 (для сообщений): на Windows path хранит wchar_t.
[[nodiscard]] std::string path_to_utf8(const std::filesystem::path& p);

}  // namespace p2p
