#include "p2pcore/errors.hpp"

#include <cerrno>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

namespace p2p {

#ifdef _WIN32
namespace {

std::string wide_to_utf8(const wchar_t* w, int len) {
    if (len <= 0) return {};
    const int n = WideCharToMultiByte(CP_UTF8, 0, w, len, nullptr, 0, nullptr, nullptr);
    std::string out(static_cast<std::size_t>(n), '\0');
    WideCharToMultiByte(CP_UTF8, 0, w, len, out.data(), n, nullptr, nullptr);
    return out;
}

std::string windows_message(int code) {
    wchar_t* buf = nullptr;
    const DWORD n = FormatMessageW(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        nullptr, static_cast<DWORD>(code), 0, reinterpret_cast<wchar_t*>(&buf), 0, nullptr);
    std::string text;
    if (n > 0 && buf != nullptr) {
        int len = static_cast<int>(n);
        while (len > 0 && (buf[len - 1] == L'\r' || buf[len - 1] == L'\n' || buf[len - 1] == L'.')) --len;
        text = wide_to_utf8(buf, len);
    }
    if (buf != nullptr) LocalFree(buf);
    return text;
}

}  // namespace
#endif

std::string error_text(const std::error_code& ec) {
    std::string text;
#ifdef _WIN32
    // Системные коды Windows (и WSA-коды сокетов) переводим сами, сразу в UTF-8.
    // Категорию сравниваем по имени: у asio и у стандартной библиотеки разные объекты категорий.
    const std::string cat = ec.category().name();
    if (cat == "system" || cat == "asio.system") {
        text = windows_message(ec.value());
    } else {
        text = ec.message();
        // Остальные категории (asio.misc, generic) дают ASCII; если вдруг нет — не рискуем.
        for (const char c : text) {
            if (static_cast<unsigned char>(c) >= 0x80) {
                text.clear();
                break;
            }
        }
    }
#else
    text = ec.message();
#endif
    if (text.empty()) text = "error";
    return text + " (" + std::to_string(ec.value()) + ")";
}

std::error_code last_os_error() noexcept {
#ifdef _WIN32
    return {static_cast<int>(GetLastError()), std::system_category()};
#else
    return {errno, std::generic_category()};
#endif
}

std::string path_to_utf8(const std::filesystem::path& p) {
    const std::u8string s = p.u8string();
    return std::string(s.begin(), s.end());
}

}  // namespace p2p
