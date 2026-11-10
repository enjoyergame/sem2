#pragma once
// Golden-векторы протокола: эталонные кадры, общие для C++ и Python.
//
// Один и тот же набор кадров:
//   * кодируется кодеком ядра и записывается в tests/vectors/frames.json
//     (программа p2pcore_gen_vectors, cpp/tools/gen_vectors.cpp);
//   * проверяется C++-тестом test_vectors.cpp: файл в репозитории обязан байт в байт
//     совпадать с тем, что кодек выдаёт сейчас;
//   * читается Python-тестом test_protocol_vectors.py, который разбирает те же байты
//     независимо, через struct.
// Если кто-то поменяет формат кадра только с одной стороны, один из тестов упадёт.
// Формат файла описан в docs/PROTOCOL.md, раздел 6.

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "p2pcore/protocol.hpp"

namespace p2p::golden {

// Корректный кадр. payload_len и payload_crc в header уже посчитаны.
struct Frame {
    std::string name;
    std::string note;
    FrameHeader header;
    std::vector<std::uint8_t> payload;
    std::string fields;  // расшифровка нагрузки — готовый JSON-объект или "null"
};

// Заголовок и то, что обязан вернуть decode_header.
struct HeaderCheck {
    std::string name;
    std::string note;
    std::array<std::uint8_t, kHeaderSize> bytes{};
    DecodeError expected = DecodeError::None;
};

// Нагрузка, которую декодер обязан отвергнуть (неверная длина).
struct BadPayload {
    std::string name;
    std::string note;
    MsgType type = MsgType::Ping;
    std::vector<std::uint8_t> payload;
};

[[nodiscard]] std::vector<Frame> frames();
[[nodiscard]] std::vector<HeaderCheck> header_checks();
[[nodiscard]] std::vector<BadPayload> bad_payloads();

// Кадр целиком: 40 байт заголовка + нагрузка.
[[nodiscard]] std::vector<std::uint8_t> frame_bytes(const Frame& f);

// Имя результата в JSON: "OK", "BAD_MAGIC", "BAD_VERSION", "PAYLOAD_TOO_LARGE", "UNKNOWN_TYPE".
[[nodiscard]] std::string result_name(DecodeError e);

// Текст файла frames.json (UTF-8, переводы строк \n).
[[nodiscard]] std::string build_json();

}  // namespace p2p::golden
