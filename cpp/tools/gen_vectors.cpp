// p2pcore_gen_vectors: записывает golden-векторы протокола в tests/vectors/frames.json.
//
// Запускать после осознанного изменения формата кадра или набора векторов:
//   cmake --build build --target p2pcore_gen_vectors
//   build/p2pcore_gen_vectors            (путь по умолчанию — файл в репозитории)
// Затем посмотреть git diff файла: каждое изменившееся значение должно быть объяснимо.
// Напарнику сообщить — его Python-тест читает тот же файл.
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

#include "golden_vectors.hpp"

int main(int argc, char** argv) {
    const std::filesystem::path path = argc > 1 ? argv[1] : P2P_VECTORS_PATH;
    const std::string json = p2p::golden::build_json();

    // binary: на Windows текстовый режим превратил бы \n в \r\n.
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(json.data(), static_cast<std::streamsize>(json.size()));
    out.close();
    if (!out) {
        std::fprintf(stderr, "cannot write %s\n", path.string().c_str());
        return 1;
    }
    std::printf("wrote %zu bytes to %s\n", json.size(), path.string().c_str());
    return 0;
}
