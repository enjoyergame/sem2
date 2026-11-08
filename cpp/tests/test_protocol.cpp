#include <doctest/doctest.h>

#include "p2pcore/protocol.hpp"

TEST_CASE("что-то простое работает") {
    CHECK(p2p::kHeaderSize == 40);
}