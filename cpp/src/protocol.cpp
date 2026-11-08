#include "p2pcore/protocol.hpp"

namespace p2p {
// Здесь позже будет реализация encode_header и decode_header

    void encode_header(const FrameHeader &h,std::span<std::uint8_t, kHeaderSize> out){
        
    }

    DecodeError decode_header(std::span<const std::uint8_t, kHeaderSize> in, FrameHeader& h) {

    return DecodeError::None;
}
}