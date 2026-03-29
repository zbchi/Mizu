#pragma once

#include <cstddef>

namespace lsmtree {

// record 头依次保存 payload 长度和 CRC32C
inline constexpr std::size_t kWalRecordHeaderSize = 8U;
inline constexpr std::size_t kMaxWalRecordPayloadSize = 64U * 1024U * 1024U;

}
