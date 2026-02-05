#pragma once

#include <cstdint>

#include "lsmtree/db.h"

namespace lsmtree {

// 返回未经掩码处理的 CRC-32C Castagnoli 校验和
std::uint32_t crc32c(Slice input);

}
