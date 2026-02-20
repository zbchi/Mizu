#pragma once

#include <cstdint>
#include <string>

#include "lsmtree/db.h"

namespace lsmtree {

// 在 4 字节缓冲区读写 little-endian 整数
void encodeFixed32(char* dst, std::uint32_t value) noexcept;
std::uint32_t decodeFixed32(const char* src) noexcept;

// 按小端序将 value 追加到 dst
void putFixed32(std::string& dst, std::uint32_t value);
void putFixed64(std::string& dst, std::uint64_t value);

// 成功时从 input 开头读取 4 或 8 字节并前移 input
// 输入不足时不修改 input 和 value
bool getFixed32(Slice& input, std::uint32_t& value);
bool getFixed64(Slice& input, std::uint64_t& value);

}
