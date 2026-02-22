#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace lsmtree {

inline constexpr char kSSTableMagic[] = "LSMTREE1";
constexpr std::size_t kSSTableMagicSize = sizeof(kSSTableMagic) - 1U;
constexpr std::uint32_t kSSTableVersion = 1;
constexpr std::uint8_t kNoCompression = 0;
constexpr std::size_t kBlockTrailerSize =
    sizeof(std::uint8_t) + sizeof(std::uint32_t);
constexpr std::size_t kBlockHandleSize = 2U * sizeof(std::uint64_t);
constexpr std::size_t kSSTableFooterSize =
    kSSTableMagicSize + 2U * sizeof(std::uint32_t) + kBlockHandleSize;

static_assert(kSSTableFooterSize == 32U);

struct BlockHandle {
  std::uint64_t offset = 0;
  std::uint64_t size = 0;
};

// 以固定长度编码 block 位置和大小
void putBlockHandle(std::string& destination, const BlockHandle& handle);

// 以固定长度编码包含索引位置的 footer
void putSSTableFooter(std::string& destination,
                      const BlockHandle& index_handle);

}
