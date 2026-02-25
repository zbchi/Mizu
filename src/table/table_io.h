#pragma once

#include <cstdint>
#include <string>

#include "lsmtree/db.h"
#include "table/table_format.h"

namespace lsmtree {

// 读取并校验文件末尾的 SSTable footer
Status readSSTableFooter(int fd, std::uint64_t file_size,
                         BlockHandle& index_handle);

// 读取 footer 前的 block payload 并按需校验 checksum
Status readBlock(int fd, std::uint64_t file_size, const BlockHandle& handle,
                 bool verify_checksum, std::string& payload);

}
