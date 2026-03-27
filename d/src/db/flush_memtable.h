#pragma once

#include <filesystem>

#include "db/memtable.h"
#include "lsmtree/db.h"
#include "table/sstable_builder.h"

namespace lsmtree {

// 将不可变 MemTable 有序写入一个完整的 L0 SSTable
// 只生成文件和元数据 不修改 WAL 或数据库可见状态
Status buildLevel0Table(const MemTable& memtable,
                        const std::filesystem::path& temporary_path,
                        const std::filesystem::path& final_path,
                        const SSTableBuilderOptions& options,
                        SSTableMeta& meta);

}
