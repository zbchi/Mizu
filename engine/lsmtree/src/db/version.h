#pragma once

#include <array>
#include <filesystem>
#include <memory>
#include <vector>

#include "db/level.h"
#include "db/lookup_result.h"
#include "db/manifest.h"
#include "lsmtree/db.h"

namespace lsmtree {

class SSTableReader;

// 已发布的磁盘读取状态不可变 读操作可以安全持有旧 Version
class Version final {
 public:
  // reader 和元数据共享生命周期 旧 Version 释放前文件始终可读
  struct Table {
    TableMeta meta;
    std::shared_ptr<const SSTableReader> reader;
  };

  // 打开 Manifest 引用的全部文件 全部成功后才发布 Version
  static Status open(const std::filesystem::path& directory,
                     const ManifestState& manifest,
                     std::shared_ptr<const Version>& version);

  // L0 顺序查找后在互不重叠的 L1/L2 中每层至多读取一个文件
  Status get(const ReadOptions& options, Slice user_key,
             SequenceNumber visible_sequence, LookupResult& result,
             std::string& value) const;

  // 保留当前磁盘状态并将新 flush 文件放到 L0 最前面
  std::shared_ptr<const Version> withLevel0Table(
      TableMeta meta, std::shared_ptr<const SSTableReader> reader) const;

  // 原子描述一次相邻层区间替换；outputs 为空表示全部输入已被回收。
  std::shared_ptr<const Version> withCompaction(
      std::size_t input_level, IndexRange inputs, IndexRange overlaps,
      std::vector<Table> outputs) const;

  const std::vector<Table>& level(std::size_t level) const noexcept;

 private:
  std::array<std::vector<Table>, kNumLevels> levels_;
};

}  // namespace lsmtree
