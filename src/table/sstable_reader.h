#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "db/internal_key.h"
#include "db/lookup_result.h"
#include "lsmtree/db.h"

namespace lsmtree {

// 打开一个不可变的 SSTable 并根据 InternalKey 顺序执行点查
// index block 常驻内存 data block 只在查询命中其范围时读取
class SSTableReader final {
 public:
  static Status open(const std::filesystem::path& path,
                     std::unique_ptr<SSTableReader>& reader);

  ~SSTableReader();

  SSTableReader(const SSTableReader&) = delete;
  SSTableReader& operator=(const SSTableReader&) = delete;

  // 查找不晚于 visible_sequence 的第一个版本
  // kAbsent 允许上层继续查询更老的存储层 kDeleted 则终止查询
  Status get(const ReadOptions& options, Slice user_key,
             SequenceNumber visible_sequence, LookupResult& result,
             std::string& value) const;

 private:
  explicit SSTableReader(int fd) noexcept;

  int fd_;
  std::uint64_t file_size_ = 0;
  std::string filter_block_;
  std::string index_block_;
};

}
