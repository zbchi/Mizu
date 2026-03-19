#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "db/internal_iterator.h"
#include "db/internal_key.h"
#include "db/lookup_result.h"
#include "lsmtree/db.h"

namespace lsmtree {

class BlockIterator;
class SSTableIterator;

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

  // 返回按 InternalKey 顺序读取整张表的内部前向迭代器
  std::unique_ptr<SSTableIterator> newIterator(
      const ReadOptions& options) const;

 private:
  friend class SSTableIterator;
  explicit SSTableReader(int fd) noexcept;

  int fd_;
  std::uint64_t file_size_ = 0;
  std::string filter_block_;
  std::string index_block_;
};

// 迭代器借用 SSTableReader 调用方必须让 reader 存活得更久
class SSTableIterator final : public InternalIterator {
 public:
  ~SSTableIterator();

  SSTableIterator(const SSTableIterator&) = delete;
  SSTableIterator& operator=(const SSTableIterator&) = delete;

  void seekToFirst() override;
  void seek(Slice target) override;
  void next() override;

  bool valid() const noexcept override;
  Slice internalKey() const override;
  Slice value() const override;
  const Status& status() const noexcept override { return status_; }

 private:
  friend class SSTableReader;
  SSTableIterator(const SSTableReader& table, ReadOptions options);

  void loadDataBlock();

  const SSTableReader& table_;
  ReadOptions options_;
  std::unique_ptr<BlockIterator> index_;
  std::string data_block_;
  std::unique_ptr<BlockIterator> data_;
  Status status_;
};

}
