#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "lsmtree/db.h"

namespace lsmtree {

// 在一个 prefix-compressed block 中顺序读取和定位记录
// block 的内容必须覆盖迭代器的完整生命周期
class BlockIterator {
 public:
  explicit BlockIterator(Slice block);

  BlockIterator(const BlockIterator&) = delete;
  BlockIterator& operator=(const BlockIterator&) = delete;

  bool valid() const noexcept { return valid_; }

  void seekToFirst();

  // 借助 restart point 定位到第一条不小于 target 的记录
  void seek(Slice target);

  void next();

  // 返回的 Slice 在下一次移动前有效
  Slice key() const;
  Slice value() const;

  const Status& status() const noexcept { return status_; }

 private:
  std::uint32_t restartOffset(std::uint32_t index) const noexcept;
  bool readRestartKey(std::uint32_t index, Slice& key);
  bool seekToRestartPoint(std::uint32_t index);
  bool decodeEntry(std::size_t offset);
  bool fail(const char* message);
  void invalidate() noexcept;

  Slice block_;
  std::size_t restart_offset_ = 0;
  std::uint32_t restart_count_ = 0;
  std::size_t next_offset_ = 0;
  std::string key_;
  Slice value_;
  bool valid_ = false;
  Status status_;
};

}
