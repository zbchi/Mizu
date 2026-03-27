#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "lsmtree/db.h"

namespace lsmtree {

struct BlockBuilderOptions {
  std::size_t restart_interval = 16;
};

// 构造内存中的 prefix-compressed block payload
// trailer 由 SSTable writer 写入
// key 必须严格递增
class BlockBuilder {
 public:
  explicit BlockBuilder(BlockBuilderOptions options = {});

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  void reset();

  // finish 后必须先 reset
  // key 必须严格递增
  void add(Slice key, Slice value);

  // 写入 restart 元数据并冻结 block
  // 返回的 Slice 在 reset 或析构前有效
  Slice finish();

  bool empty() const noexcept { return entry_count_ == 0; }
  std::size_t currentSizeEstimate() const noexcept;

 private:
  BlockBuilderOptions options_;
  std::string buffer_;
  std::vector<std::uint32_t> restarts_;
  std::string last_key_;
  std::size_t entries_since_restart_ = 0;
  std::size_t entry_count_ = 0;
  bool finished_ = false;
};

}
