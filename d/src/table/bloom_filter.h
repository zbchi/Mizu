#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "lsmtree/db.h"

namespace lsmtree {

class BloomFilterBuilder final {
 public:
  // 只保留固定长度 hash，避免在 SSTable 构建期间复制所有 user key
  void add(Slice key);

  // 使用 10 bits/key 和 6 次探测生成编码 末尾保存探测次数
  std::string finish() const;

 private:
  std::vector<std::uint32_t> hashes_;
};

// 判断 key 是否可能存在 false 表示可以跳过实际数据查询
bool bloomFilterMayContain(Slice key, Slice encoded_filter) noexcept;

}
