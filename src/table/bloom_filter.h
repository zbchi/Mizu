#pragma once

#include <string>
#include <vector>

#include "lsmtree/db.h"

namespace lsmtree {

class BloomFilter final {
 public:
  // 使用 10 bits/key 和 6 次探测编码一组 user key
  // filter 末尾保存探测次数使读取方不依赖构造参数
  static std::string create(const std::vector<Slice>& keys);

  // 判断 key 是否可能存在 false 表示可以跳过实际数据查询
  static bool mayContain(Slice key, Slice encoded_filter) noexcept;
};

}
