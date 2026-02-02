#pragma once

#include <string>
#include <vector>

#include "lsmtree/db.h"

namespace lsmtree {

// 保存调用顺序和自有字符串 避免依赖传入 Slice 的生命周期
class WriteBatch::Rep {
 public:
  enum class OperationType {
    kPut,
    kDelete,
  };

  struct Operation {
    OperationType type;
    std::string key;
    std::string value;
  };

  std::vector<Operation> operations;
};

}
