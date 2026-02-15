#pragma once

#include <map>
#include <string>

#include "db/internal_key.h"

namespace lsmtree {

enum class LookupResult {
  kAbsent,
  kValue,
  kDeleted,
};

// 单个只增不删的内存有序表 读写同步由调用方负责
class MemTable {
 public:
  void add(SequenceNumber sequence, ValueType type, Slice key, Slice value);

  LookupResult get(Slice key, SequenceNumber sequence,
                   std::string* value) const;

 private:
  using Table = std::map<std::string, std::string, InternalKeyLess>;

  Table table_;
};

}
