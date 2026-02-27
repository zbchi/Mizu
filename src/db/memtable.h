#pragma once

#include <string>

#include "db/internal_key.h"
#include "db/lookup_result.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace lsmtree {

// 单个只增不删的内存有序表 读写同步由调用方负责
class MemTable {
 public:
  MemTable();

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  void add(SequenceNumber sequence, ValueType type, Slice key, Slice value);

  LookupResult get(Slice key, SequenceNumber sequence,
                   std::string* value) const;

 private:
  struct EntryComparator {
    int operator()(const char* lhs, const char* rhs) const noexcept;
  };

  using Table = SkipList<const char*, EntryComparator>;

  Arena arena_;
  Table table_;
};

}
