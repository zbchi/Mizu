#pragma once

#include <cstddef>
#include <string>

#include "db/internal_key.h"
#include "db/internal_iterator.h"
#include "db/lookup_result.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace lsmtree {

// 单个只增不删的内存有序表 读写同步由调用方负责
class MemTable {
 private:
  struct EntryComparator {
    int operator()(const char* lhs, const char* rhs) const noexcept;
  };

  using Table = SkipList<const char*, EntryComparator>;

 public:
  MemTable();

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  void add(SequenceNumber sequence, ValueType type, Slice key, Slice value);

  class Iterator final : public InternalIterator {
   public:
    bool valid() const noexcept override { return iterator_.valid(); }

    void seekToFirst() override { iterator_.seekToFirst(); }
    void seek(Slice target) override;
    void next() override { iterator_.next(); }

    // 返回的 Slice 引用 MemTable 的 Arena
    Slice internalKey() const override;
    Slice value() const override;
    const Status& status() const noexcept override { return status_; }

   private:
    friend class MemTable;

    explicit Iterator(const Table* table) : iterator_(table) {}

    Table::Iterator iterator_;
    Status status_;
  };

  // 迭代期间 MemTable 必须保持存活且不可写
  Iterator newIterator() const { return Iterator(&table_); }

  // 返回 Arena 已向系统申请的总容量
  std::size_t memoryUsage() const noexcept { return arena_.memoryUsage(); }

  bool empty() const noexcept { return entry_count_ == 0; }

  LookupResult get(Slice key, SequenceNumber sequence,
                   std::string* value) const;

 private:
  Arena arena_;
  Table table_;
  std::size_t entry_count_ = 0;
};

}
