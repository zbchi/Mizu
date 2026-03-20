#pragma once

#include <memory>
#include <string>

#include "db/internal_iterator.h"
#include "db/internal_key.h"
#include "lsmtree/db.h"

namespace lsmtree {

// 将 InternalKey 有序流转换为指定 sequence 可见的 user key/value 流
class DBIterator final : public Iterator {
 public:
  DBIterator(std::unique_ptr<InternalIterator> input,
             SequenceNumber visible_sequence);

  DBIterator(const DBIterator&) = delete;
  DBIterator& operator=(const DBIterator&) = delete;

  bool valid() const noexcept override;
  void seekToFirst() override;
  void seek(Slice target) override;
  void next() override;

  Slice key() const override;
  Slice value() const override;
  Status status() const override;

 private:
  // skipping 为 true 时忽略 skip_key_ 的所有剩余版本
  void findNextUserEntry(bool skipping);
  bool parseCurrent(ParsedInternalKey& parsed);

  std::unique_ptr<InternalIterator> input_;
  const SequenceNumber visible_sequence_;
  std::string skip_key_;
  Status status_;
  bool valid_ = false;
};

}
