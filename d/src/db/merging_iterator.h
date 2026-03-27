#pragma once

#include <cstddef>
#include <memory>
#include <queue>
#include <vector>

#include "db/internal_iterator.h"

namespace lsmtree {

// 归并多个已经按 InternalKey 排序的输入流
// 不折叠版本或处理 tombstone
// 归并器接管输入迭代器的所有权，但不延长底层 MemTable 或
// SSTableReader 的生命周期
class MergingIterator final : public InternalIterator {
 public:
  explicit MergingIterator(
      std::vector<std::unique_ptr<InternalIterator>> children);

  MergingIterator(const MergingIterator&) = delete;
  MergingIterator& operator=(const MergingIterator&) = delete;

  bool valid() const noexcept override;
  void seekToFirst() override;
  void seek(Slice target) override;
  void next() override;

  Slice internalKey() const override;
  Slice value() const override;
  const Status& status() const noexcept override { return status_; }

 private:
  struct Greater {
    const std::vector<std::unique_ptr<InternalIterator>>* children;

    bool operator()(std::size_t lhs, std::size_t rhs) const noexcept;
  };

  using Heap = std::priority_queue<std::size_t,
                                   std::vector<std::size_t>, Greater>;

  void rebuildHeap();
  bool checkChildren();
  void clearHeap() noexcept;

  std::vector<std::unique_ptr<InternalIterator>> children_;
  Heap heap_;
  Status status_;
};

}
