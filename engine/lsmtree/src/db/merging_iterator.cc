#include "db/merging_iterator.h"

#include <cassert>
#include <utility>

#include "db/internal_key.h"

namespace lsmtree {

MergingIterator::MergingIterator(
    std::vector<std::unique_ptr<InternalIterator>> children)
    : children_(std::move(children)), heap_(Greater{&children_}) {}

bool MergingIterator::valid() const noexcept {
  return status_.ok() && !heap_.empty() && children_[heap_.top()]->valid();
}

void MergingIterator::seekToFirst() {
  if (!status_.ok()) return;
  // 所有输入先定位到首条记录，再把有效输入放入最小堆。
  clearHeap();
  for (const auto& child : children_) child->seekToFirst();
  if (!checkChildren()) return;
  rebuildHeap();
}

void MergingIterator::seek(Slice target) {
  if (!status_.ok()) return;
  // 每个输入独立定位到 target，重新建堆后堆顶就是全局最小记录
  clearHeap();
  for (const auto& child : children_) child->seek(target);
  if (!checkChildren()) return;
  rebuildHeap();
}

void MergingIterator::next() {
  assert(valid());
  if (!status_.ok()) return;

  const std::size_t index = heap_.top();
  heap_.pop();
  // 堆顶输入前进一步；它仍有效时重新入堆
  // 其他输入保持原位置
  children_[index]->next();
  if (!checkChildren()) return;
  if (children_[index]->valid()) heap_.push(index);
}

Slice MergingIterator::internalKey() const {
  assert(valid());
  return children_[heap_.top()]->internalKey();
}

Slice MergingIterator::value() const {
  assert(valid());
  return children_[heap_.top()]->value();
}

bool MergingIterator::Greater::operator()(std::size_t lhs,
                                          std::size_t rhs) const noexcept {
  // 堆中只保存输入下标，比较时读取各输入当前的 InternalKey
  // priority_queue 默认把较大元素放在顶部
  // 反向比较得到最小堆
  const InternalKeyLess less;
  const Slice left = (*children)[lhs]->internalKey();
  const Slice right = (*children)[rhs]->internalKey();
  if (less(right, left)) return true;
  if (less(left, right)) return false;
  // 相同 InternalKey 用输入下标打破平局，保证归并顺序稳定
  return lhs > rhs;
}

void MergingIterator::rebuildHeap() {
  // 每个输入在堆中最多出现一次，额外空间与输入数量成正比
  for (std::size_t index = 0; index < children_.size(); ++index) {
    if (children_[index]->valid()) heap_.push(index);
  }
}

bool MergingIterator::checkChildren() {
  // 子迭代器的错误一旦出现就固定归并器状态
  // 避免继续读取损坏输入
  for (const auto& child : children_) {
    if (!child->status().ok()) {
      status_ = child->status();
      clearHeap();
      return false;
    }
  }
  return true;
}

void MergingIterator::clearHeap() noexcept {
  heap_ = Heap(Greater{&children_});
}

}
