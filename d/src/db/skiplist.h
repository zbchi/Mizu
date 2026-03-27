#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <new>
#include <utility>

#include "util/arena.h"

namespace lsmtree {

// 只增不删的有序索引，写操作依赖外部同步
// 原子链接支持一个写线程和多个读线程并发访问
// 所有节点在 Arena 销毁前始终有效
template <typename Key, typename Comparator>
class SkipList {
 private:
  struct Node {
    Node(const Key& value, std::atomic<Node*>* links)
        : key(value), next(links) {}

    const Key key;
    std::atomic<Node*>* const next;

    Node* nextAt(int level) const {
      assert(level >= 0);
      return next[level].load(std::memory_order_acquire);
    }

    void setNext(int level, Node* node) {
      assert(level >= 0);
      next[level].store(node, std::memory_order_release);
    }
  };

 public:
  static constexpr int kMaxHeight = 12;

  explicit SkipList(Comparator comparator, Arena* arena)
      : comparator_(std::move(comparator)), arena_(arena) {
    assert(arena_ != nullptr);
    head_ = newNode(Key{}, kMaxHeight);
    for (int level = 0; level < kMaxHeight; ++level) {
      head_->setNext(level, nullptr);
    }
  }

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // 要求跳表中不存在与 key 等价的键
  void insert(const Key& key) {
    Node* previous[kMaxHeight];
    Node* existing = findGreaterOrEqual(key, previous);
    assert(existing == nullptr || !equal(key, existing->key));

    const int height = randomHeight();
    const int current_max_height = max_height_.load(std::memory_order_relaxed);
    if (height > current_max_height) {
      for (int level = current_max_height; level < height; ++level) {
        previous[level] = head_;
      }
    }

    Node* node = newNode(key, height);
    // 将新节点接到每一层搜索路径记录的前驱之后
    for (int level = 0; level < height; ++level) {
      node->setNext(level, previous[level]->nextAt(level));
      previous[level]->setNext(level, node);
    }
    if (height > current_max_height) {
      max_height_.store(height, std::memory_order_release);
    }
  }

  bool contains(const Key& key) const {
    Node* node = findGreaterOrEqual(key, nullptr);
    return node != nullptr && equal(key, node->key);
  }

  class Iterator {
   public:
    explicit Iterator(const SkipList* list) : list_(list), node_(nullptr) {
      assert(list_ != nullptr);
    }

    bool valid() const noexcept { return node_ != nullptr; }

    const Key& key() const {
      assert(valid());
      return node_->key;
    }

    void next() {
      assert(valid());
      node_ = node_->nextAt(0);
    }

    void seek(const Key& target) {
      node_ = list_->findGreaterOrEqual(target, nullptr);
    }

    void seekToFirst() { node_ = list_->head_->nextAt(0); }

   private:
    const SkipList* list_;
    Node* node_;
  };

 private:
  Node* newNode(const Key& key, int height) {
    assert(height >= 1 && height <= kMaxHeight);

    // Node 和各层 next 指针连续存放在同一块 Arena 内存中
    constexpr std::size_t alignment = alignof(std::atomic<Node*>);
    const std::size_t links_offset =
        (sizeof(Node) + alignment - 1) / alignment * alignment;
    const std::size_t bytes =
        links_offset +
        sizeof(std::atomic<Node*>) * static_cast<std::size_t>(height);

    char* memory = arena_->allocateAligned(bytes);
    auto* links = reinterpret_cast<std::atomic<Node*>*>(memory + links_offset);
    Node* node = new (memory) Node(key, links);
    for (int level = 0; level < height; ++level) {
      ::new (static_cast<void*>(links + level)) std::atomic<Node*>(nullptr);
    }
    return node;
  }

  bool equal(const Key& lhs, const Key& rhs) const {
    return comparator_(lhs, rhs) == 0;
  }

  bool keyIsAfterNode(const Key& key, Node* node) const {
    return node != nullptr && comparator_(node->key, key) < 0;
  }

  Node* findGreaterOrEqual(const Key& target, Node** previous) const {
    Node* node = head_;
    int level = max_height_.load(std::memory_order_acquire) - 1;
    // 从最高层向右查找 到达边界后下降一层
    while (true) {
      Node* next = node->nextAt(level);
      if (keyIsAfterNode(target, next)) {
        node = next;
      } else {
        if (previous != nullptr) previous[level] = node;
        if (level == 0) return next;
        --level;
      }
    }
  }

  int randomHeight() {
    int height = 1;
    while (height < kMaxHeight && (nextRandom() & 3U) == 0U) ++height;
    return height;
  }

  std::uint32_t nextRandom() {
    // 使用确定性随机数保证结构测试可以复现
    random_state_ ^= random_state_ << 13U;
    random_state_ ^= random_state_ >> 17U;
    random_state_ ^= random_state_ << 5U;
    return random_state_;
  }

  Comparator comparator_;
  Arena* const arena_;
  Node* head_ = nullptr;
  std::atomic<int> max_height_{1};
  std::uint32_t random_state_ = 0x9e3779b9U;
};

}
