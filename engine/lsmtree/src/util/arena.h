#pragma once

#include <cstddef>
#include <memory>
#include <vector>

namespace lsmtree {

// Arena 是一个单线程使用的只增不删分配器，适合管理生命周期一致的对象
// 单次分配不会独立释放，Arena 析构时统一释放全部内存块
class Arena {
 public:
  static constexpr std::size_t kDefaultBlockSize = 4096;

  explicit Arena(std::size_t block_size = kDefaultBlockSize);
  ~Arena() = default;

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  // 分配 bytes 字节，不额外保证对象对齐
  // 适合存放编码后的 key 和 value，bytes 为 0 时返回 nullptr
  char* allocate(std::size_t bytes);

  // 分配 bytes 字节，并保证地址满足任意标准 C++ 对象的对齐要求
  // 适合使用 placement new 构造节点，bytes 为 0 时返回 nullptr
  char* allocateAligned(std::size_t bytes);

  // 返回已经向系统申请的总容量，包括当前内存块中尚未使用的空间
  std::size_t memoryUsage() const noexcept { return memory_usage_; }

 private:
  struct Block {
    std::unique_ptr<char[]> data;
  };

  void allocateBlock(std::size_t bytes);

  const std::size_t block_size_;
  std::vector<Block> blocks_;
  char* current_ = nullptr;
  std::size_t remaining_ = 0;
  std::size_t memory_usage_ = 0;
};

}
