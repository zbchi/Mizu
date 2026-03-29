#include "util/arena.h"

#include <cassert>
#include <cstdint>
#include <limits>

namespace lsmtree {
namespace {

constexpr std::size_t kObjectAlignment = alignof(std::max_align_t);

std::size_t alignmentSlop(const char* address) noexcept {
  const auto value = reinterpret_cast<std::uintptr_t>(address);
  const std::size_t remainder = value % kObjectAlignment;
  return remainder == 0 ? 0 : kObjectAlignment - remainder;
}

}

Arena::Arena(std::size_t block_size) : block_size_(block_size) {
  assert(block_size > 0);
}

void Arena::allocateBlock(std::size_t bytes) {
  assert(bytes > 0);
  assert(bytes <= std::numeric_limits<std::size_t>::max() - memory_usage_);

  auto block = std::make_unique<char[]>(bytes);
  current_ = block.get();
  remaining_ = bytes;
  blocks_.push_back(Block{std::move(block)});
  memory_usage_ += bytes;
}

char* Arena::allocate(std::size_t bytes) {
  if (bytes == 0) return nullptr;

  if (bytes > remaining_) {
    allocateBlock(bytes > block_size_ ? bytes : block_size_);
  }

  char* result = current_;
  current_ += bytes;
  remaining_ -= bytes;
  return result;
}

char* Arena::allocateAligned(std::size_t bytes) {
  if (bytes == 0) return nullptr;

  std::size_t slop = current_ == nullptr ? 0 : alignmentSlop(current_);
  // 当前块容不下对齐填充和对象时换一块重新计算
  if (slop > remaining_ || bytes > remaining_ - slop) {
    allocateBlock(bytes > block_size_ ? bytes : block_size_);
    slop = alignmentSlop(current_);
  }

  assert(slop <= remaining_ && bytes <= remaining_ - slop);
  char* result = current_ + slop;
  current_ += slop + bytes;
  remaining_ -= slop + bytes;
  return result;
}

}
