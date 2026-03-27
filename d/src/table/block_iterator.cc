#include "table/block_iterator.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "db/internal_key.h"
#include "util/coding.h"

namespace lsmtree {
namespace {

constexpr std::size_t kFixed32Size = sizeof(std::uint32_t);
constexpr std::size_t kEntryHeaderSize = 3U * kFixed32Size;

}

BlockIterator::BlockIterator(Slice block) : block_(block) {
  // block 末尾至少需要 4 字节保存 restart 数量
  if (block_.size() < kFixed32Size) {
    fail("block is shorter than restart count");
    return;
  }

  // 最后 4 字节记录 restart 数组中有多少个 offset
  restart_count_ = decodeFixed32(block_.data() + block_.size() - kFixed32Size);
  if (restart_count_ == 0) {
    fail("block has no restart points");
    return;
  }

  // restart 数组必须完整放在 block 内
  const std::size_t available_offsets =
      (block_.size() - kFixed32Size) / kFixed32Size;
  if (restart_count_ > available_offsets) {
    fail("restart array extends outside block");
    return;
  }

  const std::size_t restart_bytes =
      static_cast<std::size_t>(restart_count_) * kFixed32Size;
  // restart_offset_ 同时是 entry 区域的结束位置和 restart 数组的开始位置
  restart_offset_ = block_.size() - kFixed32Size - restart_bytes;

  // 第一条 entry 必须是第一个 restart point
  if (restartOffset(0) != 0) {
    fail("first restart point is not zero");
  }
}

void BlockIterator::seekToFirst() {
  if (!status_.ok()) return;

  invalidate();

  // entry 区域为空表示这是一个空 block
  if (restart_offset_ == 0) return;

  decodeEntry(0);
}

void BlockIterator::seek(Slice target) {
  if (!status_.ok()) return;

  // block 只接受合法的 InternalKey
  [[maybe_unused]] ParsedInternalKey parsed{};
  assert(parseInternalKey(target, parsed));
  invalidate();
  if (restart_offset_ == 0) return;

  // 二分查找第一个大于 target 的 restart key
  const InternalKeyLess less;
  std::uint32_t left = 0;
  std::uint32_t right = restart_count_;
  while (left < right) {
    const std::uint32_t middle = left + (right - left) / 2U;
    Slice restart_key;
    if (!readRestartKey(middle, restart_key)) return;

    if (!less(target, restart_key)) {
      // restart_key 小于等于 target 时继续搜索右半边
      left = middle + 1U;
    } else {
      // restart_key 大于 target 时保留左半边
      right = middle;
    }
  }

  // left 指向第一个大于 target 的 restart key
  // 从它前面的 restart point 开始才不会跳过 target
  const std::uint32_t restart_index = left == 0 ? 0 : left - 1U;
  if (!seekToRestartPoint(restart_index)) return;

  // 顺序前进直到当前 key 不小于 target
  while (valid_ && less(key_, target)) next();
}

void BlockIterator::next() {
  assert(valid_);

  // next_offset_ 到达 restart 数组时说明已经走完所有 entry
  if (next_offset_ == restart_offset_) {
    invalidate();
    return;
  }

  // 普通 entry 可以利用当前 key 还原共享前缀
  decodeEntry(next_offset_);
}

Slice BlockIterator::key() const {
  assert(valid_);
  return key_;
}

Slice BlockIterator::value() const {
  assert(valid_);
  return value_;
}

// 从 restart 数组中读取第 index 个 entry 在 block 内的起始位置
std::uint32_t BlockIterator::restartOffset(std::uint32_t index) const noexcept {
  assert(index < restart_count_);
  const char* offset = block_.data() + restart_offset_ +
                       static_cast<std::size_t>(index) * kFixed32Size;
  return decodeFixed32(offset);
}

// 读取 restart entry 的完整 key 并校验记录边界
bool BlockIterator::readRestartKey(std::uint32_t index, Slice& key) {
  // restart 数组中的数字指向 entry 区域内的一个位置
  const std::uint32_t offset = restartOffset(index);
  if (offset >= restart_offset_) {
    return fail("restart point is outside entry data");
  }

  // input 只覆盖 entry 区域不会读到后面的 restart 数组
  Slice input = block_.substr(offset, restart_offset_ - offset);
  if (input.size() < kEntryHeaderSize) return fail("invalid restart entry");

  // entry 头部依次记录共享长度 完整后缀长度和 value 长度
  const std::uint32_t shared = decodeFixed32(input.data());
  const std::uint32_t unshared = decodeFixed32(input.data() + kFixed32Size);
  const std::uint32_t value_size =
      decodeFixed32(input.data() + 2U * kFixed32Size);
  input.remove_prefix(kEntryHeaderSize);

  // restart entry 不依赖前一条记录所以 shared 必须为 0
  if (shared != 0 || unshared > input.size()) {
    return fail("invalid restart entry");
  }

  // key 直接引用 block 内存不产生字符串复制
  key = input.substr(0, unshared);
  input.remove_prefix(unshared);

  // 二分查找不读取 value 但仍要确认当前 entry 没有被截断
  if (value_size > input.size()) return fail("truncated restart entry");

  // block 中保存的 key 必须是合法的 InternalKey
  ParsedInternalKey parsed{};
  if (!parseInternalKey(key, parsed)) {
    return fail("invalid internal key in block");
  }
  return true;
}

// 从 restart point 重新开始解码完整 key
bool BlockIterator::seekToRestartPoint(std::uint32_t index) {
  // 清空旧 key 也能保证 restart entry 的 shared 只能为 0
  key_.clear();
  return decodeEntry(restartOffset(index));
}

// 从 offset 解码一条记录并重建完整 key
bool BlockIterator::decodeEntry(std::size_t offset) {
  // entry 只能位于 restart 数组之前
  if (offset >= restart_offset_) return fail("entry offset is outside block");

  Slice input = block_.substr(offset, restart_offset_ - offset);
  if (input.size() < kEntryHeaderSize) return fail("truncated block entry");

  // 先取出 key 前缀长度 key 后缀长度和 value 长度
  const std::uint32_t shared = decodeFixed32(input.data());
  const std::uint32_t unshared = decodeFixed32(input.data() + kFixed32Size);
  const std::uint32_t value_size =
      decodeFixed32(input.data() + 2U * kFixed32Size);
  input.remove_prefix(kEntryHeaderSize);

  if (shared > key_.size() || unshared > input.size()) {
    return fail("invalid key lengths in block entry");
  }

  // 当前完整 key 等于上一条 key 的共享前缀加上当前记录的后缀
  std::string decoded_key = key_.substr(0, shared);
  decoded_key.append(input.data(), unshared);
  input.remove_prefix(unshared);
  if (value_size > input.size()) return fail("truncated block value");

  ParsedInternalKey parsed{};
  if (!parseInternalKey(decoded_key, parsed)) {
    return fail("invalid internal key in block");
  }

  // value 直接引用 block 内存 next_offset_ 指向下一条 entry
  value_ = input.substr(0, value_size);
  input.remove_prefix(value_size);
  next_offset_ = static_cast<std::size_t>(input.data() - block_.data());
  key_ = std::move(decoded_key);
  valid_ = true;
  return true;
}

bool BlockIterator::fail(const char* message) {
  status_ = Status::corruption(message);
  invalidate();
  return false;
}

void BlockIterator::invalidate() noexcept {
  valid_ = false;
  key_.clear();
  value_ = {};
}

}
