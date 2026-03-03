#include "db/memtable.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>

#include "util/coding.h"

namespace lsmtree {
namespace {

constexpr std::size_t kLengthSize = sizeof(std::uint32_t);

// 从内存记录头部取出用于排序的 InternalKey
Slice entryInternalKey(const char* entry) noexcept {
  assert(entry != nullptr);
  const std::uint32_t length = decodeFixed32(entry);
  return Slice(entry + kLengthSize, length);
}

// 跳过 InternalKey 后读取用户 value
Slice entryValue(const char* entry) noexcept {
  const Slice internal_key = entryInternalKey(entry);
  const char* length_ptr = internal_key.data() + internal_key.size();
  const std::uint32_t length = decodeFixed32(length_ptr);
  return Slice(length_ptr + kLengthSize, length);
}

}

// Arena 必须先于跳表构造并覆盖跳表的完整生命周期
MemTable::MemTable() : table_(EntryComparator{}, &arena_) {}

Slice MemTable::Iterator::internalKey() const {
  assert(valid());
  return entryInternalKey(iterator_.key());
}

Slice MemTable::Iterator::value() const {
  assert(valid());
  return entryValue(iterator_.key());
}

// 跳表只按 InternalKey 排序 value 不参与比较
int MemTable::EntryComparator::operator()(const char* lhs,
                                          const char* rhs) const noexcept {
  const Slice lhs_key = entryInternalKey(lhs);
  const Slice rhs_key = entryInternalKey(rhs);
  const InternalKeyLess less;
  if (less(lhs_key, rhs_key)) return -1;
  if (less(rhs_key, lhs_key)) return 1;
  return 0;
}

// 将一条版本记录编码进 Arena 再把稳定地址插入跳表
void MemTable::add(SequenceNumber sequence, ValueType type, Slice key,
                   Slice value) {
  const std::string internal_key = encodeInternalKey(key, sequence, type);
  assert(internal_key.size() <= std::numeric_limits<std::uint32_t>::max());
  assert(value.size() <= std::numeric_limits<std::uint32_t>::max());

  const std::size_t entry_size =
      kLengthSize + internal_key.size() + kLengthSize + value.size();
  char* entry = arena_.allocate(entry_size);
  char* cursor = entry;

  encodeFixed32(cursor, static_cast<std::uint32_t>(internal_key.size()));
  cursor += kLengthSize;
  std::memcpy(cursor, internal_key.data(), internal_key.size());
  cursor += internal_key.size();

  encodeFixed32(cursor, static_cast<std::uint32_t>(value.size()));
  cursor += kLengthSize;
  if (!value.empty()) std::memcpy(cursor, value.data(), value.size());
  assert(cursor + value.size() == entry + entry_size);

  table_.insert(entry);
  ++entry_count_;
}

// 定位不晚于目标序号的第一个版本
LookupResult MemTable::get(Slice key, SequenceNumber sequence,
                           std::string* value) const {
  assert(value != nullptr);

  const std::string internal_key =
      encodeInternalKey(key, sequence, ValueType::kValue);
  assert(internal_key.size() <= std::numeric_limits<std::uint32_t>::max());

  std::string seek_entry(kLengthSize, '\0');
  encodeFixed32(seek_entry.data(),
                static_cast<std::uint32_t>(internal_key.size()));
  seek_entry.append(internal_key);

  Table::Iterator iterator(&table_);
  iterator.seek(seek_entry.data());
  if (!iterator.valid()) return LookupResult::kAbsent;

  const char* entry = iterator.key();
  ParsedInternalKey parsed{};
  if (!parseInternalKey(entryInternalKey(entry), parsed) ||
      parsed.user_key != key) {
    return LookupResult::kAbsent;
  }

  if (parsed.type == ValueType::kDeletion) {
    return LookupResult::kDeleted;
  }

  const Slice stored_value = entryValue(entry);
  value->assign(stored_value.data(), stored_value.size());
  return LookupResult::kValue;
}

}
