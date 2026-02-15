#include "db/memtable.h"

#include <cassert>
#include <utility>

namespace lsmtree {

void MemTable::add(SequenceNumber sequence, ValueType type, Slice key,
                   Slice value) {
  std::string internal_key = encodeInternalKey(key, sequence, type);
  const bool inserted =
      table_.emplace(std::move(internal_key), std::string(value)).second;
  assert(inserted);
}

LookupResult MemTable::get(Slice key, SequenceNumber sequence,
                           std::string* value) const {
  assert(value != nullptr);

  // lower_bound 跳过更新的版本并定位到第一条可见记录
  const std::string seek_key =
      encodeInternalKey(key, sequence, ValueType::kValue);
  const auto it = table_.lower_bound(seek_key);
  if (it == table_.end()) return LookupResult::kAbsent;

  ParsedInternalKey parsed{};
  if (!parseInternalKey(it->first, parsed) || parsed.user_key != key) {
    return LookupResult::kAbsent;
  }

  if (parsed.type == ValueType::kDeletion) {
    return LookupResult::kDeleted;
  }

  *value = it->second;
  return LookupResult::kValue;
}

}
