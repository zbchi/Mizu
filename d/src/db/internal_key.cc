#include "db/internal_key.h"

#include <cassert>

#include "util/coding.h"

namespace lsmtree {
namespace {

constexpr std::size_t kTagSize = sizeof(std::uint64_t);

bool isValidValueType(ValueType type) {
  return type == ValueType::kDeletion || type == ValueType::kValue;
}

std::uint64_t decodeTag(Slice internal_key) {
  assert(internal_key.size() >= kTagSize);
  Slice encoded_tag = internal_key.substr(internal_key.size() - kTagSize);
  std::uint64_t tag = 0;
  [[maybe_unused]] const bool decoded = getFixed64(encoded_tag, tag);
  assert(decoded);
  return tag;
}

}

std::string encodeInternalKey(Slice user_key, SequenceNumber sequence,
                              ValueType type) {
  assert(sequence <= kMaxSequenceNumber);
  assert(isValidValueType(type));

  std::string encoded(user_key);
  // tag 高 56 位保存 sequence 低 8 位保存 value type
  putFixed64(encoded, (sequence << 8U) | static_cast<std::uint8_t>(type));
  return encoded;
}

bool parseInternalKey(Slice encoded, ParsedInternalKey& output) noexcept {
  if (encoded.size() < kTagSize) return false;

  const std::uint64_t tag = decodeTag(encoded);
  const auto type = static_cast<ValueType>(tag & 0xffU);
  if (!isValidValueType(type)) return false;

  output = ParsedInternalKey{encoded.substr(0, encoded.size() - kTagSize),
                             tag >> 8U, type};
  return true;
}

bool InternalKeyLess::operator()(Slice lhs, Slice rhs) const noexcept {
  assert(lhs.size() >= kTagSize);
  assert(rhs.size() >= kTagSize);

  const Slice lhs_user_key = lhs.substr(0, lhs.size() - kTagSize);
  const Slice rhs_user_key = rhs.substr(0, rhs.size() - kTagSize);
  const int user_key_order = lhs_user_key.compare(rhs_user_key);
  if (user_key_order != 0) return user_key_order < 0;

  // 同一 user key 按 tag 逆序排列 让更新的版本优先
  return decodeTag(lhs) > decodeTag(rhs);
}

}
