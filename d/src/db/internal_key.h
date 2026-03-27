#pragma once

#include <cstdint>
#include <string>

#include "lsmtree/db.h"

namespace lsmtree {

using SequenceNumber = std::uint64_t;

constexpr SequenceNumber kMaxSequenceNumber = (SequenceNumber{1} << 56U) - 1U;

enum class ValueType : std::uint8_t {
  kDeletion = 0,
  kValue = 1,
};

struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;
};

// sequence 必须在 56 位有效范围内
std::string encodeInternalKey(Slice user_key, SequenceNumber sequence,
                              ValueType type);

// 解析成功后 output.user_key 引用 encoded 的内存
bool parseInternalKey(Slice encoded, ParsedInternalKey& output) noexcept;

// 比较对象必须是合法的 internal key
struct InternalKeyLess {
  bool operator()(Slice lhs, Slice rhs) const noexcept;
};

}
