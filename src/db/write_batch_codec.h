#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "lsmtree/db.h"
#include "wal/wal_format.h"

namespace lsmtree {

using SequenceNumber = std::uint64_t;

// sequence 只使用低 56 位
constexpr SequenceNumber kMaxSequenceNumber = (SequenceNumber{1} << 56U) - 1U;
// WriteBatch payload 必须能完整放入一条 WAL record
constexpr std::size_t kMaxWriteBatchPayloadSize = kMaxWalRecordPayloadSize;

// payload 依次保存首个 sequence 操作数和各条操作
class WriteBatchCodec {
 public:
  // 成功时替换 output 失败时保持原值
  static Status encode(const WriteBatch& batch, SequenceNumber first_sequence,
                       std::string& output);

  // 只接受一个完整 payload 失败时保持两个输出不变
  // batch 之间的 sequence 连续性由 WAL 恢复流程校验
  static Status decode(Slice input, SequenceNumber& first_sequence,
                       WriteBatch& batch);

 private:
  WriteBatchCodec() = delete;
};

}
