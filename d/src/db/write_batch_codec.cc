#include "db/write_batch_codec.h"

#include <limits>
#include <utility>

#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace lsmtree {
namespace {

constexpr std::uint8_t kPutRecord = 1;
constexpr std::uint8_t kDeleteRecord = 2;
constexpr std::size_t kBatchHeaderSize = 8U + 4U;
constexpr std::size_t kOperationHeaderSize = 1U + 4U;

// 确保 batch 中每条操作都能分配到合法且连续的 sequence
bool hasValidSequenceRange(SequenceNumber first_sequence,
                           std::size_t operation_count) {
  if (first_sequence == 0 || first_sequence > kMaxSequenceNumber ||
      operation_count == 0 ||
      operation_count > std::numeric_limits<std::uint32_t>::max()) {
    return false;
  }

  const auto sequence_increment =
      static_cast<SequenceNumber>(operation_count - 1U);
  return sequence_increment <= kMaxSequenceNumber - first_sequence;
}

// 累加编码长度时同时限制 payload 上限
bool addEncodedSize(std::size_t amount, std::size_t& total) {
  if (amount > kMaxWriteBatchPayloadSize - total) return false;
  total += amount;
  return true;
}

bool takeByte(Slice& input, std::uint8_t& value) {
  if (input.empty()) return false;
  value = static_cast<std::uint8_t>(static_cast<unsigned char>(input.front()));
  input.remove_prefix(1);
  return true;
}

bool takeBytes(Slice& input, std::uint32_t length, Slice& value) {
  if (input.size() < length) return false;
  value = input.substr(0, length);
  input.remove_prefix(length);
  return true;
}

}

Status WriteBatchCodec::encode(const WriteBatch& batch,
                               SequenceNumber first_sequence,
                               std::string& output) {
  const std::size_t operation_count = batch.rep_->operations.size();
  if (operation_count == 0) {
    return Status::invalidArgument("cannot encode an empty write batch");
  }
  if (!hasValidSequenceRange(first_sequence, operation_count)) {
    return Status::invalidArgument("invalid write batch sequence range");
  }

  // 编码前先校验所有字段 避免失败时留下部分结果
  std::size_t encoded_size = kBatchHeaderSize;
  for (const auto& operation : batch.rep_->operations) {
    if (operation.key.size() > std::numeric_limits<std::uint32_t>::max()) {
      return Status::invalidArgument("write batch key is too large");
    }
    if (!addEncodedSize(kOperationHeaderSize, encoded_size) ||
        !addEncodedSize(operation.key.size(), encoded_size)) {
      return Status::invalidArgument("write batch payload exceeds 64 MiB");
    }

    switch (operation.type) {
      case WriteBatch::Rep::OperationType::kPut:
        if (operation.value.size() >
            std::numeric_limits<std::uint32_t>::max()) {
          return Status::invalidArgument("write batch value is too large");
        }
        if (!addEncodedSize(sizeof(std::uint32_t), encoded_size) ||
            !addEncodedSize(operation.value.size(), encoded_size)) {
          return Status::invalidArgument("write batch payload exceeds 64 MiB");
        }
        break;
      case WriteBatch::Rep::OperationType::kDelete:
        break;
    }
  }

  std::string encoded;
  encoded.reserve(encoded_size);
  putFixed64(encoded, first_sequence);
  putFixed32(encoded, static_cast<std::uint32_t>(operation_count));

  // 每条记录先写类型和 key put 记录再追加 value
  for (const auto& operation : batch.rep_->operations) {
    switch (operation.type) {
      case WriteBatch::Rep::OperationType::kPut:
        encoded.push_back(static_cast<char>(kPutRecord));
        break;
      case WriteBatch::Rep::OperationType::kDelete:
        encoded.push_back(static_cast<char>(kDeleteRecord));
        break;
    }

    putFixed32(encoded, static_cast<std::uint32_t>(operation.key.size()));
    encoded.append(operation.key);
    if (operation.type == WriteBatch::Rep::OperationType::kPut) {
      putFixed32(encoded, static_cast<std::uint32_t>(operation.value.size()));
      encoded.append(operation.value);
    }
  }

  output = std::move(encoded);
  return Status::success();
}

Status WriteBatchCodec::decode(Slice input, SequenceNumber& first_sequence,
                               WriteBatch& batch) {
  if (input.size() < kBatchHeaderSize) {
    return Status::corruption("write batch payload is shorter than its header");
  }
  if (input.size() > kMaxWriteBatchPayloadSize) {
    return Status::corruption("write batch payload exceeds 64 MiB");
  }

  // 使用临时游标和 batch 保证损坏数据不会修改调用方输出
  Slice cursor = input;
  SequenceNumber decoded_sequence = 0;
  std::uint32_t operation_count = 0;
  if (!getFixed64(cursor, decoded_sequence) ||
      !getFixed32(cursor, operation_count)) {
    return Status::corruption("invalid write batch header");
  }
  if (!hasValidSequenceRange(decoded_sequence, operation_count)) {
    return Status::corruption("invalid write batch sequence range");
  }

  // 每条操作至少占用类型和 key 长度 防止伪造 count 触发过量分配
  if (operation_count > cursor.size() / kOperationHeaderSize) {
    return Status::corruption("write batch operation count exceeds payload");
  }

  WriteBatch decoded_batch;
  for (std::uint32_t index = 0; index < operation_count; ++index) {
    std::uint8_t operation_type = 0;
    if (!takeByte(cursor, operation_type)) {
      return Status::corruption("missing write batch operation type");
    }
    if (operation_type != kPutRecord && operation_type != kDeleteRecord) {
      return Status::corruption("unknown write batch operation type");
    }

    std::uint32_t key_length = 0;
    if (!getFixed32(cursor, key_length)) {
      return Status::corruption("truncated write batch key length");
    }
    Slice key;
    if (!takeBytes(cursor, key_length, key)) {
      return Status::corruption("truncated write batch key");
    }

    if (operation_type == kPutRecord) {
      std::uint32_t value_length = 0;
      if (!getFixed32(cursor, value_length)) {
        return Status::corruption("truncated write batch value length");
      }
      Slice value;
      if (!takeBytes(cursor, value_length, value)) {
        return Status::corruption("truncated write batch value");
      }
      decoded_batch.put(key, value);
    } else {
      decoded_batch.erase(key);
    }
  }

  if (!cursor.empty()) {
    return Status::corruption("write batch payload has trailing bytes");
  }

  // 完整消费 payload 后才提交解码结果
  first_sequence = decoded_sequence;
  batch = std::move(decoded_batch);
  return Status::success();
}

}
