#include "db/write_batch_codec.h"

#include <cstdint>
#include <string>

#include "test.h"
#include "util/coding.h"

namespace lsmtree {
namespace {

std::string encodeBatch(const WriteBatch& batch,
                        SequenceNumber first_sequence) {
  std::string encoded;
  ASSERT_OK(WriteBatchCodec::encode(batch, first_sequence, encoded));
  return encoded;
}

void assertDecodeCorruption(const std::string& payload) {
  // 除了错误码还要确认失败不会修改两个输出
  SequenceNumber sequence = 77;
  WriteBatch output;
  output.put("preserved", "value");
  const std::string original = encodeBatch(output, sequence);

  const Status status = WriteBatchCodec::decode(payload, sequence, output);
  ASSERT_EQ(status.code(), StatusCode::kCorruption);
  ASSERT_EQ(sequence, 77U);
  ASSERT_EQ(output.count(), 1U);
  ASSERT_EQ(encodeBatch(output, sequence), original);
}

std::string makeHeader(SequenceNumber first_sequence,
                       std::uint32_t operation_count) {
  std::string payload;
  putFixed64(payload, first_sequence);
  putFixed32(payload, operation_count);
  return payload;
}

TEST(writeBatchCodecUsesStableGoldenEncoding) {
  WriteBatch batch;
  batch.put("a", "x").erase("b");

  const std::string expected_bytes = {
      static_cast<char>(0x01), static_cast<char>(0x00), static_cast<char>(0x00),
      static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x00),
      static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x02),
      static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x00),
      static_cast<char>(0x01), static_cast<char>(0x01), static_cast<char>(0x00),
      static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x61),
      static_cast<char>(0x01), static_cast<char>(0x00), static_cast<char>(0x00),
      static_cast<char>(0x00), static_cast<char>(0x78), static_cast<char>(0x02),
      static_cast<char>(0x01), static_cast<char>(0x00), static_cast<char>(0x00),
      static_cast<char>(0x00), static_cast<char>(0x62)};

  ASSERT_EQ(encodeBatch(batch, 1), expected_bytes);
}

TEST(writeBatchCodecRoundTripsOperationsInOrder) {
  const std::string binary_key("a\0b", 3);
  const std::string binary_value("\0x\0", 3);
  WriteBatch original;
  original.put("", "")
      .put(binary_key, binary_value)
      .put("same", "old")
      .put("same", "new")
      .erase(binary_key);

  const std::string encoded = encodeBatch(original, 19);
  SequenceNumber sequence = 0;
  WriteBatch decoded;
  ASSERT_OK(WriteBatchCodec::decode(encoded, sequence, decoded));

  ASSERT_EQ(sequence, 19U);
  ASSERT_EQ(decoded.count(), original.count());
  ASSERT_EQ(encodeBatch(decoded, sequence), encoded);
}

TEST(writeBatchCodecAcceptsLargestSequenceForOneOperation) {
  WriteBatch original;
  original.put("key", "value");
  const std::string encoded = encodeBatch(original, kMaxSequenceNumber);

  SequenceNumber sequence = 0;
  WriteBatch decoded;
  ASSERT_OK(WriteBatchCodec::decode(encoded, sequence, decoded));
  ASSERT_EQ(sequence, kMaxSequenceNumber);
  ASSERT_EQ(decoded.count(), 1U);
}

TEST(writeBatchEncoderRejectsInvalidInputWithoutChangingOutput) {
  std::string output = "preserved";
  WriteBatch empty;
  ASSERT_EQ(WriteBatchCodec::encode(empty, 1, output).code(),
            StatusCode::kInvalidArgument);
  ASSERT_EQ(output, "preserved");

  WriteBatch two_operations;
  two_operations.put("a", "1").put("b", "2");
  ASSERT_EQ(WriteBatchCodec::encode(two_operations, kMaxSequenceNumber, output)
                .code(),
            StatusCode::kInvalidArgument);
  ASSERT_EQ(output, "preserved");

  ASSERT_EQ(WriteBatchCodec::encode(two_operations, 0, output).code(),
            StatusCode::kInvalidArgument);
  ASSERT_EQ(output, "preserved");
}

TEST(writeBatchEncoderRejectsPayloadLargerThanLimit) {
  WriteBatch batch;
  batch.put("key", std::string(kMaxWriteBatchPayloadSize, 'x'));
  std::string output = "preserved";

  ASSERT_EQ(WriteBatchCodec::encode(batch, 1, output).code(),
            StatusCode::kInvalidArgument);
  ASSERT_EQ(output, "preserved");
}

TEST(writeBatchDecoderRejectsInvalidHeaderAndSequence) {
  assertDecodeCorruption(std::string(11, '\0'));
  assertDecodeCorruption(makeHeader(1, 0));

  std::string zero_sequence = makeHeader(0, 1);
  zero_sequence.append("\x02\x00\x00\x00\x00", 5);
  assertDecodeCorruption(zero_sequence);

  std::string excessive_sequence = makeHeader(kMaxSequenceNumber + 1U, 1);
  excessive_sequence.append("\x02\x00\x00\x00\x00", 5);
  assertDecodeCorruption(excessive_sequence);

  std::string overflowing_sequence = makeHeader(kMaxSequenceNumber, 2);
  overflowing_sequence.append("\x02\x00\x00\x00\x00", 5);
  overflowing_sequence.append("\x02\x00\x00\x00\x00", 5);
  assertDecodeCorruption(overflowing_sequence);
}

TEST(writeBatchDecoderRejectsInvalidOperationEncoding) {
  std::string unknown_type = makeHeader(1, 1);
  unknown_type.append("\x03\x00\x00\x00\x00", 5);
  assertDecodeCorruption(unknown_type);

  std::string missing_type = makeHeader(1, 1);
  assertDecodeCorruption(missing_type);

  std::string short_key_length = makeHeader(1, 1);
  short_key_length.append("\x02\x01\x00\x00", 4);
  assertDecodeCorruption(short_key_length);

  std::string short_key = makeHeader(1, 1);
  short_key.append("\x02\x03\x00\x00\x00", 5);
  short_key.append("ab", 2);
  assertDecodeCorruption(short_key);

  std::string missing_value_length = makeHeader(1, 1);
  missing_value_length.append("\x01\x01\x00\x00\x00k", 6);
  assertDecodeCorruption(missing_value_length);

  std::string short_value = makeHeader(1, 1);
  short_value.append("\x01\x01\x00\x00\x00k", 6);
  short_value.append("\x03\x00\x00\x00xy", 6);
  assertDecodeCorruption(short_value);
}

TEST(writeBatchDecoderRejectsCountMismatchAndTrailingBytes) {
  WriteBatch one_operation;
  one_operation.put("key", "value");
  std::string too_many_operations = encodeBatch(one_operation, 1);
  too_many_operations[8] = static_cast<char>(2);
  assertDecodeCorruption(too_many_operations);

  std::string trailing_bytes = encodeBatch(one_operation, 1);
  trailing_bytes.push_back('x');
  assertDecodeCorruption(trailing_bytes);
}

TEST(writeBatchDecoderRejectsOversizedPayloadWithoutChangingOutputs) {
  assertDecodeCorruption(std::string(kMaxWriteBatchPayloadSize + 1U, '\0'));
}

}
}
