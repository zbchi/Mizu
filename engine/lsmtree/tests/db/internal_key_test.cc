#include "db/internal_key.h"

#include <string>
#include <vector>

#include "test.h"

namespace lsmtree {
namespace {

void assertRoundTrip(Slice user_key, SequenceNumber sequence, ValueType type) {
  const std::string encoded = encodeInternalKey(user_key, sequence, type);
  ParsedInternalKey parsed{};

  ASSERT_TRUE(parseInternalKey(encoded, parsed));
  ASSERT_EQ(parsed.user_key, user_key);
  ASSERT_EQ(parsed.sequence, sequence);
  ASSERT_EQ(parsed.type, type);
}

TEST(internalKeyRoundTripsEmptyBinaryAndBoundaryValues) {
  assertRoundTrip("", 0, ValueType::kValue);
  assertRoundTrip(std::string("a\0b", 3), kMaxSequenceNumber,
                  ValueType::kDeletion);
}

TEST(internalKeyUsesStableLittleEndianEncoding) {
  const std::string encoded =
      encodeInternalKey("a", 0x010203U, ValueType::kValue);
  const std::string expected_bytes = {
      static_cast<char>('a'),  static_cast<char>(0x01),
      static_cast<char>(0x03), static_cast<char>(0x02),
      static_cast<char>(0x01), static_cast<char>(0x00),
      static_cast<char>(0x00), static_cast<char>(0x00),
      static_cast<char>(0x00)};

  ASSERT_EQ(encoded, expected_bytes);
}

TEST(internalKeyParserRejectsMalformedInputWithoutChangingOutput) {
  ParsedInternalKey output{"preserved", 42, ValueType::kValue};

  ASSERT_TRUE(!parseInternalKey(std::string(7, 'x'), output));
  ASSERT_EQ(output.user_key, Slice("preserved"));
  ASSERT_EQ(output.sequence, 42U);
  ASSERT_EQ(output.type, ValueType::kValue);

  std::string invalid_type(8, '\0');
  invalid_type[0] = static_cast<char>(2);
  ASSERT_TRUE(!parseInternalKey(invalid_type, output));
  ASSERT_EQ(output.user_key, Slice("preserved"));
  ASSERT_EQ(output.sequence, 42U);
  ASSERT_EQ(output.type, ValueType::kValue);
}

TEST(internalKeyOrdersUserKeyAscendingAndTagDescending) {
  const std::vector<std::string> ordered = {
      encodeInternalKey("a", 9, ValueType::kValue),
      encodeInternalKey("a", 7, ValueType::kValue),
      encodeInternalKey("a", 7, ValueType::kDeletion),
      encodeInternalKey("a", 3, ValueType::kValue),
      encodeInternalKey("b", 100, ValueType::kValue),
  };
  const InternalKeyLess less;

  for (std::size_t index = 1; index < ordered.size(); ++index) {
    ASSERT_TRUE(less(ordered[index - 1], ordered[index]));
    ASSERT_TRUE(!less(ordered[index], ordered[index - 1]));
  }
  ASSERT_TRUE(!less(ordered.front(), ordered.front()));
}

}
}
