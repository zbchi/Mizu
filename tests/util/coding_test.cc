#include <cstdint>
#include <limits>
#include <string>

#include "test.h"
#include "util/coding.h"

namespace lsmtree {
namespace {

TEST(fixed32UsesLittleEndianAndRoundTrips) {
  std::string encoded;
  putFixed32(encoded, 0x12345678U);

  const std::string expected_bytes = {static_cast<char>(0x78),
                                      static_cast<char>(0x56),
                                      static_cast<char>(0x34),
                                      static_cast<char>(0x12)};
  ASSERT_EQ(encoded, expected_bytes);

  Slice input(encoded);
  std::uint32_t value = 0;
  ASSERT_TRUE(getFixed32(input, value));
  ASSERT_EQ(value, 0x12345678U);
  ASSERT_TRUE(input.empty());
}

TEST(fixed64UsesLittleEndianAndRoundTrips) {
  std::string encoded;
  putFixed64(encoded, 0x0123456789abcdefULL);

  const std::string expected_bytes = {static_cast<char>(0xef),
                                      static_cast<char>(0xcd),
                                      static_cast<char>(0xab),
                                      static_cast<char>(0x89),
                                      static_cast<char>(0x67),
                                      static_cast<char>(0x45),
                                      static_cast<char>(0x23),
                                      static_cast<char>(0x01)};
  ASSERT_EQ(encoded, expected_bytes);

  Slice input(encoded);
  std::uint64_t value = 0;
  ASSERT_TRUE(getFixed64(input, value));
  ASSERT_EQ(value, 0x0123456789abcdefULL);
  ASSERT_TRUE(input.empty());
}

TEST(fixedIntegersRoundTripBoundaryValuesAndPreserveTail) {
  std::string encoded;
  putFixed32(encoded, 0U);
  putFixed32(encoded, std::numeric_limits<std::uint32_t>::max());
  putFixed64(encoded, 0U);
  putFixed64(encoded, std::numeric_limits<std::uint64_t>::max());
  encoded.append("tail");

  Slice input(encoded);
  std::uint32_t first32 = 1;
  std::uint32_t second32 = 0;
  std::uint64_t first64 = 1;
  std::uint64_t second64 = 0;

  ASSERT_TRUE(getFixed32(input, first32));
  ASSERT_TRUE(getFixed32(input, second32));
  ASSERT_TRUE(getFixed64(input, first64));
  ASSERT_TRUE(getFixed64(input, second64));
  ASSERT_EQ(first32, 0U);
  ASSERT_EQ(second32, std::numeric_limits<std::uint32_t>::max());
  ASSERT_EQ(first64, 0U);
  ASSERT_EQ(second64, std::numeric_limits<std::uint64_t>::max());
  ASSERT_EQ(input, Slice("tail"));
}

TEST(fixed32RejectsShortInputWithoutMutation) {
  const std::string bytes = {static_cast<char>(0x01), static_cast<char>(0x02),
                             static_cast<char>(0x03)};
  Slice input(bytes);
  const Slice original = input;
  std::uint32_t value = 0xdeadbeefU;

  ASSERT_TRUE(!getFixed32(input, value));
  ASSERT_EQ(input, original);
  ASSERT_EQ(value, 0xdeadbeefU);
}

TEST(fixed64RejectsShortInputWithoutMutation) {
  const std::string bytes(7, 'x');
  Slice input(bytes);
  const Slice original = input;
  std::uint64_t value = 0x0123456789abcdefULL;

  ASSERT_TRUE(!getFixed64(input, value));
  ASSERT_EQ(input, original);
  ASSERT_EQ(value, 0x0123456789abcdefULL);
}

}
}
