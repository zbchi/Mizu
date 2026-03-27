#include "table/bloom_filter.h"

#include <cstdint>
#include <string>
#include <vector>

#include "test.h"
#include "util/coding.h"

namespace lsmtree {
namespace {

std::string numberedKey(std::uint32_t number) {
  std::string key;
  putFixed32(key, number);
  return key;
}

std::string buildFilter(const std::vector<std::string>& keys) {
  BloomFilterBuilder builder;
  for (const std::string& key : keys) builder.add(key);
  return builder.finish();
}

}

TEST(emptyBloomFilterRejectsEveryKey) {
  const std::string encoded = buildFilter({});
  ASSERT_EQ(encoded.size(), 9U);
  ASSERT_TRUE(!bloomFilterMayContain("hello", encoded));
  ASSERT_TRUE(!bloomFilterMayContain("", encoded));
}

TEST(bloomFilterMatchesInsertedBinaryKeys) {
  const std::vector<std::string> keys = {"hello", "world",
                                         std::string("a\0b", 3), ""};
  const std::string encoded = buildFilter(keys);

  for (const std::string& key : keys) {
    ASSERT_TRUE(bloomFilterMayContain(key, encoded));
  }
  ASSERT_TRUE(!bloomFilterMayContain("definitely-missing", encoded));
}

TEST(bloomFilterHasNoFalseNegativesAcrossSizes) {
  for (const std::size_t count : {1U, 10U, 100U, 1000U, 10000U}) {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      keys.push_back(numberedKey(static_cast<std::uint32_t>(index)));
    }

    const std::string encoded = buildFilter(keys);
    ASSERT_TRUE(encoded.size() <= (count * 10U + 7U) / 8U + 9U);
    for (const std::string& key : keys) {
      ASSERT_TRUE(bloomFilterMayContain(key, encoded));
    }
  }
}

TEST(bloomFilterKeepsFalsePositiveRateBelowTwoPercent) {
  std::vector<std::string> keys;
  keys.reserve(1000);
  for (std::uint32_t index = 0; index < 1000; ++index) {
    keys.push_back(numberedKey(index));
  }
  const std::string encoded = buildFilter(keys);

  std::size_t false_positives = 0;
  for (std::uint32_t index = 1000000; index < 1010000; ++index) {
    if (bloomFilterMayContain(numberedKey(index), encoded)) {
      ++false_positives;
    }
  }
  ASSERT_TRUE(false_positives < 200U);
}

TEST(bloomFilterTreatsUnknownEncodingAsPossibleMatch) {
  ASSERT_TRUE(bloomFilterMayContain("key", {}));

  const std::string truncated(1, '\x06');
  ASSERT_TRUE(bloomFilterMayContain("key", truncated));

  std::string future_encoding(8, '\0');
  future_encoding.push_back(static_cast<char>(31));
  ASSERT_TRUE(bloomFilterMayContain("key", future_encoding));
}

}
