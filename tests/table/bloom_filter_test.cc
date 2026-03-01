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

std::vector<Slice> slicesOf(const std::vector<std::string>& keys) {
  std::vector<Slice> slices;
  slices.reserve(keys.size());
  for (const std::string& key : keys) slices.emplace_back(key);
  return slices;
}

}

TEST(emptyBloomFilterRejectsEveryKey) {
  const std::string encoded = BloomFilter::create({});
  ASSERT_EQ(encoded.size(), 9U);
  ASSERT_TRUE(!BloomFilter::mayContain("hello", encoded));
  ASSERT_TRUE(!BloomFilter::mayContain("", encoded));
}

TEST(bloomFilterMatchesInsertedBinaryKeys) {
  const std::vector<std::string> keys = {"hello", "world",
                                         std::string("a\0b", 3), ""};
  const std::string encoded = BloomFilter::create(slicesOf(keys));

  for (const std::string& key : keys) {
    ASSERT_TRUE(BloomFilter::mayContain(key, encoded));
  }
  ASSERT_TRUE(!BloomFilter::mayContain("definitely-missing", encoded));
}

TEST(bloomFilterHasNoFalseNegativesAcrossSizes) {
  for (const std::size_t count : {1U, 10U, 100U, 1000U, 10000U}) {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      keys.push_back(numberedKey(static_cast<std::uint32_t>(index)));
    }

    const std::string encoded = BloomFilter::create(slicesOf(keys));
    ASSERT_TRUE(encoded.size() <= (count * 10U + 7U) / 8U + 9U);
    for (const std::string& key : keys) {
      ASSERT_TRUE(BloomFilter::mayContain(key, encoded));
    }
  }
}

TEST(bloomFilterKeepsFalsePositiveRateBelowTwoPercent) {
  std::vector<std::string> keys;
  keys.reserve(1000);
  for (std::uint32_t index = 0; index < 1000; ++index) {
    keys.push_back(numberedKey(index));
  }
  const std::string encoded = BloomFilter::create(slicesOf(keys));

  std::size_t false_positives = 0;
  for (std::uint32_t index = 1000000; index < 1010000; ++index) {
    if (BloomFilter::mayContain(numberedKey(index), encoded)) {
      ++false_positives;
    }
  }
  ASSERT_TRUE(false_positives < 200U);
}

TEST(bloomFilterTreatsUnknownEncodingAsPossibleMatch) {
  ASSERT_TRUE(!BloomFilter::mayContain("key", {}));

  const std::string truncated(1, '\x06');
  ASSERT_TRUE(BloomFilter::mayContain("key", truncated));

  std::string future_encoding(8, '\0');
  future_encoding.push_back(static_cast<char>(31));
  ASSERT_TRUE(BloomFilter::mayContain("key", future_encoding));
}

}
