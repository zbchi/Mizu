#include "table/block_iterator.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "db/internal_key.h"
#include "table/block_builder.h"
#include "test.h"
#include "util/coding.h"

namespace lsmtree {
namespace {

using Entry = std::pair<std::string, std::string>;

std::string buildBlock(const std::vector<Entry>& entries,
                       std::size_t restart_interval = 2) {
  BlockBuilder builder(BlockBuilderOptions{restart_interval});
  for (const auto& entry : entries) builder.add(entry.first, entry.second);
  return std::string(builder.finish());
}

std::string internalKey(Slice user_key, SequenceNumber sequence = 1) {
  return encodeInternalKey(user_key, sequence, ValueType::kValue);
}

TEST(blockIteratorWalksPrefixCompressedEntries) {
  const std::vector<Entry> entries = {
      {internalKey("apple"), "one"},
      {internalKey("application"), "two"},
      {internalKey("banana"), std::string("x\0y", 3)},
      {internalKey("carrot"), "four"},
  };
  const std::string block = buildBlock(entries);
  BlockIterator iterator(block);

  iterator.seekToFirst();
  for (const auto& entry : entries) {
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.key(), Slice(entry.first));
    ASSERT_EQ(iterator.value(), Slice(entry.second));
    iterator.next();
  }
  ASSERT_TRUE(!iterator.valid());
  ASSERT_OK(iterator.status());
}

TEST(blockIteratorSeeksWithInternalKeyOrder) {
  const std::vector<Entry> entries = {
      {internalKey("a", 7), "a7"},
      {internalKey("a", 3), "a3"},
      {internalKey("b", 9), "b9"},
      {internalKey("d", 1), "d1"},
  };
  const std::string block = buildBlock(entries);
  BlockIterator iterator(block);

  iterator.seek(internalKey(""));
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.value(), Slice("a7"));

  iterator.seek(internalKey("a", 7));
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.value(), Slice("a7"));

  iterator.seek(internalKey("a", 5));
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.value(), Slice("a3"));

  iterator.seek(internalKey("b", 9));
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.value(), Slice("b9"));

  iterator.seek(internalKey("c"));
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.value(), Slice("d1"));

  iterator.seek(internalKey("z"));
  ASSERT_TRUE(!iterator.valid());
  ASSERT_OK(iterator.status());
}

TEST(blockIteratorHandlesEmptyBlock) {
  const std::string block = buildBlock({});
  BlockIterator iterator(block);

  iterator.seekToFirst();
  ASSERT_TRUE(!iterator.valid());
  ASSERT_OK(iterator.status());

  iterator.seek(internalKey("key"));
  ASSERT_TRUE(!iterator.valid());
  ASSERT_OK(iterator.status());
}

TEST(blockIteratorReportsMalformedRestartMetadata) {
  std::string block = buildBlock({{internalKey("key"), "value"}});
  const std::size_t restart_offset = block.size() - 2U * sizeof(std::uint32_t);
  encodeFixed32(block.data() + restart_offset, 1);

  BlockIterator iterator(block);
  ASSERT_TRUE(!iterator.valid());
  ASSERT_EQ(iterator.status().code(), StatusCode::kCorruption);
}

TEST(blockIteratorReportsInvalidSharedLength) {
  std::string block = buildBlock({{internalKey("key"), "value"}});
  encodeFixed32(block.data(), 1);

  BlockIterator iterator(block);
  ASSERT_OK(iterator.status());
  iterator.seekToFirst();
  ASSERT_TRUE(!iterator.valid());
  ASSERT_EQ(iterator.status().code(), StatusCode::kCorruption);
}

}
}
