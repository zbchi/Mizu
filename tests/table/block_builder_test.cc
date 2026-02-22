#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "table/block_builder.h"
#include "test.h"
#include "util/coding.h"

namespace lsmtree {
namespace {

using Entry = std::pair<std::string, std::string>;

std::vector<Entry> decodeEntries(Slice block) {
  std::uint32_t restart_count = 0;
  ASSERT_TRUE(block.size() >= sizeof(std::uint32_t));

  Slice count_input = block.substr(block.size() - sizeof(std::uint32_t));
  ASSERT_TRUE(getFixed32(count_input, restart_count));
  const std::size_t restart_bytes =
      (static_cast<std::size_t>(restart_count) + 1U) * sizeof(std::uint32_t);
  ASSERT_TRUE(block.size() >= restart_bytes);

  Slice input = block.substr(0, block.size() - restart_bytes);
  std::vector<Entry> entries;
  std::string last_key;
  while (!input.empty()) {
    std::uint32_t shared = 0;
    std::uint32_t unshared = 0;
    std::uint32_t value_size = 0;
    ASSERT_TRUE(getFixed32(input, shared));
    ASSERT_TRUE(getFixed32(input, unshared));
    ASSERT_TRUE(getFixed32(input, value_size));
    ASSERT_TRUE(shared <= last_key.size());
    ASSERT_TRUE(input.size() >= static_cast<std::size_t>(unshared) +
                                     static_cast<std::size_t>(value_size));

    std::string key = last_key.substr(0, shared);
    key.append(input.data(), unshared);
    input = input.substr(unshared);
    std::string value(input.data(), value_size);
    input = input.substr(value_size);
    entries.emplace_back(key, value);
    last_key = std::move(key);
  }
  return entries;
}

std::vector<std::uint32_t> decodeRestarts(Slice block) {
  std::uint32_t count = 0;
  Slice count_input = block.substr(block.size() - sizeof(std::uint32_t));
  ASSERT_TRUE(getFixed32(count_input, count));

  const std::size_t offsets_start =
      block.size() - (static_cast<std::size_t>(count) + 1U) *
                         sizeof(std::uint32_t);
  Slice input = block.substr(offsets_start, count * sizeof(std::uint32_t));
  std::vector<std::uint32_t> offsets;
  for (std::uint32_t index = 0; index < count; ++index) {
    std::uint32_t offset = 0;
    ASSERT_TRUE(getFixed32(input, offset));
    offsets.push_back(offset);
  }
  return offsets;
}

}

TEST(blockBuilderEncodesFixed32Entries) {
  BlockBuilder builder;
  builder.add("key", "value");

  const Slice block = builder.finish();
  std::string expected_block;
  putFixed32(expected_block, 0);
  putFixed32(expected_block, 3);
  putFixed32(expected_block, 5);
  expected_block.append("key");
  expected_block.append("value");
  putFixed32(expected_block, 0);
  putFixed32(expected_block, 1);

  ASSERT_EQ(block, Slice(expected_block));
  ASSERT_EQ(builder.currentSizeEstimate(), expected_block.size());
}

TEST(blockBuilderCompressesPrefixesAndReconstructsEntries) {
  BlockBuilder builder;
  builder.add("apple", "1");
  builder.add("application", "2");
  builder.add("banana", "3");

  const std::vector<Entry> expected_entries = {
      {"apple", "1"}, {"application", "2"}, {"banana", "3"}};
  const Slice block = builder.finish();
  ASSERT_EQ(decodeEntries(block), expected_entries);
  const std::vector<std::uint32_t> expected_restarts = {0};
  ASSERT_EQ(decodeRestarts(block), expected_restarts);
}

TEST(blockBuilderStartsRestartAfterConfiguredInterval) {
  BlockBuilder builder(BlockBuilderOptions{2});
  builder.add("aa", "1");
  builder.add("ab", "2");
  builder.add("ac", "3");
  builder.add("ad", "4");
  builder.add("ae", "5");

  const Slice block = builder.finish();
  const std::vector<Entry> expected_entries = {
      {"aa", "1"}, {"ab", "2"}, {"ac", "3"}, {"ad", "4"}, {"ae", "5"}};
  ASSERT_EQ(decodeEntries(block), expected_entries);
  const std::vector<std::uint32_t> restarts = decodeRestarts(block);
  const std::vector<std::uint32_t> expected_restarts = {0, 29, 58};
  ASSERT_EQ(restarts, expected_restarts);
}

TEST(blockBuilderSupportsBinaryValuesAndReuse) {
  BlockBuilder builder;
  const std::string key("a\0b", 3);
  const std::string value("x\0y", 3);
  builder.add(key, value);
  const Slice first = builder.finish();
  const std::vector<Entry> first_entries = {{key, value}};
  ASSERT_EQ(decodeEntries(first), first_entries);

  builder.reset();
  ASSERT_TRUE(builder.empty());
  builder.add("next", "value");
  const std::vector<Entry> next_entries = {{"next", "value"}};
  ASSERT_EQ(decodeEntries(builder.finish()), next_entries);
}

TEST(emptyBlockHasRestartMetadata) {
  BlockBuilder builder;
  ASSERT_TRUE(builder.empty());
  const Slice block = builder.finish();
  ASSERT_EQ(block.size(), sizeof(std::uint32_t) * 2U);
  const std::vector<std::uint32_t> expected_restarts = {0};
  ASSERT_EQ(decodeRestarts(block), expected_restarts);
}

}
