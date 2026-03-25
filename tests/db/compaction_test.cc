#include "db/compaction.h"

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "db/filename.h"
#include "db/internal_key.h"
#include "db/version.h"
#include "table/sstable_builder.h"
#include "table/sstable_reader.h"
#include "test.h"

namespace lsmtree {
namespace {

struct Entry {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;
  Slice value;
};

class CompactionTempDirectory {
 public:
  CompactionTempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-compaction-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~CompactionTempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  const std::filesystem::path &path() const { return path_; }

 private:
  std::filesystem::path path_;
};

TableMeta buildTable(const CompactionTempDirectory &directory,
                     std::uint64_t number, const std::vector<Entry> &entries) {
  std::unique_ptr<SSTableBuilder> builder;
  ASSERT_OK(SSTableBuilder::open(sstableFileName(directory.path(), number), {},
                                 builder));
  for (const Entry &entry : entries) {
    ASSERT_OK(builder->add(
        encodeInternalKey(entry.user_key, entry.sequence, entry.type),
        entry.value));
  }

  SSTableMeta completed;
  ASSERT_OK(builder->finish(completed));
  return TableMeta{number, completed.file_size, completed.smallest_key,
                   completed.largest_key};
}

std::shared_ptr<const Version> openVersion(
    const CompactionTempDirectory &directory,
    const std::vector<std::vector<Entry>> &level0_entries,
    const std::vector<std::vector<Entry>> &level1_entries = {}) {
  ManifestState manifest;
  manifest.oldest_wal_number = 1;
  std::uint64_t number = 1;
  for (const auto &entries : level0_entries) {
    manifest.level0_tables.push_back(buildTable(directory, number++, entries));
  }
  for (const auto &entries : level1_entries) {
    manifest.level1_tables.push_back(buildTable(directory, number++, entries));
  }

  std::shared_ptr<const Version> version;
  ASSERT_OK(Version::open(directory.path(), manifest, version));
  return version;
}

struct OwnedEntry {
  std::string internal_key;
  std::string value;
};

std::vector<OwnedEntry> readAll(const SSTableReader &reader) {
  std::vector<OwnedEntry> entries;
  auto iterator = reader.newIterator({});
  iterator->seekToFirst();
  while (iterator->valid()) {
    entries.push_back(
        {std::string(iterator->internalKey()), std::string(iterator->value())});
    iterator->next();
  }
  ASSERT_OK(iterator->status());
  return entries;
}

}  // namespace

TEST(level0CompactionRequiresFourTablesAndSelectsOverlappingLevel1) {
  CompactionTempDirectory directory;
  auto three = openVersion(directory, {{{"bravo", 9, ValueType::kValue, "b"}},
                                       {{"delta", 8, ValueType::kValue, "d"}},
                                       {{"hotel", 7, ValueType::kValue, "h"}}});
  ASSERT_TRUE(
      !pickLevel0Compaction(std::move(three), 0).has_value());

  CompactionTempDirectory ready_directory;
  auto ready = openVersion(ready_directory,
                           {{{"bravo", 9, ValueType::kValue, "b"}},
                            {{"delta", 8, ValueType::kValue, "d"}},
                            {{"hotel", 7, ValueType::kValue, "h"}},
                            {{"foxtrot", 6, ValueType::kValue, "f"}}},
                           {{{"alpha", 5, ValueType::kValue, "a"}},
                            {{"charlie", 4, ValueType::kValue, "c"},
                             {"echo", 3, ValueType::kValue, "e"}},
                            {{"hotel", 2, ValueType::kValue, "old-h"},
                             {"india", 1, ValueType::kValue, "i"}},
                            {{"juliet", 1, ValueType::kValue, "j"}}});

  const auto plan = pickLevel0Compaction(ready, 0);
  ASSERT_TRUE(plan.has_value());
  ASSERT_TRUE(plan->input_version == ready);
  ASSERT_EQ(plan->level1_begin, 1U);
  ASSERT_EQ(plan->level1_end, 3U);
}

TEST(level0CompactionReclaimsVersionsWithoutSnapshots) {
  CompactionTempDirectory directory;
  auto version = openVersion(directory,
                             {{{"alpha", 9, ValueType::kValue, "new"},
                               {"delta", 8, ValueType::kValue, "d"}},
                              {{"alpha", 7, ValueType::kDeletion, ""}},
                              {{"bravo", 6, ValueType::kValue, "b"}},
                              {{"alpha", 5, ValueType::kValue, "old"}}},
                             {{{"alpha", 3, ValueType::kValue, "oldest"},
                               {"charlie", 2, ValueType::kValue, "c"}},
                              {{"zulu", 1, ValueType::kValue, "untouched"}}});
  const auto plan = pickLevel0Compaction(version, 9);
  ASSERT_TRUE(plan.has_value());
  ASSERT_EQ(plan->level1_begin, 0U);
  ASSERT_EQ(plan->level1_end, 1U);

  CompactionOutput output;
  ASSERT_OK(buildLevel1Table(*plan, 100, directory.path(), output));
  ASSERT_EQ(output.meta.number, 100U);
  ASSERT_TRUE(output.reader != nullptr);
  ASSERT_TRUE(std::filesystem::exists(
      sstableFileName(directory.path(), output.meta.number)));
  ASSERT_TRUE(!std::filesystem::exists(
      sstableTemporaryFileName(directory.path(), output.meta.number)));

  const std::vector<OwnedEntry> entries = readAll(*output.reader);
  const std::vector<OwnedEntry> wanted = {
      {encodeInternalKey("alpha", 9, ValueType::kValue), "new"},
      {encodeInternalKey("bravo", 6, ValueType::kValue), "b"},
      {encodeInternalKey("charlie", 2, ValueType::kValue), "c"},
      {encodeInternalKey("delta", 8, ValueType::kValue), "d"},
  };
  ASSERT_EQ(entries.size(), wanted.size());
  for (std::size_t index = 0; index < wanted.size(); ++index) {
    ASSERT_EQ(entries[index].internal_key, wanted[index].internal_key);
    ASSERT_EQ(entries[index].value, wanted[index].value);
  }

  const auto installed = plan->input_version->withLevel0Compaction(
      plan->level1_begin, plan->level1_end, output.meta, output.reader);
  ASSERT_TRUE(installed != plan->input_version);
  ASSERT_TRUE(installed->level0().empty());
  ASSERT_EQ(installed->level1().size(), 2U);
  ASSERT_EQ(installed->level1()[0].meta.number, output.meta.number);
  ASSERT_EQ(installed->level1()[1].meta.number,
            plan->input_version->level1()[1].meta.number);
  ASSERT_EQ(plan->input_version->level0().size(), 4U);
  ASSERT_EQ(plan->input_version->level1().size(), 2U);
}

TEST(level0CompactionKeepsVersionsNeededByOldestSnapshot) {
  CompactionTempDirectory directory;
  auto version = openVersion(directory,
                             {{{"alpha", 9, ValueType::kValue, "latest"}},
                              {{"alpha", 7, ValueType::kValue, "newer"}},
                              {{"alpha", 4, ValueType::kValue, "snapshot"}},
                              {{"bravo", 8, ValueType::kValue, "b"}}},
                             {{{"alpha", 3, ValueType::kValue, "obsolete"},
                               {"charlie", 2, ValueType::kValue, "c"}}});
  const auto plan = pickLevel0Compaction(version, 4);
  ASSERT_TRUE(plan.has_value());
  ASSERT_EQ(plan->oldest_snapshot, 4U);

  CompactionOutput output;
  ASSERT_OK(buildLevel1Table(*plan, 100, directory.path(), output));
  ASSERT_TRUE(output.reader != nullptr);

  const std::vector<OwnedEntry> entries = readAll(*output.reader);
  const std::vector<OwnedEntry> wanted = {
      {encodeInternalKey("alpha", 9, ValueType::kValue), "latest"},
      {encodeInternalKey("alpha", 7, ValueType::kValue), "newer"},
      {encodeInternalKey("alpha", 4, ValueType::kValue), "snapshot"},
      {encodeInternalKey("bravo", 8, ValueType::kValue), "b"},
      {encodeInternalKey("charlie", 2, ValueType::kValue), "c"},
  };
  ASSERT_EQ(entries.size(), wanted.size());
  for (std::size_t index = 0; index < wanted.size(); ++index) {
    ASSERT_EQ(entries[index].internal_key, wanted[index].internal_key);
    ASSERT_EQ(entries[index].value, wanted[index].value);
  }
}

TEST(level0CompactionDropsBaseLevelTombstoneAndHiddenValue) {
  CompactionTempDirectory directory;
  auto version = openVersion(directory,
                             {{{"key", 8, ValueType::kDeletion, ""}},
                              {{"key", 7, ValueType::kValue, "v7"}},
                              {{"key", 6, ValueType::kValue, "v6"}},
                              {{"key", 5, ValueType::kValue, "v5"}}},
                             {{{"key", 4, ValueType::kValue, "oldest"}}});
  const auto plan = pickLevel0Compaction(version, 8);
  ASSERT_TRUE(plan.has_value());
  ASSERT_EQ(plan->level1_begin, 0U);
  ASSERT_EQ(plan->level1_end, 1U);

  CompactionOutput output;
  ASSERT_OK(buildLevel1Table(*plan, 100, directory.path(), output));
  ASSERT_TRUE(output.reader == nullptr);
  ASSERT_TRUE(!std::filesystem::exists(
      sstableTemporaryFileName(directory.path(), 100)));
  ASSERT_TRUE(!std::filesystem::exists(sstableFileName(directory.path(), 100)));

  const auto installed = plan->input_version->withLevel0Compaction(
      plan->level1_begin, plan->level1_end, output.meta, output.reader);
  ASSERT_TRUE(installed->level0().empty());
  ASSERT_TRUE(installed->level1().empty());

  LookupResult result = LookupResult::kValue;
  std::string value = "unchanged";
  ASSERT_OK(installed->get({}, "key", 8, result, value));
  ASSERT_EQ(result, LookupResult::kAbsent);
  ASSERT_EQ(value, "unchanged");
}

TEST(level0CompactionRejectsDuplicateInternalKeysAndCleansOutput) {
  CompactionTempDirectory directory;
  auto version =
      openVersion(directory, {{{"duplicate", 4, ValueType::kValue, "first"}},
                              {{"duplicate", 4, ValueType::kValue, "second"}},
                              {{"middle", 3, ValueType::kValue, "m"}},
                              {{"zulu", 2, ValueType::kValue, "z"}}});
  const auto plan = pickLevel0Compaction(version, 0);
  ASSERT_TRUE(plan.has_value());

  CompactionOutput output;
  ASSERT_EQ(buildLevel1Table(*plan, 100, directory.path(), output).code(),
            StatusCode::kCorruption);
  ASSERT_TRUE(!std::filesystem::exists(
      sstableTemporaryFileName(directory.path(), 100)));
  ASSERT_TRUE(!std::filesystem::exists(sstableFileName(directory.path(), 100)));
}

}  // namespace lsmtree
