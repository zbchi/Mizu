#include "db/merging_iterator.h"

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/internal_key.h"
#include "db/memtable.h"
#include "table/sstable_builder.h"
#include "table/sstable_reader.h"
#include "test.h"

namespace lsmtree {
namespace {

class TempDirectory {
 public:
  TempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-merge-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~TempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  const std::filesystem::path& path() const noexcept { return path_; }

 private:
  std::filesystem::path path_;
};

std::unique_ptr<SSTableReader> buildAndOpenTable(
    const std::filesystem::path& path,
    const std::vector<std::pair<std::string, std::string>>& entries) {
  std::unique_ptr<SSTableBuilder> builder;
  ASSERT_OK(SSTableBuilder::open(path, SSTableBuilderOptions{1, {}}, builder));
  SequenceNumber sequence = 20;
  for (const auto& entry : entries) {
    ASSERT_OK(builder->add(
        encodeInternalKey(entry.first, sequence--, ValueType::kValue),
        entry.second));
  }
  SSTableMeta meta;
  ASSERT_OK(builder->finish(meta));

  std::unique_ptr<SSTableReader> reader;
  ASSERT_OK(SSTableReader::open(path, reader));
  return reader;
}

std::vector<std::string> collectKeys(InternalIterator& iterator) {
  std::vector<std::string> keys;
  iterator.seekToFirst();
  while (iterator.valid()) {
    keys.emplace_back(iterator.internalKey());
    iterator.next();
  }
  ASSERT_OK(iterator.status());
  return keys;
}

std::unique_ptr<MergingIterator> merge(
    std::vector<std::unique_ptr<InternalIterator>> children) {
  return std::make_unique<MergingIterator>(std::move(children));
}

}

TEST(memtableIteratorSeeksToFirstInternalKeyAtOrAfterTarget) {
  MemTable table;
  table.add(7, ValueType::kValue, "alpha", "a");
  table.add(5, ValueType::kValue, "beta", "b");
  table.add(3, ValueType::kValue, "charlie", "c");

  auto iterator = table.newIterator();
  iterator.seek(encodeInternalKey("beta", kMaxSequenceNumber,
                                  ValueType::kValue));
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.internalKey(),
            encodeInternalKey("beta", 5, ValueType::kValue));

  iterator.seek(encodeInternalKey("delta", kMaxSequenceNumber,
                                  ValueType::kValue));
  ASSERT_TRUE(!iterator.valid());
  ASSERT_OK(iterator.status());
}

TEST(sstableIteratorSeeksAcrossDataBlocks) {
  TempDirectory directory;
  auto reader = buildAndOpenTable(
      directory.path() / "table.sst", {{"alpha", "a"}, {"beta", "b"},
                                        {"charlie", "c"}});
  auto iterator = reader->newIterator({});
  iterator->seek(encodeInternalKey("beta", kMaxSequenceNumber,
                                   ValueType::kValue));
  ASSERT_TRUE(iterator->valid());
  ASSERT_EQ(iterator->internalKey(),
            encodeInternalKey("beta", 19, ValueType::kValue));
  ASSERT_EQ(iterator->value(), "b");
  ASSERT_OK(iterator->status());
}

TEST(mergingIteratorOrdersAllInputsByInternalKey) {
  MemTable first;
  first.add(9, ValueType::kValue, "alpha", "new");
  first.add(3, ValueType::kValue, "beta", "old");

  MemTable second;
  second.add(8, ValueType::kDeletion, "alpha", {});
  second.add(2, ValueType::kValue, "gamma", "g");

  std::vector<std::unique_ptr<InternalIterator>> children;
  children.push_back(std::make_unique<MemTable::Iterator>(first.newIterator()));
  children.push_back(
      std::make_unique<MemTable::Iterator>(second.newIterator()));
  auto iterator = merge(std::move(children));

  const std::vector<std::string> expected_keys = {
      encodeInternalKey("alpha", 9, ValueType::kValue),
      encodeInternalKey("alpha", 8, ValueType::kDeletion),
      encodeInternalKey("beta", 3, ValueType::kValue),
      encodeInternalKey("gamma", 2, ValueType::kValue),
  };
  ASSERT_EQ(collectKeys(*iterator), expected_keys);
}

TEST(mergingIteratorMergesMemtableAndSstableAndSupportsSeek) {
  TempDirectory directory;
  auto reader = buildAndOpenTable(directory.path() / "table.sst",
                                  {{"alpha", "a"}, {"delta", "d"}});

  MemTable table;
  table.add(30, ValueType::kValue, "beta", "b");
  table.add(29, ValueType::kValue, "epsilon", "e");

  std::vector<std::unique_ptr<InternalIterator>> children;
  children.push_back(std::make_unique<MemTable::Iterator>(table.newIterator()));
  children.push_back(reader->newIterator({}));
  auto iterator = merge(std::move(children));

  iterator->seek(encodeInternalKey("delta", kMaxSequenceNumber,
                                   ValueType::kValue));
  ASSERT_TRUE(iterator->valid());
  ASSERT_EQ(iterator->internalKey(),
            encodeInternalKey("delta", 19, ValueType::kValue));
  iterator->next();
  ASSERT_TRUE(iterator->valid());
  ASSERT_EQ(iterator->internalKey(),
            encodeInternalKey("epsilon", 29, ValueType::kValue));
  iterator->next();
  ASSERT_TRUE(!iterator->valid());
  ASSERT_OK(iterator->status());
}

class ErrorIterator final : public InternalIterator {
 public:
  bool valid() const noexcept override { return false; }
  void seekToFirst() override {}
  void seek(Slice) override {}
  void next() override {}
  Slice internalKey() const override { return {}; }
  Slice value() const override { return {}; }
  const Status& status() const noexcept override { return status_; }

 private:
  Status status_ = Status::corruption("synthetic iterator failure");
};

TEST(mergingIteratorPropagatesChildError) {
  std::vector<std::unique_ptr<InternalIterator>> children;
  children.push_back(std::make_unique<ErrorIterator>());
  auto iterator = merge(std::move(children));

  iterator->seekToFirst();
  ASSERT_TRUE(!iterator->status().ok());
  ASSERT_EQ(iterator->status().code(), StatusCode::kCorruption);
  ASSERT_TRUE(!iterator->valid());
}

}
