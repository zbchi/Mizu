#include "db/db_iterator.h"

#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/internal_iterator.h"
#include "db/memtable.h"
#include "db/merging_iterator.h"
#include "test.h"

namespace lsmtree {
namespace {

std::unique_ptr<DBIterator> makeIterator(
    SequenceNumber visible_sequence,
    std::initializer_list<const MemTable*> tables) {
  std::vector<std::unique_ptr<InternalIterator>> children;
  children.reserve(tables.size());
  for (const MemTable* table : tables) {
    children.push_back(
        std::make_unique<MemTable::Iterator>(table->newIterator()));
  }
  return std::make_unique<DBIterator>(
      std::make_unique<MergingIterator>(std::move(children)), visible_sequence);
}

std::vector<std::pair<std::string, std::string>> collect(Iterator& iterator) {
  std::vector<std::pair<std::string, std::string>> entries;
  iterator.seekToFirst();
  while (iterator.valid()) {
    entries.emplace_back(iterator.key(), iterator.value());
    iterator.next();
  }
  ASSERT_OK(iterator.status());
  return entries;
}

class InvalidKeyIterator final : public InternalIterator {
 public:
  bool valid() const noexcept override { return valid_; }
  void seekToFirst() override { valid_ = true; }
  void seek(Slice) override { valid_ = true; }
  void next() override { valid_ = false; }
  Slice internalKey() const override { return "bad"; }
  Slice value() const override { return "value"; }
  const Status& status() const noexcept override { return status_; }

 private:
  bool valid_ = false;
  Status status_;
};

class ErrorAfterFirstIterator final : public InternalIterator {
 public:
  bool valid() const noexcept override { return valid_; }
  void seekToFirst() override { valid_ = true; }
  void seek(Slice) override { valid_ = true; }
  void next() override {
    valid_ = false;
    status_ = Status::corruption("synthetic child failure");
  }
  Slice internalKey() const override { return key_; }
  Slice value() const override { return "value"; }
  const Status& status() const noexcept override { return status_; }

 private:
  bool valid_ = false;
  Status status_;
  const std::string key_ = encodeInternalKey("key", 1, ValueType::kValue);
};

}

TEST(dbIteratorReturnsNewestVisibleValueForEachUserKey) {
  MemTable newer;
  newer.add(9, ValueType::kValue, "alpha", "new");
  newer.add(8, ValueType::kDeletion, "beta", {});

  MemTable older;
  older.add(7, ValueType::kDeletion, "alpha", {});
  older.add(5, ValueType::kValue, "alpha", "old");
  older.add(4, ValueType::kValue, "beta", "hidden");
  older.add(3, ValueType::kValue, "charlie", "visible");

  auto iterator = makeIterator(kMaxSequenceNumber, {&newer, &older});
  const std::vector<std::pair<std::string, std::string>> expected_entries = {
      {"alpha", "new"}, {"charlie", "visible"}};
  ASSERT_EQ(collect(*iterator), expected_entries);
}

TEST(dbIteratorHandlesEmptyInput) {
  auto iterator = makeIterator(kMaxSequenceNumber, {});
  iterator->seekToFirst();
  ASSERT_TRUE(!iterator->valid());
  ASSERT_OK(iterator->status());

  iterator->seek("key");
  ASSERT_TRUE(!iterator->valid());
  ASSERT_OK(iterator->status());
}

TEST(dbIteratorAppliesSnapshotBeforeOverwriteAndDeletion) {
  MemTable table;
  table.add(9, ValueType::kValue, "alpha", "new");
  table.add(7, ValueType::kDeletion, "alpha", {});
  table.add(4, ValueType::kValue, "alpha", "old");
  table.add(3, ValueType::kValue, "beta", "b");

  auto latest = makeIterator(9, {&table});
  ASSERT_EQ(collect(*latest), (std::vector<std::pair<std::string, std::string>>{
                                  {"alpha", "new"}, {"beta", "b"}}));

  auto deleted = makeIterator(8, {&table});
  ASSERT_EQ(collect(*deleted),
            (std::vector<std::pair<std::string, std::string>>{{"beta", "b"}}));

  auto old = makeIterator(6, {&table});
  ASSERT_EQ(collect(*old), (std::vector<std::pair<std::string, std::string>>{
                               {"alpha", "old"}, {"beta", "b"}}));
}

TEST(dbIteratorSeekUsesUserKeyAndSnapshotSequence) {
  MemTable table;
  table.add(10, ValueType::kValue, "alpha", "future");
  table.add(5, ValueType::kValue, "alpha", "visible");
  table.add(4, ValueType::kValue, "charlie", "c");
  table.add(3, ValueType::kValue, "echo", "e");

  auto iterator = makeIterator(5, {&table});

  iterator->seek("bravo");
  ASSERT_TRUE(iterator->valid());
  ASSERT_EQ(iterator->key(), "charlie");

  iterator->seek("charlie");
  ASSERT_TRUE(iterator->valid());
  ASSERT_EQ(iterator->key(), "charlie");
  iterator->next();
  ASSERT_TRUE(iterator->valid());
  ASSERT_EQ(iterator->key(), "echo");

  iterator->seek("zulu");
  ASSERT_TRUE(!iterator->valid());
  ASSERT_OK(iterator->status());

  iterator->seekToFirst();
  ASSERT_TRUE(iterator->valid());
  ASSERT_EQ(iterator->key(), "alpha");
  ASSERT_EQ(iterator->value(), "visible");
}

TEST(dbIteratorReportsInvalidInternalKey) {
  DBIterator iterator(std::make_unique<InvalidKeyIterator>(),
                      kMaxSequenceNumber);
  iterator.seekToFirst();
  ASSERT_TRUE(!iterator.valid());
  ASSERT_EQ(iterator.status().code(), StatusCode::kCorruption);
}

TEST(dbIteratorPropagatesInputError) {
  DBIterator iterator(std::make_unique<ErrorAfterFirstIterator>(),
                      kMaxSequenceNumber);
  iterator.seekToFirst();
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.key(), "key");

  iterator.next();
  ASSERT_TRUE(!iterator.valid());
  ASSERT_EQ(iterator.status().code(), StatusCode::kCorruption);
}

}
