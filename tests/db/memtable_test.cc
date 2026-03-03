#include "db/memtable.h"

#include <string>

#include "test.h"

namespace lsmtree {
namespace {

TEST(memtableReturnsNewestVisibleVersion) {
  MemTable table;
  table.add(1, ValueType::kValue, "key", "old");
  table.add(3, ValueType::kValue, "key", "new");

  std::string value = "unchanged";
  ASSERT_EQ(table.get("key", 4, &value), LookupResult::kValue);
  ASSERT_EQ(value, "new");
  ASSERT_EQ(table.get("key", 2, &value), LookupResult::kValue);
  ASSERT_EQ(value, "old");
  ASSERT_EQ(table.get("key", 0, &value), LookupResult::kAbsent);
  ASSERT_EQ(value, "old");
}

TEST(memtableTombstoneHidesOnlyVisibleHistory) {
  MemTable table;
  table.add(1, ValueType::kValue, "key", "old");
  table.add(2, ValueType::kDeletion, "key", {});

  std::string value = "unchanged";
  ASSERT_EQ(table.get("key", 3, &value), LookupResult::kDeleted);
  ASSERT_EQ(value, "unchanged");
  ASSERT_EQ(table.get("key", 1, &value), LookupResult::kValue);
  ASSERT_EQ(value, "old");
}

TEST(memtableKeepsBinaryKeysAndEmptyValues) {
  MemTable table;
  const std::string key("a\0b", 3);
  table.add(1, ValueType::kValue, key, "");

  std::string value = "sentinel";
  ASSERT_EQ(table.get(key, 1, &value), LookupResult::kValue);
  ASSERT_EQ(value, "");
}

TEST(memtableCopiesKeysAndValuesIntoOwnedStorage) {
  MemTable table;
  std::string key = "key";
  std::string value = "value";
  table.add(1, ValueType::kValue, key, value);

  key.assign("changed-key");
  value.assign("changed-value");

  std::string stored;
  ASSERT_EQ(table.get("key", 1, &stored), LookupResult::kValue);
  ASSERT_EQ(stored, "value");
}

TEST(memtableLookupDoesNotCrossUserKeyBoundaries) {
  MemTable table;
  table.add(3, ValueType::kValue, "alpha", "a");
  table.add(2, ValueType::kValue, "charlie", "c");

  std::string value = "unchanged";
  ASSERT_EQ(table.get("bravo", 10, &value), LookupResult::kAbsent);
  ASSERT_EQ(value, "unchanged");
  ASSERT_EQ(table.get("zulu", 10, &value), LookupResult::kAbsent);
  ASSERT_EQ(value, "unchanged");
}

TEST(memtableIteratorExposesInternalOrder) {
  MemTable table;
  table.add(7, ValueType::kValue, "alpha", "new");
  table.add(5, ValueType::kDeletion, "alpha", {});
  table.add(3, ValueType::kValue, "alpha", "old");
  table.add(2, ValueType::kValue, "beta", "value");

  ASSERT_TRUE(!table.empty());
  ASSERT_TRUE(table.memoryUsage() > 0);

  auto iterator = table.newIterator();
  iterator.seekToFirst();

  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.internalKey(), encodeInternalKey("alpha", 7,
                                                       ValueType::kValue));
  ASSERT_EQ(iterator.value(), "new");
  iterator.next();
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.internalKey(), encodeInternalKey("alpha", 5,
                                                       ValueType::kDeletion));
  ASSERT_EQ(iterator.value(), "");
  iterator.next();
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.internalKey(), encodeInternalKey("alpha", 3,
                                                       ValueType::kValue));
  ASSERT_EQ(iterator.value(), "old");
  iterator.next();
  ASSERT_TRUE(iterator.valid());
  ASSERT_EQ(iterator.internalKey(), encodeInternalKey("beta", 2,
                                                       ValueType::kValue));
  ASSERT_EQ(iterator.value(), "value");
  iterator.next();
  ASSERT_TRUE(!iterator.valid());
}

TEST(emptyMemtableHasNoEntriesDespiteArenaHead) {
  MemTable table;
  ASSERT_TRUE(table.empty());
  ASSERT_TRUE(table.memoryUsage() > 0);

  auto iterator = table.newIterator();
  iterator.seekToFirst();
  ASSERT_TRUE(!iterator.valid());
}

}
}
