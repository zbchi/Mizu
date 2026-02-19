#include <random>
#include <set>
#include <vector>

#include "db/skiplist.h"
#include "test.h"
#include "util/arena.h"

namespace lsmtree {
namespace {

struct IntComparator {
  int operator()(int lhs, int rhs) const noexcept {
    if (lhs < rhs) return -1;
    if (lhs > rhs) return 1;
    return 0;
  }
};

using TestSkipList = SkipList<int, IntComparator>;

std::vector<int> collect(const TestSkipList& list) {
  std::vector<int> result;
  TestSkipList::Iterator it(&list);
  for (it.seekToFirst(); it.valid(); it.next()) {
    result.push_back(it.key());
  }
  return result;
}

TEST(skiplistStartsEmpty) {
  Arena arena;
  TestSkipList list(IntComparator{}, &arena);
  TestSkipList::Iterator it(&list);

  it.seekToFirst();
  ASSERT_TRUE(!it.valid());
  it.seek(10);
  ASSERT_TRUE(!it.valid());
}

TEST(skiplistInsertsInOrderAndFindsKeys) {
  Arena arena;
  TestSkipList list(IntComparator{}, &arena);

  list.insert(3);
  list.insert(1);
  list.insert(2);

  ASSERT_EQ(collect(list), (std::vector<int>{1, 2, 3}));
  ASSERT_TRUE(list.contains(1));
  ASSERT_TRUE(list.contains(2));
  ASSERT_TRUE(list.contains(3));
  ASSERT_TRUE(!list.contains(4));
}

TEST(skiplistSeekHandlesAllBoundaries) {
  Arena arena;
  TestSkipList list(IntComparator{}, &arena);
  list.insert(10);
  list.insert(20);
  list.insert(30);
  TestSkipList::Iterator it(&list);

  it.seek(5);
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.key(), 10);

  it.seek(20);
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.key(), 20);

  it.seek(25);
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.key(), 30);

  it.seek(31);
  ASSERT_TRUE(!it.valid());
}

TEST(skiplistMatchesOrderedReferenceForRandomOperations) {
  Arena arena;
  TestSkipList list(IntComparator{}, &arena);
  std::set<int> reference;
  std::mt19937 random(12345);
  std::uniform_int_distribution<int> keys(-1000, 1000);

  for (int i = 0; i < 10000; ++i) {
    const int key = keys(random);
    const bool expected_insert = reference.insert(key).second;
    if (expected_insert) list.insert(key);
    ASSERT_TRUE(list.contains(key));
  }

  ASSERT_EQ(collect(list),
            std::vector<int>(reference.begin(), reference.end()));

  for (int target = -1100; target <= 1100; ++target) {
    TestSkipList::Iterator list_iterator(&list);
    list_iterator.seek(target);
    const auto reference_iterator = reference.lower_bound(target);
    const bool reference_valid = reference_iterator != reference.end();
    ASSERT_EQ(list_iterator.valid(), reference_valid);
    if (reference_valid) {
      ASSERT_EQ(list_iterator.key(), *reference_iterator);
    }
  }
}

}
}
