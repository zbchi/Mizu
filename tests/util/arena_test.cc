#include <cstdint>
#include <cstring>
#include <string>

#include "test.h"
#include "util/arena.h"

namespace lsmtree {
namespace {

TEST(arenaReturnsDistinctStorageAndTracksBlockCapacity) {
  Arena arena(32);
  char* first = arena.allocate(8);
  char* second = arena.allocate(8);

  ASSERT_TRUE(first != nullptr);
  ASSERT_TRUE(second != nullptr);
  ASSERT_TRUE(first != second);
  ASSERT_EQ(arena.memoryUsage(), 32U);

  char* large = arena.allocate(33);
  ASSERT_TRUE(large != nullptr);
  ASSERT_EQ(arena.memoryUsage(), 65U);
}

TEST(arenaPreservesEarlierAllocationsAcrossGrowth) {
  Arena arena(16);
  char* first = arena.allocate(6);
  std::memcpy(first, "before", 6);

  ASSERT_TRUE(arena.allocate(32) != nullptr);
  ASSERT_EQ(std::string(first, 6), "before");
}

TEST(arenaProvidesObjectAlignment) {
  Arena arena(17);
  ASSERT_TRUE(arena.allocate(1) != nullptr);

  char* aligned = arena.allocateAligned(sizeof(std::uint64_t));
  const auto address = reinterpret_cast<std::uintptr_t>(aligned);
  ASSERT_EQ(address % alignof(std::max_align_t), 0U);
}

TEST(arenaHandlesAlignmentAtBlockBoundary) {
  Arena arena(32);
  ASSERT_TRUE(arena.allocate(31) != nullptr);

  char* aligned = arena.allocateAligned(1);
  const auto address = reinterpret_cast<std::uintptr_t>(aligned);
  ASSERT_EQ(address % alignof(std::max_align_t), 0U);
  ASSERT_TRUE(arena.memoryUsage() >= 32U);
}

TEST(arenaZeroByteAllocationsDoNotReserveMemory) {
  Arena arena;
  ASSERT_TRUE(arena.allocate(0) == nullptr);
  ASSERT_TRUE(arena.allocateAligned(0) == nullptr);
  ASSERT_EQ(arena.memoryUsage(), 0U);
}

}
}
