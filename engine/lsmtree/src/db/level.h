#pragma once

#include <cstddef>

namespace lsmtree {

constexpr std::size_t kLevel0 = 0U;
constexpr std::size_t kLevel1 = 1U;
constexpr std::size_t kLevel2 = 2U;
constexpr std::size_t kNumLevels = 3U;

struct IndexRange {
  std::size_t begin = 0;
  std::size_t end = 0;
};

}  // namespace lsmtree
