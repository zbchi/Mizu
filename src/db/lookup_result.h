#pragma once

namespace lsmtree {

enum class LookupResult {
  kAbsent,
  kValue,
  kDeleted,
};

}
