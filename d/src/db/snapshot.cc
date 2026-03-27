#include "db/snapshot.h"

#include <cassert>

namespace lsmtree {

void SnapshotTracker::add(SequenceNumber sequence) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++counts_[sequence];
}

void SnapshotTracker::remove(SequenceNumber sequence) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto iterator = counts_.find(sequence);
  assert(iterator != counts_.end());
  assert(iterator->second > 0);
  if (--iterator->second == 0)
    counts_.erase(iterator);
}

SequenceNumber SnapshotTracker::oldest(SequenceNumber fallback) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return counts_.empty() ? fallback : counts_.begin()->first;
}

}
