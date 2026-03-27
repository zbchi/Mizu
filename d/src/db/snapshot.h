#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>

#include "db/internal_key.h"

namespace lsmtree {

// 只跟踪活跃 Snapshot 的 sequence，不参与读路径。
// Snapshot 和 DBImpl 共享它，因此快照可晚于 DB 安全释放。
class SnapshotTracker final {
 public:
  void add(SequenceNumber sequence);
  void remove(SequenceNumber sequence);

  // 没有活跃 Snapshot 时返回 fallback。
  SequenceNumber oldest(SequenceNumber fallback) const;

 private:
  mutable std::mutex mutex_;
  std::map<SequenceNumber, std::size_t> counts_;
};

}
