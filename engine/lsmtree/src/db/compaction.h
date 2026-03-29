#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "db/level.h"
#include "db/manifest.h"
#include "lsmtree/db.h"

namespace lsmtree {

class SSTableReader;
class Version;

constexpr std::size_t kLevel0CompactionTrigger = 4U;
constexpr std::uint64_t kLevel1CompactionBytes = 10U * 1024U * 1024U;
constexpr std::size_t kCompactionOutputBytes = 2U * 1024U * 1024U;

struct CompactionOptions {
  std::uint64_t level1_bytes = kLevel1CompactionBytes;
  std::size_t output_bytes = kCompactionOutputBytes;
};

// 一次 compaction 是固定 Version 上相邻两层的区间替换。
struct CompactionPlan {
  std::shared_ptr<const Version> input_version;
  SequenceNumber oldest_snapshot = 0;
  std::size_t input_level = kLevel0;
  IndexRange inputs;
  IndexRange overlaps;

  std::size_t outputLevel() const noexcept { return input_level + 1U; }
};

bool needsCompaction(const Version& version,
                     CompactionOptions options = {}) noexcept;

// L0 达到文件数阈值时优先选择全部 L0；否则在 L1 超过字节预算时
// 选择第一张 L1。两种情况都带上下一层的完整重叠区间。
std::optional<CompactionPlan> pickCompaction(
    std::shared_ptr<const Version> version, SequenceNumber oldest_snapshot,
    CompactionOptions options = {});

struct CompactionOutput {
  TableMeta meta;
  std::shared_ptr<const SSTableReader> reader;
};

using FileNumberAllocator = std::function<Status(std::uint64_t& file_number)>;

// 归并 plan 的两组输入，执行 Snapshot-aware 版本回收，并在 user-key
// 边界按目标大小切分。成功时 outputs 可为空，表示所有输入都已回收。
Status buildCompactionTables(const CompactionPlan& plan,
                             const std::filesystem::path& directory,
                             const FileNumberAllocator& allocate_file_number,
                             std::vector<CompactionOutput>& outputs,
                             CompactionOptions options = {});

}  // namespace lsmtree
