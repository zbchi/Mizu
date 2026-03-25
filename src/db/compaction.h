#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

#include "db/manifest.h"
#include "lsmtree/db.h"

namespace lsmtree {

class SSTableReader;
class Version;

constexpr std::size_t kLevel0CompactionTrigger = 4U;

// Compaction 分为选择和构建两步：先从一个不可变 Version 中固定全部 L0
// 以及与其重叠的连续 L1 区间，再把这些有序 SSTable 归并成一个新 L1 文件
struct CompactionPlan {
  // 固定输入文件及 reader 的生命周期，压缩期间旧 Version 可以继续服务读请求
  std::shared_ptr<const Version> input_version;
  // 本次压缩期间仍需服务的最老 Snapshot 边界；计划创建后保持不变。
  SequenceNumber oldest_snapshot = 0;
  // input_version->level1() 中参与归并的半开区间 [begin, end)
  std::size_t level1_begin = 0;
  std::size_t level1_end = 0;
};

// L0 未达到阈值时返回空；否则计算全部 L0 的 user-key 范围并固定重叠 L1
std::optional<CompactionPlan> pickLevel0Compaction(
    std::shared_ptr<const Version> version,
    SequenceNumber oldest_snapshot);

// 已完整写入并能被 reader 打开的 SSTable，但尚未加入 Manifest 和 Version
struct CompactionOutput {
  TableMeta meta;
  // 为空表示所有输入版本都被回收，不发布空 SSTable。
  std::shared_ptr<const SSTableReader> reader;
};

// 将 plan 中的每张 SSTable 视为一个有序流，通过最小堆按 InternalKey
// 归并，并按 oldest_snapshot
// 回收已经不可见的历史版本。成功时返回文件元数据和已打开的 reader；
// 失败时清理本次创建的文件并保持 output 不变。
// 此函数只构建候选输出，不修改 Manifest 或数据库可见状态。
Status buildLevel1Table(const CompactionPlan& plan, std::uint64_t output_number,
                        const std::filesystem::path& directory,
                        CompactionOutput& output);

}
