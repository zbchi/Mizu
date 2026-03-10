#include "db/compaction.h"

#include <algorithm>
#include <cassert>
#include <queue>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "db/db_files.h"
#include "db/filename.h"
#include "db/internal_key.h"
#include "db/version.h"
#include "table/sstable_builder.h"
#include "table/sstable_reader.h"

namespace lsmtree {
namespace {

// Version 已校验 InternalKey 此处只提取用于范围计算的 user key
Slice userKey(Slice internal_key) {
  ParsedInternalKey parsed{};
  const bool valid = parseInternalKey(internal_key, parsed);
  assert(valid);
  return parsed.user_key;
}

struct IteratorGreater {
  const std::vector<std::unique_ptr<SSTableIterator>>* iterators;

  bool operator()(std::size_t lhs, std::size_t rhs) const {
    // 堆中只保存 iterator 下标，比较时读取各 iterator 当前指向的 InternalKey
    // priority_queue 默认把“较大”元素放在顶部，此处反向比较得到最小堆
    const Slice left = (*iterators)[lhs]->internalKey();
    const Slice right = (*iterators)[rhs]->internalKey();
    const InternalKeyLess less;
    return less(right, left);
  }
};

Status renameTable(const std::filesystem::path& temporary_path,
                   const std::filesystem::path& final_path) {
  // builder 只负责完成临时文件，rename 才把它变成可被后续 Manifest 引用的 SST
  std::error_code error;
  std::filesystem::rename(temporary_path, final_path, error);
  if (!error) return Status::success();

  // rename 失败时临时文件不再有用 立即清理避免阻塞重试
  removeFileBestEffort(temporary_path);
  return filesystemError("rename", temporary_path, error);
}

}

std::optional<CompactionPlan> pickLevel0Compaction(
    std::shared_ptr<const Version> version) {
  // picker 不创建文件，只在 L0 文件数达到阈值后描述本次归并的输入集合
  if (!version || version->level0().size() < kLevel0CompactionTrigger) {
    return std::nullopt;
  }

  // L0 可以互相重叠 使用全部 L0 的 user key 并集确定压缩范围
  Slice smallest = userKey(version->level0().front().meta.smallest_key);
  Slice largest = userKey(version->level0().front().meta.largest_key);
  for (const Version::Table& table : version->level0()) {
    const Slice table_smallest = userKey(table.meta.smallest_key);
    const Slice table_largest = userKey(table.meta.largest_key);
    if (table_smallest < smallest) smallest = table_smallest;
    if (largest < table_largest) largest = table_largest;
  }

  // L1 有序且不重叠 所有相交文件组成一个连续区间
  const auto begin =
      std::lower_bound(version->level1().begin(), version->level1().end(),
                       smallest, [](const Version::Table& table, Slice key) {
                         return userKey(table.meta.largest_key) < key;
                       });
  const auto end = std::find_if(
      begin, version->level1().end(), [largest](const Version::Table& table) {
        return largest < userKey(table.meta.smallest_key);
      });

  const std::size_t begin_index =
      static_cast<std::size_t>(begin - version->level1().begin());
  const std::size_t end_index =
      static_cast<std::size_t>(end - version->level1().begin());
  // plan 持有 Version，下面记录的下标和对应 reader 在整个构建期间都有效
  return CompactionPlan{std::move(version), begin_index, end_index};
}

Status buildLevel1Table(const CompactionPlan& plan,
                        std::uint64_t output_number,
                        const std::filesystem::path& directory,
                        CompactionOutput& output) {
  if (!plan.input_version) {
    return Status::invalidArgument("compaction plan has no input version");
  }
  const auto& level0 = plan.input_version->level0();
  const auto& level1 = plan.input_version->level1();
  if (level0.empty() || plan.level1_begin > plan.level1_end ||
      plan.level1_end > level1.size()) {
    return Status::invalidArgument("invalid compaction input range");
  }
  if (output_number == 0) {
    return Status::invalidArgument("compaction output number must be positive");
  }

  // 新表先写入带同一文件号的 .sst.tmp，完整关闭后才改成正式 .sst
  // 正式文件必须尚不存在，避免错误的文件号覆盖已经发布的数据
  const std::filesystem::path temporary_path =
      sstableTemporaryFileName(directory, output_number);
  const std::filesystem::path final_path =
      sstableFileName(directory, output_number);
  std::error_code error;
  if (std::filesystem::exists(final_path, error)) {
    if (error) return filesystemError("stat", final_path, error);
    return Status::alreadyExists("compaction output already exists: " +
                                 final_path.string());
  }
  if (error) return filesystemError("stat", final_path, error);

  const std::size_t input_count =
      level0.size() + (plan.level1_end - plan.level1_begin);
  std::vector<std::unique_ptr<SSTableIterator>> iterators;
  iterators.reserve(input_count);

  // 把每张输入 SSTable 变成一个停在首条记录的有序流。先准备并校验全部
  // iterator，再创建输出文件，输入损坏时不会留下无用的临时输出。
  ReadOptions read_options;
  read_options.verify_checksums = true;
  const auto add_iterator = [&](const Version::Table& table) -> Status {
    auto iterator = table.reader->newIterator(read_options);
    iterator->seekToFirst();
    if (!iterator->status().ok()) return iterator->status();
    if (!iterator->valid()) {
      return Status::corruption("compaction input SSTable is empty");
    }
    iterators.push_back(std::move(iterator));
    return Status::success();
  };

  // L0 可能互相重叠，因此全部加入；L1 只加入 picker 找到的重叠区间
  for (const Version::Table& table : level0) {
    Status status = add_iterator(table);
    if (!status.ok()) return status;
  }
  for (std::size_t index = plan.level1_begin; index < plan.level1_end;
       ++index) {
    Status status = add_iterator(level1[index]);
    if (!status.ok()) return status;
  }

  // 堆元素是 iterator 下标，堆顶下标始终指向所有有序流中最小的当前
  // InternalKey。每个输入在堆中至多出现一次，所以额外空间为 O(输入文件数)。
  IteratorGreater greater{&iterators};
  std::priority_queue<std::size_t, std::vector<std::size_t>, IteratorGreater>
      heap(greater);
  for (std::size_t index = 0; index < iterators.size(); ++index) {
    heap.push(index);
  }

  std::unique_ptr<SSTableBuilder> builder;
  Status status = SSTableBuilder::open(temporary_path, {}, builder);
  if (!status.ok()) return status;

  const InternalKeyLess less;
  std::string previous_key;
  // 每轮弹出全局最小记录，写入输出，再推进它所属的 iterator 并放回堆。
  // 此阶段不按 user key 折叠记录，旧版本和 tombstone 都原样保留。
  while (!heap.empty()) {
    const std::size_t index = heap.top();
    heap.pop();
    SSTableIterator& iterator = *iterators[index];
    const Slice key = iterator.internalKey();

    // SSTableBuilder 要求 key 严格递增；相同 InternalKey 表示输入违反了
    // 全局 sequence 唯一的数据库约束，继续写会生成含义不明确的结果。
    if (!previous_key.empty() && !less(previous_key, key)) {
      return Status::corruption(
          "compaction inputs contain duplicate internal keys");
    }
    status = builder->add(key, iterator.value());
    if (!status.ok()) return status;
    // iterator.next() 会使当前 Slice 失效，推进前复制一份用于下轮顺序检查
    previous_key.assign(key.data(), key.size());

    iterator.next();
    if (!iterator.status().ok()) return iterator.status();
    if (iterator.valid()) heap.push(index);
  }

  // finish 写完尾部 data block、filter、index 和 footer，同步并关闭临时文件，
  // completed 则带回安装 Version 所需的大小和 InternalKey 边界。
  SSTableMeta completed;
  status = builder->finish(completed);
  if (!status.ok()) return status;

  // rename 后文件名已经正式，但 Manifest 尚未引用它，因此数据库仍看不到新表
  status = renameTable(temporary_path, final_path);
  if (!status.ok()) return status;

  // 沿正常读路径重新打开输出，确保 footer、filter 和 index 都能被解析；
  // 打不开的文件不能进入候选 Version，立即删除。
  std::unique_ptr<SSTableReader> opened;
  status = SSTableReader::open(final_path, opened);
  if (!status.ok()) {
    removeFileBestEffort(final_path);
    return status;
  }

  // 使用局部 result 组装完整结果，只有全部步骤成功后才修改调用方的 output
  CompactionOutput result;
  result.meta = TableMeta{output_number, completed.file_size,
                          std::move(completed.smallest_key),
                          std::move(completed.largest_key)};
  result.reader = std::move(opened);
  output = std::move(result);
  return Status::success();
}

}
