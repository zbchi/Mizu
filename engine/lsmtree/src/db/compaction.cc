#include "db/compaction.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "db/db_files.h"
#include "db/filename.h"
#include "db/internal_key.h"
#include "db/merging_iterator.h"
#include "db/version.h"
#include "table/sstable_builder.h"
#include "table/sstable_reader.h"

namespace lsmtree {
namespace {

Slice userKey(Slice internal_key) {
  ParsedInternalKey parsed{};
  const bool valid = parseInternalKey(internal_key, parsed);
  assert(valid);
  return parsed.user_key;
}

bool levelAtLimit(const std::vector<Version::Table>& tables,
                  std::uint64_t limit) noexcept {
  if (tables.empty()) return false;
  if (limit == 0) return true;

  std::uint64_t remaining = limit;
  for (const Version::Table& table : tables) {
    if (table.meta.file_size >= remaining) return true;
    remaining -= table.meta.file_size;
  }
  return false;
}

std::pair<Slice, Slice> userRange(const std::vector<Version::Table>& tables,
                                  IndexRange range) {
  assert(range.begin < range.end);
  assert(range.end <= tables.size());
  Slice smallest = userKey(tables[range.begin].meta.smallest_key);
  Slice largest = userKey(tables[range.begin].meta.largest_key);
  for (std::size_t index = range.begin + 1U; index < range.end; ++index) {
    const Slice table_smallest = userKey(tables[index].meta.smallest_key);
    const Slice table_largest = userKey(tables[index].meta.largest_key);
    if (table_smallest < smallest) smallest = table_smallest;
    if (largest < table_largest) largest = table_largest;
  }
  return {smallest, largest};
}

IndexRange overlappingRange(const std::vector<Version::Table>& tables,
                            Slice smallest, Slice largest) {
  const auto begin =
      std::lower_bound(tables.begin(), tables.end(), smallest,
                       [](const Version::Table& table, Slice key) {
                         return userKey(table.meta.largest_key) < key;
                       });
  const auto end =
      std::find_if(begin, tables.end(), [largest](const Version::Table& table) {
        return largest < userKey(table.meta.smallest_key);
      });
  return {static_cast<std::size_t>(begin - tables.begin()),
          static_cast<std::size_t>(end - tables.begin())};
}

Status renameTable(const std::filesystem::path& temporary_path,
                   const std::filesystem::path& final_path) {
  std::error_code error;
  std::filesystem::rename(temporary_path, final_path, error);
  if (!error) return Status::success();

  removeFileBestEffort(temporary_path);
  return filesystemError("rename", temporary_path, error);
}

void removeOutputs(const std::filesystem::path& directory,
                   std::vector<CompactionOutput>& outputs) {
  for (CompactionOutput& output : outputs) {
    output.reader.reset();
    removeFileBestEffort(sstableFileName(directory, output.meta.number));
  }
  outputs.clear();
}

// 管理 compaction 输出 SST 的完整生命周期。调用方只需按序 add；任一步
// 失败时析构函数会删除临时文件和已经完成但尚未发布的输出。
class CompactionOutputWriter {
 public:
  CompactionOutputWriter(const std::filesystem::path& directory,
                         const FileNumberAllocator& allocate_file_number,
                         std::size_t target_file_size)
      : directory_(directory),
        allocate_file_number_(allocate_file_number),
        target_file_size_(target_file_size) {}

  ~CompactionOutputWriter() {
    builder_.reset();
    if (!released_) removeOutputs(directory_, completed_outputs_);
  }

  Status add(Slice internal_key, Slice user_key, Slice value) {
    // 目标大小是软上限；同一 user key 的所有版本必须留在同一文件中。
    if (builder_ && last_user_key_ != user_key &&
        builder_->estimatedFileSize() >= target_file_size_) {
      Status status = finishOutput();
      if (!status.ok()) return status;
    }
    if (!builder_) {
      Status status = openOutput();
      if (!status.ok()) return status;
    }

    Status status = builder_->add(internal_key, value);
    if (!status.ok()) return status;
    last_user_key_.assign(user_key.data(), user_key.size());
    return Status::success();
  }

  Status finish(std::vector<CompactionOutput>& outputs) {
    if (builder_) {
      Status status = finishOutput();
      if (!status.ok()) return status;
    }
    outputs = std::move(completed_outputs_);
    released_ = true;
    return Status::success();
  }

 private:
  Status openOutput() {
    Status status = allocate_file_number_(current_number_);
    if (!status.ok()) return status;
    if (current_number_ == 0) {
      return Status::invalidArgument("compaction file number must be positive");
    }

    const auto final_path = sstableFileName(directory_, current_number_);
    std::error_code error;
    if (std::filesystem::exists(final_path, error)) {
      if (error) return filesystemError("stat", final_path, error);
      return Status::alreadyExists("compaction output already exists: " +
                                   final_path.string());
    }
    if (error) return filesystemError("stat", final_path, error);

    return SSTableBuilder::open(
        sstableTemporaryFileName(directory_, current_number_), {}, builder_);
  }

  Status finishOutput() {
    assert(builder_ && current_number_ != 0);
    SSTableMeta completed;
    Status status = builder_->finish(completed);
    if (!status.ok()) return status;
    builder_.reset();

    const auto temporary_path =
        sstableTemporaryFileName(directory_, current_number_);
    const auto final_path = sstableFileName(directory_, current_number_);
    status = renameTable(temporary_path, final_path);
    if (!status.ok()) return status;

    std::unique_ptr<SSTableReader> opened;
    status = SSTableReader::open(final_path, opened);
    if (!status.ok()) {
      removeFileBestEffort(final_path);
      return status;
    }

    completed_outputs_.push_back(
        {TableMeta{current_number_, completed.file_size,
                   std::move(completed.smallest_key),
                   std::move(completed.largest_key)},
         std::move(opened)});
    current_number_ = 0;
    last_user_key_.clear();
    return Status::success();
  }

  const std::filesystem::path& directory_;
  const FileNumberAllocator& allocate_file_number_;
  std::size_t target_file_size_;
  std::unique_ptr<SSTableBuilder> builder_;
  std::vector<CompactionOutput> completed_outputs_;
  std::uint64_t current_number_ = 0;
  std::string last_user_key_;
  bool released_ = false;
};

// user key 按升序到达，因而每个更低层只需维护一个单调前进的游标。
class BaseLevelChecker {
 public:
  explicit BaseLevelChecker(const CompactionPlan& plan)
      : version_(*plan.input_version), first_level_(plan.outputLevel() + 1U) {}

  bool isBaseLevelForKey(Slice user_key) {
    for (std::size_t level = first_level_; level < kNumLevels; ++level) {
      const auto& tables = version_.level(level);
      std::size_t& index = indices_[level];
      while (index < tables.size() &&
             userKey(tables[index].meta.largest_key) < user_key) {
        ++index;
      }
      if (index < tables.size() &&
          userKey(tables[index].meta.smallest_key) <= user_key) {
        return false;
      }
    }
    return true;
  }

 private:
  const Version& version_;
  std::size_t first_level_;
  std::array<std::size_t, kNumLevels> indices_{};
};

}  // namespace

bool needsCompaction(const Version& version,
                     CompactionOptions options) noexcept {
  return version.level(kLevel0).size() >= kLevel0CompactionTrigger ||
         levelAtLimit(version.level(kLevel1), options.level1_bytes);
}

std::optional<CompactionPlan> pickCompaction(
    std::shared_ptr<const Version> version, SequenceNumber oldest_snapshot,
    CompactionOptions options) {
  if (!version) return std::nullopt;

  std::size_t input_level = kNumLevels;
  IndexRange inputs;
  if (version->level(kLevel0).size() >= kLevel0CompactionTrigger) {
    input_level = kLevel0;
    inputs = {0, version->level(kLevel0).size()};
  } else if (levelAtLimit(version->level(kLevel1), options.level1_bytes)) {
    input_level = kLevel1;
    inputs = {0, 1};
  } else {
    return std::nullopt;
  }

  const auto [smallest, largest] =
      userRange(version->level(input_level), inputs);
  const IndexRange overlaps =
      overlappingRange(version->level(input_level + 1U), smallest, largest);
  return CompactionPlan{std::move(version), oldest_snapshot, input_level,
                        inputs, overlaps};
}

Status buildCompactionTables(const CompactionPlan& plan,
                             const std::filesystem::path& directory,
                             const FileNumberAllocator& allocate_file_number,
                             std::vector<CompactionOutput>& outputs,
                             CompactionOptions options) {
  if (!plan.input_version) {
    return Status::invalidArgument("compaction plan has no input version");
  }
  if (plan.input_level + 1U >= kNumLevels ||
      plan.inputs.begin >= plan.inputs.end || options.output_bytes == 0 ||
      !allocate_file_number) {
    return Status::invalidArgument("invalid compaction plan or options");
  }

  const auto& source = plan.input_version->level(plan.input_level);
  const auto& target = plan.input_version->level(plan.outputLevel());
  if (plan.inputs.end > source.size() ||
      plan.overlaps.begin > plan.overlaps.end ||
      plan.overlaps.end > target.size()) {
    return Status::invalidArgument("invalid compaction input range");
  }

  std::vector<std::unique_ptr<InternalIterator>> iterators;
  iterators.reserve((plan.inputs.end - plan.inputs.begin) +
                    (plan.overlaps.end - plan.overlaps.begin));

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

  for (std::size_t index = plan.inputs.begin; index < plan.inputs.end;
       ++index) {
    Status status = add_iterator(source[index]);
    if (!status.ok()) return status;
  }
  for (std::size_t index = plan.overlaps.begin; index < plan.overlaps.end;
       ++index) {
    Status status = add_iterator(target[index]);
    if (!status.ok()) return status;
  }

  const InternalKeyLess less;
  std::string previous_key;
  std::string current_user_key;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  BaseLevelChecker base_level(plan);
  CompactionOutputWriter output_writer(directory, allocate_file_number,
                                       options.output_bytes);

  MergingIterator iterator(std::move(iterators));
  iterator.seekToFirst();
  if (!iterator.status().ok()) return iterator.status();
  while (iterator.valid()) {
    const Slice key = iterator.internalKey();
    if (!previous_key.empty() && !less(previous_key, key)) {
      return Status::corruption(
          "compaction inputs contain duplicate internal keys");
    }

    ParsedInternalKey parsed{};
    if (!parseInternalKey(key, parsed)) {
      return Status::corruption(
          "compaction input contains invalid internal key");
    }
    if (current_user_key != parsed.user_key) {
      current_user_key.assign(parsed.user_key.data(), parsed.user_key.size());
      last_sequence_for_key = kMaxSequenceNumber;
    }

    // InternalKey 让同 key 的 sequence 降序出现。只要上一条已经落到最老
    // Snapshot 边界内，当前及后续版本就对所有活跃 Snapshot 都不可见。
    bool drop = last_sequence_for_key <= plan.oldest_snapshot;
    if (!drop && parsed.type == ValueType::kDeletion &&
        parsed.sequence <= plan.oldest_snapshot &&
        base_level.isBaseLevelForKey(parsed.user_key)) {
      // tombstone 只有确认更低层不可能再暴露旧值时才能一起回收。
      drop = true;
    }
    last_sequence_for_key = parsed.sequence;

    if (!drop) {
      Status status = output_writer.add(key, parsed.user_key, iterator.value());
      if (!status.ok()) return status;
    }

    previous_key.assign(key.data(), key.size());
    iterator.next();
    if (!iterator.status().ok()) return iterator.status();
  }

  return output_writer.finish(outputs);
}

}  // namespace lsmtree
