#include "db/version.h"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <system_error>
#include <utility>

#include "db/db_files.h"
#include "db/filename.h"
#include "table/sstable_reader.h"

namespace lsmtree {
namespace {

// Manifest 已校验 InternalKey 此处只提取用于层级定位的 user key
Slice userKey(Slice internal_key) {
  ParsedInternalKey parsed{};
  const bool valid = parseInternalKey(internal_key, parsed);
  assert(valid);
  return parsed.user_key;
}

Status openTable(const std::filesystem::path& directory, const TableMeta& meta,
                 Version::Table& table) {
  const std::filesystem::path path = sstableFileName(directory, meta.number);
  std::error_code error;
  const bool exists = std::filesystem::exists(path, error);
  if (error) return filesystemError("stat", path, error);
  if (!exists) {
    return Status::corruption("manifest references a missing SSTable: " +
                              path.string());
  }
  const std::uintmax_t size = std::filesystem::file_size(path, error);
  if (error) return filesystemError("stat", path, error);
  if (size != meta.file_size) {
    return Status::corruption("SSTable size does not match manifest: " +
                              path.string());
  }

  // reader 完成 footer filter 和 index 校验后才加入 Version
  std::unique_ptr<SSTableReader> reader;
  Status status = SSTableReader::open(path, reader);
  if (!status.ok()) return status;
  table = Version::Table{meta, std::move(reader)};
  return Status::success();
}

Status openLevel(const std::filesystem::path& directory,
                 const std::vector<TableMeta>& descriptors,
                 std::vector<Version::Table>& tables) {
  tables.reserve(descriptors.size());
  for (const TableMeta& descriptor : descriptors) {
    Version::Table table;
    Status status = openTable(directory, descriptor, table);
    if (!status.ok()) return status;
    tables.push_back(std::move(table));
  }
  return Status::success();
}

}  // namespace

Status Version::open(const std::filesystem::path& directory,
                     const ManifestState& manifest,
                     std::shared_ptr<const Version>& version) {
  // opened 保持私有 任一文件失败都不会修改调用方的当前 Version
  auto opened = std::make_shared<Version>();
  for (std::size_t level = 0; level < kNumLevels; ++level) {
    Status status =
        openLevel(directory, manifest.levels[level], opened->levels_[level]);
    if (!status.ok()) return status;
  }
  version = std::move(opened);
  return Status::success();
}

Status Version::get(const ReadOptions& options, Slice user_key,
                    SequenceNumber visible_sequence, LookupResult& result,
                    std::string& value) const {
  // L0 文件范围可以重叠 必须按新文件到旧文件逐个查找
  for (const Table& table : levels_[kLevel0]) {
    Status status =
        table.reader->get(options, user_key, visible_sequence, result, value);
    if (!status.ok() || result != LookupResult::kAbsent) return status;
  }

  // 非 L0 层按 user key 范围有序且不重叠，每层至多有一个候选。候选
  // 对旧 Snapshot 返回 absent 时仍要继续向下查找。
  for (std::size_t level = kLevel1; level < kNumLevels; ++level) {
    const auto& tables = levels_[level];
    const auto candidate =
        std::lower_bound(tables.begin(), tables.end(), user_key,
                         [](const Table& table, Slice key) {
                           return userKey(table.meta.largest_key) < key;
                         });
    if (candidate == tables.end() ||
        userKey(candidate->meta.smallest_key) > user_key) {
      continue;
    }
    Status status = candidate->reader->get(options, user_key, visible_sequence,
                                           result, value);
    if (!status.ok() || result != LookupResult::kAbsent) return status;
  }

  result = LookupResult::kAbsent;
  return Status::success();
}

std::shared_ptr<const Version> Version::withLevel0Table(
    TableMeta meta, std::shared_ptr<const SSTableReader> reader) const {
  // 复制元数据和 shared_ptr 不复制 SSTable 内容
  auto next = std::make_shared<Version>();
  next->levels_ = levels_;
  auto& level0 = next->levels_[kLevel0];
  level0.insert(level0.begin(), Table{std::move(meta), std::move(reader)});
  return next;
}

std::shared_ptr<const Version> Version::withCompaction(
    std::size_t input_level, IndexRange inputs, IndexRange overlaps,
    std::vector<Table> outputs) const {
  assert(input_level + 1U < kNumLevels);
  assert(inputs.begin < inputs.end);
  assert(inputs.end <= levels_[input_level].size());
  assert(overlaps.begin <= overlaps.end);
  assert(overlaps.end <= levels_[input_level + 1U].size());

  auto next = std::make_shared<Version>();
  next->levels_ = levels_;

  auto& source = next->levels_[input_level];
  source.erase(source.begin() + inputs.begin, source.begin() + inputs.end);

  auto& target = next->levels_[input_level + 1U];
  target.erase(target.begin() + overlaps.begin, target.begin() + overlaps.end);
  target.insert(target.begin() + overlaps.begin,
                std::make_move_iterator(outputs.begin()),
                std::make_move_iterator(outputs.end()));
  return next;
}

const std::vector<Version::Table>& Version::level(
    std::size_t level) const noexcept {
  assert(level < kNumLevels);
  return levels_[level];
}

}  // namespace lsmtree
