#include "db/version.h"

#include <algorithm>
#include <cassert>
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

Status openTable(const std::filesystem::path& directory,
                 const TableMeta& meta, Version::Table& table) {
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

}

Status Version::open(const std::filesystem::path& directory,
                     const ManifestState& manifest,
                     std::shared_ptr<const Version>& version) {
  // opened 保持私有 任一文件失败都不会修改调用方的当前 Version
  auto opened = std::make_shared<Version>();
  Status status = openLevel(directory, manifest.level0_tables, opened->level0_);
  if (!status.ok()) return status;
  status = openLevel(directory, manifest.level1_tables, opened->level1_);
  if (!status.ok()) return status;
  version = std::move(opened);
  return Status::success();
}

Status Version::get(const ReadOptions& options, Slice user_key,
                    SequenceNumber visible_sequence, LookupResult& result,
                    std::string& value) const {
  // L0 文件范围可以重叠 必须按新文件到旧文件逐个查找
  for (const Table& table : level0_) {
    Status status = table.reader->get(options, user_key, visible_sequence,
                                      result, value);
    if (!status.ok() || result != LookupResult::kAbsent) return status;
  }

  // L1 按 user key 范围有序且不重叠 第一个末端不小于 key 的文件是唯一候选
  const auto candidate = std::lower_bound(
      level1_.begin(), level1_.end(), user_key,
      [](const Table& table, Slice key) {
        return userKey(table.meta.largest_key) < key;
      });
  if (candidate != level1_.end() &&
      userKey(candidate->meta.smallest_key) <= user_key) {
    return candidate->reader->get(options, user_key, visible_sequence, result,
                                  value);
  }

  result = LookupResult::kAbsent;
  return Status::success();
}

std::shared_ptr<const Version> Version::withLevel0Table(
    TableMeta meta, std::shared_ptr<const SSTableReader> reader) const {
  // 复制元数据和 shared_ptr 不复制 SSTable 内容
  auto next = std::make_shared<Version>();
  next->level0_.reserve(level0_.size() + 1U);
  next->level0_.push_back(Table{std::move(meta), std::move(reader)});
  next->level0_.insert(next->level0_.end(), level0_.begin(), level0_.end());
  next->level1_ = level1_;
  return next;
}

}
