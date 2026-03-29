#include "db/flush_memtable.h"

#include <cassert>
#include <system_error>
#include <utility>

namespace lsmtree {
namespace {

Status renameError(const std::filesystem::path& from,
                   const std::filesystem::path& to,
                   const std::error_code& error) {
  return Status::ioError("rename " + from.string() + " to " + to.string() +
                         ": " + error.message());
}

}

Status buildLevel0Table(const MemTable& memtable,
                        const std::filesystem::path& temporary_path,
                        const std::filesystem::path& final_path,
                        const SSTableBuilderOptions& options,
                        SSTableMeta& meta) {
  assert(!memtable.empty());

  std::unique_ptr<SSTableBuilder> builder;
  Status status =
      SSTableBuilder::open(temporary_path, options, builder);
  if (!status.ok()) return status;

  MemTable::Iterator iterator = memtable.newIterator();
  iterator.seekToFirst();
  while (iterator.valid()) {
    status = builder->add(iterator.internalKey(), iterator.value());
    if (!status.ok()) return status;
    iterator.next();
  }

  SSTableMeta completed;
  status = builder->finish(completed);
  if (!status.ok()) return status;

  std::error_code error;
  std::filesystem::rename(temporary_path, final_path, error);
  if (error) {
    std::error_code cleanup_error;
    std::filesystem::remove(temporary_path, cleanup_error);
    return renameError(temporary_path, final_path, error);
  }

  meta = std::move(completed);
  return Status::success();
}

}
