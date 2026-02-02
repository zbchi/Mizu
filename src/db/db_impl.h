#pragma once

#include <map>
#include <memory>
#include <shared_mutex>

#include "lsmtree/db.h"

namespace lsmtree {

// 通过 RAII 持有数据库目录的 POSIX 文件锁
class FileLock {
 public:
  FileLock() = default;
  explicit FileLock(int fd) noexcept;
  ~FileLock();

  FileLock(FileLock&& other) noexcept;
  FileLock& operator=(FileLock&& other) noexcept;

  FileLock(const FileLock&) = delete;
  FileLock& operator=(const FileLock&) = delete;

 private:
  void reset() noexcept;

  int fd_ = -1;
};

class DBImpl final : public DB {
 public:
  static Status open(const DBOptions& options,
                     const std::filesystem::path& directory, DB::Handle* db);

  DBImpl() = default;

  Status write(const WriteOptions& options,
               const WriteBatch& batch) override;
  Status get(const ReadOptions& options, Slice key,
             std::string* value) const override;
  Status newSnapshot(SnapshotHandle* snapshot) const override;
  Status newIterator(const ReadOptions& options,
                     std::unique_ptr<Iterator>* iterator) const override;

 private:
  FileLock lock_;
  mutable std::shared_mutex mutex_;
  std::map<std::string, std::string, std::less<>> data_;
};

}
