#pragma once

#include <cstdint>
#include <filesystem>
#include <system_error>
#include <vector>

#include "lsmtree/db.h"

namespace lsmtree {

struct ManifestState;

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

struct DirectoryContents {
  std::vector<std::uint64_t> wal_numbers;
  bool has_sstable = false;
  std::uint64_t maximum_number = 0;
};

Status filesystemError(const char* operation,
                       const std::filesystem::path& path,
                       const std::error_code& error);

Status prepareDatabaseDirectory(OpenMode mode,
                                const std::filesystem::path& directory);
Status acquireDatabaseLock(const std::filesystem::path& directory,
                           FileLock& lock);
Status scanDatabaseDirectory(const std::filesystem::path& directory,
                             DirectoryContents& contents);

void removeFileBestEffort(const std::filesystem::path& path);
void removeObsoleteWalFilesBestEffort(
    const std::filesystem::path& directory,
    std::uint64_t live_wal_number);
void removeObsoleteSSTableFilesBestEffort(
    const std::filesystem::path& directory,
    const ManifestState& manifest);

}
