#include "db/db_files.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <set>
#include <utility>

#include "db/filename.h"
#include "db/manifest.h"

namespace lsmtree {
namespace {

Status posixError(const char* operation, const std::filesystem::path& path) {
  return Status::ioError(std::string(operation) + " " + path.string() + ": " +
                         std::strerror(errno));
}

}

FileLock::FileLock(int fd) noexcept : fd_(fd) {}

FileLock::~FileLock() { reset(); }

FileLock::FileLock(FileLock&& other) noexcept : fd_(other.fd_) {
  other.fd_ = -1;
}

FileLock& FileLock::operator=(FileLock&& other) noexcept {
  if (this != &other) {
    reset();
    fd_ = other.fd_;
    other.fd_ = -1;
  }
  return *this;
}

void FileLock::reset() noexcept {
  if (fd_ < 0) return;
  flock(fd_, LOCK_UN);
  close(fd_);
  fd_ = -1;
}

Status filesystemError(const char* operation,
                       const std::filesystem::path& path,
                       const std::error_code& error) {
  return Status::ioError(std::string(operation) + " " + path.string() + ": " +
                         error.message());
}

Status prepareDatabaseDirectory(OpenMode mode,
                                const std::filesystem::path& directory) {
  if (directory.empty()) {
    return Status::invalidArgument("database directory must not be empty");
  }

  std::error_code error;
  const bool exists = std::filesystem::exists(directory, error);
  if (error) return filesystemError("stat", directory, error);

  if (exists && !std::filesystem::is_directory(directory, error)) {
    if (error) return filesystemError("stat", directory, error);
    return Status::invalidArgument("database path is not a directory: " +
                                   directory.string());
  }
  if (error) return filesystemError("stat", directory, error);

  if (mode == OpenMode::kOpenExisting && !exists) {
    return Status::notFound("database directory does not exist: " +
                            directory.string());
  }
  if (mode == OpenMode::kCreateNew && exists) {
    return Status::alreadyExists("database directory already exists: " +
                                 directory.string());
  }
  if (!exists) {
    std::filesystem::create_directories(directory, error);
    if (error) return filesystemError("create directory", directory, error);
  }

  return Status::success();
}

Status acquireDatabaseLock(const std::filesystem::path& directory,
                           FileLock& lock) {
  const std::filesystem::path path = lockFileName(directory);
  const int fd = open(path.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd < 0) return posixError("open", path);

  if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
    const int saved_errno = errno;
    close(fd);
    if (saved_errno == EWOULDBLOCK || saved_errno == EAGAIN) {
      return Status::busy("database is already open: " + directory.string());
    }
    errno = saved_errno;
    return posixError("lock", path);
  }

  lock = FileLock(fd);
  return Status::success();
}

Status scanDatabaseDirectory(const std::filesystem::path& directory,
                             DirectoryContents& contents) {
  DirectoryContents scanned;
  std::error_code error;
  std::filesystem::directory_iterator iterator(directory, error);
  if (error) return filesystemError("list directory", directory, error);

  for (const auto& entry : iterator) {
    std::uint64_t number = 0;
    NumberedFileType type = NumberedFileType::kWal;
    if (!parseNumberedFileName(entry.path(), number, type)) continue;

    scanned.maximum_number = std::max(scanned.maximum_number, number);
    if (type == NumberedFileType::kWal) {
      scanned.wal_numbers.push_back(number);
    } else if (type == NumberedFileType::kSSTable) {
      scanned.has_sstable = true;
    }
  }

  std::sort(scanned.wal_numbers.begin(), scanned.wal_numbers.end());
  contents = std::move(scanned);
  return Status::success();
}

void removeFileBestEffort(const std::filesystem::path& path) {
  std::error_code ignored;
  std::filesystem::remove(path, ignored);
}

void removeObsoleteWalFilesBestEffort(
    const std::filesystem::path& directory,
    std::uint64_t live_wal_number) {
  std::error_code error;
  std::filesystem::directory_iterator iterator(directory, error);
  if (error) return;

  for (const auto& entry : iterator) {
    std::uint64_t number = 0;
    NumberedFileType type = NumberedFileType::kWal;
    if (!parseNumberedFileName(entry.path(), number, type) ||
        type != NumberedFileType::kWal || number >= live_wal_number) {
      continue;
    }
    std::error_code ignored;
    std::filesystem::remove(entry.path(), ignored);
  }
}

void removeObsoleteSSTableFilesBestEffort(
    const std::filesystem::path& directory,
    const ManifestState& manifest) {
  std::set<std::uint64_t> live;
  for (const TableMeta& table : manifest.level0_tables) {
    live.insert(table.number);
  }
  for (const TableMeta& table : manifest.level1_tables) {
    live.insert(table.number);
  }

  std::error_code error;
  std::filesystem::directory_iterator iterator(directory, error);
  if (error) return;

  for (const auto& entry : iterator) {
    std::uint64_t number = 0;
    NumberedFileType type = NumberedFileType::kWal;
    if (!parseNumberedFileName(entry.path(), number, type)) continue;
    if (type == NumberedFileType::kSSTable && live.count(number) != 0) {
      continue;
    }
    if (type == NumberedFileType::kSSTable ||
        type == NumberedFileType::kSSTableTemporary) {
      removeFileBestEffort(entry.path());
    }
  }
}

}
