#include "db/db_impl.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <system_error>
#include <sys/file.h>
#include <unistd.h>
#include <utility>

#include "db/filename.h"
#include "db/write_batch_internal.h"

namespace lsmtree {
namespace {

Status filesystemError(const char* operation, const std::filesystem::path& path,
                       const std::error_code& error) {
  return Status::ioError(std::string(operation) + " " + path.string() + ": " +
                         error.message());
}

Status posixError(const char* operation, const std::filesystem::path& path) {
  return Status::ioError(std::string(operation) + " " + path.string() + ": " +
                         std::strerror(errno));
}

// 根据打开模式校验或创建数据库目录
Status prepareDirectory(OpenMode mode, const std::filesystem::path& directory) {
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

// 用非阻塞排他锁保证同一目录一次只被一个 DB 实例打开
Status acquireDatabaseLock(const std::filesystem::path& db_directory,
                           FileLock& lock) {
  const std::filesystem::path lock_path = lockFileName(db_directory);
  const int fd = open(lock_path.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd < 0) return posixError("open", lock_path);

  if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
    const int saved_errno = errno;
    close(fd);
    if (saved_errno == EWOULDBLOCK || saved_errno == EAGAIN) {
      return Status::busy("database is already open: " + db_directory.string());
    }
    errno = saved_errno;
    return posixError("lock", lock_path);
  }

  lock = FileLock(fd);
  return Status::success();
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

Status DBImpl::open(const DBOptions& options,
                    const std::filesystem::path& directory, DB::Handle* db) {
  if (db == nullptr) return Status::invalidArgument("db must not be null");
  if (options.write_buffer_size == 0) {
    return Status::invalidArgument("write_buffer_size must be positive");
  }
  if (options.target_sstable_size == 0) {
    return Status::invalidArgument("target_sstable_size must be positive");
  }

  Status status = prepareDirectory(options.open_mode, directory);
  if (!status.ok()) return status;

  auto impl = std::make_unique<DBImpl>();
  status = acquireDatabaseLock(directory, impl->lock_);
  if (!status.ok()) return status;

  *db = std::move(impl);
  return Status::success();
}

Status DBImpl::write(const WriteOptions& options, const WriteBatch& batch) {
  static_cast<void>(options);  // WAL 接入后由它实现 durability 语义

  // 在同一把写锁内按记录顺序应用整个 batch
  std::unique_lock<std::shared_mutex> lock(mutex_);
  for (const auto& operation : batch.rep_->operations) {
    if (operation.type == WriteBatch::Rep::OperationType::kPut) {
      data_[operation.key] = operation.value;
    } else {
      data_.erase(operation.key);
    }
  }
  return Status::success();
}

Status DBImpl::get(const ReadOptions& options, Slice key, std::string* value) const {
  if (value == nullptr) return Status::invalidArgument("value must not be null");
  if (options.snapshot) {
    return Status::notSupported("snapshots are not implemented yet");
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto it = data_.find(key);
  if (it == data_.end()) return Status::notFound("key does not exist");

  *value = it->second;
  return Status::success();
}

Status DBImpl::newSnapshot(SnapshotHandle* snapshot) const {
  static_cast<void>(snapshot);
  return Status::notSupported("snapshots are not implemented yet");
}

Status DBImpl::newIterator(const ReadOptions& options,
                           std::unique_ptr<Iterator>* iterator) const {
  static_cast<void>(options);
  static_cast<void>(iterator);
  return Status::notSupported("iterators are not implemented yet");
}

}
