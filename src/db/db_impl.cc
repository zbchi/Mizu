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
#include "db/write_batch_codec.h"
#include "db/write_batch_internal.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"

namespace lsmtree {
namespace {

// 快照只保存创建时已经提交的最大 sequence
class SnapshotImpl final : public Snapshot {
 public:
  explicit SnapshotImpl(SequenceNumber sequence) : sequence(sequence) {}

  SequenceNumber sequence;
};

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

DBImpl::~DBImpl() = default;

Status DBImpl::open(const DBOptions& options,
                    const std::filesystem::path& directory, DB::Handle* db) {
  if (db == nullptr) return Status::invalidArgument("db must not be null");
  *db = nullptr;
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

  impl->directory_ = directory;
  status = impl->recoverWal();
  if (!status.ok()) return status;

  status = WalWriter::open(walFileName(directory), impl->wal_);
  if (!status.ok()) return status;

  *db = std::move(impl);
  return Status::success();
}

Status DBImpl::write(const WriteOptions& options, const WriteBatch& batch) {
  if (batch.empty()) return Status::success();

  // 在同一把写锁内按记录顺序应用整个 batch
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const SequenceNumber first_sequence = last_sequence_ + 1U;

  std::string payload;
  Status status = WriteBatchCodec::encode(batch, first_sequence, payload);
  if (!status.ok()) return status;

  // 先记录 WAL 再更新内存 避免内存状态领先于日志
  status = wal_->append(payload);
  if (!status.ok()) return status;

  // kSync 在修改内存前等待日志持久化
  if (options.durability == Durability::kSync) {
    status = wal_->sync();
    if (!status.ok()) return status;
  }

  applyBatch(batch, first_sequence);
  last_sequence_ += batch.count();
  return Status::success();
}

Status DBImpl::recoverWal() {
  const std::filesystem::path path = walFileName(directory_);
  std::error_code error;
  const bool exists = std::filesystem::exists(path, error);
  if (error) return filesystemError("stat", path, error);
  if (!exists) return Status::success();

  std::unique_ptr<WalReader> reader;
  Status status = WalReader::open(path, reader);
  if (!status.ok()) return status;

  // 按日志顺序重放 batch 并校验 sequence 连续递增
  while (true) {
    std::string payload;
    WalReadResult result = WalReadResult::kEnd;
    status = reader->readNext(payload, result);
    if (!status.ok()) return status;
    if (result == WalReadResult::kEnd) break;

    WriteBatch batch;
    SequenceNumber first_sequence = 0;
    status = WriteBatchCodec::decode(payload, first_sequence, batch);
    if (!status.ok()) return status;
    if (first_sequence != last_sequence_ + 1U) {
      return Status::corruption("write batch sequence is not contiguous");
    }

    applyBatch(batch, first_sequence);
    last_sequence_ += batch.count();
  }

  const std::uint64_t valid_bytes = reader->validBytes();
  reader.reset();

  const std::uintmax_t file_size = std::filesystem::file_size(path, error);
  if (error) return filesystemError("stat", path, error);
  // 丢弃崩溃留下的不完整尾部 让后续 append 紧接最后一条完整记录
  if (file_size > valid_bytes) {
    std::filesystem::resize_file(path, valid_bytes, error);
    if (error) return filesystemError("truncate", path, error);
  }

  return Status::success();
}

void DBImpl::applyBatch(const WriteBatch& batch,
                        SequenceNumber first_sequence) {
  // batch 中的每个操作依次占用一个 sequence
  SequenceNumber sequence = first_sequence;
  for (const auto& operation : batch.rep_->operations) {
    if (operation.type == WriteBatch::Rep::OperationType::kPut) {
      memtable_.add(sequence, ValueType::kValue, operation.key,
                    operation.value);
    } else {
      memtable_.add(sequence, ValueType::kDeletion, operation.key, {});
    }
    ++sequence;
  }
}

Status DBImpl::get(const ReadOptions& options, Slice key, std::string* value) const {
  if (value == nullptr) return Status::invalidArgument("value must not be null");

  SequenceNumber visible_sequence = 0;
  if (options.snapshot) {
    const auto snapshot =
        std::dynamic_pointer_cast<const SnapshotImpl>(options.snapshot);
    if (!snapshot) {
      return Status::invalidArgument("invalid snapshot");
    }
    visible_sequence = snapshot->sequence;
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (!options.snapshot) visible_sequence = last_sequence_;

  const LookupResult result = memtable_.get(key, visible_sequence, value);
  if (result == LookupResult::kValue) return Status::success();
  return Status::notFound("key does not exist");
}

Status DBImpl::newSnapshot(SnapshotHandle* snapshot) const {
  if (snapshot == nullptr) {
    return Status::invalidArgument("snapshot must not be null");
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);
  *snapshot = std::make_shared<SnapshotImpl>(last_sequence_);
  return Status::success();
}

Status DBImpl::newIterator(const ReadOptions& options,
                           std::unique_ptr<Iterator>* iterator) const {
  static_cast<void>(options);
  static_cast<void>(iterator);
  return Status::notSupported("iterators are not implemented yet");
}

}
