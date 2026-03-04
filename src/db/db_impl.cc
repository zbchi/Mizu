#include "db/db_impl.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <mutex>
#include <system_error>
#include <utility>
#include <vector>

#include "db/filename.h"
#include "db/flush_memtable.h"
#include "db/write_batch_codec.h"
#include "db/write_batch_internal.h"
#include "table/sstable_reader.h"
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

struct DirectoryContents {
  std::vector<std::uint64_t> wal_numbers;
  bool has_sstable = false;
  std::uint64_t maximum_number = 0;
};

// 扫描目录中的编号文件并收集 WAL 与最大文件编号
Status scanDirectory(const std::filesystem::path& directory,
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

// 提交前失败时最佳努力删除尚未发布的文件
void removeUnpublishedFilesBestEffort(
    const std::filesystem::path& table_path,
    const std::filesystem::path& wal_path) {
  std::error_code ignored;
  std::filesystem::remove(table_path, ignored);
  ignored.clear();
  std::filesystem::remove(wal_path, ignored);
}

// Manifest 提交后最佳努力删除不再参与恢复的旧 WAL
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

// 恢复或创建完整数据库状态 成功后才向调用方发布 handle
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

  impl->options_ = options;
  impl->directory_ = directory;

  DirectoryContents files;
  status = scanDirectory(directory, files);
  if (!status.ok()) return status;
  if (files.maximum_number == std::numeric_limits<std::uint64_t>::max()) {
    return Status::corruption("database file number space is exhausted");
  }
  impl->next_file_number_ = files.maximum_number + 1U;

  const std::filesystem::path manifest_path = manifestFileName(directory);
  std::error_code manifest_error;
  const bool has_manifest =
      std::filesystem::exists(manifest_path, manifest_error);
  if (manifest_error) {
    return filesystemError("stat", manifest_path, manifest_error);
  }

  if (has_manifest) {
    status = readManifest(manifest_path, impl->manifest_);
    if (!status.ok()) return status;
    status = impl->loadLevel0Tables();
    if (!status.ok()) return status;
    status = impl->recoverWalFiles(files.wal_numbers);
    if (!status.ok()) return status;
  } else {
    if (!files.wal_numbers.empty() || files.has_sstable) {
      return Status::corruption("database files exist without a manifest");
    }
    const std::uint64_t wal_number = impl->next_file_number_++;
    status = WalWriter::open(walFileName(directory, wal_number), impl->wal_);
    if (!status.ok()) return status;
    impl->wal_number_ = wal_number;
    impl->manifest_.oldest_wal_number = wal_number;
    status = writeManifest(manifest_path, manifestTemporaryFileName(directory),
                           impl->manifest_);
    if (!status.ok()) return status;
  }

  *db = std::move(impl);
  return Status::success();
}

// 写入前检查是否需要 checkpoint WAL 成功后才更新 MemTable
Status DBImpl::write(const WriteOptions& options, const WriteBatch& batch) {
  if (batch.empty()) return Status::success();

  // 在同一把写锁内按记录顺序应用整个 batch
  std::unique_lock<std::shared_mutex> lock(mutex_);
  Status status = makeRoomForWrite();
  if (!status.ok()) return status;

  const SequenceNumber first_sequence = last_sequence_ + 1U;

  std::string payload;
  status = WriteBatchCodec::encode(batch, first_sequence, payload);
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

// 按 Manifest 顺序打开 L0 全部成功后再发布读取状态
Status DBImpl::loadLevel0Tables() {
  std::vector<L0Table> loaded;
  loaded.reserve(manifest_.level0_tables.size());

  for (const ManifestTable& descriptor : manifest_.level0_tables) {
    const std::filesystem::path path =
        sstableFileName(directory_, descriptor.number);
    std::error_code error;
    const bool exists = std::filesystem::exists(path, error);
    if (error) return filesystemError("stat", path, error);
    if (!exists) {
      return Status::corruption("manifest references a missing SSTable: " +
                                path.string());
    }
    const std::uintmax_t size = std::filesystem::file_size(path, error);
    if (error) return filesystemError("stat", path, error);
    if (size != descriptor.file_size) {
      return Status::corruption("SSTable size does not match manifest: " +
                                path.string());
    }

    std::unique_ptr<SSTableReader> reader;
    Status status = SSTableReader::open(path, reader);
    if (!status.ok()) return status;
    loaded.push_back(L0Table{descriptor, std::move(reader)});
  }

  level0_tables_ = std::move(loaded);
  return Status::success();
}

// 从最老有效 WAL 开始恢复并继续使用最大编号 WAL
Status DBImpl::recoverWalFiles(const std::vector<std::uint64_t>& wal_numbers) {
  if (manifest_.oldest_wal_number == 0) {
    return Status::corruption("manifest has no live WAL number");
  }

  const auto first = std::lower_bound(wal_numbers.begin(), wal_numbers.end(),
                                      manifest_.oldest_wal_number);
  if (first == wal_numbers.end() || *first != manifest_.oldest_wal_number) {
    return Status::corruption("manifest references a missing WAL");
  }

  last_sequence_ = manifest_.flushed_sequence;
  for (auto iterator = first; iterator != wal_numbers.end(); ++iterator) {
    Status status = recoverWalFile(walFileName(directory_, *iterator));
    if (!status.ok()) return status;
    wal_number_ = *iterator;
  }

  return WalWriter::open(walFileName(directory_, wal_number_), wal_);
}

// 重放单个 WAL 并丢弃最后一条不完整记录
Status DBImpl::recoverWalFile(const std::filesystem::path& path) {
  std::error_code error;

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

// MemTable 达到上限时在下一次写入前执行 checkpoint
Status DBImpl::makeRoomForWrite() {
  if (memtable_->empty() ||
      memtable_->memoryUsage() < options_.write_buffer_size) {
    return Status::success();
  }
  return checkpointMemTable();
}

// 同步生成一个 L0 SST 并以 Manifest 替换作为提交点
Status DBImpl::checkpointMemTable() {
  if (next_file_number_ > std::numeric_limits<std::uint64_t>::max() - 2U) {
    return Status::ioError("database file number space is exhausted");
  }

  const std::uint64_t table_number = next_file_number_++;
  const std::uint64_t new_wal_number = next_file_number_++;
  const std::filesystem::path temporary_table =
      sstableTemporaryFileName(directory_, table_number);
  const std::filesystem::path final_table =
      sstableFileName(directory_, table_number);
  const std::filesystem::path new_wal_path =
      walFileName(directory_, new_wal_number);

  SSTableMeta table_meta;
  Status status = buildLevel0Table(*memtable_, temporary_table, final_table,
                                   SSTableBuilderOptions{}, table_meta);
  if (!status.ok()) return status;

  std::unique_ptr<SSTableReader> table_reader;
  status = SSTableReader::open(final_table, table_reader);
  if (!status.ok()) {
    std::error_code ignored;
    std::filesystem::remove(final_table, ignored);
    return status;
  }

  std::unique_ptr<WalWriter> new_wal;
  status = WalWriter::open(new_wal_path, new_wal);
  if (!status.ok()) {
    removeUnpublishedFilesBestEffort(final_table, new_wal_path);
    return status;
  }

  ManifestTable descriptor{table_number, table_meta.file_size,
                           table_meta.smallest_key, table_meta.largest_key};
  ManifestState candidate = manifest_;
  candidate.flushed_sequence = last_sequence_;
  candidate.oldest_wal_number = new_wal_number;
  candidate.level0_tables.insert(candidate.level0_tables.begin(), descriptor);

  // 在提交 Manifest 前完成所有可能分配内存的准备
  auto new_memtable = std::make_unique<MemTable>();
  level0_tables_.reserve(level0_tables_.size() + 1U);
  status = writeManifest(manifestFileName(directory_),
                         manifestTemporaryFileName(directory_), candidate);
  if (!status.ok()) {
    removeUnpublishedFilesBestEffort(final_table, new_wal_path);
    return status;
  }

  // Manifest 已提交 后续只做不会失败的内存发布和最佳努力清理
  wal_ = std::move(new_wal);
  wal_number_ = new_wal_number;
  memtable_ = std::move(new_memtable);
  level0_tables_.insert(
      level0_tables_.begin(),
      L0Table{std::move(descriptor), std::move(table_reader)});
  manifest_ = std::move(candidate);
  removeObsoleteWalFilesBestEffort(directory_, new_wal_number);
  return Status::success();
}

// batch 中的每个操作依次占用一个 sequence
void DBImpl::applyBatch(const WriteBatch& batch,
                        SequenceNumber first_sequence) {
  SequenceNumber sequence = first_sequence;
  for (const auto& operation : batch.rep_->operations) {
    if (operation.type == WriteBatch::Rep::OperationType::kPut) {
      memtable_->add(sequence, ValueType::kValue, operation.key,
                     operation.value);
    } else {
      memtable_->add(sequence, ValueType::kDeletion, operation.key, {});
    }
    ++sequence;
  }
}

// 按 MemTable 到 L0 新文件到旧文件的顺序执行点查
Status DBImpl::get(const ReadOptions& options, Slice key,
                   std::string* value) const {
  if (value == nullptr)
    return Status::invalidArgument("value must not be null");

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

  LookupResult result = memtable_->get(key, visible_sequence, value);
  if (result == LookupResult::kValue) return Status::success();
  if (result == LookupResult::kDeleted) {
    return Status::notFound("key does not exist");
  }

  for (const L0Table& table : level0_tables_) {
    Status status =
        table.reader->get(options, key, visible_sequence, result, *value);
    if (!status.ok()) return status;
    if (result == LookupResult::kValue) return Status::success();
    if (result == LookupResult::kDeleted) {
      return Status::notFound("key does not exist");
    }
  }
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
