#include "db/db_impl.h"

#include <algorithm>
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

}

DBImpl::~DBImpl() {
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    shutting_down_ = true;
    background_cv_.notify_all();
  }
  if (background_thread_.joinable()) background_thread_.join();
}

// 恢复或创建完整数据库状态 成功后才向调用方发布 handle
Status DBImpl::open(const DBOptions& options,
                    const std::filesystem::path& directory, DB::Handle* db) {
  if (db == nullptr) return Status::invalidArgument("db must not be null");
  *db = nullptr;
  if (options.write_buffer_size == 0) {
    return Status::invalidArgument("write_buffer_size must be positive");
  }
  Status status = prepareDatabaseDirectory(options.open_mode, directory);
  if (!status.ok()) return status;

  auto impl = std::make_unique<DBImpl>();
  status = acquireDatabaseLock(directory, impl->lock_);
  if (!status.ok()) return status;

  impl->options_ = options;
  impl->directory_ = directory;

  DirectoryContents files;
  status = scanDatabaseDirectory(directory, files);
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
    status = impl->recoverDatabase(files);
  } else {
    status = impl->initializeNewDatabase(files);
  }
  if (!status.ok()) return status;

  try {
    impl->background_thread_ =
        std::thread(&DBImpl::backgroundLoop, impl.get());
  } catch (const std::system_error& error) {
    return Status::ioError("start background flush thread: " +
                           std::string(error.what()));
  }

  *db = std::move(impl);
  return Status::success();
}

// 无 Manifest 时只允许从空的编号文件集初始化数据库
Status DBImpl::initializeNewDatabase(const DirectoryContents& files) {
  if (!files.wal_numbers.empty() || files.has_sstable) {
    return Status::corruption("database files exist without a manifest");
  }

  const std::uint64_t wal_number = next_file_number_++;
  Status status = WalWriter::open(walFileName(directory_, wal_number), wal_);
  if (!status.ok()) return status;

  wal_number_ = wal_number;
  manifest_.oldest_wal_number = wal_number;
  return writeManifest(manifestFileName(directory_),
                       manifestTemporaryFileName(directory_), manifest_);
}

// 已有 Manifest 时依次恢复 L0 serving state 和仍然存活的 WAL
Status DBImpl::recoverDatabase(const DirectoryContents& files) {
  Status status = readManifest(manifestFileName(directory_), manifest_);
  if (!status.ok()) return status;

  status = loadLevel0Tables();
  if (!status.ok()) return status;
  return recoverWalFiles(files.wal_numbers);
}

// 写入前检查是否需要轮转 WAL 成功后才更新 MemTable
Status DBImpl::write(const WriteOptions& options, const WriteBatch& batch) {
  if (batch.empty()) return Status::success();

  // 在同一把写锁内按记录顺序应用整个 batch
  std::unique_lock<std::shared_mutex> lock(mutex_);
  Status status = makeRoomForWrite(lock);
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

// MemTable 达到上限时等待前一次 flush 或快速切换到新 WAL
Status DBImpl::makeRoomForWrite(
    std::unique_lock<std::shared_mutex>& lock) {
  while (true) {
    if (!background_error_.ok()) return background_error_;
    if (memtable_->empty() ||
        memtable_->memoryUsage() < options_.write_buffer_size) {
      return Status::success();
    }
    if (!immutable_) return rotateMemTable();

    // 最多允许一个 immutable MemTable 防止内存和 WAL 无界增长
    background_cv_.wait(lock);
  }
}

// 先准备好新 MemTable 和 WAL 再一次性发布轮转后的内存状态
Status DBImpl::rotateMemTable() {
  if (next_file_number_ > std::numeric_limits<std::uint64_t>::max() - 2U) {
    return Status::ioError("database file number space is exhausted");
  }

  const std::uint64_t table_number = next_file_number_;
  const std::uint64_t new_wal_number = next_file_number_ + 1U;
  auto new_memtable = std::make_unique<MemTable>();
  std::unique_ptr<WalWriter> new_wal;
  Status status =
      WalWriter::open(walFileName(directory_, new_wal_number), new_wal);
  if (!status.ok()) return status;

  next_file_number_ += 2U;
  immutable_.emplace(ImmutableMemTable{std::move(memtable_), table_number,
                                       last_sequence_});
  memtable_ = std::move(new_memtable);
  wal_ = std::move(new_wal);
  wal_number_ = new_wal_number;
  background_cv_.notify_all();
  return Status::success();
}

// 唯一后台线程串行消费 immutable MemTable
void DBImpl::backgroundLoop() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  while (true) {
    background_cv_.wait(
        lock, [this] { return shutting_down_ || immutable_.has_value(); });
    if (!immutable_) return;

    lock.unlock();
    const Status status = flushImmutableMemTable();
    lock.lock();

    if (!status.ok()) {
      background_error_ = status;
      background_cv_.notify_all();
      return;
    }
    if (shutting_down_ && !immutable_) return;
  }
}

// 生成 SST 和 Manifest 时 immutable 始终保留在读取路径中
Status DBImpl::flushImmutableMemTable() {
  const std::uint64_t table_number = immutable_->table_number;
  const SequenceNumber flushed_sequence = immutable_->last_sequence;
  const std::filesystem::path temporary_table =
      sstableTemporaryFileName(directory_, table_number);
  const std::filesystem::path final_table =
      sstableFileName(directory_, table_number);

  SSTableMeta table_meta;
  Status status =
      buildLevel0Table(*immutable_->memtable, temporary_table, final_table,
                       SSTableBuilderOptions{}, table_meta);
  if (!status.ok()) return status;

  std::unique_ptr<SSTableReader> table_reader;
  status = SSTableReader::open(final_table, table_reader);
  if (!status.ok()) {
    removeFileBestEffort(final_table);
    return status;
  }

  ManifestTable descriptor{table_number, table_meta.file_size,
                           table_meta.smallest_key, table_meta.largest_key};
  ManifestState candidate;
  std::uint64_t live_wal_number = 0;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    candidate = manifest_;
    candidate.flushed_sequence = flushed_sequence;
    candidate.oldest_wal_number = wal_number_;
    candidate.level0_tables.insert(candidate.level0_tables.begin(),
                                   descriptor);
    live_wal_number = wal_number_;

    // Manifest 提交后发布 L0 不再分配 vector 存储
    level0_tables_.reserve(level0_tables_.size() + 1U);
  }

  status = writeManifest(manifestFileName(directory_),
                         manifestTemporaryFileName(directory_), candidate);
  if (!status.ok()) {
    removeFileBestEffort(final_table);
    return status;
  }

  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    // Manifest 已提交 后续内存发布不会执行可能失败的文件操作
    level0_tables_.insert(
        level0_tables_.begin(),
        L0Table{std::move(descriptor), std::move(table_reader)});
    manifest_ = std::move(candidate);
    immutable_.reset();
    background_cv_.notify_all();
  }

  removeObsoleteWalFilesBestEffort(directory_, live_wal_number);
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

// 按 mutable、immutable、L0 新文件到旧文件的顺序执行点查
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

  if (immutable_) {
    result = immutable_->memtable->get(key, visible_sequence, value);
    if (result == LookupResult::kValue) return Status::success();
    if (result == LookupResult::kDeleted) {
      return Status::notFound("key does not exist");
    }
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
