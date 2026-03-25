#include "db/db_impl.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <mutex>
#include <system_error>
#include <utility>
#include <vector>

#include "db/compaction.h"
#include "db/db_iterator.h"
#include "db/filename.h"
#include "db/flush_memtable.h"
#include "db/internal_iterator.h"
#include "db/merging_iterator.h"
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
  SnapshotImpl(SequenceNumber sequence,
               std::shared_ptr<SnapshotTracker> tracker)
      : sequence(sequence), tracker_(std::move(tracker)) {
    tracker_->add(sequence);
  }

  ~SnapshotImpl() override { tracker_->remove(sequence); }

  bool isFrom(const std::shared_ptr<SnapshotTracker>& tracker) const noexcept {
    return tracker_ == tracker;
  }

  SequenceNumber sequence;

 private:
  std::shared_ptr<SnapshotTracker> tracker_;
};

}  // namespace

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
    impl->background_thread_ = std::thread(&DBImpl::backgroundLoop, impl.get());
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
  status = writeManifest(manifestFileName(directory_),
                         manifestTemporaryFileName(directory_), manifest_);
  if (!status.ok()) return status;
  status = loadVersion();
  if (!status.ok()) return status;
  removeObsoleteSSTableFilesBestEffort(directory_, manifest_);
  return Status::success();
}

// 已有 Manifest 时依次恢复 L0 serving state 和仍然存活的 WAL
Status DBImpl::recoverDatabase(const DirectoryContents& files) {
  Status status = readManifest(manifestFileName(directory_), manifest_);
  if (!status.ok()) return status;

  status = loadVersion();
  if (!status.ok()) return status;
  status = recoverWalFiles(files.wal_numbers);
  if (!status.ok()) return status;
  // 多个 WAL 可能共同承载尚未 flush 的连续数据。恢复把它们合并进一个
  // MemTable 后，Manifest 仍引用原来的 replay 下界；在下一次 flush 提交前
  // 只能删除低于该边界的 WAL，否则再次崩溃会缺失恢复输入。
  removeObsoleteWalFilesBestEffort(directory_, manifest_.oldest_wal_number);
  removeObsoleteSSTableFilesBestEffort(directory_, manifest_);
  return Status::success();
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

Status DBImpl::loadVersion() {
  return Version::open(directory_, manifest_, current_version_);
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
Status DBImpl::makeRoomForWrite(std::unique_lock<std::shared_mutex>& lock) {
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
  auto new_memtable = std::make_shared<MemTable>();
  std::unique_ptr<WalWriter> new_wal;
  Status status =
      WalWriter::open(walFileName(directory_, new_wal_number), new_wal);
  if (!status.ok()) return status;

  next_file_number_ += 2U;
  immutable_.emplace(
      ImmutableMemTable{std::move(memtable_), table_number, last_sequence_});
  memtable_ = std::move(new_memtable);
  wal_ = std::move(new_wal);
  wal_number_ = new_wal_number;
  background_cv_.notify_all();
  return Status::success();
}

bool DBImpl::hasCompactionWork() const noexcept {
  return current_version_ && needsCompaction(*current_version_);
}

// 唯一后台线程串行执行相邻层压缩和 immutable MemTable flush
void DBImpl::backgroundLoop() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  while (true) {
    background_cv_.wait(lock, [this] {
      return shutting_down_ || immutable_.has_value() || hasCompactionWork();
    });
    if (shutting_down_ && !immutable_) return;

    // 达到层级阈值后先压缩，限制 L0 文件数和 L1 字节数；关闭时只完成
    // 已经存在的 immutable flush，不额外启动 compaction。
    const bool should_compact = !shutting_down_ && hasCompactionWork();

    lock.unlock();
    const Status status = should_compact ? compact() : flushImmutableMemTable();
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

  TableMeta descriptor{table_number, table_meta.file_size,
                       table_meta.smallest_key, table_meta.largest_key};
  ManifestState candidate;
  std::shared_ptr<const Version> candidate_version;
  std::uint64_t live_wal_number = 0;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    candidate = manifest_;
    candidate.flushed_sequence = flushed_sequence;
    candidate.oldest_wal_number = wal_number_;
    auto& level0 = candidate.levels[kLevel0];
    level0.insert(level0.begin(), descriptor);
    live_wal_number = wal_number_;
    candidate_version =
        current_version_->withLevel0Table(descriptor, std::move(table_reader));
  }

  status = writeManifest(manifestFileName(directory_),
                         manifestTemporaryFileName(directory_), candidate);
  if (!status.ok()) {
    removeFileBestEffort(final_table);
    return status;
  }

  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    // Manifest 已提交 后续发布只替换已经构造完成的内存状态
    current_version_ = std::move(candidate_version);
    manifest_ = std::move(candidate);
    immutable_.reset();
    background_cv_.notify_all();
  }

  removeObsoleteWalFilesBestEffort(directory_, live_wal_number);
  return Status::success();
}

// 输出文件和候选状态全部准备完成后，先提交 Manifest，再切换内存 Version
Status DBImpl::compact() {
  std::optional<CompactionPlan> plan;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    plan = pickCompaction(current_version_, snapshots_->oldest(last_sequence_));
    if (!plan) return Status::success();
  }

  const FileNumberAllocator allocate_file_number =
      [this](std::uint64_t& number) -> Status {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (next_file_number_ == std::numeric_limits<std::uint64_t>::max()) {
      return Status::ioError("database file number space is exhausted");
    }
    number = next_file_number_++;
    return Status::success();
  };

  std::vector<CompactionOutput> outputs;
  Status status =
      buildCompactionTables(*plan, directory_, allocate_file_number, outputs);
  if (!status.ok()) return status;

  ManifestState candidate;
  std::shared_ptr<const Version> candidate_version;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    // 磁盘 Version 只由这一条后台线程修改，构建期间输入不会被替换
    assert(current_version_ == plan->input_version);
    assert(plan->input_level + 1U < kNumLevels);

    candidate = manifest_;
    auto& source = candidate.levels[plan->input_level];
    auto& target = candidate.levels[plan->outputLevel()];
    assert(plan->inputs.end <= source.size());
    assert(plan->overlaps.end <= target.size());
    source.erase(source.begin() + plan->inputs.begin,
                 source.begin() + plan->inputs.end);
    target.erase(target.begin() + plan->overlaps.begin,
                 target.begin() + plan->overlaps.end);

    std::vector<Version::Table> output_tables;
    output_tables.reserve(outputs.size());
    std::vector<TableMeta> output_metadata;
    output_metadata.reserve(outputs.size());
    for (const CompactionOutput& output : outputs) {
      output_metadata.push_back(output.meta);
      output_tables.push_back({output.meta, output.reader});
    }
    target.insert(target.begin() + plan->overlaps.begin,
                  std::make_move_iterator(output_metadata.begin()),
                  std::make_move_iterator(output_metadata.end()));
    candidate_version = plan->input_version->withCompaction(
        plan->input_level, plan->inputs, plan->overlaps,
        std::move(output_tables));
  }

  status = writeManifest(manifestFileName(directory_),
                         manifestTemporaryFileName(directory_), candidate);
  if (!status.ok()) {
    // Manifest 仍指向旧 Version，新输出都只是未发布文件。
    candidate_version.reset();
    for (CompactionOutput& output : outputs) {
      output.reader.reset();
      removeFileBestEffort(sstableFileName(directory_, output.meta.number));
    }
    return status;
  }

  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    assert(current_version_ == plan->input_version);
    current_version_ = std::move(candidate_version);
    manifest_ = std::move(candidate);
  }
  removeObsoleteSSTableFilesBestEffort(directory_, manifest_);
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

// 按 mutable、immutable、当前磁盘 Version 的顺序执行点查
Status DBImpl::get(const ReadOptions& options, Slice key,
                   std::string* value) const {
  if (value == nullptr)
    return Status::invalidArgument("value must not be null");

  SequenceNumber visible_sequence = 0;
  if (options.snapshot) {
    const auto snapshot =
        std::dynamic_pointer_cast<const SnapshotImpl>(options.snapshot);
    if (!snapshot || !snapshot->isFrom(snapshots_)) {
      return Status::invalidArgument("invalid snapshot");
    }
    visible_sequence = snapshot->sequence;
  }

  std::shared_ptr<const Version> version;
  LookupResult result = LookupResult::kAbsent;
  {
    // 锁内固定可见序号和磁盘 Version 后续 SST I/O 不占用数据库锁
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!options.snapshot) visible_sequence = last_sequence_;

    result = memtable_->get(key, visible_sequence, value);
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
    version = current_version_;
  }

  Status status = version->get(options, key, visible_sequence, result, *value);
  if (!status.ok()) return status;
  if (result == LookupResult::kValue) return Status::success();
  return Status::notFound("key does not exist");
}

Status DBImpl::newSnapshot(SnapshotHandle* snapshot) const {
  if (snapshot == nullptr) {
    return Status::invalidArgument("snapshot must not be null");
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);
  const SequenceNumber sequence = last_sequence_;
  *snapshot = std::make_shared<SnapshotImpl>(sequence, snapshots_);
  return Status::success();
}

Status DBImpl::newIterator(const ReadOptions& options,
                           std::unique_ptr<Iterator>* iterator) const {
  if (iterator == nullptr) {
    return Status::invalidArgument("iterator must not be null");
  }
  *iterator = nullptr;

  SequenceNumber visible_sequence = 0;
  if (options.snapshot) {
    const auto snapshot =
        std::dynamic_pointer_cast<const SnapshotImpl>(options.snapshot);
    if (!snapshot || !snapshot->isFrom(snapshots_)) {
      return Status::invalidArgument("invalid snapshot");
    }
    visible_sequence = snapshot->sequence;
  }

  std::shared_ptr<const MemTable> mutable_memtable;
  std::shared_ptr<const MemTable> immutable_memtable;
  std::shared_ptr<const Version> version;
  {
    // 锁内固定可见序号和全部读取层
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!options.snapshot) visible_sequence = last_sequence_;
    mutable_memtable = memtable_;
    if (immutable_) immutable_memtable = immutable_->memtable;
    version = current_version_;
  }

  std::vector<std::unique_ptr<InternalIterator>> children;
  std::size_t table_count = 0;
  for (std::size_t level = 0; level < kNumLevels; ++level) {
    table_count += version->level(level).size();
  }
  children.reserve(1U + (immutable_memtable ? 1U : 0U) + table_count);
  children.push_back(
      std::make_unique<MemTable::Iterator>(mutable_memtable->newIterator()));
  if (immutable_memtable) {
    children.push_back(std::make_unique<MemTable::Iterator>(
        immutable_memtable->newIterator()));
  }
  for (std::size_t level = 0; level < kNumLevels; ++level) {
    for (const Version::Table& table : version->level(level)) {
      children.push_back(table.reader->newIterator(options));
    }
  }

  auto merged = std::make_unique<MergingIterator>(std::move(children));
  *iterator = std::make_unique<DBIterator>(
      std::move(merged), visible_sequence, std::move(mutable_memtable),
      std::move(immutable_memtable), std::move(version));
  return Status::success();
}

}  // namespace lsmtree
