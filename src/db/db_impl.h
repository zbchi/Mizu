#pragma once

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "db/db_files.h"
#include "db/manifest.h"
#include "db/memtable.h"
#include "db/version.h"
#include "lsmtree/db.h"

namespace lsmtree {

class WalWriter;
class DBImpl final : public DB {
 public:
  static Status open(const DBOptions& options,
                     const std::filesystem::path& directory, DB::Handle* db);

  DBImpl() = default;
  ~DBImpl() override;

  Status write(const WriteOptions& options, const WriteBatch& batch) override;
  Status get(const ReadOptions& options, Slice key,
             std::string* value) const override;
  Status newSnapshot(SnapshotHandle* snapshot) const override;
  Status newIterator(const ReadOptions& options,
                     std::unique_ptr<Iterator>* iterator) const override;

 private:
  struct ImmutableMemTable {
    std::unique_ptr<MemTable> memtable;
    std::uint64_t table_number = 0;
    SequenceNumber last_sequence = 0;
  };

  Status initializeNewDatabase(const DirectoryContents& files);
  Status recoverDatabase(const DirectoryContents& files);
  // 恢复阶段打开 Manifest 引用的完整磁盘状态
  Status loadVersion();
  // 从 Manifest 指定的 WAL 下界开始顺序重放
  Status recoverWalFiles(const std::vector<std::uint64_t>& wal_numbers);
  // 重放单个 WAL 并截断崩溃留下的不完整尾部
  Status recoverWalFile(const std::filesystem::path& path);
  // 写入新 batch 前等待或轮转已经达到上限的 MemTable
  Status makeRoomForWrite(std::unique_lock<std::shared_mutex>& lock);
  // 创建新 WAL 并将当前 MemTable 原子切换为 immutable
  Status rotateMemTable();
  // 后台线程串行处理 immutable flush 和 L0 compaction
  void backgroundLoop();
  bool needsLevel0Compaction() const noexcept;
  Status flushImmutableMemTable();
  // 构建并原子发布一次 L0 到 L1 的压缩结果
  Status compactLevel0();
  // 使用给定起始序号将完整 batch 写入 MemTable
  void applyBatch(const WriteBatch& batch, SequenceNumber first_sequence);

  DBOptions options_;
  std::filesystem::path directory_;
  FileLock lock_;
  std::unique_ptr<WalWriter> wal_;
  std::uint64_t wal_number_ = 0;
  std::uint64_t next_file_number_ = 1;
  ManifestState manifest_;
  std::shared_ptr<const Version> current_version_;
  SequenceNumber last_sequence_ = 0;
  mutable std::shared_mutex mutex_;
  std::unique_ptr<MemTable> memtable_ = std::make_unique<MemTable>();
  std::optional<ImmutableMemTable> immutable_;
  std::condition_variable_any background_cv_;
  std::thread background_thread_;
  bool shutting_down_ = false;
  Status background_error_;
};

}
