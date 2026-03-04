#pragma once

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "db/manifest.h"
#include "db/memtable.h"
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

class WalWriter;
class SSTableReader;

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
  struct L0Table {
    ManifestTable meta;
    std::unique_ptr<SSTableReader> reader;
  };

  // 恢复阶段按 Manifest 顺序打开全部 L0 文件
  Status loadLevel0Tables();
  // 从 Manifest 指定的 WAL 下界开始顺序重放
  Status recoverWalFiles(const std::vector<std::uint64_t>& wal_numbers);
  // 重放单个 WAL 并截断崩溃留下的不完整尾部
  Status recoverWalFile(const std::filesystem::path& path);
  // 写入新 batch 前检查是否需要同步 checkpoint
  Status makeRoomForWrite();
  // 将当前 MemTable 发布为一个 L0 SST 并切换 WAL
  Status checkpointMemTable();
  // 使用给定起始序号将完整 batch 写入 MemTable
  void applyBatch(const WriteBatch& batch, SequenceNumber first_sequence);

  DBOptions options_;
  std::filesystem::path directory_;
  FileLock lock_;
  std::unique_ptr<WalWriter> wal_;
  std::uint64_t wal_number_ = 0;
  std::uint64_t next_file_number_ = 1;
  ManifestState manifest_;
  std::vector<L0Table> level0_tables_;
  SequenceNumber last_sequence_ = 0;
  mutable std::shared_mutex mutex_;
  std::unique_ptr<MemTable> memtable_ = std::make_unique<MemTable>();
};

}
