#include "lsmtree/db.h"

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "db/filename.h"
#include "db/internal_key.h"
#include "db/manifest.h"
#include "db/write_batch_codec.h"
#include "table/sstable_builder.h"
#include "table/sstable_reader.h"
#include "test.h"
#include "wal/wal_writer.h"

namespace lsmtree {
namespace {

class TempDirectory {
 public:
  TempDirectory() {
    // 组合进程号和单调时钟 避免并行测试共用同一目录
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
  }

  ~TempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

DB::Handle openOrCreate(const std::filesystem::path& path) {
  DB::Handle db;
  DBOptions options;
  options.open_mode = OpenMode::kOpenOrCreate;
  ASSERT_OK(DB::open(options, path, &db));
  return db;
}

DB::Handle openWithTinyWriteBuffer(const std::filesystem::path& path) {
  DB::Handle db;
  DBOptions options;
  options.open_mode = OpenMode::kOpenOrCreate;
  options.write_buffer_size = 1;
  ASSERT_OK(DB::open(options, path, &db));
  return db;
}

std::string get(DB& db, Slice key) {
  std::string value;
  ASSERT_OK(db.get({}, key, &value));
  return value;
}

std::vector<std::pair<std::string, std::string>> collect(Iterator& iterator) {
  std::vector<std::pair<std::string, std::string>> entries;
  iterator.seekToFirst();
  while (iterator.valid()) {
    entries.emplace_back(iterator.key(), iterator.value());
    iterator.next();
  }
  ASSERT_OK(iterator.status());
  return entries;
}

void flipLastByte(const std::filesystem::path& path) {
  std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(file.is_open());

  file.seekg(-1, std::ios::end);
  char byte = 0;
  file.read(&byte, 1);
  ASSERT_TRUE(file.good());

  byte ^= 1;
  file.seekp(-1, std::ios::end);
  file.write(&byte, 1);
  ASSERT_TRUE(file.good());
}

void appendEncodedBatch(const std::filesystem::path& path,
                        SequenceNumber first_sequence,
                        const WriteBatch& batch) {
  std::string payload;
  ASSERT_OK(WriteBatchCodec::encode(batch, first_sequence, payload));

  std::unique_ptr<WalWriter> writer;
  ASSERT_OK(WalWriter::open(path, writer));
  ASSERT_OK(writer->append(payload));
}

TableMeta buildSingleEntryTable(const std::filesystem::path& directory,
                                std::uint64_t number, Slice key,
                                SequenceNumber sequence, Slice value) {
  std::unique_ptr<SSTableBuilder> builder;
  ASSERT_OK(
      SSTableBuilder::open(sstableFileName(directory, number), {}, builder));
  ASSERT_OK(
      builder->add(encodeInternalKey(key, sequence, ValueType::kValue), value));
  SSTableMeta completed;
  ASSERT_OK(builder->finish(completed));
  return TableMeta{number, completed.file_size, completed.smallest_key,
                   completed.largest_key};
}

ManifestState waitForCompaction(const std::filesystem::path& directory,
                                std::uint64_t previous_output = 0) {
  for (int attempt = 0; attempt < 2000; ++attempt) {
    ManifestState state;
    ASSERT_OK(readManifest(manifestFileName(directory), state));
    if (state.levels[kLevel0].empty() && state.levels[kLevel1].size() == 1U &&
        state.levels[kLevel1][0].number != previous_output) {
      return state;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(false);
  return {};
}

ManifestState waitForLevel1Compaction(const std::filesystem::path& directory) {
  for (int attempt = 0; attempt < 5000; ++attempt) {
    ManifestState state;
    ASSERT_OK(readManifest(manifestFileName(directory), state));
    if (state.levels[kLevel1].empty() && !state.levels[kLevel2].empty()) {
      return state;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(false);
  return {};
}

bool hasOnlyManifestSSTables(const std::filesystem::path& directory,
                             const ManifestState& manifest) {
  std::set<std::uint64_t> live;
  for (const auto& level : manifest.levels) {
    for (const TableMeta& table : level) live.insert(table.number);
  }

  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    std::uint64_t number = 0;
    NumberedFileType type = NumberedFileType::kWal;
    if (!parseNumberedFileName(entry.path(), number, type)) continue;
    if (type == NumberedFileType::kSSTableTemporary) return false;
    if (type == NumberedFileType::kSSTable && live.count(number) == 0) {
      return false;
    }
  }
  return true;
}

void waitForObsoleteSSTableCleanup(const std::filesystem::path& directory,
                                   const ManifestState& manifest) {
  for (int attempt = 0; attempt < 2000; ++attempt) {
    if (hasOnlyManifestSSTables(directory, manifest)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(false);
}

std::size_t countLevel1Versions(const std::filesystem::path& directory,
                                const ManifestState& manifest, Slice key) {
  std::size_t count = 0;
  for (const TableMeta& meta : manifest.levels[kLevel1]) {
    std::unique_ptr<SSTableReader> reader;
    ASSERT_OK(
        SSTableReader::open(sstableFileName(directory, meta.number), reader));
    auto iterator = reader->newIterator({});
    iterator->seekToFirst();
    while (iterator->valid()) {
      ParsedInternalKey parsed{};
      ASSERT_TRUE(parseInternalKey(iterator->internalKey(), parsed));
      if (parsed.user_key == key) ++count;
      iterator->next();
    }
    ASSERT_OK(iterator->status());
  }
  return count;
}

}  // namespace

TEST(openExistingRequiresDirectory) {
  TempDirectory directory;
  DB::Handle db;
  DBOptions options;
  options.open_mode = OpenMode::kOpenExisting;

  const Status status = DB::open(options, directory.path(), &db);
  ASSERT_EQ(status.code(), StatusCode::kNotFound);
  ASSERT_TRUE(db == nullptr);
}

TEST(createNewRejectsExistingDirectory) {
  TempDirectory directory;
  std::filesystem::create_directories(directory.path());

  DB::Handle db;
  DBOptions options;
  options.open_mode = OpenMode::kCreateNew;

  const Status status = DB::open(options, directory.path(), &db);
  ASSERT_EQ(status.code(), StatusCode::kAlreadyExists);
  ASSERT_TRUE(db == nullptr);
}

TEST(putGetOverwriteEraseAndEmptyStrings) {
  TempDirectory directory;
  DB::Handle db = openOrCreate(directory.path());

  ASSERT_OK(db->put({}, "", ""));
  std::string value = "sentinel";
  ASSERT_OK(db->get({}, "", &value));
  ASSERT_EQ(value, "");

  ASSERT_OK(db->put({}, "key", "first"));
  ASSERT_OK(db->put({}, "key", "second"));
  ASSERT_OK(db->get({}, "key", &value));
  ASSERT_EQ(value, "second");

  ASSERT_OK(db->erase({}, "key"));
  ASSERT_EQ(db->get({}, "key", &value).code(), StatusCode::kNotFound);
}

TEST(missingGetPreservesValue) {
  TempDirectory directory;
  DB::Handle db = openOrCreate(directory.path());
  std::string value = "unchanged";

  const Status status = db->get({}, "missing", &value);
  ASSERT_EQ(status.code(), StatusCode::kNotFound);
  ASSERT_EQ(value, "unchanged");
}

TEST(writeBatchIsAtomicAndPreservesOperationOrder) {
  TempDirectory directory;
  DB::Handle db = openOrCreate(directory.path());

  WriteBatch batch;
  batch.put("one", "old").put("two", "2").put("one", "new").erase("two");
  ASSERT_OK(db->write({}, batch));

  std::string value;
  ASSERT_OK(db->get({}, "one", &value));
  ASSERT_EQ(value, "new");
  ASSERT_EQ(db->get({}, "two", &value).code(), StatusCode::kNotFound);
}

TEST(walRecoversWritesOverwritesDeletesAndBinaryValues) {
  TempDirectory directory;
  {
    DB::Handle db = openOrCreate(directory.path());
    WriteBatch batch;
    const std::string binary_value("a\0b", 3);
    batch.put("one", "old")
        .put("one", "new")
        .put("binary", binary_value)
        .put("deleted", "value")
        .erase("deleted");
    ASSERT_OK(db->write({Durability::kSync}, batch));
  }

  DB::Handle db = openOrCreate(directory.path());
  ASSERT_EQ(get(*db, "one"), "new");
  ASSERT_EQ(get(*db, "binary"), std::string("a\0b", 3));
  std::string value;
  ASSERT_EQ(db->get({}, "deleted", &value).code(), StatusCode::kNotFound);
}

TEST(walRecoveryTruncatesIncompleteTailBeforeAppending) {
  TempDirectory directory;
  std::uintmax_t first_record_end = 0;
  {
    DB::Handle db = openOrCreate(directory.path());
    WriteBatch first;
    first.put("x", "1").put("y", "2");
    ASSERT_OK(db->write({}, first));
  }

  const auto wal_path = walFileName(directory.path());
  first_record_end = std::filesystem::file_size(wal_path);
  {
    DB::Handle db = openOrCreate(directory.path());
    WriteBatch second;
    second.put("x", "new").erase("y");
    ASSERT_OK(db->write({}, second));
  }

  const std::uintmax_t complete_size = std::filesystem::file_size(wal_path);
  ASSERT_TRUE(complete_size > first_record_end);
  std::filesystem::resize_file(wal_path, complete_size - 1U);

  {
    DB::Handle db = openOrCreate(directory.path());
    ASSERT_EQ(get(*db, "x"), "1");
    ASSERT_EQ(get(*db, "y"), "2");
    ASSERT_EQ(std::filesystem::file_size(wal_path), first_record_end);
    ASSERT_OK(db->put({}, "z", "3"));
  }

  DB::Handle db = openOrCreate(directory.path());
  ASSERT_EQ(get(*db, "x"), "1");
  ASSERT_EQ(get(*db, "y"), "2");
  ASSERT_EQ(get(*db, "z"), "3");
}

TEST(walRecoveryRejectsChecksumMismatch) {
  TempDirectory directory;
  {
    DB::Handle db = openOrCreate(directory.path());
    ASSERT_OK(db->put({}, "key", "value"));
  }

  flipLastByte(walFileName(directory.path()));

  DB::Handle db;
  const Status status = DB::open({}, directory.path(), &db);
  ASSERT_EQ(status.code(), StatusCode::kCorruption);
  ASSERT_TRUE(db == nullptr);
}

TEST(walRecoveryRejectsMalformedBatchAndSequenceGap) {
  {
    TempDirectory directory;
    { DB::Handle db = openOrCreate(directory.path()); }
    std::unique_ptr<WalWriter> writer;
    ASSERT_OK(WalWriter::open(walFileName(directory.path()), writer));
    ASSERT_OK(writer->append("not a write batch"));
    writer.reset();

    DB::Handle db;
    ASSERT_EQ(DB::open({}, directory.path(), &db).code(),
              StatusCode::kCorruption);
    ASSERT_TRUE(db == nullptr);
  }

  {
    TempDirectory directory;
    { DB::Handle db = openOrCreate(directory.path()); }
    const auto wal_path = walFileName(directory.path());
    WriteBatch first;
    first.put("one", "1");
    appendEncodedBatch(wal_path, 1, first);
    WriteBatch second;
    second.put("two", "2");
    appendEncodedBatch(wal_path, 3, second);

    DB::Handle db;
    ASSERT_EQ(DB::open({}, directory.path(), &db).code(),
              StatusCode::kCorruption);
    ASSERT_TRUE(db == nullptr);
  }
}

TEST(openRejectsNumberedFilesWithoutManifest) {
  TempDirectory directory;
  std::filesystem::create_directories(directory.path());
  std::unique_ptr<WalWriter> writer;
  ASSERT_OK(WalWriter::open(walFileName(directory.path()), writer));
  writer.reset();

  DB::Handle db;
  ASSERT_EQ(DB::open({}, directory.path(), &db).code(),
            StatusCode::kCorruption);
  ASSERT_TRUE(db == nullptr);
}

TEST(secondOpenIsBusyUntilFirstHandleIsReleased) {
  TempDirectory directory;
  DB::Handle first = openOrCreate(directory.path());
  DB::Handle second;

  const Status busy = DB::open({}, directory.path(), &second);
  ASSERT_EQ(busy.code(), StatusCode::kBusy);
  ASSERT_TRUE(second == nullptr);

  first.reset();
  ASSERT_OK(DB::open({}, directory.path(), &second));
  ASSERT_TRUE(second != nullptr);
}

TEST(snapshotWorksForPointReadsAndIterators) {
  TempDirectory directory;
  DB::Handle db = openOrCreate(directory.path());
  SnapshotHandle snapshot;
  std::unique_ptr<Iterator> iterator;

  ASSERT_OK(db->put({}, "key", "before"));
  ASSERT_OK(db->newSnapshot(&snapshot));
  ASSERT_TRUE(snapshot != nullptr);
  ASSERT_OK(db->put({}, "key", "after"));

  std::string value;
  ASSERT_OK(db->get({}, "key", &value));
  ASSERT_EQ(value, "after");
  ASSERT_OK(db->get(ReadOptions{snapshot}, "key", &value));
  ASSERT_EQ(value, "before");

  ASSERT_OK(db->erase({}, "key"));
  ASSERT_EQ(db->get({}, "key", &value).code(), StatusCode::kNotFound);
  ASSERT_OK(db->get(ReadOptions{snapshot}, "key", &value));
  ASSERT_EQ(value, "before");

  ASSERT_OK(db->newIterator(ReadOptions{snapshot}, &iterator));
  ASSERT_TRUE(iterator != nullptr);
  ASSERT_EQ(
      collect(*iterator),
      (std::vector<std::pair<std::string, std::string>>{{"key", "before"}}));

  ASSERT_OK(db->newIterator({}, &iterator));
  ASSERT_TRUE(collect(*iterator).empty());
}

TEST(snapshotBeforeFirstWriteSeesAnEmptyDatabase) {
  TempDirectory directory;
  DB::Handle db = openOrCreate(directory.path());
  SnapshotHandle snapshot;
  ASSERT_OK(db->newSnapshot(&snapshot));

  ASSERT_OK(db->put({}, "key", "value"));
  std::string value = "unchanged";
  ASSERT_EQ(db->get(ReadOptions{snapshot}, "key", &value).code(),
            StatusCode::kNotFound);
  ASSERT_EQ(value, "unchanged");
  ASSERT_EQ(db->newSnapshot(nullptr).code(), StatusCode::kInvalidArgument);
  ASSERT_EQ(db->newIterator({}, nullptr).code(), StatusCode::kInvalidArgument);
}

TEST(snapshotFromAnotherDatabaseIsRejected) {
  TempDirectory first_directory;
  TempDirectory second_directory;
  DB::Handle first = openOrCreate(first_directory.path());
  DB::Handle second = openOrCreate(second_directory.path());

  SnapshotHandle snapshot;
  ASSERT_OK(first->newSnapshot(&snapshot));
  std::string value;
  ASSERT_EQ(second->get(ReadOptions{snapshot}, "key", &value).code(),
            StatusCode::kInvalidArgument);

  std::unique_ptr<Iterator> iterator;
  ASSERT_EQ(second->newIterator(ReadOptions{snapshot}, &iterator).code(),
            StatusCode::kInvalidArgument);
  ASSERT_TRUE(iterator == nullptr);
}

TEST(snapshotCanOutliveDatabaseHandle) {
  TempDirectory directory;
  SnapshotHandle snapshot;
  {
    DB::Handle db = openOrCreate(directory.path());
    ASSERT_OK(db->newSnapshot(&snapshot));
  }

  ASSERT_TRUE(snapshot != nullptr);
  snapshot.reset();
}

TEST(iteratorHidesLaterWritesToTheSameMutableMemTable) {
  TempDirectory directory;
  DB::Handle db = openOrCreate(directory.path());
  ASSERT_OK(db->put({}, "alpha", "old-a"));
  ASSERT_OK(db->put({}, "charlie", "old-c"));

  std::unique_ptr<Iterator> iterator;
  ASSERT_OK(db->newIterator({}, &iterator));

  ASSERT_OK(db->put({}, "alpha", "new-a"));
  ASSERT_OK(db->put({}, "bravo", "new-b"));
  ASSERT_OK(db->erase({}, "charlie"));

  ASSERT_EQ(collect(*iterator),
            (std::vector<std::pair<std::string, std::string>>{
                {"alpha", "old-a"}, {"charlie", "old-c"}}));
}

TEST(iteratorPinsReadStateAcrossCompaction) {
  TempDirectory directory;
  { DB::Handle db = openOrCreate(directory.path()); }

  ManifestState manifest;
  manifest.flushed_sequence = 3;
  manifest.oldest_wal_number = 1;
  manifest.levels[kLevel0].insert(
      manifest.levels[kLevel0].begin(),
      buildSingleEntryTable(directory.path(), 2, "alpha", 1, "a"));
  manifest.levels[kLevel0].insert(
      manifest.levels[kLevel0].begin(),
      buildSingleEntryTable(directory.path(), 3, "charlie", 2, "c"));
  manifest.levels[kLevel0].insert(
      manifest.levels[kLevel0].begin(),
      buildSingleEntryTable(directory.path(), 4, "echo", 3, "e"));
  ASSERT_OK(writeManifest(manifestFileName(directory.path()),
                          manifestTemporaryFileName(directory.path()),
                          manifest));

  DB::Handle db = openWithTinyWriteBuffer(directory.path());
  ASSERT_OK(db->put({}, "delta", "d"));

  std::unique_ptr<Iterator> iterator;
  ASSERT_OK(db->newIterator({}, &iterator));
  iterator->seekToFirst();
  ASSERT_TRUE(iterator->valid());
  ASSERT_EQ(iterator->key(), "alpha");

  // 新写入轮转旧 MemTable 并触发 L0 compaction
  ASSERT_OK(db->put({}, "foxtrot", "f"));
  const ManifestState compacted = waitForCompaction(directory.path());
  waitForObsoleteSSTableCleanup(directory.path(), compacted);
  for (std::uint64_t number = 2; number <= 5; ++number) {
    ASSERT_TRUE(
        !std::filesystem::exists(sstableFileName(directory.path(), number)));
  }

  std::vector<std::pair<std::string, std::string>> entries;
  entries.emplace_back(iterator->key(), iterator->value());
  for (iterator->next(); iterator->valid(); iterator->next()) {
    entries.emplace_back(iterator->key(), iterator->value());
  }
  ASSERT_OK(iterator->status());
  ASSERT_EQ(
      entries,
      (std::vector<std::pair<std::string, std::string>>{
          {"alpha", "a"}, {"charlie", "c"}, {"delta", "d"}, {"echo", "e"}}));
}

TEST(backgroundFlushRecoversSSTableAndCurrentWal) {
  TempDirectory directory;
  const auto old_wal_copy = directory.path() / "old-wal-copy";
  {
    DB::Handle db = openWithTinyWriteBuffer(directory.path());
    ASSERT_OK(db->put({}, "flushed", "from-sstable"));
    std::filesystem::copy_file(walFileName(directory.path(), 1), old_wal_copy);
    // 前一条写入使 MemTable 超限 本次写入先轮转再写入新 WAL
    ASSERT_OK(db->put({}, "tail", "from-wal"));

    ASSERT_EQ(get(*db, "flushed"), "from-sstable");
    ASSERT_EQ(get(*db, "tail"), "from-wal");
  }

  ASSERT_TRUE(std::filesystem::exists(manifestFileName(directory.path())));
  ASSERT_TRUE(std::filesystem::exists(sstableFileName(directory.path(), 2)));
  ASSERT_TRUE(std::filesystem::exists(walFileName(directory.path(), 3)));
  ASSERT_TRUE(!std::filesystem::exists(walFileName(directory.path(), 1)));

  // 模拟 Manifest 提交后、旧 WAL 删除前进程退出
  std::filesystem::copy_file(old_wal_copy, walFileName(directory.path(), 1));
  DB::Handle db = openWithTinyWriteBuffer(directory.path());
  ASSERT_EQ(get(*db, "flushed"), "from-sstable");
  ASSERT_EQ(get(*db, "tail"), "from-wal");
}

TEST(backgroundCompactionRepeatsAndPreservesSnapshots) {
  TempDirectory directory;
  SnapshotHandle snapshot;
  std::uint64_t first_output = 0;
  {
    DB::Handle db = openWithTinyWriteBuffer(directory.path());
    for (std::uint64_t index = 0; index < 5; ++index) {
      ASSERT_OK(db->put({}, "key", "value-" + std::to_string(index)));
    }
    ManifestState state = waitForCompaction(directory.path());
    first_output = state.levels[kLevel1][0].number;
    ASSERT_OK(db->newSnapshot(&snapshot));

    for (std::uint64_t index = 5; index < 9; ++index) {
      ASSERT_OK(db->put({}, "key", "value-" + std::to_string(index)));
    }
    state = waitForCompaction(directory.path(), first_output);
    ASSERT_EQ(get(*db, "key"), "value-8");
    std::string value;
    ASSERT_OK(db->get(ReadOptions{snapshot}, "key", &value));
    ASSERT_EQ(value, "value-4");
    waitForObsoleteSSTableCleanup(directory.path(), state);
    ASSERT_TRUE(!std::filesystem::exists(
        sstableFileName(directory.path(), first_output)));

    // 活跃 Snapshot 固定在 value-4；回收必须保留它所需的版本。
    ASSERT_EQ(countLevel1Versions(directory.path(), state, "key"), 4U);

    const std::uint64_t protected_output = state.levels[kLevel1][0].number;
    snapshot.reset();
    for (std::uint64_t index = 9; index < 14; ++index) {
      ASSERT_OK(db->put({}, "key", "value-" + std::to_string(index)));
    }
    state = waitForCompaction(directory.path(), protected_output);

    // Snapshot 释放后，下一次 compaction 只需保留输入中的最新可见版本。
    ASSERT_EQ(countLevel1Versions(directory.path(), state, "key"), 1U);
  }

  DB::Handle db = openWithTinyWriteBuffer(directory.path());
  ASSERT_EQ(get(*db, "key"), "value-13");
}

TEST(backgroundLevel1CompactionPublishesLevel2AndSurvivesRestart) {
  TempDirectory directory;
  { DB::Handle db = openOrCreate(directory.path()); }

  const std::string large_value(11U * 1024U * 1024U, 'x');
  ManifestState manifest;
  manifest.flushed_sequence = 1;
  manifest.oldest_wal_number = 1;
  manifest.levels[kLevel1].push_back(
      buildSingleEntryTable(directory.path(), 2, "large", 1, large_value));
  ASSERT_OK(writeManifest(manifestFileName(directory.path()),
                          manifestTemporaryFileName(directory.path()),
                          manifest));

  ManifestState compacted;
  {
    DB::Handle db = openOrCreate(directory.path());
    ASSERT_EQ(get(*db, "large"), large_value);
    compacted = waitForLevel1Compaction(directory.path());
    ASSERT_TRUE(compacted.levels[kLevel0].empty());
    ASSERT_TRUE(compacted.levels[kLevel1].empty());
    ASSERT_EQ(compacted.levels[kLevel2].size(), 1U);
    ASSERT_EQ(get(*db, "large"), large_value);
    std::unique_ptr<Iterator> iterator;
    ASSERT_OK(db->newIterator({}, &iterator));
    ASSERT_EQ(collect(*iterator),
              (std::vector<std::pair<std::string, std::string>>{
                  {"large", large_value}}));
    waitForObsoleteSSTableCleanup(directory.path(), compacted);
    ASSERT_TRUE(!std::filesystem::exists(sstableFileName(directory.path(), 2)));
  }

  DB::Handle db = openOrCreate(directory.path());
  ASSERT_EQ(get(*db, "large"), large_value);
}

TEST(recoveryRemovesUnreferencedSSTablesAndTemporaryFiles) {
  TempDirectory directory;
  { DB::Handle db = openOrCreate(directory.path()); }

  const auto orphan = sstableFileName(directory.path(), 2);
  const auto temporary = sstableTemporaryFileName(directory.path(), 3);
  std::ofstream(orphan).put('x');
  std::ofstream(temporary).put('x');

  DB::Handle db = openOrCreate(directory.path());
  ASSERT_TRUE(!std::filesystem::exists(orphan));
  ASSERT_TRUE(!std::filesystem::exists(temporary));
}

TEST(recoveryReplaysWalAndRemovesValidUnreferencedSSTable) {
  TempDirectory directory;
  {
    DB::Handle db = openOrCreate(directory.path());
    ASSERT_OK(db->put({Durability::kSync}, "from-wal", "value"));
  }

  // 模拟 SST 已经完成并 rename 但 Manifest 尚未提交；旧 WAL 仍是事实来源。
  buildSingleEntryTable(directory.path(), 2, "orphan", 2, "stale-value");
  ASSERT_TRUE(std::filesystem::exists(sstableFileName(directory.path(), 2)));

  DB::Handle db = openOrCreate(directory.path());
  ASSERT_EQ(get(*db, "from-wal"), "value");
  std::string value = "unchanged";
  ASSERT_EQ(db->get({}, "orphan", &value).code(), StatusCode::kNotFound);
  ASSERT_EQ(value, "unchanged");
  ASSERT_TRUE(!std::filesystem::exists(sstableFileName(directory.path(), 2)));
}

TEST(recoveryKeepsEveryManifestRequiredWalAcrossRepeatedRestart) {
  TempDirectory directory;
  { DB::Handle db = openOrCreate(directory.path()); }

  WriteBatch first;
  first.put("one", "1");
  appendEncodedBatch(walFileName(directory.path(), 1), 1, first);

  WriteBatch second;
  second.put("two", "2");
  appendEncodedBatch(walFileName(directory.path(), 2), 2, second);

  {
    DB::Handle db = openOrCreate(directory.path());
    ASSERT_EQ(get(*db, "one"), "1");
    ASSERT_EQ(get(*db, "two"), "2");
    ASSERT_TRUE(std::filesystem::exists(walFileName(directory.path(), 1)));
    ASSERT_TRUE(std::filesystem::exists(walFileName(directory.path(), 2)));
  }

  // 首次 recovery 后立即再次崩溃/重启，仍必须拥有 Manifest 要求的全部 WAL。
  DB::Handle db = openOrCreate(directory.path());
  ASSERT_EQ(get(*db, "one"), "1");
  ASSERT_EQ(get(*db, "two"), "2");
}

TEST(recoveryUsesCommittedManifestAndCleansOldFiles) {
  TempDirectory directory;
  { DB::Handle db = openOrCreate(directory.path()); }

  buildSingleEntryTable(directory.path(), 2, "key", 1, "old");
  const TableMeta new_table =
      buildSingleEntryTable(directory.path(), 3, "key", 2, "new");

  // WAL 1 和 SST 2 是提交前的旧状态；WAL 4 是提交后的当前日志。
  std::unique_ptr<WalWriter> current_wal;
  ASSERT_OK(WalWriter::open(walFileName(directory.path(), 4), current_wal));
  current_wal.reset();

  ManifestState committed;
  committed.flushed_sequence = 2;
  committed.oldest_wal_number = 4;
  committed.levels[kLevel1].push_back(new_table);
  ASSERT_OK(writeManifest(manifestFileName(directory.path()),
                          manifestTemporaryFileName(directory.path()),
                          committed));

  DB::Handle db = openOrCreate(directory.path());
  ASSERT_EQ(get(*db, "key"), "new");
  ASSERT_TRUE(!std::filesystem::exists(walFileName(directory.path(), 1)));
  ASSERT_TRUE(!std::filesystem::exists(sstableFileName(directory.path(), 2)));
  ASSERT_TRUE(std::filesystem::exists(sstableFileName(directory.path(), 3)));

  ASSERT_OK(db->put({}, "next", "value"));
  db.reset();
  db = openOrCreate(directory.path());
  ASSERT_EQ(get(*db, "key"), "new");
  ASSERT_EQ(get(*db, "next"), "value");
}

TEST(compactionManifestFailureKeepsOldVersionAndRetriesAfterRestart) {
  TempDirectory directory;
  { DB::Handle db = openOrCreate(directory.path()); }

  ManifestState manifest;
  manifest.flushed_sequence = 4;
  manifest.oldest_wal_number = 1;
  for (std::uint64_t index = 0; index < 4; ++index) {
    manifest.levels[kLevel0].insert(
        manifest.levels[kLevel0].begin(),
        buildSingleEntryTable(directory.path(), index + 2U,
                              "key-" + std::to_string(index), index + 1U,
                              "value-" + std::to_string(index)));
  }
  ASSERT_OK(writeManifest(manifestFileName(directory.path()),
                          manifestTemporaryFileName(directory.path()),
                          manifest));

  const auto manifest_temporary = manifestTemporaryFileName(directory.path());
  std::filesystem::create_directory(manifest_temporary);
  {
    DB::Handle db = openOrCreate(directory.path());
    Status status;
    for (int attempt = 0; attempt < 2000; ++attempt) {
      status = db->put({}, "tail-" + std::to_string(attempt), "value");
      if (!status.ok()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(status.code(), StatusCode::kIOError);
    ASSERT_EQ(get(*db, "key-0"), "value-0");
    ASSERT_TRUE(!std::filesystem::exists(sstableFileName(directory.path(), 6)));
  }

  std::filesystem::remove(manifest_temporary);
  {
    DB::Handle db = openOrCreate(directory.path());
    const ManifestState state = waitForCompaction(directory.path());
    waitForObsoleteSSTableCleanup(directory.path(), state);
    for (std::uint64_t index = 0; index < 4; ++index) {
      ASSERT_EQ(get(*db, "key-" + std::to_string(index)),
                "value-" + std::to_string(index));
    }
  }
}

TEST(backgroundFlushFailureStopsWritesAndWalRemainsRecoverable) {
  TempDirectory directory;
  const auto manifest_temporary = manifestTemporaryFileName(directory.path());
  {
    DB::Handle db = openWithTinyWriteBuffer(directory.path());
    ASSERT_OK(db->put({}, "immutable", "old-wal"));

    // 目录无法以普通文件方式打开 强制后台 Manifest 发布失败
    std::filesystem::create_directory(manifest_temporary);
    ASSERT_OK(db->put({}, "mutable", "new-wal"));

    const Status status = db->put({}, "rejected", "value");
    ASSERT_EQ(status.code(), StatusCode::kIOError);
    ASSERT_EQ(get(*db, "immutable"), "old-wal");
    ASSERT_EQ(get(*db, "mutable"), "new-wal");

    std::string value;
    ASSERT_EQ(db->get({}, "rejected", &value).code(), StatusCode::kNotFound);
  }

  std::filesystem::remove(manifest_temporary);
  DB::Handle db = openWithTinyWriteBuffer(directory.path());
  ASSERT_EQ(get(*db, "immutable"), "old-wal");
  ASSERT_EQ(get(*db, "mutable"), "new-wal");
}

TEST(level0NewestTableTombstoneAndSnapshotOrderingAreCorrect) {
  TempDirectory directory;
  {
    DB::Handle db = openWithTinyWriteBuffer(directory.path());
    ASSERT_OK(db->put({}, "key", "v1"));

    SnapshotHandle snapshot;
    ASSERT_OK(db->newSnapshot(&snapshot));

    ASSERT_OK(db->put({}, "key", "v2"));
    ASSERT_OK(db->erase({}, "key"));
    ASSERT_OK(db->put({}, "sentinel", "flush tombstone"));

    std::string value = "unchanged";
    ASSERT_EQ(db->get({}, "key", &value).code(), StatusCode::kNotFound);
    ASSERT_EQ(value, "unchanged");
    ASSERT_OK(db->get(ReadOptions{snapshot}, "key", &value));
    ASSERT_EQ(value, "v1");

    ASSERT_OK(db->put({}, "key", "v4"));
    ASSERT_EQ(get(*db, "key"), "v4");
    ASSERT_OK(db->get(ReadOptions{snapshot}, "key", &value));
    ASSERT_EQ(value, "v1");
  }

  DB::Handle db = openWithTinyWriteBuffer(directory.path());
  ASSERT_EQ(get(*db, "key"), "v4");
  ASSERT_EQ(get(*db, "sentinel"), "flush tombstone");
}

TEST(openRejectsCorruptManifestAndMissingReferencedSSTable) {
  {
    TempDirectory directory;
    {
      DB::Handle db = openOrCreate(directory.path());
      ASSERT_OK(db->put({}, "key", "value"));
    }
    flipLastByte(manifestFileName(directory.path()));

    DB::Handle db;
    ASSERT_EQ(DB::open({}, directory.path(), &db).code(),
              StatusCode::kCorruption);
    ASSERT_TRUE(db == nullptr);
  }

  {
    TempDirectory directory;
    {
      DB::Handle db = openWithTinyWriteBuffer(directory.path());
      ASSERT_OK(db->put({}, "flushed", "value"));
      ASSERT_OK(db->put({}, "tail", "value"));
    }

    ManifestState state;
    ASSERT_OK(readManifest(manifestFileName(directory.path()), state));
    ASSERT_EQ(state.levels[kLevel0].size(), 1U);
    std::filesystem::remove(
        sstableFileName(directory.path(), state.levels[kLevel0][0].number));

    DB::Handle db;
    ASSERT_EQ(DB::open({}, directory.path(), &db).code(),
              StatusCode::kCorruption);
    ASSERT_TRUE(db == nullptr);
  }
}

}  // namespace lsmtree
