#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include <unistd.h>

#include "db/filename.h"
#include "db/write_batch_codec.h"
#include "lsmtree/db.h"
#include "test.h"
#include "wal/wal_writer.h"

namespace lsmtree {
namespace {

class TempDirectory {
 public:
  TempDirectory() {
    // 组合进程号和单调时钟 避免并行测试共用同一目录
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
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

std::string get(DB& db, Slice key) {
  std::string value;
  ASSERT_OK(db.get({}, key, &value));
  return value;
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

}

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
    std::filesystem::create_directories(directory.path());
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
    std::filesystem::create_directories(directory.path());
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

TEST(snapshotWorksAndIteratorRemainsUnsupported) {
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

  ASSERT_EQ(db->newIterator({}, &iterator).code(), StatusCode::kNotSupported);
  ASSERT_TRUE(!iterator);
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
}

}
