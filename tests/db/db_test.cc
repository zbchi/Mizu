#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include <unistd.h>

#include "lsmtree/db.h"
#include "test.h"

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

TEST(snapshotAndIteratorAreExplicitlyUnsupported) {
  TempDirectory directory;
  DB::Handle db = openOrCreate(directory.path());
  SnapshotHandle snapshot;
  std::unique_ptr<Iterator> iterator;

  ASSERT_EQ(db->newSnapshot(&snapshot).code(), StatusCode::kNotSupported);
  ASSERT_TRUE(!snapshot);
  ASSERT_EQ(db->newIterator({}, &iterator).code(), StatusCode::kNotSupported);
  ASSERT_TRUE(!iterator);
}

}
