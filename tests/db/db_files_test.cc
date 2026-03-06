#include "db/db_files.h"

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include "db/filename.h"
#include "test.h"

namespace lsmtree {
namespace {

class DatabaseFilesTempDirectory {
 public:
  DatabaseFilesTempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-db-files-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~DatabaseFilesTempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

void touch(const std::filesystem::path& path) {
  std::ofstream file(path, std::ios::binary);
  ASSERT_TRUE(file.is_open());
}

}

TEST(databaseDirectoryScanAndWalCleanupUseNumberedFilesOnly) {
  DatabaseFilesTempDirectory directory;
  touch(walFileName(directory.path(), 9));
  touch(walFileName(directory.path(), 3));
  touch(sstableFileName(directory.path(), 12));
  touch(sstableTemporaryFileName(directory.path(), 15));
  touch(directory.path() / "unrelated");

  DirectoryContents contents;
  ASSERT_OK(scanDatabaseDirectory(directory.path(), contents));
  ASSERT_EQ(contents.wal_numbers.size(), 2U);
  ASSERT_EQ(contents.wal_numbers[0], 3U);
  ASSERT_EQ(contents.wal_numbers[1], 9U);
  ASSERT_TRUE(contents.has_sstable);
  ASSERT_EQ(contents.maximum_number, 15U);

  removeObsoleteWalFilesBestEffort(directory.path(), 9);
  ASSERT_TRUE(!std::filesystem::exists(walFileName(directory.path(), 3)));
  ASSERT_TRUE(std::filesystem::exists(walFileName(directory.path(), 9)));
  ASSERT_TRUE(std::filesystem::exists(sstableFileName(directory.path(), 12)));
}

}
