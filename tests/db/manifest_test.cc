#include "db/manifest.h"

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include "db/filename.h"
#include "db/internal_key.h"
#include "test.h"

namespace lsmtree {
namespace {

class ManifestTempDirectory {
 public:
  ManifestTempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-manifest-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~ManifestTempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

ManifestTable table(std::uint64_t number, Slice first_user_key,
                    Slice last_user_key) {
  ManifestTable result;
  result.number = number;
  result.file_size = number * 100U;
  result.smallest_key =
      encodeInternalKey(first_user_key, number, ValueType::kValue);
  result.largest_key =
      encodeInternalKey(last_user_key, 1, ValueType::kDeletion);
  return result;
}

}

TEST(manifestRoundTripsCompleteLevel0State) {
  ManifestTempDirectory directory;
  const auto path = manifestFileName(directory.path());
  const auto temporary = manifestTemporaryFileName(directory.path());

  ManifestState state;
  state.flushed_sequence = 42;
  state.oldest_wal_number = 9;
  state.level0_tables.push_back(table(8, std::string("a\0", 2), "z"));
  state.level0_tables.push_back(table(6, "alpha", "omega"));
  ASSERT_OK(writeManifest(path, temporary, state));
  ASSERT_TRUE(std::filesystem::exists(path));
  ASSERT_TRUE(!std::filesystem::exists(temporary));

  ManifestState decoded;
  ASSERT_OK(readManifest(path, decoded));
  ASSERT_EQ(decoded.flushed_sequence, 42U);
  ASSERT_EQ(decoded.oldest_wal_number, 9U);
  ASSERT_EQ(decoded.level0_tables.size(), 2U);
  ASSERT_EQ(decoded.level0_tables[0].number, 8U);
  ASSERT_EQ(decoded.level0_tables[0].file_size, 800U);
  ASSERT_EQ(decoded.level0_tables[0].smallest_key,
            state.level0_tables[0].smallest_key);
  ASSERT_EQ(decoded.level0_tables[1].number, 6U);
}

TEST(manifestRejectsChecksumMismatchAndInvalidState) {
  ManifestTempDirectory directory;
  const auto path = manifestFileName(directory.path());
  const auto temporary = manifestTemporaryFileName(directory.path());

  ManifestState state;
  state.oldest_wal_number = 1;
  ASSERT_OK(writeManifest(path, temporary, state));

  std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(file.is_open());
  file.seekg(-1, std::ios::end);
  char byte = 0;
  file.read(&byte, 1);
  byte ^= 1;
  file.seekp(-1, std::ios::end);
  file.write(&byte, 1);
  file.close();

  ManifestState decoded;
  ASSERT_EQ(readManifest(path, decoded).code(), StatusCode::kCorruption);

  ManifestState invalid;
  invalid.oldest_wal_number = 0;
  ASSERT_EQ(writeManifest(path, temporary, invalid).code(),
            StatusCode::kInvalidArgument);
}

TEST(numberedFileNamesRoundTrip) {
  ManifestTempDirectory directory;
  std::uint64_t number = 0;
  NumberedFileType type = NumberedFileType::kWal;

  ASSERT_TRUE(
      parseNumberedFileName(walFileName(directory.path(), 17), number, type));
  ASSERT_EQ(number, 17U);
  ASSERT_EQ(type, NumberedFileType::kWal);

  ASSERT_TRUE(parseNumberedFileName(
      sstableTemporaryFileName(directory.path(), 23), number, type));
  ASSERT_EQ(number, 23U);
  ASSERT_EQ(type, NumberedFileType::kSSTableTemporary);

  ASSERT_TRUE(
      !parseNumberedFileName(directory.path() / "MANIFEST", number, type));
  ASSERT_TRUE(
      !parseNumberedFileName(directory.path() / "000000.log", number, type));
}

}
