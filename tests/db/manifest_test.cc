#include "db/manifest.h"

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include "db/filename.h"
#include "db/internal_key.h"
#include "test.h"
#include "util/coding.h"
#include "util/crc32c.h"

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

TableMeta table(std::uint64_t number, Slice first_user_key,
                Slice last_user_key) {
  TableMeta result;
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
  state.level1_tables.push_back(table(10, "aardvark", "beta"));
  state.level1_tables.push_back(table(11, "delta", "zulu"));
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
  ASSERT_EQ(decoded.level1_tables.size(), 2U);
  ASSERT_EQ(decoded.level1_tables[0].number, 10U);
  ASSERT_EQ(decoded.level1_tables[1].number, 11U);
}

TEST(manifestReadsLegacyLevel0OnlyState) {
  ManifestTempDirectory directory;
  const auto path = manifestFileName(directory.path());

  const TableMeta legacy_table = table(7, "alpha", "omega");
  std::string encoded = "LSMMAN01";
  putFixed32(encoded, 1);
  putFixed64(encoded, 12);
  putFixed64(encoded, 3);
  putFixed32(encoded, 1);
  putFixed64(encoded, legacy_table.number);
  putFixed64(encoded, legacy_table.file_size);
  putFixed32(encoded,
             static_cast<std::uint32_t>(legacy_table.smallest_key.size()));
  encoded.append(legacy_table.smallest_key);
  putFixed32(encoded,
             static_cast<std::uint32_t>(legacy_table.largest_key.size()));
  encoded.append(legacy_table.largest_key);
  putFixed32(encoded, crc32c(encoded));

  std::ofstream file(path, std::ios::binary);
  file.write(encoded.data(), static_cast<std::streamsize>(encoded.size()));
  file.close();

  ManifestState decoded;
  ASSERT_OK(readManifest(path, decoded));
  ASSERT_EQ(decoded.flushed_sequence, 12U);
  ASSERT_EQ(decoded.level0_tables.size(), 1U);
  ASSERT_EQ(decoded.level0_tables[0].number, 7U);
  ASSERT_TRUE(decoded.level1_tables.empty());
}

TEST(manifestRejectsInvalidLevel1Layout) {
  ManifestTempDirectory directory;
  const auto path = manifestFileName(directory.path());
  const auto temporary = manifestTemporaryFileName(directory.path());

  ManifestState state;
  state.oldest_wal_number = 1;
  state.level1_tables.push_back(table(2, "alpha", "delta"));
  state.level1_tables.push_back(table(3, "delta", "omega"));
  ASSERT_EQ(writeManifest(path, temporary, state).code(),
            StatusCode::kInvalidArgument);

  state.level1_tables = {table(2, "delta", "omega"),
                         table(3, "alpha", "beta")};
  ASSERT_EQ(writeManifest(path, temporary, state).code(),
            StatusCode::kInvalidArgument);

  state.level0_tables.push_back(table(2, "x", "z"));
  state.level1_tables = {table(2, "alpha", "beta")};
  ASSERT_EQ(writeManifest(path, temporary, state).code(),
            StatusCode::kInvalidArgument);

  state.level0_tables.clear();
  state.level1_tables = {table(2, "", ""), table(3, "", "alpha")};
  ASSERT_EQ(writeManifest(path, temporary, state).code(),
            StatusCode::kInvalidArgument);
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
