#include "db/version.h"

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include "db/filename.h"
#include "db/internal_key.h"
#include "table/sstable_builder.h"
#include "test.h"

namespace lsmtree {
namespace {

class VersionTempDirectory {
 public:
  VersionTempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-version-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~VersionTempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

TableMeta buildTable(const VersionTempDirectory& directory,
                     std::uint64_t number, Slice first_key,
                     Slice first_value, Slice last_key, Slice last_value) {
  std::unique_ptr<SSTableBuilder> builder;
  ASSERT_OK(SSTableBuilder::open(sstableFileName(directory.path(), number), {},
                                 builder));
  ASSERT_OK(builder->add(
      encodeInternalKey(first_key, number, ValueType::kValue), first_value));
  if (first_key != last_key) {
    ASSERT_OK(builder->add(
        encodeInternalKey(last_key, number, ValueType::kValue), last_value));
  }

  SSTableMeta meta;
  ASSERT_OK(builder->finish(meta));
  return TableMeta{number, meta.file_size, meta.smallest_key,
                   meta.largest_key};
}

std::string get(const Version& version, Slice key) {
  LookupResult result = LookupResult::kAbsent;
  std::string value;
  ASSERT_OK(version.get({}, key, kMaxSequenceNumber, result, value));
  ASSERT_EQ(result, LookupResult::kValue);
  return value;
}

}

TEST(versionReadsLevel0BeforeSingleCandidateInLevel1) {
  VersionTempDirectory directory;
  ManifestState manifest;
  manifest.oldest_wal_number = 1;
  manifest.level0_tables.push_back(
      buildTable(directory, 4, "beta", "l0", "beta", "l0"));
  manifest.level1_tables.push_back(
      buildTable(directory, 2, "alpha", "a", "charlie", "l1"));
  manifest.level1_tables.push_back(
      buildTable(directory, 3, "delta", "d", "omega", "o"));

  std::shared_ptr<const Version> version;
  ASSERT_OK(Version::open(directory.path(), manifest, version));
  ASSERT_EQ(get(*version, "beta"), "l0");
  ASSERT_EQ(get(*version, "charlie"), "l1");
  ASSERT_EQ(get(*version, "delta"), "d");

  LookupResult result = LookupResult::kValue;
  std::string value = "preserved";
  ASSERT_OK(version->get({}, "between", kMaxSequenceNumber, result, value));
  ASSERT_EQ(result, LookupResult::kAbsent);
  ASSERT_EQ(value, "preserved");
}

}
