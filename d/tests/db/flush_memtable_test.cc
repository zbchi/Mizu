#include "db/flush_memtable.h"

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include "db/internal_key.h"
#include "table/sstable_reader.h"
#include "test.h"

namespace lsmtree {
namespace {

class FlushTempDirectory {
 public:
  FlushTempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-flush-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~FlushTempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  std::filesystem::path file(const char* name) const { return path_ / name; }

 private:
  std::filesystem::path path_;
};

}

TEST(buildLevel0TableWritesAndReadsAllMemtableVersions) {
  FlushTempDirectory directory;
  MemTable table;
  table.add(7, ValueType::kValue, "alpha", "new");
  table.add(5, ValueType::kDeletion, "alpha", {});
  table.add(3, ValueType::kValue, "alpha", "old");
  table.add(2, ValueType::kValue, "beta", "value");

  const auto temporary_path = directory.file("000001.sst.tmp");
  const auto final_path = directory.file("000001.sst");
  SSTableMeta meta;
  ASSERT_OK(buildLevel0Table(table, temporary_path, final_path,
                             SSTableBuilderOptions{1, {}}, meta));

  ASSERT_TRUE(!std::filesystem::exists(temporary_path));
  ASSERT_TRUE(std::filesystem::exists(final_path));
  ASSERT_EQ(meta.entry_count, 4U);
  ASSERT_EQ(meta.smallest_key,
            encodeInternalKey("alpha", 7, ValueType::kValue));
  ASSERT_EQ(meta.largest_key,
            encodeInternalKey("beta", 2, ValueType::kValue));

  std::unique_ptr<SSTableReader> reader;
  ASSERT_OK(SSTableReader::open(final_path, reader));

  struct Expected {
    SequenceNumber sequence;
    LookupResult result;
    const char* value;
  };
  const Expected cases[] = {
      {7, LookupResult::kValue, "new"},
      {6, LookupResult::kDeleted, "unchanged"},
      {4, LookupResult::kValue, "old"},
      {2, LookupResult::kAbsent, "unchanged"},
  };

  for (const Expected& test_case : cases) {
    LookupResult result = LookupResult::kAbsent;
    std::string value = "unchanged";
    ASSERT_OK(reader->get({}, "alpha", test_case.sequence, result, value));
    ASSERT_EQ(result, test_case.result);
    ASSERT_EQ(value, test_case.value);
  }

  LookupResult result = LookupResult::kAbsent;
  std::string value;
  ASSERT_OK(reader->get({}, "beta", 2, result, value));
  ASSERT_EQ(result, LookupResult::kValue);
  ASSERT_EQ(value, "value");
}

}
