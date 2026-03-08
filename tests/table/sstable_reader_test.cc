#include "table/sstable_reader.h"

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/internal_key.h"
#include "table/block_iterator.h"
#include "table/sstable_builder.h"
#include "table/table_format.h"
#include "table/table_io.h"
#include "test.h"

namespace lsmtree {
namespace {

struct Entry {
  std::string user_key;
  SequenceNumber sequence;
  ValueType type;
  std::string value;
};

class ReaderTempDirectory {
 public:
  ReaderTempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-reader-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~ReaderTempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  std::filesystem::path file(const char* name) const { return path_ / name; }

 private:
  std::filesystem::path path_;
};

std::filesystem::path buildTable(const ReaderTempDirectory& directory,
                                 const std::vector<Entry>& entries,
                                 std::size_t block_size = 4096) {
  const auto path = directory.file("table.sst");
  std::unique_ptr<SSTableBuilder> builder;
  ASSERT_OK(SSTableBuilder::open(path, SSTableBuilderOptions{block_size, {}},
                                 builder));
  for (const Entry& entry : entries) {
    ASSERT_OK(builder->add(
        encodeInternalKey(entry.user_key, entry.sequence, entry.type),
        entry.value));
  }

  SSTableMeta meta;
  ASSERT_OK(builder->finish(meta));
  return path;
}

std::unique_ptr<SSTableReader> openReader(const std::filesystem::path& path) {
  std::unique_ptr<SSTableReader> reader;
  ASSERT_OK(SSTableReader::open(path, reader));
  ASSERT_TRUE(reader != nullptr);
  return reader;
}

struct FooterHandles {
  BlockHandle filter;
  BlockHandle index;
};

FooterHandles readFooterHandles(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  ASSERT_TRUE(fd >= 0);

  FooterHandles handles;
  ASSERT_OK(readSSTableFooter(fd, std::filesystem::file_size(path),
                             handles.filter, handles.index));
  ASSERT_TRUE(::close(fd) == 0);
  return handles;
}

BlockHandle readFirstDataHandle(const std::filesystem::path& path) {
  const std::uint64_t file_size = std::filesystem::file_size(path);
  const int fd = ::open(path.c_str(), O_RDONLY);
  ASSERT_TRUE(fd >= 0);

  BlockHandle filter_handle;
  BlockHandle index_handle;
  ASSERT_OK(
      readSSTableFooter(fd, file_size, filter_handle, index_handle));
  std::string index_block;
  ASSERT_OK(readBlock(fd, file_size, index_handle, true, index_block));
  ASSERT_TRUE(::close(fd) == 0);

  BlockIterator index(index_block);
  index.seekToFirst();
  ASSERT_TRUE(index.valid());

  Slice encoded_handle = index.value();
  BlockHandle data_handle;
  ASSERT_TRUE(getBlockHandle(encoded_handle, data_handle));
  ASSERT_TRUE(encoded_handle.empty());
  return data_handle;
}

void flipByte(const std::filesystem::path& path, std::uint64_t offset) {
  std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(file.is_open());

  file.seekg(static_cast<std::streamoff>(offset));
  char byte = 0;
  file.get(byte);
  ASSERT_TRUE(file.good());

  file.seekp(static_cast<std::streamoff>(offset));
  file.put(static_cast<char>(byte ^ 1));
  file.flush();
  ASSERT_TRUE(file.good());
}

}

TEST(sstableReaderReadsAcrossDataBlocks) {
  ReaderTempDirectory directory;
  const auto path = buildTable(directory,
                               {{"alpha", 3, ValueType::kValue, "one"},
                                {"beta", 2, ValueType::kValue, "two"},
                                {"delta", 1, ValueType::kValue, "four"}},
                               1);
  auto reader = openReader(path);

  for (const auto& test_case : std::vector<std::pair<std::string, std::string>>{
           {"alpha", "one"}, {"beta", "two"}, {"delta", "four"}}) {
    LookupResult result = LookupResult::kAbsent;
    std::string value;
    ASSERT_OK(
        reader->get({}, test_case.first, kMaxSequenceNumber, result, value));
    ASSERT_EQ(result, LookupResult::kValue);
    ASSERT_EQ(value, test_case.second);
  }
}

TEST(sstableIteratorReadsInternalEntriesAcrossDataBlocks) {
  ReaderTempDirectory directory;
  const std::vector<Entry> entries = {
      {"alpha", 7, ValueType::kValue, "new"},
      {"alpha", 5, ValueType::kDeletion, ""},
      {"alpha", 3, ValueType::kValue, "old"},
      {"beta", 2, ValueType::kValue, "value"},
  };
  auto reader = openReader(buildTable(directory, entries, 1));
  auto iterator = reader->newIterator({});

  iterator->seekToFirst();
  std::size_t index = 0;
  while (iterator->valid()) {
    ASSERT_TRUE(index < entries.size());
    ASSERT_EQ(iterator->internalKey(),
              encodeInternalKey(entries[index].user_key,
                                entries[index].sequence, entries[index].type));
    ASSERT_EQ(iterator->value(), entries[index].value);
    ++index;
    iterator->next();
  }
  ASSERT_OK(iterator->status());
  ASSERT_EQ(index, entries.size());
}

TEST(sstableReaderSelectsVisibleVersionAndTombstone) {
  ReaderTempDirectory directory;
  const auto path = buildTable(directory,
                               {{"alpha", 7, ValueType::kValue, "new"},
                                {"alpha", 5, ValueType::kDeletion, ""},
                                {"alpha", 3, ValueType::kValue, "old"}},
                               1);
  auto reader = openReader(path);

  struct Expected {
    SequenceNumber sequence;
    LookupResult result;
    const char* value;
  };
  const std::vector<Expected> cases = {
      {7, LookupResult::kValue, "new"},
      {6, LookupResult::kDeleted, "preserved"},
      {5, LookupResult::kDeleted, "preserved"},
      {4, LookupResult::kValue, "old"},
      {3, LookupResult::kValue, "old"},
      {2, LookupResult::kAbsent, "preserved"},
  };

  for (const Expected& test_case : cases) {
    LookupResult result = LookupResult::kValue;
    std::string value = "preserved";
    ASSERT_OK(reader->get({}, "alpha", test_case.sequence, result, value));
    ASSERT_EQ(result, test_case.result);
    ASSERT_EQ(value, std::string(test_case.value));
  }
}

TEST(sstableReaderPreservesValueForMissingKeys) {
  ReaderTempDirectory directory;
  const auto path = buildTable(directory,
                               {{"beta", 2, ValueType::kValue, "two"},
                                {"delta", 1, ValueType::kDeletion, ""}},
                               1);
  auto reader = openReader(path);

  for (const Slice key : {Slice("alpha"), Slice("charlie"), Slice("zeta")}) {
    LookupResult result = LookupResult::kValue;
    std::string value = "preserved";
    ASSERT_OK(reader->get({}, key, kMaxSequenceNumber, result, value));
    ASSERT_EQ(result, LookupResult::kAbsent);
    ASSERT_EQ(value, std::string("preserved"));
  }

  LookupResult result = LookupResult::kValue;
  std::string value = "preserved";
  ASSERT_OK(reader->get({}, "delta", kMaxSequenceNumber, result, value));
  ASSERT_EQ(result, LookupResult::kDeleted);
  ASSERT_EQ(value, std::string("preserved"));
}

TEST(sstableReaderOpensEmptyTable) {
  ReaderTempDirectory directory;
  auto reader = openReader(buildTable(directory, {}));

  LookupResult result = LookupResult::kValue;
  std::string value = "preserved";
  ASSERT_OK(reader->get({}, "key", kMaxSequenceNumber, result, value));
  ASSERT_EQ(result, LookupResult::kAbsent);
  ASSERT_EQ(value, std::string("preserved"));
}

TEST(sstableReaderRejectsCorruptIndexDuringOpen) {
  ReaderTempDirectory directory;
  const auto path =
      buildTable(directory, {{"key", 1, ValueType::kValue, "value"}});
  const FooterHandles handles = readFooterHandles(path);
  flipByte(path, handles.index.offset);

  std::unique_ptr<SSTableReader> reader;
  ASSERT_EQ(SSTableReader::open(path, reader).code(), StatusCode::kCorruption);
  ASSERT_TRUE(reader == nullptr);
}

TEST(sstableReaderRejectsCorruptFilterDuringOpen) {
  ReaderTempDirectory directory;
  const auto path =
      buildTable(directory, {{"key", 1, ValueType::kValue, "value"}});
  const FooterHandles handles = readFooterHandles(path);
  flipByte(path, handles.filter.offset);

  std::unique_ptr<SSTableReader> reader;
  ASSERT_EQ(SSTableReader::open(path, reader).code(), StatusCode::kCorruption);
  ASSERT_TRUE(reader == nullptr);
}

TEST(sstableReaderReportsCorruptDataBlock) {
  ReaderTempDirectory directory;
  const auto path =
      buildTable(directory, {{"key", 1, ValueType::kValue, "value"}});
  const BlockHandle data_handle = readFirstDataHandle(path);
  auto reader = openReader(path);
  flipByte(path, data_handle.offset);

  ReadOptions options;
  options.verify_checksums = true;
  LookupResult result = LookupResult::kDeleted;
  std::string value = "preserved";
  ASSERT_EQ(
      reader->get(options, "key", kMaxSequenceNumber, result, value).code(),
      StatusCode::kCorruption);
  ASSERT_EQ(result, LookupResult::kDeleted);
  ASSERT_EQ(value, std::string("preserved"));
}

}
