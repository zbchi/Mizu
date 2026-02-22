#include "table/sstable_builder.h"

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/internal_key.h"
#include "table/table_format.h"
#include "test.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace lsmtree {
namespace {

using Entry = std::pair<std::string, std::string>;

class SSTableTempDirectory {
 public:
  SSTableTempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-sstable-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~SSTableTempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  std::filesystem::path file(const char* name) const { return path_ / name; }

 private:
  std::filesystem::path path_;
};

std::string readFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  ASSERT_TRUE(input.is_open());
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

std::vector<Entry> decodeBlockEntries(Slice payload) {
  ASSERT_TRUE(payload.size() >= sizeof(std::uint32_t));
  Slice count_input = payload.substr(payload.size() - sizeof(std::uint32_t));
  std::uint32_t restart_count = 0;
  ASSERT_TRUE(getFixed32(count_input, restart_count));

  const std::size_t restart_bytes =
      (static_cast<std::size_t>(restart_count) + 1U) * sizeof(std::uint32_t);
  ASSERT_TRUE(payload.size() >= restart_bytes);
  Slice input = payload.substr(0, payload.size() - restart_bytes);

  std::vector<Entry> entries;
  std::string last_key;
  while (!input.empty()) {
    std::uint32_t shared = 0;
    std::uint32_t unshared = 0;
    std::uint32_t value_size = 0;
    ASSERT_TRUE(getFixed32(input, shared));
    ASSERT_TRUE(getFixed32(input, unshared));
    ASSERT_TRUE(getFixed32(input, value_size));
    ASSERT_TRUE(shared <= last_key.size());
    ASSERT_TRUE(input.size() >= static_cast<std::size_t>(unshared) +
                                    static_cast<std::size_t>(value_size));

    std::string key = last_key.substr(0, shared);
    key.append(input.data(), unshared);
    input.remove_prefix(unshared);
    std::string value(input.data(), value_size);
    input.remove_prefix(value_size);
    entries.emplace_back(key, value);
    last_key = std::move(key);
  }
  return entries;
}

BlockHandle decodeBlockHandle(Slice encoded) {
  ASSERT_EQ(encoded.size(), kBlockHandleSize);
  BlockHandle handle;
  ASSERT_TRUE(getFixed64(encoded, handle.offset));
  ASSERT_TRUE(getFixed64(encoded, handle.size));
  ASSERT_TRUE(encoded.empty());
  return handle;
}

BlockHandle decodeFooter(Slice file) {
  ASSERT_TRUE(file.size() >= kSSTableFooterSize);
  Slice footer = file.substr(file.size() - kSSTableFooterSize);
  ASSERT_EQ(footer.substr(0, kSSTableMagicSize),
            Slice(kSSTableMagic, kSSTableMagicSize));
  footer.remove_prefix(kSSTableMagicSize);

  std::uint32_t version = 0;
  std::uint32_t reserved = 1;
  ASSERT_TRUE(getFixed32(footer, version));
  ASSERT_TRUE(getFixed32(footer, reserved));
  ASSERT_EQ(version, kSSTableVersion);
  ASSERT_EQ(reserved, 0U);
  return decodeBlockHandle(footer);
}

Slice checkedBlockPayload(Slice file, const BlockHandle& handle) {
  ASSERT_TRUE(handle.offset <= file.size());
  ASSERT_TRUE(handle.size <= file.size() - handle.offset);
  ASSERT_TRUE(kBlockTrailerSize <= file.size() - handle.offset - handle.size);

  const Slice payload = file.substr(handle.offset, handle.size);
  const std::size_t trailer_offset = handle.offset + handle.size;
  ASSERT_EQ(static_cast<unsigned char>(file[trailer_offset]), kNoCompression);

  Slice checksum_input = file.substr(trailer_offset + 1, sizeof(std::uint32_t));
  std::uint32_t stored_checksum = 0;
  ASSERT_TRUE(getFixed32(checksum_input, stored_checksum));
  ASSERT_TRUE(checksum_input.empty());

  std::string checksummed(payload);
  checksummed.push_back(static_cast<char>(kNoCompression));
  ASSERT_EQ(stored_checksum, crc32c(checksummed));
  return payload;
}

std::unique_ptr<SSTableBuilder> openBuilder(const std::filesystem::path& path,
                                            std::size_t block_size = 4096) {
  std::unique_ptr<SSTableBuilder> builder;
  ASSERT_OK(SSTableBuilder::open(path, SSTableBuilderOptions{block_size, {}},
                                 builder));
  ASSERT_TRUE(builder != nullptr);
  return builder;
}

}

TEST(sstableBuilderWritesDataIndexAndFooter) {
  SSTableTempDirectory directory;
  const auto path = directory.file("000002.sst.tmp");
  auto builder = openBuilder(path, 1);

  const std::vector<Entry> records = {
      {encodeInternalKey("alpha", 3, ValueType::kValue), "old"},
      {encodeInternalKey("alpha", 1, ValueType::kValue), "new"},
      {encodeInternalKey("beta", 2, ValueType::kDeletion), ""},
  };
  for (const auto& entry : records) {
    ASSERT_OK(builder->add(entry.first, entry.second));
  }

  SSTableMeta meta;
  ASSERT_OK(builder->finish(meta));
  const std::string file = readFile(path);
  ASSERT_EQ(meta.file_size, file.size());
  ASSERT_EQ(meta.entry_count, records.size());
  ASSERT_EQ(meta.smallest_key, records.front().first);
  ASSERT_EQ(meta.largest_key, records.back().first);

  const BlockHandle index_handle = decodeFooter(file);
  const auto index_entries =
      decodeBlockEntries(checkedBlockPayload(file, index_handle));
  ASSERT_EQ(index_entries.size(), records.size());

  for (std::size_t index = 0; index < records.size(); ++index) {
    ASSERT_EQ(index_entries[index].first, records[index].first);
    const BlockHandle data_handle =
        decodeBlockHandle(index_entries[index].second);
    const auto data_entries =
        decodeBlockEntries(checkedBlockPayload(file, data_handle));
    ASSERT_EQ(data_entries, (std::vector<Entry>{records[index]}));
  }
}

TEST(sstableBuilderWritesAnEmptyTable) {
  SSTableTempDirectory directory;
  const auto path = directory.file("empty.sst.tmp");
  auto builder = openBuilder(path);

  SSTableMeta meta;
  ASSERT_OK(builder->finish(meta));
  const std::string file = readFile(path);
  ASSERT_EQ(meta.file_size, file.size());
  ASSERT_EQ(meta.entry_count, 0U);
  ASSERT_TRUE(meta.smallest_key.empty());
  ASSERT_TRUE(meta.largest_key.empty());

  const BlockHandle index_handle = decodeFooter(file);
  ASSERT_TRUE(
      decodeBlockEntries(checkedBlockPayload(file, index_handle)).empty());
}

TEST(sstableBuilderOwnsItsTemporaryFileLifecycle) {
  SSTableTempDirectory directory;
  const auto finished_path = directory.file("finished.sst.tmp");
  auto finished = openBuilder(finished_path);
  const std::string key = encodeInternalKey("key", 1, ValueType::kValue);
  ASSERT_OK(finished->add(key, "value"));

  SSTableMeta meta;
  ASSERT_OK(finished->finish(meta));
  ASSERT_TRUE(std::filesystem::exists(finished_path));

  const auto abandoned_path = directory.file("abandoned.sst.tmp");
  auto abandoned = openBuilder(abandoned_path);
  ASSERT_OK(abandoned->add(key, "value"));
  abandoned->abandon();
  ASSERT_TRUE(!std::filesystem::exists(abandoned_path));
}

TEST(sstableBuilderOpenValidatesOptionsAndDoesNotOverwrite) {
  SSTableTempDirectory directory;
  const auto path = directory.file("existing.sst.tmp");
  {
    std::ofstream output(path);
    output << "preserved";
  }

  std::unique_ptr<SSTableBuilder> builder;
  ASSERT_EQ(SSTableBuilder::open(path, {}, builder).code(),
            StatusCode::kAlreadyExists);
  ASSERT_TRUE(builder == nullptr);
  ASSERT_EQ(readFile(path), "preserved");

  ASSERT_EQ(SSTableBuilder::open(directory.file("zero-block.sst.tmp"),
                                 SSTableBuilderOptions{0, {}}, builder)
                .code(),
            StatusCode::kInvalidArgument);
  ASSERT_EQ(SSTableBuilder::open(
                directory.file("zero-restart.sst.tmp"),
                SSTableBuilderOptions{4096, BlockBuilderOptions{0}}, builder)
                .code(),
            StatusCode::kInvalidArgument);
}

}
