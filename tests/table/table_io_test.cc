#include "table/table_io.h"

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include "db/internal_key.h"
#include "table/sstable_builder.h"
#include "test.h"

namespace lsmtree {
namespace {

class TemporaryDirectory {
 public:
  TemporaryDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-table-io-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~TemporaryDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  std::filesystem::path file(const char* name) const { return path_ / name; }

 private:
  std::filesystem::path path_;
};

std::filesystem::path buildTable(const TemporaryDirectory& directory) {
  const auto path = directory.file("000001.sst.tmp");
  std::unique_ptr<SSTableBuilder> builder;
  ASSERT_OK(SSTableBuilder::open(path, SSTableBuilderOptions{1, {}}, builder));
  ASSERT_OK(builder->add(encodeInternalKey("alpha", 2, ValueType::kValue),
                         "value"));
  ASSERT_OK(builder->add(encodeInternalKey("beta", 1, ValueType::kValue),
                         "other"));
  SSTableMeta meta;
  ASSERT_OK(builder->finish(meta));
  return path;
}

int openReadOnly(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  ASSERT_TRUE(fd >= 0);
  return fd;
}

}

TEST(tableFormatDecodesFooter) {
  std::string encoded;
  putSSTableFooter(encoded, BlockHandle{12, 34});

  BlockHandle index_handle;
  ASSERT_OK(decodeSSTableFooter(encoded, index_handle));
  ASSERT_EQ(index_handle.offset, 12U);
  ASSERT_EQ(index_handle.size, 34U);
}

TEST(tableFormatRejectsInvalidFooter) {
  std::string encoded;
  putSSTableFooter(encoded, BlockHandle{12, 34});

  encoded[0] = 'X';
  BlockHandle index_handle;
  ASSERT_EQ(decodeSSTableFooter(encoded, index_handle).code(),
            StatusCode::kCorruption);

  encoded.clear();
  putSSTableFooter(encoded, BlockHandle{12, 34});
  encoded[kSSTableMagicSize] = 2;
  ASSERT_EQ(decodeSSTableFooter(encoded, index_handle).code(),
            StatusCode::kNotSupported);
}

TEST(tableFormatDoesNotConsumeMalformedHandle) {
  std::string encoded("short");
  Slice input(encoded);
  BlockHandle handle{7, 8};
  ASSERT_TRUE(!getBlockHandle(input, handle));
  ASSERT_EQ(input, Slice(encoded));
  ASSERT_EQ(handle.offset, 7U);
  ASSERT_EQ(handle.size, 8U);
}

TEST(tableIoReadsFooterAndBlocks) {
  TemporaryDirectory directory;
  const auto path = buildTable(directory);
  const std::uint64_t file_size = std::filesystem::file_size(path);
  const int fd = openReadOnly(path);

  BlockHandle index_handle;
  ASSERT_OK(readSSTableFooter(fd, file_size, index_handle));
  std::string payload;
  ASSERT_OK(readBlock(fd, file_size, index_handle, true, payload));
  ASSERT_TRUE(!payload.empty());
  ASSERT_TRUE(::close(fd) == 0);
}

TEST(tableIoRejectsOutOfRangeHandle) {
  TemporaryDirectory directory;
  const auto path = buildTable(directory);
  const std::uint64_t file_size = std::filesystem::file_size(path);
  const int fd = openReadOnly(path);

  std::string payload;
  ASSERT_EQ(readBlock(fd, file_size, BlockHandle{file_size, 0}, true, payload)
                .code(),
            StatusCode::kCorruption);
  ASSERT_EQ(readBlock(fd, file_size, BlockHandle{0, file_size}, true, payload)
                .code(),
            StatusCode::kCorruption);
  ASSERT_TRUE(::close(fd) == 0);
}

TEST(tableIoDetectsCorruptBlockChecksum) {
  TemporaryDirectory directory;
  const auto path = buildTable(directory);
  const std::uint64_t file_size = std::filesystem::file_size(path);
  const int fd = openReadOnly(path);

  BlockHandle index_handle;
  ASSERT_OK(readSSTableFooter(fd, file_size, index_handle));
  std::string payload;
  ASSERT_OK(readBlock(fd, file_size, index_handle, true, payload));
  ASSERT_TRUE(::close(fd) == 0);

  std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(file.is_open());
  file.seekp(static_cast<std::streamoff>(index_handle.offset));
  const char original = payload[0];
  file.put(static_cast<char>(original ^ 1));
  file.flush();
  file.close();

  const int corrupted_fd = openReadOnly(path);
  ASSERT_EQ(readBlock(corrupted_fd, file_size, index_handle, true, payload)
                .code(),
            StatusCode::kCorruption);
  ASSERT_TRUE(::close(corrupted_fd) == 0);
}

TEST(tableIoReportsShortReadAsCorruption) {
  TemporaryDirectory directory;
  const auto path = directory.file("short.sst");
  std::ofstream file(path, std::ios::binary);
  file << "short";
  file.close();

  const int fd = openReadOnly(path);
  BlockHandle index_handle;
  ASSERT_EQ(readSSTableFooter(fd, 5, index_handle).code(),
            StatusCode::kCorruption);
  ASSERT_TRUE(::close(fd) == 0);
}

}
