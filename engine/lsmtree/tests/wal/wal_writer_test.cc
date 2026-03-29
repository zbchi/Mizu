#include "wal/wal_writer.h"

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>

#include "test.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "wal/wal_format.h"

namespace lsmtree {
namespace {

class TempDirectory {
 public:
  TempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-wal-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~TempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  std::filesystem::path file(const char* name = "000001.log") const {
    return path_ / name;
  }

 private:
  std::filesystem::path path_;
};

std::unique_ptr<WalWriter> openWriter(const std::filesystem::path& path) {
  std::unique_ptr<WalWriter> writer;
  ASSERT_OK(WalWriter::open(path, writer));
  ASSERT_TRUE(writer != nullptr);
  return writer;
}

std::string readFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  ASSERT_TRUE(input.is_open());
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

std::string encodeRecord(Slice payload) {
  // 独立构造期望字节 避免用被测 writer 验证自身格式
  std::string record;
  putFixed32(record, static_cast<std::uint32_t>(payload.size()));
  putFixed32(record, crc32c(payload));
  record.append(payload);
  return record;
}

TEST(walWriterUsesStableRecordEncoding) {
  TempDirectory directory;
  const auto path = directory.file();
  auto writer = openWriter(path);

  ASSERT_OK(writer->append("abc"));
  writer.reset();

  ASSERT_EQ(readFile(path), encodeRecord("abc"));
}

TEST(walWriterAppendsConsecutiveBinaryRecords) {
  TempDirectory directory;
  const auto path = directory.file();
  auto writer = openWriter(path);
  const std::string binary_payload("a\0b\0c", 5);

  ASSERT_OK(writer->append(binary_payload));
  ASSERT_OK(writer->append("second"));
  writer.reset();

  ASSERT_EQ(readFile(path),
            encodeRecord(binary_payload) + encodeRecord("second"));
}

TEST(walWriterReopensAtEndOfExistingLog) {
  TempDirectory directory;
  const auto path = directory.file();
  {
    auto writer = openWriter(path);
    ASSERT_OK(writer->append("first"));
  }
  {
    auto writer = openWriter(path);
    ASSERT_OK(writer->append("second"));
  }

  ASSERT_EQ(readFile(path), encodeRecord("first") + encodeRecord("second"));
}

TEST(walWriterRejectsInvalidPayloadWithoutPoisoningWriter) {
  TempDirectory directory;
  const auto path = directory.file();
  auto writer = openWriter(path);

  ASSERT_EQ(writer->append("").code(), StatusCode::kInvalidArgument);
  ASSERT_EQ(
      writer->append(std::string(kMaxWalRecordPayloadSize + 1U, 'x')).code(),
      StatusCode::kInvalidArgument);
  ASSERT_OK(writer->append("valid"));
  writer.reset();

  ASSERT_EQ(readFile(path), encodeRecord("valid"));
}

TEST(walWriterPreservesOutputWhenOpenFails) {
  TempDirectory directory;
  auto writer = openWriter(directory.file());
  WalWriter* const original = writer.get();

  ASSERT_EQ(
      WalWriter::open(directory.file("missing/000001.log"), writer).code(),
      StatusCode::kIOError);
  ASSERT_TRUE(writer.get() == original);
}

TEST(walWriterSyncsRegularFile) {
  TempDirectory directory;
  auto writer = openWriter(directory.file());

  ASSERT_OK(writer->append("record"));
  ASSERT_OK(writer->sync());
}

TEST(walWriterLatchesFirstIOError) {
  // /dev/full 可稳定触发写入失败
  std::unique_ptr<WalWriter> writer;
  ASSERT_OK(WalWriter::open("/dev/full", writer));

  const Status first = writer->append("record");
  const Status second = writer->append("another");
  const Status sync = writer->sync();

  ASSERT_EQ(first.code(), StatusCode::kIOError);
  ASSERT_EQ(second.toString(), first.toString());
  ASSERT_EQ(sync.toString(), first.toString());
}

}
}
