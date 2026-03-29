#include "wal/wal_reader.h"

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "test.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "wal/wal_format.h"

namespace lsmtree {
namespace {

class ReaderTempDirectory {
 public:
  ReaderTempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("lsmtree-wal-reader-test-" + std::to_string(getpid()) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~ReaderTempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  std::filesystem::path file(const char* name = "000001.log") const {
    return path_ / name;
  }

 private:
  std::filesystem::path path_;
};

std::string encodeWalRecord(Slice payload) {
  // 独立构造输入记录 便于精确截断或破坏其中的字节
  std::string record;
  putFixed32(record, static_cast<std::uint32_t>(payload.size()));
  putFixed32(record, crc32c(payload));
  record.append(payload);
  return record;
}

void writeFile(const std::filesystem::path& path, Slice contents) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.is_open());
  output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  ASSERT_TRUE(output.good());
}

std::unique_ptr<WalReader> openReader(const std::filesystem::path& path) {
  std::unique_ptr<WalReader> reader;
  ASSERT_OK(WalReader::open(path, reader));
  ASSERT_TRUE(reader != nullptr);
  return reader;
}

TEST(walReaderReturnsEndForEmptyFileWithoutChangingPayload) {
  ReaderTempDirectory directory;
  writeFile(directory.file(), "");
  auto reader = openReader(directory.file());

  std::string batch_payload = "unchanged";
  WalReadResult result = WalReadResult::kRecord;
  ASSERT_OK(reader->readNext(batch_payload, result));

  ASSERT_EQ(result, WalReadResult::kEnd);
  ASSERT_EQ(batch_payload, "unchanged");
  ASSERT_EQ(reader->validBytes(), 0U);
}

TEST(walReaderReadsConsecutiveBinaryRecords) {
  ReaderTempDirectory directory;
  const std::string first("a\0b", 3);
  const std::string first_record = encodeWalRecord(first);
  const std::string second_record = encodeWalRecord("second");
  writeFile(directory.file(), first_record + second_record);
  auto reader = openReader(directory.file());

  std::string batch_payload;
  WalReadResult result = WalReadResult::kEnd;
  ASSERT_OK(reader->readNext(batch_payload, result));
  ASSERT_EQ(result, WalReadResult::kRecord);
  ASSERT_EQ(batch_payload, first);
  ASSERT_EQ(reader->validBytes(), first_record.size());

  ASSERT_OK(reader->readNext(batch_payload, result));
  ASSERT_EQ(result, WalReadResult::kRecord);
  ASSERT_EQ(batch_payload, "second");
  ASSERT_EQ(reader->validBytes(), first_record.size() + second_record.size());

  batch_payload = "unchanged";
  ASSERT_OK(reader->readNext(batch_payload, result));
  ASSERT_EQ(result, WalReadResult::kEnd);
  ASSERT_EQ(batch_payload, "unchanged");
}

TEST(walReaderIgnoresEveryIncompleteHeaderTail) {
  ReaderTempDirectory directory;
  const std::string record = encodeWalRecord("payload");

  for (std::size_t size = 1; size < kWalRecordHeaderSize; ++size) {
    writeFile(directory.file(), Slice(record.data(), size));
    auto reader = openReader(directory.file());

    std::string batch_payload = "unchanged";
    WalReadResult result = WalReadResult::kRecord;
    ASSERT_OK(reader->readNext(batch_payload, result));
    ASSERT_EQ(result, WalReadResult::kEnd);
    ASSERT_EQ(batch_payload, "unchanged");
    ASSERT_EQ(reader->validBytes(), 0U);
  }
}

TEST(walReaderIgnoresEveryIncompletePayloadTail) {
  ReaderTempDirectory directory;
  const std::string record = encodeWalRecord("payload");

  for (std::size_t size = kWalRecordHeaderSize; size < record.size(); ++size) {
    writeFile(directory.file(), Slice(record.data(), size));
    auto reader = openReader(directory.file());

    std::string batch_payload = "unchanged";
    WalReadResult result = WalReadResult::kRecord;
    ASSERT_OK(reader->readNext(batch_payload, result));
    ASSERT_EQ(result, WalReadResult::kEnd);
    ASSERT_EQ(batch_payload, "unchanged");
    ASSERT_EQ(reader->validBytes(), 0U);
  }
}

TEST(walReaderPreservesLastValidBoundaryBeforeTruncatedTail) {
  ReaderTempDirectory directory;
  const std::string first_record = encodeWalRecord("first");
  const std::string second_record = encodeWalRecord("second");
  writeFile(directory.file(),
            first_record + second_record.substr(0, second_record.size() - 1U));
  auto reader = openReader(directory.file());

  std::string batch_payload;
  WalReadResult result = WalReadResult::kEnd;
  ASSERT_OK(reader->readNext(batch_payload, result));
  ASSERT_EQ(result, WalReadResult::kRecord);
  ASSERT_EQ(batch_payload, "first");

  batch_payload = "unchanged";
  ASSERT_OK(reader->readNext(batch_payload, result));
  ASSERT_EQ(result, WalReadResult::kEnd);
  ASSERT_EQ(batch_payload, "unchanged");
  ASSERT_EQ(reader->validBytes(), first_record.size());
}

TEST(walReaderRejectsChecksumMismatchWithoutChangingOutputs) {
  ReaderTempDirectory directory;
  std::string record = encodeWalRecord("payload");
  record.back() ^= 1;
  writeFile(directory.file(), record);
  auto reader = openReader(directory.file());

  std::string batch_payload = "unchanged";
  WalReadResult result = WalReadResult::kEnd;
  const Status status = reader->readNext(batch_payload, result);
  ASSERT_EQ(status.code(), StatusCode::kCorruption);
  ASSERT_EQ(batch_payload, "unchanged");
  ASSERT_EQ(result, WalReadResult::kEnd);
  ASSERT_EQ(reader->validBytes(), 0U);
}

TEST(walReaderRejectsInvalidPayloadLengths) {
  ReaderTempDirectory directory;
  for (const std::uint32_t length :
       {0U, static_cast<std::uint32_t>(kMaxWalRecordPayloadSize + 1U)}) {
    std::string header;
    putFixed32(header, length);
    putFixed32(header, 0);
    writeFile(directory.file(), header);
    auto reader = openReader(directory.file());

    std::string batch_payload;
    WalReadResult result = WalReadResult::kEnd;
    ASSERT_EQ(reader->readNext(batch_payload, result).code(),
              StatusCode::kCorruption);
    ASSERT_EQ(reader->validBytes(), 0U);
  }
}

TEST(walReaderPreservesOutputWhenOpenFails) {
  ReaderTempDirectory directory;
  writeFile(directory.file(), "");
  auto reader = openReader(directory.file());
  WalReader* const original = reader.get();

  ASSERT_EQ(WalReader::open(directory.file("missing.log"), reader).code(),
            StatusCode::kIOError);
  ASSERT_TRUE(reader.get() == original);
}

}
}
