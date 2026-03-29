#include "wal/wal_reader.h"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <string>
#include <system_error>
#include <utility>

#include "util/coding.h"
#include "util/crc32c.h"
#include "wal/wal_format.h"

namespace lsmtree {

Status WalReader::open(const std::filesystem::path& path,
                       std::unique_ptr<WalReader>& reader) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    const int error_number = errno;
    const std::error_code error(error_number, std::generic_category());
    return Status::ioError("open WAL " + path.string() + ": " +
                           error.message());
  }

  auto opened = std::unique_ptr<WalReader>(new WalReader(fd, path));
  reader = std::move(opened);
  return Status::success();
}

WalReader::WalReader(int fd, std::filesystem::path path)
    : fd_(fd), path_(std::move(path)) {}

WalReader::~WalReader() {
  if (fd_ >= 0) ::close(fd_);
}

Status WalReader::readNext(std::string& batch_payload,
                           WalReadResult& result) {
  std::array<char, kWalRecordHeaderSize> header{};
  std::size_t header_bytes = 0;
  Status status = readUpTo(header.data(), header.size(), header_bytes);
  if (!status.ok()) return status;
  // 文件末尾的残缺记录来自未完成写入 按正常结束处理
  if (header_bytes < header.size()) {
    result = WalReadResult::kEnd;
    return Status::success();
  }

  Slice header_input(header.data(), header.size());
  std::uint32_t payload_length = 0;
  std::uint32_t expected_checksum = 0;
  if (!getFixed32(header_input, payload_length) ||
      !getFixed32(header_input, expected_checksum)) {
    return corruptionAt("invalid WAL record header");
  }
  if (payload_length == 0) {
    return corruptionAt("empty WAL record payload");
  }
  if (payload_length > kMaxWalRecordPayloadSize) {
    return corruptionAt("WAL record payload exceeds 64 MiB");
  }

  // 先读入临时字符串 校验通过前不修改调用方输出
  std::string decoded_payload;
  decoded_payload.resize(payload_length);
  std::size_t payload_bytes = 0;
  status = readUpTo(decoded_payload.data(), decoded_payload.size(),
                    payload_bytes);
  if (!status.ok()) return status;
  if (payload_bytes < decoded_payload.size()) {
    result = WalReadResult::kEnd;
    return Status::success();
  }
  if (crc32c(decoded_payload) != expected_checksum) {
    return corruptionAt("WAL record checksum mismatch");
  }

  // 只有完整且校验和正确的记录才推进有效边界
  valid_bytes_ += kWalRecordHeaderSize + payload_length;
  batch_payload = std::move(decoded_payload);
  result = WalReadResult::kRecord;
  return Status::success();
}

Status WalReader::readUpTo(char* destination, std::size_t size,
                           std::size_t& bytes_read) {
  // 处理短读和信号中断 EOF 时返回实际读到的字节数
  std::size_t total = 0;
  while (total < size) {
    const ssize_t count = ::read(fd_, destination + total, size - total);
    if (count > 0) {
      total += static_cast<std::size_t>(count);
      continue;
    }
    if (count == 0) break;

    const int error_number = errno;
    if (error_number == EINTR) continue;

    const std::error_code error(error_number, std::generic_category());
    return Status::ioError("read WAL " + path_.string() + ": " +
                           error.message());
  }

  bytes_read = total;
  return Status::success();
}

Status WalReader::corruptionAt(const char* message) const {
  return Status::corruption(std::string(message) + " at offset " +
                            std::to_string(valid_bytes_));
}

}
