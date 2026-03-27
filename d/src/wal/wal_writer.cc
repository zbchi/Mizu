#include "wal/wal_writer.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <string>
#include <system_error>
#include <utility>

#include "util/coding.h"
#include "util/crc32c.h"
#include "wal/wal_format.h"

namespace lsmtree {

Status WalWriter::open(const std::filesystem::path& path,
                       std::unique_ptr<WalWriter>& writer) {
  const int fd =
      ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) {
    const int error_number = errno;
    const std::error_code error(error_number, std::generic_category());
    return Status::ioError("open WAL " + path.string() + ": " +
                           error.message());
  }

  auto opened = std::unique_ptr<WalWriter>(new WalWriter(fd, path));
  writer = std::move(opened);
  return Status::success();
}

WalWriter::WalWriter(int fd, std::filesystem::path path)
    : fd_(fd), path_(std::move(path)) {}

WalWriter::~WalWriter() {
  if (fd_ >= 0) ::close(fd_);
}

Status WalWriter::append(Slice payload) {
  if (!error_.ok()) return error_;
  if (payload.empty()) {
    return Status::invalidArgument("WAL record payload must not be empty");
  }
  if (payload.size() > kMaxWalRecordPayloadSize) {
    return Status::invalidArgument("WAL record payload exceeds 64 MiB");
  }

  // 校验和只覆盖 payload reader 可以先按固定头取出长度
  std::string header;
  header.reserve(kWalRecordHeaderSize);
  putFixed32(header, static_cast<std::uint32_t>(payload.size()));
  putFixed32(header, crc32c(payload));

  Status status = writeAll(header);
  if (!status.ok()) return status;
  return writeAll(payload);
}

Status WalWriter::sync() {
  if (!error_.ok()) return error_;

  while (::fdatasync(fd_) != 0) {
    const int error_number = errno;
    if (error_number == EINTR) continue;
    return latchIOError("sync WAL", error_number);
  }
  return Status::success();
}

Status WalWriter::writeAll(Slice data) {
  // 处理短写和信号中断 直到消费完全部数据
  while (!data.empty()) {
    const ssize_t written = ::write(fd_, data.data(), data.size());
    if (written > 0) {
      data.remove_prefix(static_cast<std::size_t>(written));
      continue;
    }

    const int error_number = written == 0 ? EIO : errno;
    if (written < 0 && error_number == EINTR) continue;
    return latchIOError("write WAL", error_number);
  }
  return Status::success();
}

Status WalWriter::latchIOError(const char* operation, int error_number) {
  // 保留首次错误 避免残缺 WAL 上继续写入
  if (error_.ok()) {
    const std::error_code error(error_number, std::generic_category());
    error_ = Status::ioError(std::string(operation) + " " + path_.string() +
                             ": " + error.message());
  }
  return error_;
}

}
