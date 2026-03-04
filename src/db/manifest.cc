#include "db/manifest.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <limits>
#include <set>
#include <string>
#include <system_error>
#include <utility>

#include "util/coding.h"
#include "util/crc32c.h"

namespace lsmtree {
namespace {

constexpr char kManifestMagic[] = "LSMMAN01";
constexpr std::size_t kManifestMagicSize = sizeof(kManifestMagic) - 1U;
constexpr std::uint32_t kManifestVersion = 1;
constexpr std::size_t kManifestChecksumSize = sizeof(std::uint32_t);
constexpr std::size_t kInternalKeyTagSize = sizeof(std::uint64_t);
constexpr std::size_t kMinimumTableEncodingSize =
    sizeof(std::uint64_t) * 2U + sizeof(std::uint32_t) * 2U +
    kInternalKeyTagSize * 2U;
constexpr std::size_t kMaximumManifestSize = 64U * 1024U * 1024U;

// 将 Manifest 文件操作错误统一转换为 Status
Status ioError(const char* operation, const std::filesystem::path& path,
               int error_number) {
  const std::error_code error(error_number, std::generic_category());
  return Status::ioError(std::string(operation) + " manifest " + path.string() +
                         ": " + error.message());
}

// 检查即将编码或已经解码的 Manifest 状态是否自洽
const char* validateState(const ManifestState& state) {
  if (state.flushed_sequence > kMaxSequenceNumber) {
    return "manifest flushed sequence is out of range";
  }
  if (state.oldest_wal_number == 0) {
    return "manifest WAL number must be positive";
  }
  if (state.level0_tables.size() > std::numeric_limits<std::uint32_t>::max()) {
    return "manifest has too many L0 tables";
  }

  std::set<std::uint64_t> numbers;
  for (const ManifestTable& table : state.level0_tables) {
    if (table.number == 0 || table.file_size == 0) {
      return "manifest contains invalid table metadata";
    }
    if (!numbers.insert(table.number).second) {
      return "manifest contains a duplicate table number";
    }
    if (table.smallest_key.size() > std::numeric_limits<std::uint32_t>::max() ||
        table.largest_key.size() > std::numeric_limits<std::uint32_t>::max()) {
      return "manifest table key is too large";
    }

    ParsedInternalKey smallest{};
    ParsedInternalKey largest{};
    if (!parseInternalKey(table.smallest_key, smallest) ||
        !parseInternalKey(table.largest_key, largest) ||
        InternalKeyLess{}(table.largest_key, table.smallest_key)) {
      return "manifest contains an invalid table key range";
    }
  }
  return nullptr;
}

// 从输入开头取出一个 fixed32 长度前缀字符串
bool takeString(Slice& input, std::string& value) {
  std::uint32_t size = 0;
  if (!getFixed32(input, size) || input.size() < size) return false;
  value.assign(input.data(), size);
  input.remove_prefix(size);
  return true;
}

// 将完整 Manifest 状态编码为带校验和的磁盘格式
Status encodeManifest(const ManifestState& state, std::string& output) {
  if (const char* error = validateState(state)) {
    return Status::invalidArgument(error);
  }

  std::string encoded(kManifestMagic, kManifestMagicSize);
  putFixed32(encoded, kManifestVersion);
  putFixed64(encoded, state.flushed_sequence);
  putFixed64(encoded, state.oldest_wal_number);
  putFixed32(encoded, static_cast<std::uint32_t>(state.level0_tables.size()));

  for (const ManifestTable& table : state.level0_tables) {
    putFixed64(encoded, table.number);
    putFixed64(encoded, table.file_size);
    putFixed32(encoded, static_cast<std::uint32_t>(table.smallest_key.size()));
    encoded.append(table.smallest_key);
    putFixed32(encoded, static_cast<std::uint32_t>(table.largest_key.size()));
    encoded.append(table.largest_key);
    if (encoded.size() > kMaximumManifestSize - kManifestChecksumSize) {
      return Status::invalidArgument("manifest exceeds 64 MiB");
    }
  }

  putFixed32(encoded, crc32c(encoded));
  output = std::move(encoded);
  return Status::success();
}

// 校验并解码 Manifest 失败时不修改调用方输出
Status decodeManifest(Slice input, ManifestState& output) {
  constexpr std::size_t kHeaderSize =
      kManifestMagicSize + sizeof(std::uint32_t) + sizeof(std::uint64_t) * 2U +
      sizeof(std::uint32_t);
  if (input.size() < kHeaderSize + kManifestChecksumSize) {
    return Status::corruption("manifest is shorter than its header");
  }
  if (input.size() > kMaximumManifestSize) {
    return Status::corruption("manifest exceeds 64 MiB");
  }

  const Slice contents = input.substr(0, input.size() - kManifestChecksumSize);
  const std::uint32_t stored_checksum =
      decodeFixed32(input.data() + input.size() - kManifestChecksumSize);
  if (crc32c(contents) != stored_checksum) {
    return Status::corruption("manifest checksum mismatch");
  }

  Slice cursor = contents;
  if (cursor.substr(0, kManifestMagicSize) !=
      Slice(kManifestMagic, kManifestMagicSize)) {
    return Status::corruption("invalid manifest magic");
  }
  cursor.remove_prefix(kManifestMagicSize);

  std::uint32_t version = 0;
  ManifestState decoded;
  std::uint32_t table_count = 0;
  if (!getFixed32(cursor, version) || version != kManifestVersion) {
    return Status::corruption("unsupported manifest version");
  }
  if (!getFixed64(cursor, decoded.flushed_sequence) ||
      !getFixed64(cursor, decoded.oldest_wal_number) ||
      !getFixed32(cursor, table_count)) {
    return Status::corruption("truncated manifest header");
  }
  if (table_count > cursor.size() / kMinimumTableEncodingSize) {
    return Status::corruption("manifest table count exceeds input");
  }

  decoded.level0_tables.reserve(table_count);
  for (std::uint32_t index = 0; index < table_count; ++index) {
    ManifestTable table;
    if (!getFixed64(cursor, table.number) ||
        !getFixed64(cursor, table.file_size) ||
        !takeString(cursor, table.smallest_key) ||
        !takeString(cursor, table.largest_key)) {
      return Status::corruption("truncated manifest table metadata");
    }
    decoded.level0_tables.push_back(std::move(table));
  }
  if (!cursor.empty()) {
    return Status::corruption("manifest has trailing bytes");
  }

  if (const char* error = validateState(decoded)) {
    return Status::corruption(error);
  }
  output = std::move(decoded);
  return Status::success();
}

// 处理短写和信号中断直到写完全部内容
Status writeAll(int fd, Slice data, const std::filesystem::path& path) {
  while (!data.empty()) {
    const ssize_t written = ::write(fd, data.data(), data.size());
    if (written > 0) {
      data.remove_prefix(static_cast<std::size_t>(written));
      continue;
    }
    if (written < 0 && errno == EINTR) continue;
    return ioError("write", path, written == 0 ? EIO : errno);
  }
  return Status::success();
}

// 将临时 Manifest 内容同步到磁盘
Status syncFile(int fd, const std::filesystem::path& path) {
  while (::fdatasync(fd) != 0) {
    if (errno == EINTR) continue;
    return ioError("sync", path, errno);
  }
  return Status::success();
}

// 完整读取 Manifest 文件并限制最大文件大小
Status readFile(const std::filesystem::path& path, std::string& output) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) return ioError("open", path, errno);

  struct stat info {};
  if (::fstat(fd, &info) != 0) {
    const Status status = ioError("stat", path, errno);
    ::close(fd);
    return status;
  }
  if (!S_ISREG(info.st_mode) || info.st_size < 0 ||
      static_cast<std::uint64_t>(info.st_size) > kMaximumManifestSize) {
    ::close(fd);
    return Status::corruption("manifest is not a valid regular file");
  }

  std::string bytes(static_cast<std::size_t>(info.st_size), '\0');
  std::size_t completed = 0;
  while (completed < bytes.size()) {
    const ssize_t read_size =
        ::read(fd, bytes.data() + completed, bytes.size() - completed);
    if (read_size > 0) {
      completed += static_cast<std::size_t>(read_size);
      continue;
    }
    if (read_size < 0 && errno == EINTR) continue;
    const Status status = read_size == 0
                              ? Status::corruption("manifest was truncated")
                              : ioError("read", path, errno);
    ::close(fd);
    return status;
  }
  if (::close(fd) != 0) return ioError("close", path, errno);
  output = std::move(bytes);
  return Status::success();
}

}

// 先读完整文件再提交解码结果
Status readManifest(const std::filesystem::path& path, ManifestState& state) {
  std::string encoded;
  Status status = readFile(path, encoded);
  if (!status.ok()) return status;
  return decodeManifest(encoded, state);
}

// 同步临时文件后通过 rename 原子发布新 Manifest
Status writeManifest(const std::filesystem::path& path,
                     const std::filesystem::path& temporary_path,
                     const ManifestState& state) {
  std::string encoded;
  Status status = encodeManifest(state, encoded);
  if (!status.ok()) return status;

  const int fd = ::open(temporary_path.c_str(),
                        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) return ioError("open", temporary_path, errno);

  status = writeAll(fd, encoded, temporary_path);
  if (status.ok()) status = syncFile(fd, temporary_path);
  if (::close(fd) != 0 && status.ok()) {
    status = ioError("close", temporary_path, errno);
  }
  if (!status.ok()) {
    ::unlink(temporary_path.c_str());
    return status;
  }

  if (::rename(temporary_path.c_str(), path.c_str()) != 0) {
    status = ioError("rename", temporary_path, errno);
    ::unlink(temporary_path.c_str());
    return status;
  }
  return Status::success();
}

}
