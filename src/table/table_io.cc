#include "table/table_io.h"

#include <cerrno>
#include <cstdint>
#include <string>
#include <system_error>
#include <utility>

#include <sys/types.h>
#include <unistd.h>

#include "util/crc32c.h"
#include "util/coding.h"

namespace lsmtree {
namespace {

// 完整读取指定文件范围并重试中断
Status readExactly(int fd, std::uint64_t offset, std::size_t size,
                   std::string& output) {
  std::string bytes(size, '\0');
  std::size_t completed = 0;
  while (completed < size) {
    const ssize_t result = ::pread(
        fd, bytes.data() + completed, size - completed,
        static_cast<off_t>(offset + completed));
    if (result > 0) {
      completed += static_cast<std::size_t>(result);
      continue;
    }
    if (result == 0) {
      return Status::corruption("truncated SSTable input");
    }
    if (errno == EINTR) continue;

    const std::error_code error(errno, std::generic_category());
    return Status::ioError("read SSTable: " + error.message());
  }

  output = std::move(bytes);
  return Status::success();
}

// 校验 block 和 trailer 均位于 footer 之前
Status validateBlockHandle(std::uint64_t file_size,
                           const BlockHandle& handle) {
  if (file_size < kSSTableFooterSize) {
    return Status::corruption("SSTable is shorter than its footer");
  }

  const std::uint64_t block_limit = file_size - kSSTableFooterSize;
  if (handle.offset > block_limit) {
    return Status::corruption("block offset is outside SSTable");
  }

  const std::uint64_t remaining = block_limit - handle.offset;
  if (handle.size > remaining ||
      kBlockTrailerSize > remaining - handle.size) {
    return Status::corruption("block extends outside SSTable");
  }

  return Status::success();
}

}

Status readSSTableFooter(int fd, std::uint64_t file_size,
                         BlockHandle& index_handle) {
  if (file_size < kSSTableFooterSize) {
    return Status::corruption("SSTable is shorter than its footer");
  }

  std::string encoded;
  Status status = readExactly(fd, file_size - kSSTableFooterSize,
                              kSSTableFooterSize, encoded);
  if (!status.ok()) return status;

  BlockHandle decoded_handle;
  status = decodeSSTableFooter(encoded, decoded_handle);
  if (!status.ok()) return status;

  status = validateBlockHandle(file_size, decoded_handle);
  if (!status.ok()) return status;

  index_handle = decoded_handle;
  return Status::success();
}

Status readBlock(int fd, std::uint64_t file_size, const BlockHandle& handle,
                 bool verify_checksum, std::string& payload) {
  Status status = validateBlockHandle(file_size, handle);
  if (!status.ok()) return status;

  const std::size_t payload_size = static_cast<std::size_t>(handle.size);
  const std::size_t block_size = payload_size + kBlockTrailerSize;
  std::string block;
  status = readExactly(fd, handle.offset, block_size, block);
  if (!status.ok()) return status;

  // payload 后的 trailer 依次保存压缩类型和校验和
  const char* trailer = block.data() + payload_size;
  const std::uint8_t compression =
      static_cast<std::uint8_t>(trailer[0]);
  if (verify_checksum) {
    const std::uint32_t stored_checksum = decodeFixed32(trailer + 1);
    const std::uint32_t computed_checksum =
        crc32c(Slice(block.data(), payload_size + 1));
    if (stored_checksum != computed_checksum) {
      return Status::corruption("SSTable block checksum mismatch");
    }
  }

  if (compression != kNoCompression) {
    return Status::corruption("unsupported SSTable block compression");
  }

  block.resize(payload_size);
  payload = std::move(block);
  return Status::success();
}

}
