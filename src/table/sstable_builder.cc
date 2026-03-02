#include "table/sstable_builder.h"

#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <system_error>
#include <utility>

#include "db/internal_key.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace lsmtree {

Status SSTableBuilder::open(const std::filesystem::path& temporary_path,
                            SSTableBuilderOptions options,
                            std::unique_ptr<SSTableBuilder>& builder) {
  if (temporary_path.empty()) {
    return Status::invalidArgument("SSTable path must not be empty");
  }
  if (options.block_size == 0) {
    return Status::invalidArgument("SSTable block size must be positive");
  }
  if (options.block_options.restart_interval == 0) {
    return Status::invalidArgument("block restart interval must be positive");
  }

  const int fd = ::open(temporary_path.c_str(),
                        O_WRONLY | O_CREAT | O_EXCL, 0644);
  if (fd < 0) {
    const int error_number = errno;
    if (error_number == EEXIST) {
      return Status::alreadyExists("SSTable already exists: " +
                                   temporary_path.string());
    }
    const std::error_code error(error_number, std::generic_category());
    return Status::ioError("open SSTable " + temporary_path.string() + ": " +
                           error.message());
  }

  builder = std::unique_ptr<SSTableBuilder>(
      new SSTableBuilder(fd, temporary_path, options));
  return Status::success();
}

SSTableBuilder::SSTableBuilder(int fd, std::filesystem::path path,
                               SSTableBuilderOptions options)
    : fd_(fd),
      path_(std::move(path)),
      block_size_(options.block_size),
      data_block_(options.block_options),
      index_block_(options.block_options) {}

SSTableBuilder::~SSTableBuilder() { abandon(); }

Status SSTableBuilder::add(Slice internal_key, Slice value) {
  if (!error_.ok()) return error_;
  assert(state_ == State::kBuilding);

  ParsedInternalKey parsed{};
  if (!parseInternalKey(internal_key, parsed)) {
    return Status::invalidArgument("invalid SSTable internal key");
  }
  assert(entry_count_ == 0 ||
         InternalKeyLess{}(Slice(last_key_), internal_key));

  bloom_filter_.add(parsed.user_key);

  if (entry_count_ == 0) {
    first_key_.assign(internal_key.data(), internal_key.size());
  }
  last_key_.assign(internal_key.data(), internal_key.size());
  data_block_.add(internal_key, value);
  ++entry_count_;

  if (data_block_.currentSizeEstimate() >= block_size_) {
    return flushDataBlock();
  }
  return Status::success();
}

Status SSTableBuilder::finish(SSTableMeta& meta) {
  if (!error_.ok()) return error_;
  assert(state_ == State::kBuilding);

  Status status = flushDataBlock();
  if (!status.ok()) return status;

  BlockHandle filter_handle;
  status = writeBlock(bloom_filter_.finish(), filter_handle);
  if (!status.ok()) return status;

  BlockHandle index_handle;
  status = writeBlock(index_block_.finish(), index_handle);
  if (!status.ok()) return status;

  std::string footer;
  footer.reserve(kSSTableFooterSize);
  putSSTableFooter(footer, filter_handle, index_handle);
  status = writeAll(footer);
  if (!status.ok()) return status;
  file_offset_ += footer.size();

  status = sync();
  if (!status.ok()) return status;

  const int fd = fd_;
  fd_ = -1;
  if (::close(fd) != 0) {
    return latchIOError("close SSTable", errno);
  }

  SSTableMeta completed;
  completed.file_size = file_offset_;
  completed.entry_count = entry_count_;
  completed.smallest_key = first_key_;
  completed.largest_key = last_key_;
  meta = std::move(completed);
  state_ = State::kFinished;
  return Status::success();
}

void SSTableBuilder::abandon() noexcept {
  if (state_ == State::kFinished || state_ == State::kAbandoned) return;

  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  ::unlink(path_.c_str());
  state_ = State::kAbandoned;
}

Status SSTableBuilder::flushDataBlock() {
  if (data_block_.empty()) return Status::success();

  BlockHandle handle;
  Status status = writeBlock(data_block_.finish(), handle);
  if (!status.ok()) return status;

  std::string encoded_handle;
  encoded_handle.reserve(kBlockHandleSize);
  putBlockHandle(encoded_handle, handle);
  index_block_.add(last_key_, encoded_handle);
  data_block_.reset();
  return Status::success();
}

Status SSTableBuilder::writeBlock(Slice payload, BlockHandle& handle) {
  std::string block_contents(payload);
  block_contents.push_back(static_cast<char>(kNoCompression));
  const std::uint32_t checksum = crc32c(block_contents);
  putFixed32(block_contents, checksum);

  handle = BlockHandle{file_offset_, payload.size()};
  Status status = writeAll(block_contents);
  if (!status.ok()) return status;
  file_offset_ += block_contents.size();
  return Status::success();
}

Status SSTableBuilder::writeAll(Slice data) {
  while (!data.empty()) {
    const ssize_t written = ::write(fd_, data.data(), data.size());
    if (written > 0) {
      data.remove_prefix(static_cast<std::size_t>(written));
      continue;
    }

    const int error_number = written == 0 ? EIO : errno;
    if (written < 0 && error_number == EINTR) continue;
    return latchIOError("write SSTable", error_number);
  }
  return Status::success();
}

Status SSTableBuilder::sync() {
  while (::fdatasync(fd_) != 0) {
    const int error_number = errno;
    if (error_number == EINTR) continue;
    return latchIOError("sync SSTable", error_number);
  }
  return Status::success();
}

Status SSTableBuilder::latchIOError(const char* operation, int error_number) {
  if (error_.ok()) {
    const std::error_code error(error_number, std::generic_category());
    error_ = Status::ioError(std::string(operation) + " " + path_.string() +
                             ": " + error.message());
  }
  return error_;
}

}
