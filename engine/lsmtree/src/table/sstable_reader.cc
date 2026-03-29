#include "table/sstable_reader.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <system_error>
#include <utility>

#include "table/block_iterator.h"
#include "table/bloom_filter.h"
#include "table/table_format.h"
#include "table/table_io.h"

namespace lsmtree {
namespace {

Status ioError(const char* operation, const std::filesystem::path& path,
               int error_number) {
  const std::error_code error(error_number, std::generic_category());
  return Status::ioError(std::string(operation) + " SSTable " + path.string() +
                         ": " + error.message());
}

}

Status SSTableReader::open(const std::filesystem::path& path,
                           std::unique_ptr<SSTableReader>& reader) {
  if (path.empty()) {
    return Status::invalidArgument("SSTable path must not be empty");
  }

  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) return ioError("open", path, errno);

  // 立刻交给 reader 管理保证后续任意失败路径都会关闭 fd
  auto opened = std::unique_ptr<SSTableReader>(new SSTableReader(fd));

  // 从已经打开的 fd 获取大小避免 path 在 open 后被替换产生竞态
  struct stat file_info {};
  if (::fstat(fd, &file_info) != 0) {
    return ioError("stat", path, errno);
  }
  if (!S_ISREG(file_info.st_mode)) {
    return Status::invalidArgument("SSTable path is not a regular file: " +
                                   path.string());
  }
  if (file_info.st_size < 0) {
    return Status::corruption("SSTable has a negative file size");
  }
  opened->file_size_ = static_cast<std::uint64_t>(file_info.st_size);

  BlockHandle filter_handle;
  BlockHandle index_handle;
  Status status = readSSTableFooter(fd, opened->file_size_, filter_handle,
                                    index_handle);
  if (!status.ok()) return status;

  // filter 和 index 会影响后续寻址 打开时总是校验 checksum
  status = readBlock(fd, opened->file_size_, filter_handle, true,
                     opened->filter_block_);
  if (!status.ok()) return status;

  status = readBlock(fd, opened->file_size_, index_handle, true,
                     opened->index_block_);
  if (!status.ok()) return status;

  // 构造迭代器会检查 restart 元数据并允许合法的空 index block
  const BlockIterator index(opened->index_block_);
  if (!index.status().ok()) return index.status();

  // 只有文件完全可读后才向调用者发布 reader
  reader = std::move(opened);
  return Status::success();
}

SSTableReader::SSTableReader(int fd) noexcept : fd_(fd) {}

SSTableReader::~SSTableReader() {
  if (fd_ >= 0) ::close(fd_);
}

Status SSTableReader::get(const ReadOptions& options, Slice user_key,
                          SequenceNumber visible_sequence, LookupResult& result,
                          std::string& value) const {
  assert(visible_sequence <= kMaxSequenceNumber);

  if (!bloomFilterMayContain(user_key, filter_block_)) {
    result = LookupResult::kAbsent;
    return Status::success();
  }

  // kValue 是同一 sequence 下最大的 ValueType 也就是查询下界
  const std::string lookup_key =
      encodeInternalKey(user_key, visible_sequence, ValueType::kValue);

  // index key 是对应 data block 的最后一个 InternalKey
  // 第一个不小于 lookup_key 的 index entry 覆盖目标可能出现的位置
  BlockIterator index(index_block_);
  if (!index.status().ok()) return index.status();
  index.seek(lookup_key);
  if (!index.status().ok()) return index.status();
  if (!index.valid()) {
    result = LookupResult::kAbsent;
    return Status::success();
  }

  Slice encoded_handle = index.value();
  BlockHandle data_handle;
  // 当前 format 的 index value 必须恰好包含一个固定长度 BlockHandle
  if (!getBlockHandle(encoded_handle, data_handle) || !encoded_handle.empty()) {
    return Status::corruption("invalid data block handle in SSTable index");
  }

  std::string data_block;
  Status status = readBlock(fd_, file_size_, data_handle,
                            options.verify_checksums, data_block);
  if (!status.ok()) return status;

  BlockIterator data(data_block);
  if (!data.status().ok()) return data.status();
  data.seek(lookup_key);
  if (!data.status().ok()) return data.status();
  // index key 声明这个 block 的末尾不小于 lookup_key
  // 因此 seek 无结果说明 index 和 data block 互相矛盾
  if (!data.valid()) {
    return Status::corruption("SSTable index does not cover its data block");
  }

  ParsedInternalKey parsed{};
  if (!parseInternalKey(data.key(), parsed)) {
    return Status::corruption("invalid internal key in SSTable data block");
  }
  // seek 可能停在下一个 user key 此时当前 SSTable 没有可见版本
  if (parsed.user_key != user_key) {
    result = LookupResult::kAbsent;
    return Status::success();
  }
  if (parsed.type == ValueType::kDeletion) {
    // tombstone 会遮蔽更老存储层中的 value
    result = LookupResult::kDeleted;
    return Status::success();
  }

  const Slice stored_value = data.value();
  std::string found_value(stored_value.data(), stored_value.size());
  value = std::move(found_value);
  result = LookupResult::kValue;
  return Status::success();
}

std::unique_ptr<SSTableIterator> SSTableReader::newIterator(
    const ReadOptions& options) const {
  return std::unique_ptr<SSTableIterator>(new SSTableIterator(*this, options));
}

SSTableIterator::SSTableIterator(const SSTableReader& table,
                                 ReadOptions options)
    : table_(table),
      options_(std::move(options)),
      index_(std::make_unique<BlockIterator>(table.index_block_)) {
  // index 常驻 reader 内存 构造时先锁存其格式错误
  if (!index_->status().ok()) status_ = index_->status();
}

SSTableIterator::~SSTableIterator() = default;

void SSTableIterator::seekToFirst() {
  if (!status_.ok()) return;
  // 重置当前 data block 后从第一条 index entry 重新定位
  data_.reset();
  data_block_.clear();
  index_->seekToFirst();
  if (!index_->status().ok()) {
    status_ = index_->status();
    return;
  }
  if (index_->valid()) loadDataBlock();
}

void SSTableIterator::seek(Slice target) {
  if (!status_.ok()) return;
  ParsedInternalKey parsed{};
  if (!parseInternalKey(target, parsed)) {
    status_ = Status::invalidArgument("invalid InternalKey seek target");
    data_.reset();
    return;
  }

  data_.reset();
  data_block_.clear();
  // index entry 保存对应 data block 的最后一个 InternalKey。
  // 先定位可能覆盖 target 的 block，再在 block 内继续 seek。
  index_->seek(target);
  if (!index_->status().ok()) {
    status_ = index_->status();
    return;
  }
  if (!index_->valid()) return;

  loadDataBlock();
  if (!status_.ok()) return;
  data_->seek(target);
  if (!data_->status().ok()) {
    status_ = data_->status();
    data_.reset();
    return;
  }
  if (!data_->valid()) {
    status_ = Status::corruption("SSTable index does not cover seek target");
    data_.reset();
  }
}

void SSTableIterator::next() {
  assert(valid());
  data_->next();
  if (!data_->status().ok()) {
    status_ = data_->status();
    data_.reset();
    return;
  }
  if (data_->valid()) return;

  // 当前 data block 耗尽后由下一条 index entry 加载下一块
  index_->next();
  if (!index_->status().ok()) {
    status_ = index_->status();
    data_.reset();
    return;
  }
  data_.reset();
  if (index_->valid()) loadDataBlock();
}

bool SSTableIterator::valid() const noexcept {
  return status_.ok() && data_ && data_->valid();
}

Slice SSTableIterator::internalKey() const {
  assert(valid());
  return data_->key();
}

Slice SSTableIterator::value() const {
  assert(valid());
  return data_->value();
}

void SSTableIterator::loadDataBlock() {
  // index value 必须完整编码且只能编码一个 BlockHandle
  Slice encoded_handle = index_->value();
  BlockHandle handle;
  if (!getBlockHandle(encoded_handle, handle) || !encoded_handle.empty()) {
    status_ = Status::corruption("invalid data block handle in SSTable index");
    return;
  }

  status_ = readBlock(table_.fd_, table_.file_size_, handle,
                      options_.verify_checksums, data_block_);
  if (!status_.ok()) return;

  // BlockIterator 借用 data_block_ 切换 block 前必须先销毁旧迭代器
  data_ = std::make_unique<BlockIterator>(data_block_);
  if (!data_->status().ok()) {
    status_ = data_->status();
    data_.reset();
    return;
  }
  data_->seekToFirst();
  if (!data_->status().ok()) {
    status_ = data_->status();
    data_.reset();
    return;
  }
  if (!data_->valid()) {
    status_ = Status::corruption("SSTable index references an empty data block");
    data_.reset();
  }
}

}
