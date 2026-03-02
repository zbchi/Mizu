#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "lsmtree/db.h"
#include "table/block_builder.h"
#include "table/bloom_filter.h"
#include "table/table_format.h"

namespace lsmtree {

struct SSTableBuilderOptions {
  std::size_t block_size = 4U * 1024U;
  BlockBuilderOptions block_options;
};

struct SSTableMeta {
  std::uint64_t file_size = 0;
  std::uint64_t entry_count = 0;
  std::string smallest_key;
  std::string largest_key;
};

class SSTableBuilder final {
 public:
  // 创建临时 SSTable 文件且不覆盖已有文件
  static Status open(const std::filesystem::path& temporary_path,
                     SSTableBuilderOptions options,
                     std::unique_ptr<SSTableBuilder>& builder);

  ~SSTableBuilder();

  SSTableBuilder(const SSTableBuilder&) = delete;
  SSTableBuilder& operator=(const SSTableBuilder&) = delete;

  // 在可写状态加入严格递增的 InternalKey
  Status add(Slice internal_key, Slice value);

  // 在可写状态写出索引和 footer 并同步关闭文件
  Status finish(SSTableMeta& meta);

  // 关闭并删除未完成的临时文件
  void abandon() noexcept;

 private:
  enum class State {
    kBuilding,
    kFinished,
    kAbandoned,
  };

  SSTableBuilder(int fd, std::filesystem::path path,
                 SSTableBuilderOptions options);

  // 写出 data block 并记录索引位置
  Status flushDataBlock();

  // 写出 block payload 及带校验和的 trailer
  Status writeBlock(Slice payload, BlockHandle& handle);

  // 完整写入数据并处理短写
  Status writeAll(Slice data);

  // 同步文件数据并重试中断
  Status sync();

  // 锁存并返回第一个 IO 错误
  Status latchIOError(const char* operation, int error_number);

  int fd_;
  std::filesystem::path path_;
  std::size_t block_size_;
  State state_ = State::kBuilding;
  Status error_;
  BlockBuilder data_block_;
  BlockBuilder index_block_;
  BloomFilterBuilder bloom_filter_;
  std::uint64_t file_offset_ = 0;
  std::uint64_t entry_count_ = 0;
  std::string first_key_;
  std::string last_key_;
};

}
