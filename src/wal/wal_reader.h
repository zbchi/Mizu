#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "lsmtree/db.h"

namespace lsmtree {

enum class WalReadResult {
  kRecord,
  kEnd,
};

class WalReader final {
 public:
  // 打开已有 WAL 并从头顺序读取 失败时保持 reader 不变
  static Status open(const std::filesystem::path& path,
                     std::unique_ptr<WalReader>& reader);

  ~WalReader();

  WalReader(const WalReader&) = delete;
  WalReader& operator=(const WalReader&) = delete;

  // 完整记录校验通过时返回 kRecord 文件结束或尾部记录残缺时返回 kEnd
  // 失败时保持两个输出不变 kEnd 不修改 batch_payload
  Status readNext(std::string& batch_payload, WalReadResult& result);

  // 最后一条完整且校验通过的记录之后的文件偏移
  std::uint64_t validBytes() const noexcept { return valid_bytes_; }

 private:
  WalReader(int fd, std::filesystem::path path);

  Status readUpTo(char* destination, std::size_t size,
                  std::size_t& bytes_read);
  Status corruptionAt(const char* message) const;

  int fd_;
  std::filesystem::path path_;
  std::uint64_t valid_bytes_ = 0;
};

}
