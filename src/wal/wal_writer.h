#pragma once

#include <filesystem>
#include <memory>

#include "lsmtree/db.h"

namespace lsmtree {

class WalWriter final {
 public:
  // 以追加模式打开文件 失败时保持 writer 不变
  static Status open(const std::filesystem::path& path,
                     std::unique_ptr<WalWriter>& writer);

  ~WalWriter();

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  // 追加一条完整逻辑记录 但不主动同步到磁盘
  Status append(Slice payload);
  Status sync();

 private:
  WalWriter(int fd, std::filesystem::path path);

  Status writeAll(Slice data);
  Status latchIOError(const char* operation, int error_number);

  int fd_;
  std::filesystem::path path_;
  Status error_;
};

}
