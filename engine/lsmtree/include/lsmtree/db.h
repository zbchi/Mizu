#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

namespace lsmtree {

// Slice 只借用外部内存 调用方需要保证使用期间内容有效
using Slice = std::string_view;

enum class StatusCode : std::uint8_t {
  kOk,
  kNotFound,
  kAlreadyExists,
  kInvalidArgument,
  kIOError,
  kCorruption,
  kBusy,
  kNotSupported,
};

class [[nodiscard]] Status {
 public:
  Status();

  static Status success();
  static Status notFound(std::string message);
  static Status alreadyExists(std::string message);
  static Status invalidArgument(std::string message);
  static Status ioError(std::string message);
  static Status corruption(std::string message);
  static Status busy(std::string message);
  static Status notSupported(std::string message);

  bool ok() const noexcept;
  bool isNotFound() const noexcept;
  StatusCode code() const noexcept;
  const std::string& message() const noexcept;
  std::string toString() const;

 private:
  Status(StatusCode code, std::string message);

  StatusCode code_;
  std::string message_;
};

enum class OpenMode {
  kOpenExisting,
  kCreateNew,
  kOpenOrCreate,
};

enum class Durability {
  kAsync,
  kSync,
};

enum class CacheMode {
  kFill,
  kBypass,
};

struct DBOptions {
  OpenMode open_mode = OpenMode::kOpenOrCreate;
  std::size_t write_buffer_size = 4U * 1024U * 1024U;
};

class Snapshot {
 public:
  virtual ~Snapshot() = default;

 protected:
  Snapshot() = default;
};

using SnapshotHandle = std::shared_ptr<const Snapshot>;

struct ReadOptions {
  SnapshotHandle snapshot{};
  bool verify_checksums = false;
  CacheMode cache_mode = CacheMode::kFill;
};

struct WriteOptions {
  Durability durability = Durability::kAsync;
};

class DBImpl;
class WriteBatchCodec;

class WriteBatch {
 public:
  WriteBatch();
  WriteBatch(WriteBatch&&) noexcept;
  WriteBatch& operator=(WriteBatch&&) noexcept;
  ~WriteBatch();

  WriteBatch(const WriteBatch&) = delete;
  WriteBatch& operator=(const WriteBatch&) = delete;

  WriteBatch& put(Slice key, Slice value);
  WriteBatch& erase(Slice key);
  void clear() noexcept;
  std::size_t count() const noexcept;
  bool empty() const noexcept;

 private:
  class Rep;
  std::unique_ptr<Rep> rep_;

  friend class DBImpl;
  friend class WriteBatchCodec;
};

class Iterator {
 public:
  virtual ~Iterator() = default;

  virtual bool valid() const noexcept = 0;
  virtual void seekToFirst() = 0;
  virtual void seek(Slice target) = 0;
  virtual void next() = 0;

  // 仅在 valid() 为 true 时读取 返回内容在迭代器下次移动前有效
  virtual Slice key() const = 0;
  virtual Slice value() const = 0;
  virtual Status status() const = 0;
};

class DB {
 public:
  using Handle = std::unique_ptr<DB>;

  static Status open(const DBOptions& options,
                     const std::filesystem::path& directory, Handle* db);

  Status put(const WriteOptions& options, Slice key, Slice value);
  Status erase(const WriteOptions& options, Slice key);

  virtual Status write(const WriteOptions& options,
                       const WriteBatch& batch) = 0;
  virtual Status get(const ReadOptions& options, Slice key,
                     std::string* value) const = 0;
  virtual Status newSnapshot(SnapshotHandle* snapshot) const = 0;
  virtual Status newIterator(const ReadOptions& options,
                             std::unique_ptr<Iterator>* iterator) const = 0;

  virtual ~DB() = default;

  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

 protected:
  DB() = default;
};

}
