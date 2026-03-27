#include "db/filename.h"

#include <limits>
#include <string>
#include <string_view>

namespace lsmtree {
namespace {

// 将文件编号补足六位并追加类型后缀
std::string numberedName(std::uint64_t number, const char* suffix) {
  std::string digits = std::to_string(number);
  if (digits.size() < 6) digits.insert(0, 6 - digits.size(), '0');
  return digits + suffix;
}

// 解析十进制文件编号并检查溢出
bool parseNumber(std::string_view text, std::uint64_t& number) noexcept {
  if (text.empty()) return false;

  std::uint64_t parsed = 0;
  for (char character : text) {
    if (character < '0' || character > '9') return false;
    const std::uint64_t digit = static_cast<std::uint64_t>(character - '0');
    if (parsed > (std::numeric_limits<std::uint64_t>::max() - digit) / 10U) {
      return false;
    }
    parsed = parsed * 10U + digit;
  }
  if (parsed == 0) return false;
  number = parsed;
  return true;
}

}

std::filesystem::path lockFileName(const std::filesystem::path& db_directory) {
  return db_directory / "LOCK";
}

std::filesystem::path manifestFileName(
    const std::filesystem::path& db_directory) {
  return db_directory / "MANIFEST";
}

std::filesystem::path manifestTemporaryFileName(
    const std::filesystem::path& db_directory) {
  return db_directory / "MANIFEST.tmp";
}

std::filesystem::path walFileName(const std::filesystem::path& db_directory,
                                  std::uint64_t number) {
  return db_directory / numberedName(number, ".log");
}

std::filesystem::path sstableFileName(const std::filesystem::path& db_directory,
                                      std::uint64_t number) {
  return db_directory / numberedName(number, ".sst");
}

std::filesystem::path sstableTemporaryFileName(
    const std::filesystem::path& db_directory, std::uint64_t number) {
  return db_directory / numberedName(number, ".sst.tmp");
}

bool parseNumberedFileName(const std::filesystem::path& path,
                           std::uint64_t& number,
                           NumberedFileType& type) noexcept {
  const std::string name = path.filename().string();

  const struct {
    const char* suffix;
    NumberedFileType type;
  } suffixes[] = {
      {".sst.tmp", NumberedFileType::kSSTableTemporary},
      {".sst", NumberedFileType::kSSTable},
      {".log", NumberedFileType::kWal},
  };

  for (const auto& suffix : suffixes) {
    const std::string suffix_text = suffix.suffix;
    if (name.size() <= suffix_text.size() ||
        name.compare(name.size() - suffix_text.size(), suffix_text.size(),
                     suffix_text) != 0) {
      continue;
    }

    std::uint64_t parsed = 0;
    if (!parseNumber(
            std::string_view(name.data(), name.size() - suffix_text.size()),
            parsed)) {
      return false;
    }
    number = parsed;
    type = suffix.type;
    return true;
  }
  return false;
}

}
