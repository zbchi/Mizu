#pragma once

#include <cstdint>
#include <filesystem>

namespace lsmtree {

enum class NumberedFileType {
  kWal,
  kSSTable,
  kSSTableTemporary,
};

// 构造数据库目录内各类文件的完整路径
std::filesystem::path lockFileName(const std::filesystem::path& db_directory);
std::filesystem::path manifestFileName(
    const std::filesystem::path& db_directory);
std::filesystem::path manifestTemporaryFileName(
    const std::filesystem::path& db_directory);
std::filesystem::path walFileName(const std::filesystem::path& db_directory,
                                  std::uint64_t number = 1);
std::filesystem::path sstableFileName(const std::filesystem::path& db_directory,
                                      std::uint64_t number);
std::filesystem::path sstableTemporaryFileName(
    const std::filesystem::path& db_directory, std::uint64_t number);

// 只识别本项目生成的编号文件 文件编号必须大于零
bool parseNumberedFileName(const std::filesystem::path& path,
                           std::uint64_t& number,
                           NumberedFileType& type) noexcept;

}
