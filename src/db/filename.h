#pragma once

#include <filesystem>

namespace lsmtree {

std::filesystem::path lockFileName(const std::filesystem::path& db_directory);
std::filesystem::path walFileName(const std::filesystem::path& db_directory);

}
