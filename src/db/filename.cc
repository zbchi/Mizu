#include "db/filename.h"

namespace lsmtree {

std::filesystem::path lockFileName(const std::filesystem::path& db_directory) {
  return db_directory / "LOCK";
}

std::filesystem::path walFileName(const std::filesystem::path& db_directory) {
  return db_directory / "000001.log";
}

}
