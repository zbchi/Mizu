#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "db/internal_key.h"
#include "lsmtree/db.h"

namespace lsmtree {

struct ManifestTable {
  std::uint64_t number = 0;
  std::uint64_t file_size = 0;
  std::string smallest_key;
  std::string largest_key;
};

// 固定 MANIFEST 保存当前可见 L0 文件集的完整快照
struct ManifestState {
  SequenceNumber flushed_sequence = 0;
  std::uint64_t oldest_wal_number = 0;
  std::vector<ManifestTable> level0_tables;  // 新文件在前
};

// 读取并完整校验一份已经发布的 Manifest
Status readManifest(const std::filesystem::path& path, ManifestState& state);

// 先同步 temporary_path 再原子替换 path
Status writeManifest(const std::filesystem::path& path,
                     const std::filesystem::path& temporary_path,
                     const ManifestState& state);

}
