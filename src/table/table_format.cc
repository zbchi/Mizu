#include "table/table_format.h"

#include "util/coding.h"

namespace lsmtree {

void putBlockHandle(std::string& destination, const BlockHandle& handle) {
  putFixed64(destination, handle.offset);
  putFixed64(destination, handle.size);
}

bool getBlockHandle(Slice& input, BlockHandle& handle) {
  Slice remaining = input;
  BlockHandle decoded;
  if (!getFixed64(remaining, decoded.offset) ||
      !getFixed64(remaining, decoded.size)) {
    return false;
  }

  input = remaining;
  handle = decoded;
  return true;
}

void putSSTableFooter(std::string& destination,
                      const BlockHandle& index_handle) {
  destination.append(kSSTableMagic, kSSTableMagicSize);
  putFixed32(destination, kSSTableVersion);
  putFixed32(destination, 0);
  putBlockHandle(destination, index_handle);
}

Status decodeSSTableFooter(Slice encoded, BlockHandle& index_handle) {
  if (encoded.size() != kSSTableFooterSize) {
    return Status::corruption("invalid SSTable footer size");
  }

  const Slice magic(encoded.data(), kSSTableMagicSize);
  if (magic != Slice(kSSTableMagic, kSSTableMagicSize)) {
    return Status::corruption("invalid SSTable magic");
  }

  Slice input = encoded.substr(kSSTableMagicSize);
  std::uint32_t version = 0;
  std::uint32_t reserved = 0;
  BlockHandle decoded_handle;
  if (!getFixed32(input, version) || !getFixed32(input, reserved) ||
      !getBlockHandle(input, decoded_handle) || !input.empty()) {
    return Status::corruption("invalid SSTable footer");
  }
  if (version != kSSTableVersion) {
    return Status::notSupported("unsupported SSTable version");
  }
  if (reserved != 0) {
    return Status::corruption("non-zero SSTable footer reserved field");
  }

  index_handle = decoded_handle;
  return Status::success();
}

}
