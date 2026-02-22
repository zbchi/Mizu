#include "table/table_format.h"

#include "util/coding.h"

namespace lsmtree {

void putBlockHandle(std::string& destination, const BlockHandle& handle) {
  putFixed64(destination, handle.offset);
  putFixed64(destination, handle.size);
}

void putSSTableFooter(std::string& destination,
                      const BlockHandle& index_handle) {
  destination.append(kSSTableMagic, kSSTableMagicSize);
  putFixed32(destination, kSSTableVersion);
  putFixed32(destination, 0);
  putBlockHandle(destination, index_handle);
}

}
