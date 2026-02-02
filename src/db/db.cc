#include "lsmtree/db.h"

#include "db/db_impl.h"

namespace lsmtree {

Status DB::open(const DBOptions& options,
                const std::filesystem::path& directory, Handle* db) {
  return DBImpl::open(options, directory, db);
}

Status DB::put(const WriteOptions& options, Slice key, Slice value) {
  WriteBatch batch;
  batch.put(key, value);
  return write(options, batch);
}

Status DB::erase(const WriteOptions& options, Slice key) {
  WriteBatch batch;
  batch.erase(key);
  return write(options, batch);
}

}
