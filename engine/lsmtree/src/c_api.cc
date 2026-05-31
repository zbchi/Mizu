#include "lsmtree/c_api.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <new>
#include <string>

#include "lsmtree/db.h"

struct lsm_db {
  std::shared_ptr<lsmtree::DB> db;
};

struct lsm_reader {
  std::shared_ptr<lsmtree::DB> db;
  lsmtree::SnapshotHandle snapshot;
};

struct lsm_batch {
  lsmtree::WriteBatch batch;
};

struct lsm_iterator {
  std::unique_ptr<lsmtree::Iterator> iterator;
};

namespace {

thread_local std::string last_error;

lsm_status_t success() { return {0, nullptr}; }

lsm_status_t status(const lsmtree::Status& value) {
  if (value.ok()) return success();
  last_error = value.toString();
  return {static_cast<int>(value.code()), last_error.c_str()};
}

lsm_status_t invalid(const char* message) {
  last_error = message;
  return {static_cast<int>(lsmtree::StatusCode::kInvalidArgument),
          last_error.c_str()};
}

lsm_status_t exception(const std::exception& error) {
  last_error = error.what();
  return {static_cast<int>(lsmtree::StatusCode::kIOError),
          last_error.c_str()};
}

lsm_status_t unknownException() {
  last_error = "unknown lsmtree exception";
  return {static_cast<int>(lsmtree::StatusCode::kIOError),
          last_error.c_str()};
}

lsm_status_t copySlice(lsmtree::Slice slice, uint8_t** output,
                       size_t* output_size) {
  if (output == nullptr || output_size == nullptr) return invalid("null output");
  *output = nullptr;
  *output_size = slice.size();
  if (slice.empty()) return success();

  auto* bytes = static_cast<uint8_t*>(std::malloc(slice.size()));
  if (bytes == nullptr) return exception(std::bad_alloc());
  std::memcpy(bytes, slice.data(), slice.size());
  *output = bytes;
  return success();
}

lsmtree::Slice makeSlice(const uint8_t* bytes, size_t size) {
  if (size == 0) return {};
  return {reinterpret_cast<const char*>(bytes), size};
}

}  // namespace

extern "C" {

lsm_status_t lsm_db_open(const char* directory, lsm_db_t** db) {
  if (directory == nullptr || db == nullptr) return invalid("null open argument");
  *db = nullptr;
  try {
    auto holder = std::make_unique<lsm_db>();
    lsmtree::DB::Handle opened;
    lsmtree::DBOptions options;
    lsmtree::Status result = lsmtree::DB::open(options, directory, &opened);
    if (!result.ok()) return status(result);
    holder->db = std::shared_ptr<lsmtree::DB>(opened.release());
    *db = holder.release();
    return success();
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

void lsm_db_close(lsm_db_t* db) { delete db; }

lsm_status_t lsm_reader_open(lsm_db_t* db, lsm_reader_t** reader) {
  if (db == nullptr || reader == nullptr) return invalid("null reader argument");
  *reader = nullptr;
  try {
    auto holder = std::make_unique<lsm_reader>();
    holder->db = db->db;
    lsmtree::Status result = holder->db->newSnapshot(&holder->snapshot);
    if (!result.ok()) return status(result);
    *reader = holder.release();
    return success();
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

void lsm_reader_close(lsm_reader_t* reader) { delete reader; }

lsm_status_t lsm_reader_get(const lsm_reader_t* reader, const uint8_t* key,
                            size_t key_size, uint8_t** value,
                            size_t* value_size, int* found) {
  if (reader == nullptr || reader->db == nullptr || value == nullptr || value_size == nullptr ||
      found == nullptr || (key == nullptr && key_size != 0)) {
    return invalid("null get argument");
  }
  *value = nullptr;
  *value_size = 0;
  *found = 0;
  try {
    lsmtree::ReadOptions options;
    options.snapshot = reader->snapshot;
    std::string result_value;
    const lsmtree::Status get_result =
        reader->db->get(options, makeSlice(key, key_size), &result_value);
    if (get_result.isNotFound()) return success();
    if (!get_result.ok()) return status(get_result);
    *found = 1;
    return copySlice(result_value, value, value_size);
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

lsm_status_t lsm_reader_new_iterator(const lsm_reader_t* reader,
                                     lsm_iterator_t** iterator) {
  if (reader == nullptr || reader->db == nullptr || iterator == nullptr) {
    return invalid("null iterator argument");
  }
  *iterator = nullptr;
  try {
    auto holder = std::make_unique<lsm_iterator>();
    lsmtree::ReadOptions options;
    options.snapshot = reader->snapshot;
    lsmtree::Status result =
        reader->db->newIterator(options, &holder->iterator);
    if (!result.ok()) return status(result);
    *iterator = holder.release();
    return success();
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

int lsm_iterator_valid(const lsm_iterator_t* iterator) {
  return iterator != nullptr && iterator->iterator != nullptr &&
         iterator->iterator->valid();
}

void lsm_iterator_seek(lsm_iterator_t* iterator, const uint8_t* key,
                       size_t key_size) {
  if (iterator == nullptr || iterator->iterator == nullptr) return;
  iterator->iterator->seek(makeSlice(key, key_size));
}

void lsm_iterator_next(lsm_iterator_t* iterator) {
  if (iterator == nullptr || iterator->iterator == nullptr) return;
  iterator->iterator->next();
}

lsm_status_t lsm_iterator_key(const lsm_iterator_t* iterator, uint8_t** key,
                              size_t* key_size) {
  if (iterator == nullptr || iterator->iterator == nullptr ||
      !iterator->iterator->valid()) {
    return invalid("invalid iterator");
  }
  try {
    return copySlice(iterator->iterator->key(), key, key_size);
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

lsm_status_t lsm_iterator_value(const lsm_iterator_t* iterator,
                                uint8_t** value, size_t* value_size) {
  if (iterator == nullptr || iterator->iterator == nullptr ||
      !iterator->iterator->valid()) {
    return invalid("invalid iterator");
  }
  try {
    return copySlice(iterator->iterator->value(), value, value_size);
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

lsm_status_t lsm_iterator_status(const lsm_iterator_t* iterator) {
  if (iterator == nullptr || iterator->iterator == nullptr) {
    return invalid("invalid iterator");
  }
  return status(iterator->iterator->status());
}

void lsm_iterator_close(lsm_iterator_t* iterator) { delete iterator; }

lsm_status_t lsm_batch_new(lsm_batch_t** batch) {
  if (batch == nullptr) return invalid("null batch argument");
  *batch = nullptr;
  try {
    *batch = new lsm_batch();
    return success();
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

void lsm_batch_clear(lsm_batch_t* batch) {
  if (batch != nullptr) batch->batch.clear();
}

void lsm_batch_close(lsm_batch_t* batch) { delete batch; }

lsm_status_t lsm_batch_put(lsm_batch_t* batch, const uint8_t* key,
                           size_t key_size, const uint8_t* value,
                           size_t value_size) {
  if (batch == nullptr || (key == nullptr && key_size != 0) ||
      (value == nullptr && value_size != 0)) {
    return invalid("null put argument");
  }
  try {
    batch->batch.put(makeSlice(key, key_size), makeSlice(value, value_size));
    return success();
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

lsm_status_t lsm_batch_erase(lsm_batch_t* batch, const uint8_t* key,
                             size_t key_size) {
  if (batch == nullptr || (key == nullptr && key_size != 0)) {
    return invalid("null erase argument");
  }
  try {
    batch->batch.erase(makeSlice(key, key_size));
    return success();
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

lsm_status_t lsm_db_write(lsm_db_t* db, lsm_batch_t* batch, int sync) {
  if (db == nullptr || batch == nullptr) return invalid("null write argument");
  try {
    lsmtree::WriteOptions options;
    options.durability = sync ? lsmtree::Durability::kSync
                               : lsmtree::Durability::kAsync;
    return status(db->db->write(options, batch->batch));
  } catch (const std::exception& error) {
    return exception(error);
  } catch (...) {
    return unknownException();
  }
}

void lsm_bytes_free(void* bytes) { std::free(bytes); }

}  // extern "C"
