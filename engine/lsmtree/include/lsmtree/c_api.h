#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct lsm_db lsm_db_t;
typedef struct lsm_reader lsm_reader_t;
typedef struct lsm_batch lsm_batch_t;
typedef struct lsm_iterator lsm_iterator_t;

typedef struct {
  int code;
  const char* message;
} lsm_status_t;

lsm_status_t lsm_db_open(const char* directory, lsm_db_t** db);
void lsm_db_close(lsm_db_t* db);

lsm_status_t lsm_reader_open(lsm_db_t* db, lsm_reader_t** reader);
void lsm_reader_close(lsm_reader_t* reader);
lsm_status_t lsm_reader_get(const lsm_reader_t* reader, const uint8_t* key,
                            size_t key_size, uint8_t** value,
                            size_t* value_size, int* found);
lsm_status_t lsm_reader_new_iterator(const lsm_reader_t* reader,
                                     lsm_iterator_t** iterator);

int lsm_iterator_valid(const lsm_iterator_t* iterator);
void lsm_iterator_seek(lsm_iterator_t* iterator, const uint8_t* key,
                       size_t key_size);
void lsm_iterator_next(lsm_iterator_t* iterator);
lsm_status_t lsm_iterator_key(const lsm_iterator_t* iterator, uint8_t** key,
                              size_t* key_size);
lsm_status_t lsm_iterator_value(const lsm_iterator_t* iterator,
                                uint8_t** value, size_t* value_size);
lsm_status_t lsm_iterator_status(const lsm_iterator_t* iterator);
void lsm_iterator_close(lsm_iterator_t* iterator);

lsm_status_t lsm_batch_new(lsm_batch_t** batch);
void lsm_batch_clear(lsm_batch_t* batch);
void lsm_batch_close(lsm_batch_t* batch);
lsm_status_t lsm_batch_put(lsm_batch_t* batch, const uint8_t* key,
                           size_t key_size, const uint8_t* value,
                           size_t value_size);
lsm_status_t lsm_batch_erase(lsm_batch_t* batch, const uint8_t* key,
                             size_t key_size);
lsm_status_t lsm_db_write(lsm_db_t* db, lsm_batch_t* batch, int sync);

void lsm_bytes_free(void* bytes);

#ifdef __cplusplus
}
#endif
