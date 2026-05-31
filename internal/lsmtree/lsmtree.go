// Package lsmtree is the small cgo boundary for the C++ lsmtree engine.
// Database-specific details stay here so the rest of Mizu remains pure Go.
package lsmtree

/*
#cgo CFLAGS: -I${SRCDIR}/../../engine/lsmtree/include
#cgo linux LDFLAGS: ${SRCDIR}/../../engine/lsmtree/build/liblsmtree.a -lstdc++ -pthread
#include <stdlib.h>
#include "lsmtree/c_api.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

type DB struct {
	ptr *C.lsm_db_t
}

type Reader struct {
	ptr *C.lsm_reader_t
}

type Batch struct {
	ptr *C.lsm_batch_t
}

type Iterator struct {
	ptr *C.lsm_iterator_t
}

type Item struct {
	Key   []byte
	Value []byte
}

type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("lsmtree error (code %d)", e.Code)
	}
	return e.Message
}

func statusError(status C.lsm_status_t) error {
	if status.code == 0 {
		return nil
	}
	message := ""
	if status.message != nil {
		message = C.GoString(status.message)
	}
	return &Error{Code: int(status.code), Message: message}
}

func bytesPointer(value []byte) (*C.uint8_t, C.size_t) {
	if len(value) == 0 {
		return nil, 0
	}
	return (*C.uint8_t)(unsafe.Pointer(&value[0])), C.size_t(len(value))
}

func copyBytes(value *C.uint8_t, size C.size_t) []byte {
	if value == nil || size == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(value), C.int(size))
}

func Open(directory string) (*DB, error) {
	cDirectory := C.CString(directory)
	defer C.free(unsafe.Pointer(cDirectory))

	var ptr *C.lsm_db_t
	status := C.lsm_db_open(cDirectory, &ptr)
	if err := statusError(status); err != nil {
		return nil, err
	}
	return &DB{ptr: ptr}, nil
}

func (db *DB) Close() {
	if db == nil || db.ptr == nil {
		return
	}
	C.lsm_db_close(db.ptr)
	db.ptr = nil
}

func (db *DB) Reader() (*Reader, error) {
	if db == nil || db.ptr == nil {
		return nil, fmt.Errorf("lsmtree database is closed")
	}
	var ptr *C.lsm_reader_t
	status := C.lsm_reader_open(db.ptr, &ptr)
	if err := statusError(status); err != nil {
		return nil, err
	}
	return &Reader{ptr: ptr}, nil
}

func (reader *Reader) Close() {
	if reader == nil || reader.ptr == nil {
		return
	}
	C.lsm_reader_close(reader.ptr)
	reader.ptr = nil
}

func (reader *Reader) Get(key []byte) ([]byte, bool, error) {
	if reader == nil || reader.ptr == nil {
		return nil, false, fmt.Errorf("lsmtree reader is closed")
	}
	cKey, keySize := bytesPointer(key)
	var value *C.uint8_t
	var valueSize C.size_t
	var found C.int
	status := C.lsm_reader_get(reader.ptr, cKey, keySize, &value, &valueSize, &found)
	runtime.KeepAlive(key)
	if err := statusError(status); err != nil {
		return nil, false, err
	}
	defer C.lsm_bytes_free(unsafe.Pointer(value))
	return copyBytes(value, valueSize), found != 0, nil
}

func (reader *Reader) Iterator() (*Iterator, error) {
	if reader == nil || reader.ptr == nil {
		return nil, fmt.Errorf("lsmtree reader is closed")
	}
	var ptr *C.lsm_iterator_t
	status := C.lsm_reader_new_iterator(reader.ptr, &ptr)
	if err := statusError(status); err != nil {
		return nil, err
	}
	return &Iterator{ptr: ptr}, nil
}

func (iterator *Iterator) Seek(key []byte) {
	if iterator == nil || iterator.ptr == nil {
		return
	}
	cKey, keySize := bytesPointer(key)
	C.lsm_iterator_seek(iterator.ptr, cKey, keySize)
	runtime.KeepAlive(key)
}

func (iterator *Iterator) Valid() bool {
	return iterator != nil && iterator.ptr != nil && C.lsm_iterator_valid(iterator.ptr) != 0
}

func (iterator *Iterator) Next() {
	if iterator == nil || iterator.ptr == nil {
		return
	}
	C.lsm_iterator_next(iterator.ptr)
}

func (iterator *Iterator) Item() (Item, error) {
	if !iterator.Valid() {
		return Item{}, fmt.Errorf("invalid lsmtree iterator")
	}
	var key *C.uint8_t
	var keySize C.size_t
	status := C.lsm_iterator_key(iterator.ptr, &key, &keySize)
	if err := statusError(status); err != nil {
		return Item{}, err
	}
	defer C.lsm_bytes_free(unsafe.Pointer(key))

	var value *C.uint8_t
	var valueSize C.size_t
	status = C.lsm_iterator_value(iterator.ptr, &value, &valueSize)
	if err := statusError(status); err != nil {
		return Item{}, err
	}
	defer C.lsm_bytes_free(unsafe.Pointer(value))
	return Item{Key: copyBytes(key, keySize), Value: copyBytes(value, valueSize)}, nil
}

func (iterator *Iterator) Status() error {
	if iterator == nil || iterator.ptr == nil {
		return fmt.Errorf("lsmtree iterator is closed")
	}
	return statusError(C.lsm_iterator_status(iterator.ptr))
}

func (iterator *Iterator) Close() {
	if iterator == nil || iterator.ptr == nil {
		return
	}
	C.lsm_iterator_close(iterator.ptr)
	iterator.ptr = nil
}

func (db *DB) NewBatch() (*Batch, error) {
	if db == nil || db.ptr == nil {
		return nil, fmt.Errorf("lsmtree database is closed")
	}
	var ptr *C.lsm_batch_t
	status := C.lsm_batch_new(&ptr)
	if err := statusError(status); err != nil {
		return nil, err
	}
	return &Batch{ptr: ptr}, nil
}

func (batch *Batch) Clear() {
	if batch == nil || batch.ptr == nil {
		return
	}
	C.lsm_batch_clear(batch.ptr)
}

func (batch *Batch) Close() {
	if batch == nil || batch.ptr == nil {
		return
	}
	C.lsm_batch_close(batch.ptr)
	batch.ptr = nil
}

func (batch *Batch) Put(key, value []byte) error {
	if batch == nil || batch.ptr == nil {
		return fmt.Errorf("lsmtree batch is closed")
	}
	cKey, keySize := bytesPointer(key)
	cValue, valueSize := bytesPointer(value)
	status := C.lsm_batch_put(batch.ptr, cKey, keySize, cValue, valueSize)
	runtime.KeepAlive(key)
	runtime.KeepAlive(value)
	return statusError(status)
}

func (batch *Batch) Erase(key []byte) error {
	if batch == nil || batch.ptr == nil {
		return fmt.Errorf("lsmtree batch is closed")
	}
	cKey, keySize := bytesPointer(key)
	status := C.lsm_batch_erase(batch.ptr, cKey, keySize)
	runtime.KeepAlive(key)
	return statusError(status)
}

func (db *DB) Write(batch *Batch, sync bool) error {
	if db == nil || db.ptr == nil {
		return fmt.Errorf("lsmtree database is closed")
	}
	if batch == nil || batch.ptr == nil {
		return fmt.Errorf("lsmtree batch is closed")
	}
	return statusError(C.lsm_db_write(db.ptr, batch.ptr, C.int(boolInt(sync))))
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
