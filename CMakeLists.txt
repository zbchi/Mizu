cmake_minimum_required(VERSION 3.20)

project(lsmtree VERSION 0.1.0 LANGUAGES CXX)

option(LSMTREE_BUILD_TESTS "Build lsmtree tests" ON)
find_package(Threads REQUIRED)

add_library(lsmtree
  src/db/db.cc
  src/db/db_impl.cc
  src/db/flush_memtable.cc
  src/db/filename.cc
  src/db/internal_key.cc
  src/db/manifest.cc
  src/db/memtable.cc
  src/db/write_batch.cc
  src/db/write_batch_codec.cc
  src/table/bloom_filter.cc
  src/table/block_iterator.cc
  src/table/block_builder.cc
  src/table/sstable_builder.cc
  src/table/sstable_reader.cc
  src/table/table_io.cc
  src/table/table_format.cc
  src/util/arena.cc
  src/util/coding.cc
  src/util/crc32c.cc
  src/util/status.cc
  src/wal/wal_reader.cc
  src/wal/wal_writer.cc
)
add_library(lsmtree::lsmtree ALIAS lsmtree)
target_link_libraries(lsmtree PRIVATE Threads::Threads)

target_compile_features(lsmtree PUBLIC cxx_std_17)
target_include_directories(lsmtree
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_options(lsmtree PRIVATE -Wall -Wextra -Wpedantic)

if(LSMTREE_BUILD_TESTS)
  enable_testing()

  add_executable(lsmtree_tests
    tests/test_main.cc
    tests/db/db_test.cc
    tests/db/flush_memtable_test.cc
    tests/db/internal_key_test.cc
    tests/db/manifest_test.cc
    tests/db/memtable_test.cc
    tests/db/skiplist_test.cc
    tests/db/write_batch_codec_test.cc
    tests/table/bloom_filter_test.cc
    tests/table/block_iterator_test.cc
    tests/table/block_builder_test.cc
    tests/table/sstable_builder_test.cc
    tests/table/sstable_reader_test.cc
    tests/table/table_io_test.cc
    tests/util/coding_test.cc
    tests/util/crc32c_test.cc
    tests/util/arena_test.cc
    tests/wal/wal_reader_test.cc
    tests/wal/wal_writer_test.cc
  )
  target_link_libraries(lsmtree_tests PRIVATE lsmtree::lsmtree)
  target_compile_features(lsmtree_tests PRIVATE cxx_std_17)
  target_include_directories(lsmtree_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/tests
  )
  target_compile_options(lsmtree_tests PRIVATE -Wall -Wextra -Wpedantic)

  add_test(NAME lsmtree_tests COMMAND lsmtree_tests)
endif()
