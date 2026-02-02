cmake_minimum_required(VERSION 3.20)

project(lsmtree VERSION 0.1.0 LANGUAGES CXX)

option(LSMTREE_BUILD_TESTS "Build lsmtree tests" ON)

add_library(lsmtree
  src/db/db.cc
  src/db/db_impl.cc
  src/db/filename.cc
  src/db/write_batch.cc
  src/util/status.cc
)
add_library(lsmtree::lsmtree ALIAS lsmtree)

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
  )
  target_link_libraries(lsmtree_tests PRIVATE lsmtree::lsmtree)
  target_compile_features(lsmtree_tests PRIVATE cxx_std_17)
  target_include_directories(lsmtree_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  target_compile_options(lsmtree_tests PRIVATE -Wall -Wextra -Wpedantic)

  add_test(NAME lsmtree_tests COMMAND lsmtree_tests)
endif()
