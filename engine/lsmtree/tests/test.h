#pragma once

#include <exception>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "lsmtree/db.h"

namespace lsmtree::test {

struct TestCase {
  const char* name;
  std::function<void()> function;
};

inline std::vector<TestCase>& Registry() {
  static std::vector<TestCase> registry;
  return registry;
}

class Registrar {
 public:
  Registrar(const char* name, std::function<void()> function) {
    Registry().push_back({name, std::move(function)});
  }
};

inline void Fail(const char* expression, const char* file, int line,
                 const std::string& detail = {}) {
  std::ostringstream output;
  output << file << ':' << line << ": assertion failed: " << expression;
  if (!detail.empty()) output << " (" << detail << ')';
  throw std::runtime_error(output.str());
}

inline void AssertOk(const Status& status, const char* expression,
                     const char* file, int line) {
  if (!status.ok()) Fail(expression, file, line, status.toString());
}

}

// 定义函数时同步注册 测试主程序无需维护显式列表
#define TEST(name)                                                          \
  void name();                                                              \
  static ::lsmtree::test::Registrar name##_registrar(#name, name);         \
  void name()

#define ASSERT_TRUE(expression)                                             \
  do {                                                                      \
    if (!(expression)) ::lsmtree::test::Fail(#expression, __FILE__, __LINE__); \
  } while (false)

#define ASSERT_EQ(left, right)                                              \
  do {                                                                      \
    const auto& actual = (left);                                            \
    const auto& expected = (right);                                        \
    if (!(actual == expected)) {                                            \
      ::lsmtree::test::Fail(#left " == " #right, __FILE__, __LINE__);      \
    }                                                                       \
  } while (false)

#define ASSERT_OK(expression)                                               \
  ::lsmtree::test::AssertOk((expression), #expression, __FILE__, __LINE__)
