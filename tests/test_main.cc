#include "test.h"

int main() {
  int failures = 0;
  for (const auto& test : lsmtree::test::Registry()) {
    try {
      test.function();
      std::cout << "[PASS] " << test.name << '\n';
    } catch (const std::exception& error) {
      ++failures;
      std::cerr << "[FAIL] " << test.name << ": " << error.what() << '\n';
    }
  }
  return failures == 0 ? 0 : 1;
}
