#include <cstdint>

#include "test.h"
#include "util/crc32c.h"

namespace lsmtree {
namespace {

TEST(crc32cMatchesStandardVector) {
  ASSERT_EQ(crc32c("123456789"), 0xe3069283U);
}

TEST(crc32cOfEmptyInputIsZero) {
  ASSERT_EQ(crc32c(""), 0U);
}

TEST(crc32cChangesWhenPayloadChanges) {
  ASSERT_TRUE(crc32c("hello") != crc32c("jello"));
}

}
}
