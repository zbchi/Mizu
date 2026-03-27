#include "util/crc32c.h"

namespace lsmtree {
namespace {

// Castagnoli 多项式的反射形式
constexpr std::uint32_t kCrc32cPolynomial = 0x82f63b78U;

}

std::uint32_t crc32c(Slice input) {
  std::uint32_t crc = 0xffffffffU;

  for (const char character : input) {
    crc ^= static_cast<unsigned char>(character);
    for (int bit = 0; bit < 8; ++bit) {
      if ((crc & 1U) != 0U) {
        crc = (crc >> 1U) ^ kCrc32cPolynomial;
      } else {
        crc >>= 1U;
      }
    }
  }

  return ~crc;
}

}
