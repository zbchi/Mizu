#include "util/coding.h"

#include <cassert>
#include <cstddef>

namespace lsmtree {
namespace {

std::uint64_t decodeFixed64(const char* data) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(
                 static_cast<unsigned char>(data[index]))
             << (index * 8U);
  }
  return value;
}

}

void encodeFixed32(char* dst, std::uint32_t value) noexcept {
  assert(dst != nullptr);
  dst[0] = static_cast<char>(value & 0xffU);
  dst[1] = static_cast<char>((value >> 8U) & 0xffU);
  dst[2] = static_cast<char>((value >> 16U) & 0xffU);
  dst[3] = static_cast<char>((value >> 24U) & 0xffU);
}

std::uint32_t decodeFixed32(const char* src) noexcept {
  assert(src != nullptr);
  return static_cast<std::uint32_t>(static_cast<unsigned char>(src[0])) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(src[1]))
          << 8U) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(src[2]))
          << 16U) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(src[3]))
          << 24U);
}

void putFixed32(std::string& dst, std::uint32_t value) {
  char encoded[sizeof(value)];
  encodeFixed32(encoded, value);
  dst.append(encoded, sizeof(encoded));
}

void putFixed64(std::string& dst, std::uint64_t value) {
  for (std::size_t index = 0; index < 8; ++index) {
    dst.push_back(static_cast<char>((value >> (index * 8U)) & 0xffU));
  }
}

bool getFixed32(Slice& input, std::uint32_t& value) {
  if (input.size() < 4) return false;

  const std::uint32_t decoded = decodeFixed32(input.data());
  input = input.substr(4);
  value = decoded;
  return true;
}

bool getFixed64(Slice& input, std::uint64_t& value) {
  if (input.size() < 8) return false;

  const std::uint64_t decoded = decodeFixed64(input.data());
  input = input.substr(8);
  value = decoded;
  return true;
}

}
