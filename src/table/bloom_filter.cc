#include "table/bloom_filter.h"

#include <algorithm>
#include <cstdint>

#include "util/coding.h"

namespace lsmtree {
namespace {

constexpr std::uint32_t kBloomHashSeed = 0xbc9f1d34U;
constexpr std::size_t kBloomBitsPerKey = 10;
constexpr unsigned char kBloomProbes = 6;
constexpr std::size_t kMinimumBloomBits = 64;
constexpr unsigned char kMaximumProbes = 30;

std::uint32_t bloomHash(Slice key) noexcept {
  constexpr std::uint32_t multiplier = 0xc6a4a793U;
  constexpr unsigned int remainder_shift = 24;

  const char* data = key.data();
  std::size_t remaining = key.size();
  std::uint32_t hash =
      kBloomHashSeed ^ (static_cast<std::uint32_t>(remaining) * multiplier);

  while (remaining >= sizeof(std::uint32_t)) {
    hash += decodeFixed32(data);
    hash *= multiplier;
    hash ^= hash >> 16U;
    data += sizeof(std::uint32_t);
    remaining -= sizeof(std::uint32_t);
  }

  switch (remaining) {
    case 3:
      hash += static_cast<std::uint32_t>(static_cast<unsigned char>(data[2]))
              << 16U;
      [[fallthrough]];
    case 2:
      hash += static_cast<std::uint32_t>(static_cast<unsigned char>(data[1]))
              << 8U;
      [[fallthrough]];
    case 1:
      hash += static_cast<unsigned char>(data[0]);
      hash *= multiplier;
      hash ^= hash >> remainder_shift;
      break;
    case 0:
      break;
  }
  return hash;
}

std::uint32_t rotatedDelta(std::uint32_t hash) noexcept {
  return (hash >> 17U) | (hash << 15U);
}

}

std::string BloomFilter::create(const std::vector<Slice>& keys) {
  std::size_t bit_count = keys.size() * kBloomBitsPerKey;
  // 小集合至少使用 64 bit 避免误判率过高
  bit_count = std::max(bit_count, kMinimumBloomBits);
  const std::size_t byte_count = (bit_count + 7U) / 8U;
  bit_count = byte_count * 8U;

  std::string encoded(byte_count, '\0');
  encoded.push_back(static_cast<char>(kBloomProbes));

  for (const Slice key : keys) {
    std::uint32_t hash = bloomHash(key);
    // 用一次基础 hash 和固定增量生成所有探测位置
    const std::uint32_t delta = rotatedDelta(hash);
    for (unsigned char probe = 0; probe < kBloomProbes; ++probe) {
      const std::size_t bit = hash % bit_count;
      // bit / 8 定位目标字节 bit % 8 定位字节内的目标位
      // 转为 unsigned char 避免 char 的符号扩展影响位运算
      encoded[bit / 8U] =
          static_cast<char>(static_cast<unsigned char>(encoded[bit / 8U]) |
                            static_cast<unsigned char>(1U << (bit % 8U)));
      hash += delta;
    }
  }
  return encoded;
}

bool BloomFilter::mayContain(Slice key, Slice encoded_filter) noexcept {
  if (encoded_filter.empty()) return false;
  if (encoded_filter.size() == 1) return true;

  const std::size_t byte_count = encoded_filter.size() - 1U;
  const std::size_t bit_count = byte_count * 8U;
  const auto probes = static_cast<unsigned char>(encoded_filter[byte_count]);
  if (probes == 0 || probes > kMaximumProbes) {
    // 未识别编码按可能命中处理避免错误跳过实际数据查询
    return true;
  }

  std::uint32_t hash = bloomHash(key);
  const std::uint32_t delta = rotatedDelta(hash);
  for (unsigned char probe = 0; probe < probes; ++probe) {
    const std::size_t bit = hash % bit_count;
    const auto byte = static_cast<unsigned char>(encoded_filter[bit / 8U]);
    if ((byte & static_cast<unsigned char>(1U << (bit % 8U))) == 0) {
      return false;
    }
    hash += delta;
  }
  return true;
}

}
