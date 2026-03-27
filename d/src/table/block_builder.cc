#include "table/block_builder.h"

#include <algorithm>
#include <cassert>
#include <limits>

#include "util/coding.h"

namespace lsmtree {
namespace {

constexpr std::size_t kFixed32Size = sizeof(std::uint32_t);

std::size_t commonPrefix(Slice lhs, Slice rhs) noexcept {
  const std::size_t limit = std::min(lhs.size(), rhs.size());
  std::size_t shared = 0;
  while (shared < limit && lhs[shared] == rhs[shared]) ++shared;
  return shared;
}

void appendBytes(std::string& destination, Slice bytes) {
  if (!bytes.empty()) destination.append(bytes.data(), bytes.size());
}

}

BlockBuilder::BlockBuilder(BlockBuilderOptions options) : options_(options) {
  assert(options_.restart_interval > 0);
  restarts_.push_back(0);
}

void BlockBuilder::reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);
  last_key_.clear();
  entries_since_restart_ = 0;
  entry_count_ = 0;
  finished_ = false;
}

void BlockBuilder::add(Slice key, Slice value) {
  assert(!finished_);
  assert(key.size() <= std::numeric_limits<std::uint32_t>::max());
  assert(value.size() <= std::numeric_limits<std::uint32_t>::max());

  std::size_t shared = 0;
  if (entries_since_restart_ == options_.restart_interval) {
    assert(buffer_.size() <= std::numeric_limits<std::uint32_t>::max());
    restarts_.push_back(static_cast<std::uint32_t>(buffer_.size()));
    entries_since_restart_ = 0;
  } else {
    shared = commonPrefix(last_key_, key);
  }

  const std::size_t unshared = key.size() - shared;
  putFixed32(buffer_, static_cast<std::uint32_t>(shared));
  putFixed32(buffer_, static_cast<std::uint32_t>(unshared));
  putFixed32(buffer_, static_cast<std::uint32_t>(value.size()));
  appendBytes(buffer_, key.substr(shared));
  appendBytes(buffer_, value);

  last_key_.clear();
  if (!key.empty()) last_key_.assign(key.data(), key.size());
  ++entries_since_restart_;
  ++entry_count_;
}

Slice BlockBuilder::finish() {
  assert(!finished_);
  assert(restarts_.size() <= std::numeric_limits<std::uint32_t>::max());

  for (const std::uint32_t restart : restarts_) {
    putFixed32(buffer_, restart);
  }
  putFixed32(buffer_, static_cast<std::uint32_t>(restarts_.size()));
  finished_ = true;
  return Slice(buffer_);
}

std::size_t BlockBuilder::currentSizeEstimate() const noexcept {
  if (finished_) return buffer_.size();
  return buffer_.size() + restarts_.size() * kFixed32Size + kFixed32Size;
}

}
