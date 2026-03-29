#include "lsmtree/db.h"

#include <utility>

#include "db/write_batch_internal.h"

namespace lsmtree {

WriteBatch::WriteBatch() : rep_(std::make_unique<Rep>()) {}

WriteBatch::WriteBatch(WriteBatch&&) noexcept = default;
WriteBatch& WriteBatch::operator=(WriteBatch&&) noexcept = default;
WriteBatch::~WriteBatch() = default;

WriteBatch& WriteBatch::put(Slice key, Slice value) {
  rep_->operations.push_back(
      {Rep::OperationType::kPut, std::string(key), std::string(value)});
  return *this;
}

WriteBatch& WriteBatch::erase(Slice key) {
  rep_->operations.push_back({Rep::OperationType::kDelete, std::string(key),
                              std::string()});
  return *this;
}

void WriteBatch::clear() noexcept { rep_->operations.clear(); }

std::size_t WriteBatch::count() const noexcept {
  return rep_->operations.size();
}

bool WriteBatch::empty() const noexcept { return rep_->operations.empty(); }

}
