#include "db/db_iterator.h"

#include <cassert>
#include <utility>

namespace lsmtree {

DBIterator::DBIterator(std::unique_ptr<InternalIterator> input,
                       SequenceNumber visible_sequence)
    : input_(std::move(input)), visible_sequence_(visible_sequence) {
  assert(input_ != nullptr);
  assert(visible_sequence_ <= kMaxSequenceNumber);
}

bool DBIterator::valid() const noexcept {
  return valid_ && status_.ok() && input_->status().ok() && input_->valid();
}

void DBIterator::seekToFirst() {
  if (!status_.ok()) return;
  valid_ = false;
  skip_key_.clear();
  input_->seekToFirst();
  findNextUserEntry(false);
}

void DBIterator::seek(Slice target) {
  if (!status_.ok()) return;
  valid_ = false;
  skip_key_.clear();
  const std::string internal_target =
      encodeInternalKey(target, visible_sequence_, ValueType::kValue);
  input_->seek(internal_target);
  findNextUserEntry(false);
}

void DBIterator::next() {
  assert(valid());

  ParsedInternalKey current{};
  const bool parsed = parseInternalKey(input_->internalKey(), current);
  assert(parsed);
  skip_key_.assign(current.user_key.data(), current.user_key.size());

  valid_ = false;
  input_->next();
  findNextUserEntry(true);
}

Slice DBIterator::key() const {
  assert(valid());
  ParsedInternalKey current{};
  const bool parsed = parseInternalKey(input_->internalKey(), current);
  assert(parsed);
  return current.user_key;
}

Slice DBIterator::value() const {
  assert(valid());
  return input_->value();
}

Status DBIterator::status() const {
  if (!status_.ok()) return status_;
  return input_->status();
}

void DBIterator::findNextUserEntry(bool skipping) {
  valid_ = false;
  if (!input_->status().ok()) return;

  while (input_->valid()) {
    ParsedInternalKey current{};
    if (!parseCurrent(current)) return;

    if (skipping) {
      if (current.user_key == skip_key_) {
        input_->next();
        if (!input_->status().ok()) return;
        continue;
      }
      skipping = false;
      skip_key_.clear();
    }

    if (current.sequence > visible_sequence_) {
      input_->next();
      if (!input_->status().ok()) return;
      continue;
    }

    if (current.type == ValueType::kDeletion) {
      skip_key_.assign(current.user_key.data(), current.user_key.size());
      skipping = true;
      input_->next();
      if (!input_->status().ok()) return;
      continue;
    }

    valid_ = true;
    return;
  }

  skip_key_.clear();
}

bool DBIterator::parseCurrent(ParsedInternalKey& parsed) {
  if (parseInternalKey(input_->internalKey(), parsed)) return true;
  status_ = Status::corruption("invalid internal key in DB iterator");
  valid_ = false;
  skip_key_.clear();
  return false;
}

}
