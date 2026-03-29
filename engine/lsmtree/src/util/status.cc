#include "lsmtree/db.h"

#include <utility>

namespace lsmtree {
namespace {

const char* codeName(StatusCode code) {
  switch (code) {
    case StatusCode::kOk:
      return "OK";
    case StatusCode::kNotFound:
      return "NotFound";
    case StatusCode::kAlreadyExists:
      return "AlreadyExists";
    case StatusCode::kInvalidArgument:
      return "InvalidArgument";
    case StatusCode::kIOError:
      return "IOError";
    case StatusCode::kCorruption:
      return "Corruption";
    case StatusCode::kBusy:
      return "Busy";
    case StatusCode::kNotSupported:
      return "NotSupported";
  }
  return "Unknown";
}

}

Status::Status() : code_(StatusCode::kOk) {}

Status::Status(StatusCode code, std::string message)
    : code_(code), message_(std::move(message)) {}

Status Status::success() { return Status(); }

Status Status::notFound(std::string message) {
  return Status(StatusCode::kNotFound, std::move(message));
}

Status Status::alreadyExists(std::string message) {
  return Status(StatusCode::kAlreadyExists, std::move(message));
}

Status Status::invalidArgument(std::string message) {
  return Status(StatusCode::kInvalidArgument, std::move(message));
}

Status Status::ioError(std::string message) {
  return Status(StatusCode::kIOError, std::move(message));
}

Status Status::corruption(std::string message) {
  return Status(StatusCode::kCorruption, std::move(message));
}

Status Status::busy(std::string message) {
  return Status(StatusCode::kBusy, std::move(message));
}

Status Status::notSupported(std::string message) {
  return Status(StatusCode::kNotSupported, std::move(message));
}

bool Status::ok() const noexcept { return code_ == StatusCode::kOk; }

bool Status::isNotFound() const noexcept {
  return code_ == StatusCode::kNotFound;
}

StatusCode Status::code() const noexcept { return code_; }

const std::string& Status::message() const noexcept { return message_; }

std::string Status::toString() const {
  if (ok()) return codeName(code_);
  return std::string(codeName(code_)) + ": " + message_;
}

}
