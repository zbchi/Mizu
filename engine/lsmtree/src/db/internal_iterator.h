#pragma once

#include "lsmtree/db.h"

namespace lsmtree {

// 按 InternalKey 顺序读取一个有序输入流。
// 迭代器不拥有底层 MemTable 或 SSTableReader
// 调用方必须保证底层对象在迭代期间存活
class InternalIterator {
 public:
  virtual ~InternalIterator() = default;

  virtual bool valid() const noexcept = 0;
  virtual void seekToFirst() = 0;
  virtual void seek(Slice target) = 0;
  virtual void next() = 0;

  virtual Slice internalKey() const = 0;
  virtual Slice value() const = 0;
  virtual const Status& status() const noexcept = 0;
};

}
