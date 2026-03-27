/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_BLOCK_WRITER_CONCURRENT_GUARD_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_BLOCK_WRITER_CONCURRENT_GUARD_H_

#include <new>
#include "lib/oblog/ob_log.h"
#include "lib/ob_abort.h"

namespace oceanbase
{
namespace blocksstable
{

class ObBlockWriterConcurrentGuard final
{
public:
  [[nodiscard]] explicit ObBlockWriterConcurrentGuard (volatile bool &lock);
  ~ObBlockWriterConcurrentGuard();
private:
  // disallow copy
  ObBlockWriterConcurrentGuard(const ObBlockWriterConcurrentGuard &other);
  ObBlockWriterConcurrentGuard &operator=(const ObBlockWriterConcurrentGuard &other);
  // disallow new
  void *operator new(std::size_t size);
  void *operator new(std::size_t size, const std::nothrow_t &nothrow_constant) throw();
  void *operator new(std::size_t size, void *ptr) throw();
  OB_NOINLINE void on_error();
private:
  // data members
  volatile bool &lock_;
  int ret_;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif