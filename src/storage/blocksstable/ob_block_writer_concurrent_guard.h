/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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