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

#ifndef OCEANBASE_ARCHIVE_LARGE_BUFFER_POOL_H_
#define OCEANBASE_ARCHIVE_LARGE_BUFFER_POOL_H_

#include "lib/alloc/alloc_struct.h"             // AOBJECT_TAIL_SIZE
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_rwlock.h"            // RWLock
#include "lib/string/ob_fixed_length_string.h"  // ObFixedLengthString
#include "dynamic_buffer.h"                     // DynamicBuffer
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include <cstdint>
namespace oceanbase
{
namespace archive
{
// Large buffer pool, alloc and cache large buffer, and purge with usage statistics
// Always try to alloc from the head buffer node, to reduce buffer cached
class LargeBufferPool final
{
  // if buffer node is not use in last 10s, purge it
  static const int64_t BUFFER_PURGE_THRESHOLD = 10 * 1000 * 1000L;    // 10s
public:
  LargeBufferPool();
  ~LargeBufferPool();

  int init(const char *label, const int64_t total_limit);
  void destroy();
public:
  char *acquire(const int64_t size);
  void reclaim(void *ptr);
  // TODO total memory limit not support, memory limit with scheduler

  void weed_out();

  TO_STRING_KV(K_(inited), K_(total_limit), K_(label), K_(array));
private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;

  class BufferNode
  {
  public:
    BufferNode() : issued_(false), last_used_timestamp_(OB_INVALID_TIMESTAMP), buffer_() {}
    BufferNode(const BufferNode &other);
    explicit BufferNode(const char *buffer);
    ~BufferNode();

    char *acquire(const int64_t size, bool &issued);
    bool reclaim(void *ptr);
    int purge();
    int assign(const BufferNode &other);
    int check_and_purge(const int64_t purge_threshold, bool &is_purged);

    TO_STRING_KV(K_(issued), K_(last_used_timestamp), K_(buffer));
  private:
    bool issued_;
    int64_t last_used_timestamp_;
    DynamicBuffer buffer_;
    mutable RWLock rwlock_;
  };

private:
  int acquire_(const int64_t size, char *&data);
  void reclaim_(void *ptr);
  int reserve_();

private:
  typedef common::ObSEArray<BufferNode, 1> BufferNodeArray;

  bool inited_;
  int64_t total_limit_;
  common::ObFixedLengthString<lib::AOBJECT_TAIL_SIZE> label_;
  BufferNodeArray array_;

  mutable RWLock rwlock_;

private:
  DISALLOW_COPY_AND_ASSIGN(LargeBufferPool);
};
} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_LARGE_BUFFER_POOL_H_ */
