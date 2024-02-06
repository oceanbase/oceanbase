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

#ifndef OCEANBASE_ALLOCATOR_OB_BLOCK_ALLOC_MGR_H_
#define OCEANBASE_ALLOCATOR_OB_BLOCK_ALLOC_MGR_H_

#include "lib/allocator/ob_malloc.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace common
{
class ObBlockAllocMgr;
extern ObBlockAllocMgr default_blk_alloc;

class ObBlockAllocMgr
{
public:
  ObBlockAllocMgr(int64_t limit = INT64_MAX): limit_(limit), hold_(0) {}
  ~ObBlockAllocMgr() {}
  void set_limit(int64_t limit) { ATOMIC_STORE(&limit_, limit); }
  int64_t limit() const { return ATOMIC_LOAD(&limit_); }
  int64_t hold() const { return ATOMIC_LOAD(&hold_); }
  void* alloc_block(int64_t size, ObMemAttr &attr) {
    void *ret = NULL;
    int64_t used_after_alloc = ATOMIC_AAF(&hold_, size);
    if (used_after_alloc > limit_) {
      ATOMIC_AAF(&hold_, -size);
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        _OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "block alloc over limit, limit=%ld alloc_size=%ld", limit_, size);
      }
    } else if (NULL == (ret = (void*)ob_malloc(size, attr))) {
      ATOMIC_AAF(&hold_, -size);
    } else {}
    return ret;
  }
  void free_block(void *ptr, int64_t size) {
    if (NULL != ptr) {
      ob_free(ptr);
      ATOMIC_FAA(&hold_, -size);
    }
  }
  bool can_alloc_block(int64_t size) const {
    return  ((limit_ - hold_) > size);
  }
private:
  int64_t limit_;
  int64_t hold_;
};
}; // end namespace common
}; // end namespace oceanbase
#endif /* OCEANBASE_ALLOCATOR_OB_BLOCK_ALLOC_MGR_H_ */
