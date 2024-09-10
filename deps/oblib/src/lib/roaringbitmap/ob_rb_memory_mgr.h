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

#ifndef OCEABASE_LIB_OB_RB_MEMORY_MGR_
#define OCEABASE_LIB_OB_RB_MEMORY_MGR_

#include "ob_roaringbitmap.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "lib/allocator/ob_block_alloc_mgr.h"

namespace oceanbase
{
namespace common
{
static roaring_memory_t roaring_memory_mgr;

class ObRbMemMgr
{
public:
  static int init_memory_hook();

private:
  static const int64_t RB_ALLOC_CONCURRENCY = 32;

public:
  ObRbMemMgr() : is_inited_(false), block_alloc_(), allocator_() {}
  ~ObRbMemMgr() {}
  static int mtl_init(ObRbMemMgr *&rb_allocator) { return rb_allocator->init(); };

  int init();
  int start() { return OB_SUCCESS; }
  void stop() {}
  void wait() {}
  void destroy();

  void *alloc(size_t size);
  void free(void *ptr);

private:
  bool is_inited_;
  common::ObBlockAllocMgr block_alloc_;
  common::ObVSliceAlloc allocator_;
};

} // common
} // oceanbase

#endif // OCEABASE_LIB_OB_RB_MEMORY_MGR_