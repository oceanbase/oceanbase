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

#ifndef OCEANBASE_MEMTABLE_OB_CRTP_UTIL_
#define OCEANBASE_MEMTABLE_OB_CRTP_UTIL_

#include "share/ob_define.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "common/data_buffer.h"

namespace oceanbase
{
namespace memtable
{

template <int64_t N = 0>
class ObWithArenaT
{
public:
  ObWithArenaT(common::ObIAllocator &allocator, const int64_t page_size)
      : page_allocator_(allocator),
        arena_(page_size, page_allocator_) {}
  ObWithArenaT(const int64_t page_size)
      : page_allocator_(),
        arena_(page_size, page_allocator_) {}
  ObWithArenaT() {}
  //common::ModulePageAllocator &get_page_allocator() { return page_allocator_; }
  int init(const int64_t page_size, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    page_allocator_.set_allocator(&allocator);
    if (OB_FAIL(arena_.init(page_size, page_allocator_))) {
      //do nothing
    }
    return ret;
  }
  common::ModuleArena &get_arena() { return arena_; }
  void set_allocator(common::ObIAllocator *allocator) { page_allocator_.set_allocator(allocator); }
  void set_label(const char *label) { page_allocator_.set_label(label); arena_.set_label(label); }
  void set_tenant_id(uint64_t tenant_id) { page_allocator_.set_tenant_id(tenant_id); arena_.set_tenant_id(tenant_id); }
private:
  common::ModulePageAllocator page_allocator_;
  common::ModuleArena arena_;
};
typedef ObWithArenaT<> ObWithArena;

template <int64_t BUF_SIZE>
class ObWithDataBuffer
{
public:
  ObWithDataBuffer() : data_buffer_(memory_, sizeof(memory_)) {}
  common::ObDataBuffer &get_buf() { return data_buffer_; }
private:
  char memory_[BUF_SIZE];
  common::ObDataBuffer data_buffer_;
};

}
}

#endif //OCEANBASE_MEMTABLE_OB_CRTP_UTIL_
