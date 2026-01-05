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

#include "lib/allocator/ob_allocator_v2.h"
#include "lib/utility/ob_backtrace.h"
#include "lib/resource/ob_affinity_ctrl.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

ObAllocator::ObAllocator(__MemoryContext__ *mem_context, const ObMemAttr &attr)
  : mem_context_(mem_context), ta_(NULL), attr_(attr),
    hold_(0)
{
  attr_.numa_id_ = attr.tenant_id_ != OB_SYS_TENANT_ID ? AFFINITY_CTRL.get_numa_id() : 0;
  ta_ = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
  attr_.tenant_id_, attr_.ctx_id_, attr_.numa_id_);
  abort_unless(ta_ != NULL);
  head_.next_ = &head_;
  head_.prev_ = &head_;
}

void *ObAllocator::alloc(const int64_t size)
{
  return alloc(size, attr_);
}

void *ObAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  void *ptr = NULL;
  ObMemAttr inner_attr = attr_;
  inner_attr.label_ = attr.label_;
  const int64_t real_size = sizeof(AllocNode) + size;
  AllocNode *node = (AllocNode *)ta_->alloc(real_size, inner_attr);
  if (OB_LIKELY(NULL != node)) {
    AObject *obj = reinterpret_cast<AObject*>((char*)node - AOBJECT_HEADER_SIZE);
    push(node, obj->alloc_bytes_);
    ptr = node->data_;
  }
  return ptr;
}

void ObAllocator::free(void *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    AllocNode *node = reinterpret_cast<AllocNode*>((char*)ptr - sizeof(AllocNode));
    AObject *obj = reinterpret_cast<AObject*>((char*)node - AOBJECT_HEADER_SIZE);
    remove(node, obj->alloc_bytes_);
    ta_->free(node);
  }
}

void ObAllocator::reset()
{
  AllocNode *next = popall();
  if (next) {
    AObject *obj = reinterpret_cast<AObject*>((char*)next - AOBJECT_HEADER_SIZE);
    char bt[MAX_BACKTRACE_LENGTH] = {'\0'};
    PARRAY_MALLOC_BACKTRACE(obj, bt, MAX_BACKTRACE_LENGTH);
    const static int buf_len = 512;
    char buf[buf_len] = {'\0'};
    snprintf(buf, buf_len, "label: %s, backtrace: %s", obj->label_, bt);
    has_unfree_callback(buf);
    while (next) {
      void *ptr = next;
      next = next->next_;
      ta_->free(ptr);
    }
  }
}

ObParallelAllocator::ObParallelAllocator(__MemoryContext__ *mem_context, const ObMemAttr &attr)
  : ObAllocator(mem_context, attr),
    lock_()
{}

void ObParallelAllocator::push(AllocNode *node, const int64_t size)
{
  lock_.lock();
  ObAllocator::push(node, size);
  lock_.unlock();
}

void ObParallelAllocator::remove(AllocNode *node, const int64_t size)
{
  lock_.lock();
  ObAllocator::remove(node, size);
  lock_.unlock();
}

ObAllocator::AllocNode *ObParallelAllocator::popall()
{
  AllocNode *ret = nullptr;
  lock_.lock();
  ret = ObAllocator::popall();
  lock_.unlock();
  return ret;
}
} // end of namespace common
} // end of namespace oceanbase
