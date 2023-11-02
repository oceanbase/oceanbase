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

#define USING_LOG_PREFIX SERVER

#include "share/table/ob_table_load_shared_allocator.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace table
{
using namespace common;

ObTableLoadSharedAllocator::ObTableLoadSharedAllocator()
  : allocator_("TLD_share_alloc"),
    ref_count_(0)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadSharedAllocator::~ObTableLoadSharedAllocator()
{
  OB_ASSERT(0 == get_ref_count());
}

void *ObTableLoadSharedAllocator::alloc(const int64_t size)
{
  return allocator_.alloc(size);
}

void ObTableLoadSharedAllocator::free(void *ptr)
{
  allocator_.free(ptr);
}

ObTableLoadSharedAllocatorHandle::ObTableLoadSharedAllocatorHandle(
    ObTableLoadSharedAllocator *allocator)
    : allocator_(allocator)
{
  if (OB_NOT_NULL(allocator_)) {
    allocator_->inc_ref_count();
  }
}

ObTableLoadSharedAllocatorHandle::ObTableLoadSharedAllocatorHandle(
    const ObTableLoadSharedAllocatorHandle &other)
{
  allocator_ = other.allocator_;
  if (OB_NOT_NULL(allocator_)) {
    allocator_->inc_ref_count();
  }
}

ObTableLoadSharedAllocatorHandle::~ObTableLoadSharedAllocatorHandle()
{
  if (OB_NOT_NULL(allocator_)) {
    if (allocator_->dec_ref_count() == 0) {
      allocator_->~ObTableLoadSharedAllocator();
      ob_free(allocator_);
    }
    allocator_ = nullptr;
  }
}

ObTableLoadSharedAllocatorHandle &ObTableLoadSharedAllocatorHandle::operator=(
    const ObTableLoadSharedAllocatorHandle &other)
{
  if (other.allocator_ != allocator_) {
    allocator_ = other.allocator_;
    if (OB_NOT_NULL(allocator_)) {
      allocator_->inc_ref_count();
    }
  }
  return *this;
}

ObTableLoadSharedAllocator *ObTableLoadSharedAllocatorHandle::operator->()
{
  return allocator_;
}

ObTableLoadSharedAllocator *ObTableLoadSharedAllocatorHandle::operator->() const
{
  return allocator_;
}

ObTableLoadSharedAllocator &ObTableLoadSharedAllocatorHandle::operator*()
{
  return *allocator_;
}

ObTableLoadSharedAllocatorHandle::operator bool () const
{
  return OB_NOT_NULL(allocator_);
}

ObTableLoadSharedAllocatorHandle ObTableLoadSharedAllocatorHandle::make_handle()
{
  int ret = OB_SUCCESS;
  ObTableLoadSharedAllocator *shared_allocator = (ObTableLoadSharedAllocator *)ob_malloc(
      sizeof(ObTableLoadSharedAllocator), ObMemAttr(MTL_ID(), "TLD_share_alloc"));
  if (OB_ISNULL(shared_allocator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret));
  } else {
    new (shared_allocator) ObTableLoadSharedAllocator;
  }
  return ObTableLoadSharedAllocatorHandle(shared_allocator);
}

void ObTableLoadSharedAllocatorHandle::reset()
{
  if (OB_NOT_NULL(allocator_)) {
    if (allocator_->dec_ref_count() == 0) {
      allocator_->~ObTableLoadSharedAllocator();
      ob_free(allocator_);
    }
    allocator_ = nullptr;
  }
}

} // namespace table
} // namespace oceanbase
