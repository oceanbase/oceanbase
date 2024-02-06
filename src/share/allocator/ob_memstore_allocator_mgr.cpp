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

#define USING_LOG_PREFIX SHARE

#include "share/allocator/ob_memstore_allocator_mgr.h"
#include "share/allocator/ob_gmemstore_allocator.h"
#include "lib/alloc/alloc_struct.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

int64_t ObMemstoreAllocatorMgr::get_all_tenants_memstore_used()
{
  return ATOMIC_LOAD(&ObFifoArena::total_hold_);
}

ObMemstoreAllocatorMgr::ObMemstoreAllocatorMgr()
  : is_inited_(false),
    allocators_(),
    allocator_map_(),
    malloc_allocator_(NULL),
    all_tenants_memstore_used_(0)
{
  set_malloc_allocator(ObMallocAllocator::get_instance());
}

ObMemstoreAllocatorMgr::~ObMemstoreAllocatorMgr()
{}

int ObMemstoreAllocatorMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(allocator_map_.create(ALLOCATOR_MAP_BUCKET_NUM, ObModIds::OB_MEMSTORE_ALLOCATOR))) {
    LOG_WARN("failed to create allocator_map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMemstoreAllocatorMgr::get_tenant_memstore_allocator(const uint64_t tenant_id,
                                                          TAllocator *&out_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(tenant_id), K(ret));
  } else if (tenant_id < PRESERVED_TENANT_COUNT) {
    if (NULL == (out_allocator = ATOMIC_LOAD(&allocators_[tenant_id]))) {
      ObMemAttr attr;
      attr.tenant_id_ = OB_SERVER_TENANT_ID;
      attr.label_ = ObModIds::OB_MEMSTORE_ALLOCATOR;
      SET_USE_500(attr);
      void *buf = ob_malloc(sizeof(TAllocator), attr);
      if (NULL != buf) {
        TAllocator *allocator = new (buf) TAllocator();
        bool cas_succeed = false;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(allocator->init(tenant_id))) {
            LOG_WARN("failed to init tenant memstore allocator", K(tenant_id), K(ret));
          } else {
            LOG_INFO("succ to init tenant memstore allocator", K(tenant_id), K(ret));
            cas_succeed = ATOMIC_BCAS(&allocators_[tenant_id], NULL, allocator);
          }
        }

        if (OB_FAIL(ret) || !cas_succeed) {
          allocator->~TAllocator();
          ob_free(buf);
          out_allocator = ATOMIC_LOAD(&allocators_[tenant_id]);
        } else {
          out_allocator = allocator;
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(tenant_id), K(ret));
      }
    }
  } else if (OB_FAIL(allocator_map_.get_refactored(tenant_id, out_allocator))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get tenant memstore allocator", K(tenant_id), K(ret));
    } else {
      ret = OB_SUCCESS;
      ObMemAttr attr;
      attr.tenant_id_ = OB_SERVER_TENANT_ID;
      attr.label_ = ObModIds::OB_MEMSTORE_ALLOCATOR;
      void *buf = ob_malloc(sizeof(TAllocator), attr);
      if (NULL != buf) {
        TAllocator *new_allocator = new (buf) TAllocator();
        if (OB_FAIL(new_allocator->init(tenant_id))) {
          LOG_WARN("failed to init tenant memstore allocator", K(tenant_id), K(ret));
        } else if (OB_FAIL(allocator_map_.set_refactored(tenant_id, new_allocator))) {
          if (OB_HASH_EXIST == ret) {
            if (OB_FAIL(allocator_map_.get_refactored(tenant_id, out_allocator))) {
              LOG_WARN("failed to get refactor", K(tenant_id), K(ret));
            }
          } else {
            LOG_WARN("failed to set refactor", K(tenant_id), K(ret));
          }
          new_allocator->~TAllocator();
          ob_free(buf);
        } else {
          out_allocator = new_allocator;
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(tenant_id), K(ret));
      }
    }
  } else if (OB_ISNULL(out_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got allocator is NULL", K(tenant_id), K(ret));
  }
  return ret;
}

ObMemstoreAllocatorMgr &ObMemstoreAllocatorMgr::get_instance()
{
  static ObMemstoreAllocatorMgr instance_;
  return instance_;
}
