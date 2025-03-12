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

#define USING_LOG_PREFIX STORAGE

#include "ob_rb_memory_mgr.h"

namespace oceanbase
{
namespace common
{
static void *roaring_malloc(size_t size) {
  void *res_ptr = nullptr;
  void *alloc_ptr = nullptr;
  if (size > 0) {
    ObRbMemMgr *mem_mgr  = nullptr;
    uint64_t tenant_id = MTL_ID();
    // reserve header for mem_mgr, tenant_id and size, returning data ptr
    // | mem_mgr | tenant_id | size | data |
    size_t alloc_size = size + sizeof(ObRbMemMgr *) + sizeof(uint64_t) + sizeof(size_t);
    lib::ObMemAttr last_mem_attr = lib::ObMallocHookAttrGuard::get_tl_mem_attr();
    if (OB_INVALID_TENANT_ID == tenant_id) {
      // use ob_malloc
      alloc_ptr = ob_malloc(alloc_size, lib::ObLabel("RbMemMgr"));
    } else if (OB_ISNULL(mem_mgr = MTL(ObRbMemMgr *))) {
      int ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mem_mgr is null", K(tenant_id));
      ob_abort();
    } else if (last_mem_attr.label_.is_valid() &&
               last_mem_attr.label_[0] == 'V' &&
               last_mem_attr.label_[1] == 'I' &&
               last_mem_attr.label_[2] == 'B') {
      alloc_ptr = ob_malloc(alloc_size, SET_IGNORE_MEM_VERSION(lib::ObMemAttr(tenant_id, last_mem_attr.label_)));
      mem_mgr->incr_vec_idx_used(alloc_size);
    } else {
      alloc_ptr = mem_mgr->alloc(alloc_size);
    }
    if (alloc_ptr == nullptr) {
      int ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(size));
      throw std::bad_alloc();
    } else {
      size_t alloc_location = reinterpret_cast<size_t>(alloc_ptr);
      void *tid_ptr = reinterpret_cast<void *>(alloc_location + sizeof(ObRbMemMgr *) );
      size_t tid_location = reinterpret_cast<size_t>(tid_ptr);
      void *size_ptr = reinterpret_cast<void *>(tid_location + sizeof(size_t));
      res_ptr = reinterpret_cast<void *>(alloc_location + sizeof(ObRbMemMgr *) + sizeof(uint64_t) + sizeof(size_t));
      MEMCPY(alloc_ptr, &mem_mgr, sizeof(ObRbMemMgr *));
      MEMCPY(tid_ptr, &tenant_id, sizeof(uint64_t));
      MEMCPY(size_ptr, &size, sizeof(size_t));
    }
  }
  return res_ptr;
}

static void roaring_free(void *ptr) {
  if (ptr != nullptr) {
    size_t ptr_location = reinterpret_cast<size_t>(ptr);
    void *tid_ptr = reinterpret_cast<void *>(ptr_location - sizeof(uint64_t) - sizeof(size_t));
    uint64_t tenant_id = MTL_ID();
    if (tenant_id != *reinterpret_cast<uint64_t *>(tid_ptr)) {
      int ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant ID not match", K(tenant_id), K(*reinterpret_cast<uint64_t *>(tid_ptr)));
      ob_abort();
    } else {
      size_t alloc_location = ptr_location - sizeof(size_t) - sizeof(uint64_t) - sizeof(ObRbMemMgr *);
      void * alloc_ptr = reinterpret_cast<void *>(alloc_location);
      ObRbMemMgr *mem_mgr = *reinterpret_cast<ObRbMemMgr **>(alloc_ptr);
      lib::ObMemAttr last_mem_attr = lib::ObMallocHookAttrGuard::get_tl_mem_attr();
      if (OB_ISNULL(mem_mgr)) {
        ob_free(alloc_ptr);
      } else if (last_mem_attr.label_.is_valid() &&
                 last_mem_attr.label_[0] == 'V' &&
                 last_mem_attr.label_[1] == 'I' &&
                 last_mem_attr.label_[2] == 'B') {
        void *size_ptr = reinterpret_cast<void *>(ptr_location - sizeof(size_t));
        size_t free_size = *reinterpret_cast<size_t *>(size_ptr);
        ob_free(alloc_ptr);
        mem_mgr->decr_vec_idx_used(free_size + sizeof(ObRbMemMgr *) + sizeof(uint64_t) + sizeof(size_t));
      } else {
        mem_mgr->free(alloc_ptr);
      }
    }
  }
  return;
}

static void *roaring_realloc(void *ptr, size_t size) {
  void *res = nullptr;
  if (0 == size) {
    roaring_free(ptr);
  } else if (NULL == ptr) {
    res = roaring_malloc(size);
  } else {
    size_t ptr_location = reinterpret_cast<size_t>(ptr);
    void *size_ptr = reinterpret_cast<void *>(ptr_location - sizeof(size_t));
    size_t orig_size = *reinterpret_cast<size_t *>(size_ptr);
    if (orig_size > size) {
      res = ptr;
    } else {
      res = roaring_malloc(size);
      MEMCPY(res, ptr, orig_size);
      roaring_free(ptr);
    }
  }
  return res;
}

static void *roaring_calloc(size_t item_cnt, size_t size) {
  void *res = roaring_malloc(item_cnt * size);
  if (res != nullptr) {
    MEMSET(res, 0, item_cnt * size);
  }
  return res;
}

static void *roaring_aligned_malloc(size_t alignment, size_t size) {
  void *res = nullptr;
  if (size > 0) {
    size_t offset = alignment - 1 + sizeof(void *);
    void *orig_ptr = roaring_malloc(size + offset);
    if (orig_ptr == nullptr) {
      res = orig_ptr;
    } else {
      size_t orig_location = reinterpret_cast<size_t>(orig_ptr);
      size_t real_location = (orig_location + offset) & ~(alignment - 1);
      res = reinterpret_cast<void *>(real_location);
      size_t orig_ptr_stroage = real_location - sizeof(void *);
      *reinterpret_cast<void **>(orig_ptr_stroage) = orig_ptr;
    }
  }
  return res;
}

static void roaring_aligned_free(void *ptr) {
  if (ptr != nullptr) {
    size_t orig_ptr_stroage = reinterpret_cast<size_t>(ptr) - sizeof(void *);
    roaring_free(*reinterpret_cast<void **>(orig_ptr_stroage));
  }
  return;
}

int ObRbMemMgr::init_memory_hook()
{
  int ret = OB_SUCCESS;
  roaring_memory_mgr.malloc = roaring_malloc;
  roaring_memory_mgr.realloc = roaring_realloc;
  roaring_memory_mgr.calloc = roaring_calloc;
  roaring_memory_mgr.free = roaring_free;
  roaring_memory_mgr.aligned_malloc = roaring_aligned_malloc;
  roaring_memory_mgr.aligned_free = roaring_aligned_free;
  // initialize global memory hook
  roaring_init_memory_hook(roaring_memory_mgr);
  return ret;
}

int ObRbMemMgr::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  lib::ObMemAttr mem_attr(tenant_id, "RoaringBitmap");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRbMemMgr init twice.", K(ret));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_BIG_BLOCK_SIZE, block_alloc_, mem_attr))) {
    LOG_WARN("init allocator failed.", K(ret));
  } else {
    allocator_.set_nway(RB_ALLOC_CONCURRENCY);
    vec_idx_used_ = 0;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObRbMemMgr::destroy()
{
  FLOG_INFO("destroy Roaring bitmap manager", K(MTL_ID()));
  allocator_.destroy();
  is_inited_ = false;
}

void *ObRbMemMgr::alloc(size_t size)
{
  return allocator_.alloc(size);
}

void ObRbMemMgr::free(void *ptr)
{
  return allocator_.free(ptr);
}

void ObRbMemMgr::incr_vec_idx_used(size_t size)
{
  ATOMIC_AAF(&vec_idx_used_, size);
}

void ObRbMemMgr::decr_vec_idx_used(size_t size)
{
  ATOMIC_SAF(&vec_idx_used_, size);
}

} // common
} // oceanbase
