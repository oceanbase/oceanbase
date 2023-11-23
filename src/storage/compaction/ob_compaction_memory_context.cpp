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

#include "ob_compaction_memory_context.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_force_print_log.h"
#include "share/ob_thread_mgr.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "ob_tablet_merge_task.h"


namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace lib;

namespace compaction
{


/*
 * ================================================= Mem Monitor Part =================================================
 */
ObCompactionMemMonitor::ObCompactionMemMonitor()
  : buffer_hold_mem_(0),
    buffer_free_mem_(0),
    local_hold_mem_(0),
    local_free_mem_(0)
{
}

int64_t ObCompactionMemMonitor::get_hold_mem() const
{
  int64_t hold_mem = 0;
  hold_mem += ATOMIC_LOAD(&buffer_hold_mem_);
  hold_mem += ATOMIC_LOAD(&local_hold_mem_);
  hold_mem -= ATOMIC_LOAD(&buffer_free_mem_);
  hold_mem -= ATOMIC_LOAD(&local_free_mem_);

  return MAX(hold_mem, 0);
}


/*
 * ================================================= Memory Context Part =================================================
 */
ObCompactionMemoryContext::ObCompactionMemoryContext(
    const ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator)
  : arena_(allocator),
    ctx_id_(ObCtxIds::DEFAULT_CTX_ID),
    inner_arena_("SafeArena", OB_MALLOC_NORMAL_BLOCK_SIZE),
    safe_arena_(inner_arena_),
    free_lock_(),
    free_alloc_(),
    mem_monitor_(),
    mem_peak_total_(0),
    is_reserve_mode_(false)
{
  inner_init(param);
}

ObCompactionMemoryContext::ObCompactionMemoryContext(
    common::ObArenaAllocator &allocator)
  : arena_(allocator),
    ctx_id_(ObCtxIds::DEFAULT_CTX_ID),
    inner_arena_("SafeArena", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ctx_id_),
    safe_arena_(inner_arena_),
    free_lock_(),
    free_alloc_("FreeAlloc", MTL_ID()),
    mem_monitor_(),
    mem_peak_total_(0),
    is_reserve_mode_(false)
{
}

ObCompactionMemoryContext::~ObCompactionMemoryContext()
{
  destroy();
}

void ObCompactionMemoryContext::destroy()
{
  safe_arena_.clear();
  inner_arena_.reset();
}

void ObCompactionMemoryContext::inner_init(const ObTabletMergeDagParam &param)
{
  is_reserve_mode_ = param.is_reserve_mode_ && is_mini_merge(param.merge_type_);
  ctx_id_ = is_reserve_mode_
          ? ObCtxIds::MERGE_RESERVE_CTX_ID
          : ObCtxIds::MERGE_NORMAL_CTX_ID;

  lib::ObMemAttr arena_attr(MTL_ID(), "MemCtx", ctx_id_);
  lib::ObMemAttr free_attr(MTL_ID(), "FreeMemCtx", ctx_id_);

  if (is_mini_merge(param.merge_type_)) {
    arena_attr.label_ = "MiniSafeMemCtx";
    free_attr.label_ = "MiniFreeMem";
  } else if (is_major_merge_type(param.merge_type_)) {
    arena_attr.label_ =  "MajorSafeMemCtx";
    free_attr.label_ = "MajorFreeMem";
  } else if (is_minor_merge_type(param.merge_type_)) {
    arena_attr.label_ =  "MinorSafeMemCtx";
    free_attr.label_ = "MinorFreeMem";
  } else {
    // not compaction ctx
    ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
    arena_attr.ctx_id_ = ctx_id_;
    free_attr.ctx_id_ = ctx_id_;
  }


  inner_arena_.set_attr(arena_attr);
  free_alloc_.set_attr(free_attr);
  if (ObCtxIds::DEFAULT_CTX_ID != ctx_id_) {
    SET_MEM_CTX(*this);
  }
}

void* ObCompactionMemoryContext::local_alloc(const int64_t size)
{
  void *buf = nullptr;
  if (size > 0) {
    ObSpinLockGuard guard(free_lock_);
    buf = free_alloc_.alloc(size);
  }
  return buf;
}

void ObCompactionMemoryContext::local_free(void *ptr)
{
  if (OB_NOT_NULL(ptr)) {
    ObSpinLockGuard guard(free_lock_);
    free_alloc_.free(ptr);
  }
}

void ObCompactionMemoryContext::mem_click()
{
  int64_t mem_total = 0;
  mem_total += arena_.total();
  mem_total += safe_arena_.total();
  mem_total += free_alloc_.total();
  mem_total += mem_monitor_.get_hold_mem();

  mem_peak_total_ = MAX(mem_peak_total_, mem_total);
}

 /**
 * -------------------------------------------------------------------ObCompactionBuffer-------------------------------------------------------------------
 */
int ObCompactionBuffer::init(const int64_t capacity, const int64_t reserve_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "micro buffer writer is inited", K(ret), K(capacity_));
  } else if (OB_UNLIKELY(reserve_size < 0 || capacity > MAX_DATA_BUFFER_SIZE
      || capacity < reserve_size)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(capacity), K(reserve_size));
  } else {
    capacity_ = capacity;
    len_ = 0;
    data_= nullptr;
    buffer_size_ = 0;
    reset_memory_threshold_ = DEFAULT_RESET_MEMORY_THRESHOLD;
    memory_reclaim_cnt_ = 0;
  }

  if (OB_SUCC(ret)) {
    if(OB_FAIL(reserve(reserve_size))) {
      STORAGE_LOG(WARN, "failed to reserve", K(ret), K(reserve_size));
    } else {
      default_reserve_ = reserve_size;
      is_inited_ = true;
    }
  }

  return ret;
}

void ObCompactionBuffer::reset()
{
  if (data_ != nullptr) {
    allocator_.free(data_);
    data_ = nullptr;
  }
  has_expand_ = false;
  memory_reclaim_cnt_ = 0;
  reset_memory_threshold_ = 0;
  default_reserve_ = 0;
  len_ = 0;
  buffer_size_ = 0;
  capacity_ = 0;
  is_inited_ = false;
  allocator_.reset();
}

void ObCompactionBuffer::reuse()
{
  if (buffer_size_ > default_reserve_ && len_ <= default_reserve_) {
    memory_reclaim_cnt_++;
    if (memory_reclaim_cnt_ >= reset_memory_threshold_) {
      reset_memory_threshold_ <<= 1;
      memory_reclaim_cnt_ = 0;
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(default_reserve_))) {
        int ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to reclaim memory", K(ret), K(default_reserve_));
      } else {
        allocator_.free(data_);
        buffer_size_ = default_reserve_;
        data_ = reinterpret_cast<char *>(buf);
      }
    }
  } else {
    memory_reclaim_cnt_ = 0;
  }
  has_expand_ = false;
  len_ = 0;
}

int ObCompactionBuffer::expand(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(capacity_ <= buffer_size_ || size > capacity_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(size), K(buffer_size_), K(capacity_));
  } else {
    int64_t expand_size = buffer_size_ * 2;
    while (expand_size < size) {
      expand_size <<= 1;
    }
    expand_size = MIN(expand_size, capacity_);
    if (OB_FAIL(reserve(expand_size))) {
      STORAGE_LOG(WARN, "fail to reserve", K(ret), K(expand_size));
    }
  }

  return ret;
}

int ObCompactionBuffer::reserve(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size < 0 || size > capacity_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(size), K(capacity_));
  } else if (size <= buffer_size_) {//do nothing
  } else {
    void* buf = nullptr;
    const int64_t alloc_size = MAX(size, MIN_BUFFER_SIZE);
    if (OB_ISNULL(buf = allocator_.alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(alloc_size));
    } else if (data_ != nullptr) {
      has_expand_ = true;
      MEMCPY(buf, data_, len_);
      allocator_.free(data_);
      data_ = nullptr;
    }
    if (OB_SUCC(ret)) {
      data_ = reinterpret_cast<char *>(buf);
      buffer_size_ = alloc_size;
    }
  }

  return ret;
}

int ObCompactionBuffer::ensure_space(const int64_t append_size)
{
  int ret = OB_SUCCESS;

  if (len_ + append_size > capacity_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (len_ + append_size > buffer_size_) {
    if (OB_FAIL(expand(len_ + append_size))) {
      STORAGE_LOG(WARN, "failed to expand size", K(ret), K(len_), K(append_size));
    }
  }

  return ret;
}

int ObCompactionBuffer::write_nop(const int64_t size, bool is_zero)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(size), K(len_), K(capacity_));
  } else if (OB_FAIL(ensure_space(size))) {
    if (ret != OB_BUF_NOT_ENOUGH) {
      STORAGE_LOG(WARN, "failed to ensure space", K(ret), K(size));
    }
  } else {
    if (is_zero) {
      MEMSET(data_ + len_, 0, size);
    }
    len_ += size;
  }

  return ret;
}

int ObCompactionBuffer::write(const void *buf, int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(buf == nullptr || size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(buf), K(size), K(len_), K(capacity_));
  } else if (OB_FAIL(ensure_space(size))) {
    if (ret != OB_BUF_NOT_ENOUGH) {
      STORAGE_LOG(WARN, "failed to ensure space", K(ret), K(size));
    }
  } else {
    MEMCPY(data_ + len_, buf, size);
    len_ += size;
  }

  return ret;
}

int ObCompactionBuffer::advance(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size < 0 || len_ + size > buffer_size_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(size), K(len_), K(buffer_size_));
  } else {
    len_ += size;
  }
  return ret;
}

int ObCompactionBuffer::set_length(const int64_t len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(len > buffer_size_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(len), K(len_), K(buffer_size_));
  } else {
    len_ = len;
  }
  return ret;
}


} //compaction
} //oceanbase
