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
 * ================================================= ObLocalArena =================================================
 */
ObLocalArena::ObLocalArena(
    const lib::ObLabel &label,
    const int64_t page_size)
  : ref_mem_ctx_(nullptr),
    arena_(label, page_size, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID),
    hist_mem_hold_(0)
{
  ObCompactionMemoryContext *mem_ctx = CURRENT_MEM_CTX();
  if (nullptr != mem_ctx) {
    bind_mem_ctx(*mem_ctx);
  }
}

ObLocalArena::ObLocalArena(
    ObCompactionMemoryContext &mem_ctx,
    const lib::ObLabel &label,
    const int64_t page_size)
  : ref_mem_ctx_(nullptr),
    arena_(label, page_size, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID),
    hist_mem_hold_(0)
{
  bind_mem_ctx(mem_ctx);
}

void ObLocalArena::bind_mem_ctx(ObCompactionMemoryContext &mem_ctx)
{
  if (NULL == ref_mem_ctx_) {
    ref_mem_ctx_ = &mem_ctx;
  }
  arena_.set_ctx_id(ref_mem_ctx_->get_ctx_id());
}

ObLocalArena::~ObLocalArena()
{
  reset();
  ref_mem_ctx_ = nullptr;
}

void* ObLocalArena::alloc(const int64_t size)
{
  void *buf = arena_.alloc(size);
  if (OB_NOT_NULL(buf)) {
    update_mem_monitor();
  }
  return buf;
}

void* ObLocalArena::alloc(const int64_t size, const ObMemAttr &attr)
{
  void *buf = arena_.alloc(size, attr);
  if (OB_NOT_NULL(buf)) {
    update_mem_monitor();
  }
  return buf;
}

void ObLocalArena::reset()
{
  arena_.reset();
  update_mem_monitor();
}

void ObLocalArena::clear()
{
  arena_.clear();
  update_mem_monitor();
}

void ObLocalArena::update_mem_monitor()
{
  ref_mem_ctx_ = nullptr == ref_mem_ctx_
               ? CURRENT_MEM_CTX()
               : ref_mem_ctx_;

  int64_t cur_mem_hold = arena_.total();
  if (nullptr != ref_mem_ctx_ && hist_mem_hold_ != cur_mem_hold) {
    if (cur_mem_hold > hist_mem_hold_) {
      ref_mem_ctx_->inc_local_hold_mem(cur_mem_hold - hist_mem_hold_);
    } else {
      ref_mem_ctx_->inc_local_free_mem(hist_mem_hold_ - cur_mem_hold);
    }
    hist_mem_hold_ = cur_mem_hold;
  }
}


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


} //compaction
} //oceanbase
