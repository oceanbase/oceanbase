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

#include "lib/oblog/ob_log.h"
#include "lib/alloc/alloc_func.h"
#include "share/allocator/ob_gmemstore_allocator.h"
#include "share/allocator/ob_memstore_allocator_mgr.h"
#include "share/ob_force_print_log.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_tenant_freezer_common.h"

namespace oceanbase
{
using namespace lib;
namespace storage
{
typedef ObMemstoreAllocatorMgr::TAllocator ObTenantMemstoreAllocator;

DEF_TO_STRING(ObTenantFreezeArg)
{
  int64_t pos = 0;
  J_KV(K_(freeze_type));
  return pos;
}

OB_SERIALIZE_MEMBER(ObTenantFreezeArg,
                    freeze_type_,
                    try_frozen_scn_);

ObTenantFreezeCtx::ObTenantFreezeCtx()
  : mem_lower_limit_(0),
    mem_upper_limit_(0),
    mem_memstore_limit_(0),
    memstore_freeze_trigger_(0),
    max_mem_memstore_can_get_now_(0),
    kvcache_mem_(0),
    active_memstore_used_(0),
    freezable_active_memstore_used_(0),
    total_memstore_used_(0),
    total_memstore_hold_(0),
    max_cached_memstore_size_(0)
{
}

void ObTenantFreezeCtx::reset()
{
  mem_lower_limit_ = 0;
  mem_upper_limit_ = 0;
  mem_memstore_limit_ = 0;
  memstore_freeze_trigger_ = 0;
  max_mem_memstore_can_get_now_ = 0;
  kvcache_mem_ = 0;
  active_memstore_used_ = 0;
  freezable_active_memstore_used_ = 0;
  total_memstore_used_ = 0;
  total_memstore_hold_ = 0;
  max_cached_memstore_size_ = 0;
}

ObTenantStatistic::ObTenantStatistic()
  : active_memstore_used_(0),
    total_memstore_used_(0),
    total_memstore_hold_(0),
    memstore_freeze_trigger_(0),
    memstore_limit_(0),
    tenant_memory_limit_(0),
    tenant_memory_hold_(0),
    kvcache_mem_(0),
    memstore_can_get_now_(0),
    max_cached_memstore_size_(0),
    memstore_allocated_pos_(0),
    memstore_frozen_pos_(0),
    memstore_reclaimed_pos_(0)
{}

void ObTenantStatistic::reset()
{
  active_memstore_used_ = 0;
  total_memstore_used_ = 0;
  total_memstore_hold_ = 0;
  memstore_freeze_trigger_ = 0;
  memstore_limit_ = 0;
  tenant_memory_limit_ = 0;
  tenant_memory_hold_ = 0;
  kvcache_mem_ = 0;
  memstore_can_get_now_ = 0;
  max_cached_memstore_size_ = 0;
  memstore_allocated_pos_ = 0;
  memstore_frozen_pos_ = 0;
  memstore_reclaimed_pos_ = 0;
}

ObTenantInfo::ObTenantInfo()
  :	tenant_id_(INT64_MAX),
    is_loaded_(false),
    is_freezing_(false),
    last_freeze_clock_(0),
    frozen_scn_(0),
    freeze_cnt_(0),
    last_halt_ts_(0),
    slow_freeze_(false),
    slow_freeze_timestamp_(0),
    slow_freeze_min_protect_clock_(INT64_MAX),
    mem_lower_limit_(0),
    mem_upper_limit_(0),
    mem_memstore_limit_(0)
{
}

void ObTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID; // i64 max as invalid.
  is_loaded_ = false;
  is_freezing_ = false;
  frozen_scn_ = 0;
  freeze_cnt_ = 0;
  last_halt_ts_ = 0;
  slow_freeze_ = false;
  slow_freeze_timestamp_ = 0;
  slow_freeze_min_protect_clock_ = INT64_MAX;
  slow_tablet_.reset();
  mem_memstore_limit_ = 0;
  mem_lower_limit_ = 0;
  mem_upper_limit_ = 0;
}

int ObTenantInfo::update_frozen_scn(int64_t frozen_scn)
{
  int ret = OB_SUCCESS;

  if (frozen_scn > frozen_scn_) {
    frozen_scn_ = frozen_scn;
    freeze_cnt_ = 0;
  }

  return ret;
}

int64_t ObTenantInfo::mem_memstore_left() const
{
  uint64_t memstore_hold = get_tenant_memory_hold(tenant_id_, ObCtxIds::MEMSTORE_CTX_ID);
  return max(0, mem_memstore_limit_ - (int64_t)memstore_hold);
}

void ObTenantInfo::get_mem_limit(int64_t &lower_limit, int64_t &upper_limit) const
{
  SpinRLockGuard guard(lock_);
  lower_limit = mem_lower_limit_;
  upper_limit = mem_upper_limit_;
}

void ObTenantInfo::update_mem_limit(const int64_t lower_limit,
                                    const int64_t upper_limit)
{
  SpinWLockGuard guard(lock_);
  mem_lower_limit_ = lower_limit;
  mem_upper_limit_ = upper_limit;
}

void ObTenantInfo::update_memstore_limit(const int64_t memstore_limit_percentage)
{
  SpinWLockGuard guard(lock_);
  int64_t tmp_var = mem_upper_limit_ / 100;
  mem_memstore_limit_ = tmp_var * memstore_limit_percentage;
}

int64_t ObTenantInfo::get_memstore_limit() const
{
  SpinRLockGuard guard(lock_);
  return mem_memstore_limit_;
}

void ObTenantInfo::get_freeze_ctx(ObTenantFreezeCtx &ctx) const
{
  SpinRLockGuard guard(lock_);
  ctx.mem_lower_limit_ = mem_lower_limit_;
  ctx.mem_upper_limit_ = mem_upper_limit_;
  ctx.mem_memstore_limit_ = mem_memstore_limit_;
}

ObTenantFreezeGuard::ObTenantFreezeGuard(common::ObMemstoreAllocatorMgr *allocator_mgr,
                                         int &err_code,
                                         const int64_t warn_threshold)
  : allocator_mgr_(nullptr),
    pre_retire_pos_(0),
    error_code_(err_code),
    time_guard_("FREEZE_CHECKER", warn_threshold)
{
  int ret = OB_SUCCESS;
  ObTenantMemstoreAllocator *tenant_allocator = NULL;
  const uint64_t tenant_id = MTL_ID();
  if (OB_NOT_NULL(allocator_mgr)) {
    allocator_mgr_ = allocator_mgr;
    if (OB_FAIL(allocator_mgr_->get_tenant_memstore_allocator(tenant_id,
                                                              tenant_allocator))) {
      LOG_WARN("[FREEZE_CHECKER] failed to get_tenant_memstore_allocator", KR(ret), K(tenant_id));
    } else if (NULL == tenant_allocator) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("[FREEZE_CHECKER] tenant memstore allocator is NULL", KR(ret), K(tenant_id));
    } else {
      pre_retire_pos_ = tenant_allocator->get_retire_clock();
    }
  }
}

ObTenantFreezeGuard::~ObTenantFreezeGuard()
{
  int ret = OB_SUCCESS;
  ObTenantMemstoreAllocator *tenant_allocator = NULL;
  const uint64_t tenant_id = MTL_ID();
  int64_t curr_frozen_pos = 0;
  if (OB_ISNULL(allocator_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FREEZE_CHECKER]freeze guard invalid", KR(ret), K_(allocator_mgr), K(lbt()));
  } else if (OB_FAIL(error_code_)) {
    LOG_WARN("[FREEZE_CHECKER]tenant freeze failed, skip check frozen memstore", KR(ret));
  } else if (OB_FAIL(allocator_mgr_->get_tenant_memstore_allocator(tenant_id,
                                                                   tenant_allocator))) {
    LOG_WARN("[FREEZE_CHECKER] failed to get_tenant_memstore_allocator", KR(ret), K(tenant_id));
  } else if (NULL == tenant_allocator) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[FREEZE_CHECKER] tenant memstore allocator is NULL", KR(ret), K(tenant_id));
  } else {
    curr_frozen_pos = tenant_allocator->get_frozen_memstore_pos();
    const bool retired_mem_frozen = (curr_frozen_pos >= pre_retire_pos_);
    const bool has_no_active_memtable = (curr_frozen_pos == 0);
    if (!(retired_mem_frozen || has_no_active_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("[FREEZE_CHECKER]there may be frequent tenant freeze", KR(ret), K(curr_frozen_pos),
                K_(pre_retire_pos), K(retired_mem_frozen), K(has_no_active_memtable));
      char active_mt_info[DEFAULT_BUF_LENGTH];
      tenant_allocator->log_active_memstore_info(active_mt_info,
                                                 sizeof(active_mt_info));
      FLOG_INFO("[FREEZE_CHECKER] oldest active memtable", "list", active_mt_info);
    }
  }
}

} // storage
} // oceanbase
