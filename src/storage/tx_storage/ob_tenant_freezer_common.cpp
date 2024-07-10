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
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/ob_force_print_log.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_tenant_freezer_common.h"

namespace oceanbase
{
using namespace lib;
using namespace share;
namespace storage
{
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
  : mem_memstore_limit_(0),
    memstore_freeze_trigger_(0),
    max_mem_memstore_can_get_now_(0),
    active_memstore_used_(0),
    freezable_active_memstore_used_(0),
    total_memstore_used_(0),
    total_memstore_hold_(0),
    max_cached_memstore_size_(0)
{
}

void ObTenantFreezeCtx::reset()
{
  mem_memstore_limit_ = 0;
  memstore_freeze_trigger_ = 0;
  max_mem_memstore_can_get_now_ = 0;
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
  memstore_can_get_now_ = 0;
  max_cached_memstore_size_ = 0;
  memstore_allocated_pos_ = 0;
  memstore_frozen_pos_ = 0;
  memstore_reclaimed_pos_ = 0;
}

ObTenantInfo::ObTenantInfo()
  :	tenant_id_(INT64_MAX),
    is_loaded_(false),
    frozen_scn_(0),
    freeze_cnt_(0),
    last_halt_ts_(0),
    slow_freeze_(false),
    slow_freeze_timestamp_(0),
    slow_freeze_mt_retire_clock_(0),
    freeze_interval_(0),
    last_freeze_timestamp_(0),
    mem_lower_limit_(0),
    mem_upper_limit_(0),
    mem_memstore_limit_(0)
{
}

void ObTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID; // i64 max as invalid.
  is_loaded_ = false;
  frozen_scn_ = 0;
  freeze_cnt_ = 0;
  last_halt_ts_ = 0;
  slow_freeze_ = false;
  slow_freeze_timestamp_ = 0;
  slow_freeze_mt_retire_clock_ = 0;
  freeze_interval_ = 0;
  last_freeze_timestamp_ = 0;
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

void ObTenantInfo::update_memstore_limit(const int64_t tenant_memstore_limit_percentage)
{
  SpinWLockGuard guard(lock_);
  int64_t tmp_var = mem_upper_limit_ / 100;
  mem_memstore_limit_ = tmp_var * tenant_memstore_limit_percentage;
}

int64_t ObTenantInfo::get_memstore_limit() const
{
  SpinRLockGuard guard(lock_);
  return mem_memstore_limit_;
}

bool ObTenantInfo::is_memstore_limit_changed(const int64_t curr_memstore_limit_percentage) const
{
  SpinRLockGuard guard(lock_);
  const int64_t tmp_var = mem_upper_limit_ / 100;
  const int64_t curr_mem_memstore_limit = tmp_var * curr_memstore_limit_percentage;
  return (curr_mem_memstore_limit != mem_memstore_limit_);
}

void ObTenantInfo::get_freeze_ctx(ObTenantFreezeCtx &ctx) const
{
  SpinRLockGuard guard(lock_);
  ctx.mem_memstore_limit_ = mem_memstore_limit_;
}

bool ObTenantInfo::is_freeze_need_slow() const
{
  bool need_slow = false;
  SpinRLockGuard guard(lock_);
  if (slow_freeze_) {
    int64_t now = ObTimeUtility::fast_current_time();
    if (now - last_freeze_timestamp_ >= freeze_interval_) {
      need_slow = false;
    } else {
      // no need minor freeze
      need_slow = true;
    }
  }
  return need_slow;
}

void ObTenantInfo::update_slow_freeze_interval()
{
  if (!slow_freeze_) {
  } else {
    SpinWLockGuard guard(lock_);
    // if slow freeze, make freeze interval 2 times of now.
    if (slow_freeze_) {
      last_freeze_timestamp_ = ObTimeUtility::fast_current_time();
      freeze_interval_ = MIN(freeze_interval_ * 2, MAX_FREEZE_INTERVAL);
    }
  }
}

void ObTenantInfo::set_slow_freeze(
    const common::ObTabletID &tablet_id,
    const int64_t retire_clock,
    const int64_t default_interval)
{
  SpinWLockGuard guard(lock_);
  if (!slow_freeze_) {
    slow_freeze_ = true;
    slow_freeze_timestamp_ = ObTimeUtility::fast_current_time();
    slow_freeze_mt_retire_clock_ = retire_clock;
    slow_tablet_ = tablet_id;
    last_freeze_timestamp_ = ObTimeUtility::fast_current_time();
    freeze_interval_ = default_interval;
  }
}

void ObTenantInfo::unset_slow_freeze(const common::ObTabletID &tablet_id)
{
  SpinWLockGuard guard(lock_);
  if (slow_freeze_ && slow_tablet_ == tablet_id) {
    slow_freeze_ = false;
    slow_freeze_timestamp_ = 0;
    slow_freeze_mt_retire_clock_ = 0;
    last_freeze_timestamp_ = 0;
    freeze_interval_ = 0;
    slow_tablet_.reset();
  }
}

ObTenantFreezeGuard::ObTenantFreezeGuard(int &err_code, const ObTenantInfo &tenant_info, const int64_t warn_threshold)
    : tenant_info_(tenant_info),
      pre_retire_pos_(0),
      error_code_(err_code),
      time_guard_("FREEZE_CHECKER", warn_threshold)
{
  ObMemstoreAllocator &tenant_allocator = MTL(ObSharedMemAllocMgr *)->memstore_allocator();
  pre_retire_pos_ = tenant_allocator.get_retire_clock();
}

ObTenantFreezeGuard::~ObTenantFreezeGuard()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(error_code_)) {
    LOG_WARN("[FREEZE_CHECKER]tenant freeze failed, skip check frozen memstore", KR(error_code_));
  } else {
    ObMemstoreAllocator &tenant_allocator = MTL(ObSharedMemAllocMgr *)->memstore_allocator();
    int64_t curr_frozen_pos = 0;
    curr_frozen_pos = tenant_allocator.get_frozen_memstore_pos();
    const bool retired_mem_frozen = (curr_frozen_pos >= pre_retire_pos_);
    const bool has_no_active_memtable = (curr_frozen_pos == 0);
    if (!(retired_mem_frozen || has_no_active_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      if (tenant_info_.is_freeze_slowed()) {
        LOG_WARN("[FREEZE_CHECKER]there may be frequent tenant freeze, but slowed",
                 KR(ret),
                 K(curr_frozen_pos),
                 K_(pre_retire_pos),
                 K(retired_mem_frozen),
                 K(has_no_active_memtable),
                 K_(tenant_info));
      } else {
        LOG_ERROR("[FREEZE_CHECKER]there may be frequent tenant freeze",
                  KR(ret),
                  K(curr_frozen_pos),
                  K_(pre_retire_pos),
                  K(retired_mem_frozen),
                  K(has_no_active_memtable));
      }
      char active_mt_info[DEFAULT_BUF_LENGTH];
      tenant_allocator.log_active_memstore_info(active_mt_info, sizeof(active_mt_info));
      FLOG_INFO("[FREEZE_CHECKER] oldest active memtable", "list", active_mt_info);
    }
  }
}

} // storage
} // oceanbase
