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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_tenant_sql_memory_manager.h"
#include "share/rc/ob_tenant_base.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/alloc/alloc_func.h"
#include "lib/rc/ob_rc.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/px/ob_px_util.h"
#include "share/cache/ob_kv_storecache.h"

namespace oceanbase {

using namespace lib;
using namespace omt;
using namespace share;
using namespace common;
using namespace share::schema;
using namespace oceanbase::observer;

namespace sql {

////////////////////////////////////////////////////////////////////////////////////
const int64_t ObSqlWorkAreaProfile::MIN_BOUND_SIZE[ObSqlWorkAreaType::MAX_TYPE] = {
    9 * OB_MALLOC_MIDDLE_BLOCK_SIZE,      // HASH
    OB_MALLOC_MIDDLE_BLOCK_SIZE,          // SORT
    };

int64_t ObSqlWorkAreaProfile::get_dop()
{
  return dop_;
}

uint64_t ObSqlWorkAreaProfile::get_plan_id()
{
  return plan_id_;
}

uint64_t ObSqlWorkAreaProfile::get_exec_id()
{
  return exec_id_;
}

const char* ObSqlWorkAreaProfile::get_sql_id()
{
  return sql_id_;
}

uint64_t ObSqlWorkAreaProfile::get_session_id()
{
  return session_id_;
}

int ObSqlWorkAreaProfile::set_exec_info(ObExecContext &exec_ctx)
  {
    int ret = OB_SUCCESS;
    dop_ = ObPxSqcUtil::get_actual_worker_count(&exec_ctx);
    plan_id_ = ObPxSqcUtil::get_plan_id(&exec_ctx);
    exec_id_ = ObPxSqcUtil::get_exec_id(&exec_ctx);
    session_id_ = ObPxSqcUtil::get_session_id(&exec_ctx);
    ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx) && OB_NOT_NULL(plan_ctx->get_phy_plan())) {
      if (nullptr == plan_ctx->get_phy_plan()->get_sql_id()) {
        sql_id_[0] = '\0';
      } else {
        memcpy(sql_id_, plan_ctx->get_phy_plan()->get_sql_id(), OB_MAX_SQL_ID_LENGTH);
        sql_id_[OB_MAX_SQL_ID_LENGTH] = '\0';
      }
    }
    return ret;
  }

////////////////////////////////////////////////////////////////////////////////////
int ObSqlWorkAreaIntervalStat::analyze_profile(
  ObSqlWorkAreaProfile &profile,
  int64_t cache_size,
  const int64_t one_pass_size,
  const int64_t max_size,
  bool is_one_pass)
{
  int ret = OB_SUCCESS;
  if (is_one_pass) {
    if (profile.is_sort_wa()) {
      ++total_one_pass_cnt_;
      total_one_pass_size_ += cache_size;
    }
  } else {
    if (max_size <= cache_size) {
      cache_size = max_size;
      profile.init(max_size, profile.get_chunk_size());
    }
    if (profile.is_hash_join_wa()) {
      ++total_hash_cnt_;
      total_hash_size_ += cache_size;
    } else if (profile.is_sort_wa()) {
      ++total_sort_cnt_;
      total_sort_size_ += cache_size;
      total_sort_one_pass_size_ += one_pass_size;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect profile type", K(ret), K(profile.get_work_area_type()));
    }
  }
  return ret;
}

void ObSqlWorkAreaIntervalStat::reset()
{
  total_hash_cnt_ = 0;
  total_hash_size_ = 0;
  total_sort_cnt_ = 0;
  total_sort_size_ = 0;
  total_sort_one_pass_size_ = 0;
  total_one_pass_cnt_ = 0;
  total_one_pass_size_ = 0;
}

////////////////////////////////////////////////////////////////////////////////////
void ObSqlMemoryList::reset()
{
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  DLIST_FOREACH_REMOVESAFE_NORET(profile, profile_list_) {
    profile_list_.remove(profile);
    profile->set_expect_size(OB_INVALID_ID);
  }
  profile_list_.reset();
}

int ObSqlMemoryList::register_work_area_profile(ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  profile_list_.add_last(&profile);
  return ret;
}

int ObSqlMemoryList::unregister_work_area_profile(ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  profile_list_.remove(&profile);
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////
int ObTenantSqlMemoryManager::ObSqlWorkAreaCalcInfo::init(
  ObIAllocator &allocator,
  ObSqlWorkAreaInterval *wa_intervals,
  int64_t interval_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wa_intervals)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: work interval is null", K(ret));
  } else {
    wa_intervals_ = reinterpret_cast<ObSqlWorkAreaInterval*>(allocator.alloc(
      sizeof(ObSqlWorkAreaInterval) * interval_cnt));
    if (nullptr == wa_intervals_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc work area interval", K(ret), K(sizeof(ObSqlWorkAreaInterval) * interval_cnt));
    } else {
      for (int64_t i = 0; i < interval_cnt; ++i) {
        void *buf = static_cast<void *>(&wa_intervals_[i]);
        ObSqlWorkAreaInterval *wa_interval =
          new (buf) ObSqlWorkAreaInterval(i, wa_intervals[i].get_interval_cache_size());
        UNUSED(wa_interval);
      }
    }
  }
  return ret;
}

void ObTenantSqlMemoryManager::ObSqlWorkAreaCalcInfo::destroy(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(wa_intervals_)) {
    allocator.free(wa_intervals_);
    wa_intervals_ = nullptr;
  }
}

// delta计算逻辑，前一个interval和后一个interval计算相差公式为
// suppose calculate the idx interval, and pre-interval is (idx + 1)
// delta = intervals_[idx+1].total_hash_sise
//       - intervals_[idx].interval_cache_size
//       * intervals_[idx+1].total_hash_cnt + no_cache_cnt * interval_size
// interval_size = intervals_[idx+1].interval_cache_size - intervals_[idx].interval_cache_size
// 因为跨了一个interval后，之前不能cache的，bound全部需要减去一个interval大小
// 可以理解为 hash：每次减少一个interval大小
// 而sort，开始是一次性减少到one_pass_size大小，再减少，则是以interval大小减少
int ObTenantSqlMemoryManager::ObSqlWorkAreaCalcInfo::calc_memory_target(
  int64_t idx,
  const int64_t pre_mem_target)
{
  int ret = OB_SUCCESS;
  int64_t dst_mem_target = pre_mem_target;
  if (INTERVAL_NUM - 1 == idx) {
    // 最后一个，独立计算
    wa_intervals_[idx].set_mem_target(dst_mem_target);
  } else {
    ObSqlWorkAreaIntervalStat &pre_interval_stat = wa_intervals_[idx + 1].get_interval_stat();
    // hash: bound size, 这里假设如果不能全部cache，则使用bound作为work area size
    //    当bound小于cache size时，内存减少hash_size - bound_size
    int64_t hash_delta = pre_interval_stat.get_total_hash_size()
                        - wa_intervals_[idx].get_interval_cache_size()
                        * pre_interval_stat.get_total_hash_cnt()
                        + tmp_no_cache_cnt_ * (wa_intervals_[idx + 1].get_interval_cache_size()
                        - wa_intervals_[idx].get_interval_cache_size());
    // sort: one pass size as work area size
    // sort:两段：1）当bound小于sort_size时，内存减少sort_size - one_pass_size
    //           2）当bound小于one_pass_size时，内存减少one_pass_size - bound_size
    int64_t sort_delta =
      pre_interval_stat.get_total_sort_size() - pre_interval_stat.get_total_sort_one_pass_size();
    int64_t one_pass_delta = pre_interval_stat.get_total_one_pass_size()
      - wa_intervals_[idx].get_interval_cache_size() * pre_interval_stat.get_total_one_pass_cnt();
    dst_mem_target -= hash_delta;
    dst_mem_target -= sort_delta;
    dst_mem_target -= one_pass_delta;
    if (0 > hash_delta || 0 > sort_delta || 0 > dst_mem_target) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected delta size", K(hash_delta), K(sort_delta), K(dst_mem_target), K(idx),
        K(pre_mem_target), K(pre_interval_stat.get_total_hash_size()),
        K(wa_intervals_[idx].get_interval_cache_size()), K(pre_interval_stat.get_total_hash_cnt()),
        K(one_pass_delta), K(pre_interval_stat.get_total_one_pass_size()),
        K(pre_interval_stat.get_total_one_pass_cnt()), K(tmp_no_cache_cnt_));
    } else {
      wa_intervals_[idx].set_mem_target(dst_mem_target);
    }
    if (hash_delta > 0 || sort_delta > 0 || one_pass_delta > 0) {
      LOG_TRACE("trace memory target", K(hash_delta), K(sort_delta), K(dst_mem_target), K(idx),
        K(pre_mem_target), K(pre_interval_stat.get_total_hash_size()),
        K(wa_intervals_[idx].get_interval_cache_size()), K(pre_interval_stat.get_total_hash_cnt()),
        K(one_pass_delta), K(pre_interval_stat.get_total_one_pass_size()),
        K(pre_interval_stat.get_total_one_pass_cnt()), K(dst_mem_target), K(tmp_no_cache_cnt_));
    }
    // 统计点只有hash和one_pass_cnt
    tmp_no_cache_cnt_ +=
      (pre_interval_stat.get_total_hash_cnt() + pre_interval_stat.get_total_one_pass_cnt());
  }
  return ret;
}

int ObTenantSqlMemoryManager::ObSqlWorkAreaCalcInfo::find_best_interval_index_by_mem_target(
  int64_t &interval_idx, const int64_t expect_mem_target, const int64_t total_memory_size)
{
  int ret = OB_SUCCESS;
  int64_t pre_mem_target = total_memory_size;
  int64_t delta = INT64_MAX;
  interval_idx = -1;
  for (int64_t i = INTERVAL_NUM - 1; i >= 0 && OB_SUCC(ret); --i) {
    if (OB_FAIL(calc_memory_target(i, pre_mem_target))) {
      LOG_WARN("failed to calculate memory target", K(i), K(pre_mem_target));
    } else {
      int64_t mem_target = wa_intervals_[i].get_mem_target();
      if (mem_target <= expect_mem_target && expect_mem_target - mem_target <= delta) {
        interval_idx = i;
        delta = expect_mem_target - mem_target;
        break;
      }
      pre_mem_target = mem_target;
    }
  }
  mem_target_ = expect_mem_target;
  return ret;
}

int ObTenantSqlMemoryManager::ObSqlWorkAreaCalcInfo::calculate_global_bound_size(
  const int64_t wa_max_memory_size,
  const int64_t total_memory_size,
  const int64_t profile_cnt,
  const bool auto_calc)
{
  int ret = OB_SUCCESS;
  int64_t max_wa_size = wa_max_memory_size;
  // int64_t max_wa_size = wa_max_memory_size;
  // 最大占比6.25%（oracle 5%）
  // 这里改为按照8个并发来设置
  int64_t max_bound_size = (max_wa_size >> 3);
  profile_cnt_ = profile_cnt;
  int64_t avg_bound_size = (0 == profile_cnt_) ? max_bound_size : max_wa_size / profile_cnt_;
  int64_t best_interval_idx = -1;
  if (OB_FAIL(find_best_interval_index_by_mem_target(
    best_interval_idx, max_wa_size, total_memory_size))) {
    LOG_WARN("failed to find best interval index", K(ret), K(best_interval_idx), K(max_wa_size),
      K(total_memory_size));
  } else {
    int64_t calc_global_bound_size = 0;
    if (-1 == best_interval_idx) {
      global_bound_size_ = LESS_THAN_100M_INTERVAL_SIZE;
    } else {
      calc_global_bound_size = wa_intervals_[best_interval_idx].get_interval_cache_size();
      global_bound_size_ = calc_global_bound_size;
      // ???这里是否有问题
      // if (global_bound_size_ < avg_bound_size) {
      //   global_bound_size_ = avg_bound_size;
      // }
    }
    //一般是由于可能全in-memory了，导致查找到返回的idx是最后一个，所以按照一个的最大占比使用
    if (global_bound_size_ > max_bound_size) {
      global_bound_size_ = max_bound_size;
    }
    if (global_bound_size_ < min_bound_size_) {
      global_bound_size_ = min_bound_size_;
    }
    if (auto_calc) {
      LOG_INFO("timer to calc global bound size", K(ret), K(best_interval_idx),
        K(global_bound_size_), K(calc_global_bound_size), K(mem_target_), K(wa_max_memory_size),
        K(profile_cnt_), K(total_memory_size), K(max_wa_size), K(avg_bound_size), K(max_bound_size),
        K(min_bound_size_));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////
int ObTenantSqlMemoryManager::mtl_init(ObTenantSqlMemoryManager *&sql_mem_mgr)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  sql_mem_mgr = nullptr;
  // 系统租户不创建
  if (OB_MAX_RESERVED_TENANT_ID < tenant_id) {
    sql_mem_mgr = OB_NEW(ObTenantSqlMemoryManager,
                         ObMemAttr(tenant_id, "SqlMemMgr"), tenant_id);
    if (nullptr == sql_mem_mgr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tenant sql memory manager", K(ret));
    } else if (OB_FAIL(sql_mem_mgr->allocator_.init(
              lib::ObMallocAllocator::get_instance(),
              OB_MALLOC_NORMAL_BLOCK_SIZE,
              ObMemAttr(tenant_id, "SqlMemMgr")))) {
      LOG_WARN("failed to init fifo allocator", K(ret));
    } else {
      int64_t work_area_interval_size = sizeof(ObSqlWorkAreaInterval) * INTERVAL_NUM;
      sql_mem_mgr->wa_intervals_ = reinterpret_cast<ObSqlWorkAreaInterval*>(
                                    sql_mem_mgr->allocator_.alloc(work_area_interval_size));
      sql_mem_mgr->profile_lists_ = reinterpret_cast<ObSqlMemoryList*>(
                                sql_mem_mgr->allocator_.alloc(sizeof(ObSqlMemoryList) * HASH_CNT));
      if (nullptr == sql_mem_mgr->wa_intervals_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc work area interval", K(ret));
      } else if (nullptr == sql_mem_mgr->profile_lists_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc profile list", K(ret));
      } else {
        sql_mem_mgr->tenant_id_ = tenant_id;
        // 1M
        int64_t total_size = 0;
        int64_t pre_total_size = total_size;
        for (int64_t i = 0; i < INTERVAL_NUM && OB_SUCC(ret); ++i) {
          if (i < LESS_THAN_100M_CNT) {
            // 1M
            total_size += LESS_THAN_100M_INTERVAL_SIZE;
          } else if (i < LESS_THAN_500M_CNT) {
            // 2M
            total_size += LESS_THAN_500M_INTERVAL_SIZE;
          } else if (i < LESS_THAN_1G_CNT) {
            // 5M
            total_size += LESS_THAN_1G_INTERVAL_SIZE;
          } else if (i < LESS_THAN_5G_CNT) {
            // 10M
            total_size += LESS_THAN_5G_INTERVAL_SIZE;
          } else if (i < LESS_THAN_10G_CNT) {
            // 50M
            total_size += LESS_THAN_10G_INTERVAL_SIZE;
          } else if (i < LESS_THAN_100G_CNT) {
            // 900M
            total_size += LESS_THAN_100G_INTERVAL_SIZE;
          } else if (i < LESS_THAN_1T_CNT) {
            // 9000M
            total_size += LESS_THAN_1T_INTERVAL_SIZE;
          }
          void *buf = static_cast<void *>(&sql_mem_mgr->wa_intervals_[i]);
          ObSqlWorkAreaInterval *wa_interval = new (buf) ObSqlWorkAreaInterval(i, total_size);
          ObWorkareaHistogram workarea_hist(pre_total_size, total_size);
          if (OB_FAIL(sql_mem_mgr->workarea_histograms_.push_back(workarea_hist))) {
            LOG_WARN("failed to push back workarea histogram", K(ret), K(i));
          }
          UNUSED(wa_interval);
          pre_total_size = total_size;
        }
        if (MAX_INTERVAL_SIZE != total_size) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpect size", K(total_size));
        } else {
          sql_mem_mgr->min_bound_size_ = MIN_GLOBAL_BOUND_SIZE;
        }
        if (OB_SUCC(ret)) {
          char *buf = reinterpret_cast<char*>(sql_mem_mgr->profile_lists_);
          for (int64_t i = 0; i < HASH_CNT; ++i) {
            ObSqlMemoryList *list = new (buf) ObSqlMemoryList(i);
            list->get_profile_list().reset();
            buf += sizeof(ObSqlMemoryList);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sql_mem_mgr->wa_ht_.create(MAX_WORKAREA_STAT_CNT,
              "SqlMemMgr",
              "SqlMemMgr",
              tenant_id))) {
            LOG_WARN("failed to create hashmap", K(ret));
          } else if (OB_FAIL(sql_mem_mgr->workarea_stats_.prepare_allocate(
              MAX_WORKAREA_STAT_CNT))) {
            LOG_WARN("failed to prepare element", K(ret));
          } else {
            for (int64_t i = 0; i < MAX_WORKAREA_STAT_CNT; ++i) {
              ObSqlWorkAreaStat &wa_stat = sql_mem_mgr->workarea_stats_.at(i);
              wa_stat.set_seqno(i);
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
        if (nullptr != sql_mem_mgr) {
          if (nullptr != sql_mem_mgr->wa_intervals_) {
            sql_mem_mgr->allocator_.free(sql_mem_mgr->wa_intervals_);
            sql_mem_mgr->wa_intervals_ = nullptr;
          }
          if (nullptr != sql_mem_mgr->profile_lists_) {
            sql_mem_mgr->allocator_.free(sql_mem_mgr->profile_lists_);
            sql_mem_mgr->profile_lists_ = nullptr;
          }
          sql_mem_mgr->wa_ht_.destroy();
          sql_mem_mgr->workarea_stats_.reset();
          sql_mem_mgr->workarea_histograms_.reset();
        }
      }
      LOG_INFO("init sql memory manager", K(work_area_interval_size), K(tenant_id), K(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != sql_mem_mgr) {
        sql_mem_mgr->allocator_.reset();
        common::ob_delete(sql_mem_mgr);
      }
      sql_mem_mgr = nullptr;
    }
  }
  return ret;
}

void ObTenantSqlMemoryManager::mtl_destroy(ObTenantSqlMemoryManager *&sql_mem_mgr)
{
  if (nullptr != sql_mem_mgr) {
    if (nullptr != sql_mem_mgr->wa_intervals_) {
      sql_mem_mgr->allocator_.free(sql_mem_mgr->wa_intervals_);
      sql_mem_mgr->wa_intervals_ = nullptr;
    }
    if (nullptr != sql_mem_mgr->profile_lists_) {
      sql_mem_mgr->allocator_.free(sql_mem_mgr->profile_lists_);
      sql_mem_mgr->profile_lists_ = nullptr;
    }
    sql_mem_mgr->wa_ht_.destroy();
    sql_mem_mgr->workarea_stats_.reset();
    sql_mem_mgr->workarea_histograms_.reset();
    sql_mem_mgr->allocator_.reset();
    common::ob_delete(sql_mem_mgr);
  }
  sql_mem_mgr = nullptr;
}

int ObTenantSqlMemoryManager::calc_work_area_size_by_profile(
  int64_t global_bound_size,
  ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  if (profile.is_hash_join_wa()) {
    if (global_bound_size >= profile.get_cache_size()) {
      // in-memory
      profile.set_expect_size(profile.get_cache_size());
    } else if (global_bound_size >= profile.get_one_pass_size()) {
      profile.set_expect_size(global_bound_size);
    } else if (global_bound_size < profile.get_min_size()) {
      // 8个分区+1个page size
      profile.set_expect_size(profile.get_min_size());
    } else {
      profile.set_expect_size(global_bound_size);
    }
  } else if (profile.is_sort_wa()) {
    if (global_bound_size > profile.get_cache_size()) {
      // in-memory
      profile.set_expect_size(profile.get_cache_size());
    } else if (global_bound_size > profile.get_one_pass_size()) {
      // sort在one-pass情况下，增加内存对性能没有影响
      profile.set_expect_size(profile.get_one_pass_size());
    } else if (global_bound_size < profile.get_min_size()) {
      profile.set_expect_size(profile.get_min_size());
    } else {
      profile.set_expect_size(global_bound_size);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect profile type", K(profile.get_work_area_type()));
  }
  profile.set_global_bound_size(global_bound_size);
  profile.set_max_bound(min(global_bound_size, profile.get_cache_size()));
  return ret;
}

int ObTenantSqlMemoryManager::get_work_area_size(
  ObIAllocator *allocator,
  ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  if (!profile.is_registered()) {
    // disable sql memory manager
  } else if (enable_auto_memory_mgr_) {
    increase(profile.get_cache_size());
    LOG_TRACE("trace drift size", K(drift_size_), K(global_bound_size_));
    if (need_manual_calc_bound()) {
      ++manual_calc_cnt_;
      if (OB_FAIL(calculate_global_bound_size(allocator, false))) {
        LOG_WARN("failed to calculate global bound size", K(global_bound_size_));
      } else {
        profile.inc_calc_count();
        LOG_TRACE("trace manual calc global bound size", K(global_bound_size_),
          K(profile.get_one_pass_size()), K(drift_size_), K(mem_target_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc_work_area_size_by_profile(get_global_bound_size(), profile))) {
      LOG_WARN("failed to calculate worka area size by profile", K(ret), K(profile));
    }
  }
  return ret;
}

// 注册策略：满足不是小查询，即auto_sql_memory_manager is true
// profile目前存在三种状态
//  status             dynamic-perf-view     auto policy
//  register + auto    统计到性能视图           内存动态调整
//  register + manual  统计到性能视图           内存取决于xxx_area_size
//  unregister         不统计到性能视图          内存取决于xxx_area_size
//
//  is_registered:  register | unregister  只会影响是否注册，同时只有注册了才能将profile写入性能视图
//  auto_policy  :  auto|manual:  会影响内存使用策略
//  所以是否调用自动的内存调整，使用get_auto_policy来判断
//     当profile注册后，才能统计性能视图等
int ObTenantSqlMemoryManager::register_work_area_profile(ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  if (!profile.is_registered()) {
    if (OB_NOT_NULL(profile.get_prev())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: next is null, but prev is not null", K(ret));
    } else if (!ObSqlWorkAreaProfile::auto_sql_memory_manager(profile)) {
      // data is small, don't use auto memory manager
    } else {
      int64_t hash_val = get_hash_value(profile.get_id());
      if (hash_val < 0 || hash_val >= HASH_CNT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect hash val", K(hash_val), K(profile));
      } else if (OB_FAIL(profile_lists_[hash_val].register_work_area_profile(profile))) {
        LOG_WARN("failed to register work area profile", K(hash_val), K(profile));
      } else {
        profile.active_time_ = ObTimeUtility::current_time();
      }
    }
  }
  return ret;
}

int ObTenantSqlMemoryManager::update_work_area_profile(
  common::ObIAllocator *allocator,
  ObSqlWorkAreaProfile &profile,
  const int64_t delta_size)
{
  int ret = OB_SUCCESS;
  UNUSED(profile);
  if (enable_auto_memory_mgr_ && profile.get_auto_policy()) {
    // delta_size maybe negative integer
    (ATOMIC_AAF(&drift_size_, delta_size));
    if (need_manual_by_drift()) {
      int64_t pre_drift_size = drift_size_;
      ++manual_calc_cnt_;
      if (OB_ISNULL(allocator)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocator is null", K(lbt()));
      } else if (OB_FAIL(calculate_global_bound_size(allocator, false))) {
        LOG_WARN("failed to calculate global bound size", K(global_bound_size_));
      } else {
        profile.inc_calc_count();
        LOG_TRACE("trace manual calc global bound size by drift", K(global_bound_size_),
          K(profile.get_one_pass_size()), K(drift_size_), K(mem_target_), K(pre_drift_size));
      }
    }
  }
  return ret;
}

// 这里暂时对并发场景的写last record不进行并发控制
int ObTenantSqlMemoryManager::fill_workarea_stat(
  ObSqlWorkAreaStat &wa_stat,
  ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  if (profile.get_operator_type() != wa_stat.get_op_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: operator type is not match", K(profile.get_operator_type()),
      K(wa_stat.get_op_type()));
  } else {
    wa_stat.est_cache_size_ = profile.get_cache_size();
    wa_stat.est_one_pass_size_ = profile.get_one_pass_size();
    wa_stat.last_memory_used_ = profile.get_max_mem_used();
    wa_stat.last_execution_ = profile.get_number_pass();
    wa_stat.last_degree_ = profile.get_dop();
    int64_t num_executions = profile.get_number_pass();
    if (0 == num_executions) {
      wa_stat.increase_optimal_executions();
    } else if (1 == num_executions) {
      wa_stat.increase_onepass_executions();
    } else {
      wa_stat.increase_multipass_executions();
    }
    int64_t active_avg_time = wa_stat.get_total_executions() * wa_stat.get_active_avg_time();
    wa_stat.increase_total_executions();
    wa_stat.active_avg_time_ =
      (active_avg_time +
      (ObTimeUtility::current_time() - profile.get_active_time())) / wa_stat.get_total_executions();
    wa_stat.last_temp_size_ = profile.get_dumped_size();
    if (wa_stat.max_temp_size_ < wa_stat.last_temp_size_) {
      wa_stat.max_temp_size_ = wa_stat.last_temp_size_;
    }
    wa_stat.is_auto_policy_ = profile.get_auto_policy();
  }
  return ret;
}

int ObTenantSqlMemoryManager::try_fill_workarea_stat(
  ObSqlWorkAreaStat::WorkareaKey &workarea_key,
  ObSqlWorkAreaProfile &profile,
  bool &need_insert)
{
  int ret = OB_SUCCESS;
  need_insert = false;
  ObLatchRGuard guard(lock_, ObLatchIds::SQL_WA_STAT_MAP_LOCK);
  ObSqlWorkAreaStat *wa_stat = nullptr;
  if (OB_FAIL(wa_ht_.get_refactored(workarea_key, wa_stat))) {
    if (OB_HASH_NOT_EXIST == ret) {
      need_insert = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get stat", K(ret));
    }
  } else if (OB_ISNULL(wa_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: wa stat is null", K(ret), K(profile));
  } else {
    int64_t seqno = wa_stat->get_seqno();
    if (seqno < 0 || seqno >= MAX_WORKAREA_STAT_CNT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: seqno is invalid", K(ret), K(profile), K(seqno),
          K(*wa_stat));
    } else {
      ObSqlWorkAreaStat &tmp_wa_stat = workarea_stats_.at(seqno);
      if (&tmp_wa_stat != wa_stat) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: wa stat is not match", K(ret), K(profile), K(seqno));
      } else if (OB_FAIL(fill_workarea_stat(tmp_wa_stat, profile))) {
        LOG_WARN("failed to fill workarea stat", K(ret));
      }
    }
  }
  return ret;
}

// write lock and create new stat
int ObTenantSqlMemoryManager::new_and_fill_workarea_stat(
  ObSqlWorkAreaStat::WorkareaKey &workarea_key,
  ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  ObSqlWorkAreaStat *wa_stat = nullptr;
  ObLatchWGuard guard(lock_, ObLatchIds::SQL_WA_STAT_MAP_LOCK);
  if (OB_FAIL(wa_ht_.get_refactored(workarea_key, wa_stat))) {
  }
  if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    if (is_wa_full()) {
      // eliminate one
      if (wa_start_ != wa_end_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: wa is full, but start and end position is not match",
          K(wa_start_), K(wa_end_), K(wa_cnt_));
      } else {
        ObSqlWorkAreaStat *tmp_wa_stat = nullptr;
        wa_start_ = (wa_start_ + 1) % MAX_WORKAREA_STAT_CNT;
        wa_stat = &workarea_stats_.at(wa_end_);
        if (OB_FAIL(wa_ht_.erase_refactored(wa_stat->get_workarea_key(), &tmp_wa_stat))) {
          LOG_WARN("failed to erase workarea stat", K(ret), K(wa_stat->get_workarea_key()));
        } else if (wa_stat != tmp_wa_stat) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: wa stat is not match", K(ret));
        } else {
          wa_stat->reset();
          --wa_cnt_;
        }
      }
    } else {
      wa_stat = &workarea_stats_.at(wa_end_);
      wa_stat->reset();
    }
    if (OB_FAIL(ret)) {
    } else {
      // new item: key + operator type
      wa_stat->workarea_key_.set_sql_id(profile.get_sql_id());
      wa_stat->workarea_key_.set_plan_id(profile.get_plan_id());
      wa_stat->workarea_key_.set_operator_id(profile.get_operator_id());
      wa_stat->op_type_ = profile.get_operator_type();
      if (OB_FAIL(fill_workarea_stat(*wa_stat, profile))) {
        LOG_WARN("failed to fill workarea stat", K(ret));
      } else if (OB_FAIL(wa_ht_.set_refactored(workarea_key, wa_stat))) {
        LOG_WARN("failed to set refactored", K(ret));
      } else {
        wa_end_ = (wa_end_ + 1) % MAX_WORKAREA_STAT_CNT;
        ++wa_cnt_;
        LOG_TRACE("new workarea stat:", K(wa_stat->workarea_key_), K(workarea_key),
            K(wa_stat->workarea_key_ == workarea_key), K(wa_stat->seqno_),
            K(profile), K(wa_cnt_), K(wa_start_), K(wa_end_));
      }
    }
  } else if (OB_SUCC(ret)) {
    if (OB_ISNULL(wa_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: wa_stat is null", K(ret));
    } else if (OB_FAIL(fill_workarea_stat(*wa_stat, profile))) {
      LOG_WARN("failed to fill workarea stat", K(ret));
    }
  }
  return ret;
}

int ObTenantSqlMemoryManager::collect_workarea_stat(ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  if (profile.has_exec_ctx()) {
    ObSqlWorkAreaStat::WorkareaKey workarea_key(
      profile.get_plan_id(),
      profile.get_operator_id());
    workarea_key.set_sql_id(profile.get_sql_id());
    bool need_insert = false;
    if (OB_FAIL(try_fill_workarea_stat(workarea_key, profile, need_insert))) {
      LOG_WARN("failed to try fill workarea stat", K(ret));
    } else if (need_insert && OB_FAIL(new_and_fill_workarea_stat(workarea_key, profile))) {
      LOG_WARN("failed to create new and fill workarea start", K(ret));
    }
  }
  return ret;
}

int ObTenantSqlMemoryManager::fill_workarea_histogram(ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  int64_t idx = INT64_MAX;
  int64_t size = INT64_MAX;
  int64_t max_mem_used = profile.get_mem_used();
  if (OB_FAIL(find_interval_index(max_mem_used, idx, size))) {
    LOG_WARN("failed to find interval index", K(ret));
  } else if (INT64_MAX == idx || INT64_MAX == size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: can't found any interval", K(idx), K(size), K(profile));
  } else {
    ObWorkareaHistogram &hist = workarea_histograms_.at(idx);
    if (max_mem_used < hist.get_low_optimal_size()
    || max_mem_used > hist.get_high_optimal_size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: find interval error", K(ret),
        "mem used", max_mem_used,
        "low bound", hist.get_low_optimal_size(),
        "high bound", hist.get_high_optimal_size());
    } else {
      if (0 == profile.get_number_pass()) {
        hist.increase_optimal_executions();
      } else if (1 == profile.get_number_pass()) {
        hist.increase_onepass_executions();
      } else if (1 < profile.get_number_pass()) {
        hist.increase_multipass_executions();
      }
      hist.increase_total_executions();
    }
  }
  return ret;
}

int ObTenantSqlMemoryManager::unregister_work_area_profile(ObSqlWorkAreaProfile &profile)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (profile.is_registered()) {
    int64_t hash_val = get_hash_value(profile.get_id());
    if (hash_val < 0 || hash_val >= HASH_CNT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect hash val", K(hash_val), K(profile));
    } else if (OB_FAIL(profile_lists_[hash_val].unregister_work_area_profile(profile))) {
      LOG_WARN("failed to register work area profile", K(hash_val), K(profile));
    } else {
      if (enable_auto_memory_mgr_ && profile.get_auto_policy()) {
        decrease(profile.get_cache_size());
      }
      if (!profile.need_profiled()) {
      } else if (OB_FAIL(collect_workarea_stat(profile))) {
        LOG_WARN("failed to fill workarea stat", K(ret));
      } else if (OB_FAIL(fill_workarea_histogram(profile))) {
        LOG_WARN("failed to fill workarea histogram", K(ret));
      }
      LOG_TRACE("unregister workarea profile", K(profile), K(ret));
    }
  }
  return ret;
}

int ObTenantSqlMemoryManager::get_max_work_area_size(
  int64_t &max_wa_memory_size, const bool auto_calc)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSysVarSchema *var_schema = NULL;
  ObObj value;
  int64_t pctg = 0;
  max_wa_memory_size = 0;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_system_variable(
    tenant_id_, SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE, var_schema))) {
    LOG_WARN("get tenant system variable failed", K(ret), K(tenant_id_));
  } else if (OB_ISNULL(var_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var_schema is null");
  } else if (OB_FAIL(var_schema->get_value(NULL, NULL, value))) {
    LOG_WARN("get value from var_schema failed", K(ret), K(*var_schema));
  } else if (OB_FAIL(value.get_int(pctg))) {
    LOG_WARN("get int from value failed", K(ret), K(value));
  } else {
    int64_t tenant_max_memory_limit = get_tenant_memory_limit(tenant_id_);
    int64_t tenant_memory_hold = get_tenant_memory_hold(tenant_id_);
    int64_t tenant_work_area_max_size = tenant_max_memory_limit * pctg / 100;
    int64_t tenant_work_area_memory_hold =
      get_tenant_memory_hold(tenant_id_, common::ObCtxIds::WORK_AREA);
    int64_t max_tenant_memory_size = tenant_max_memory_limit - tenant_memory_hold;
    int64_t max_workarea_memory_size = tenant_work_area_max_size - tenant_work_area_memory_hold;
    int64_t washable_size = -2;
    int wash_ratio = 6; // valid value: [0-6]
    if (max_workarea_memory_size > 0 &&
        max_tenant_memory_size > 0 &&
        max_workarea_memory_size > max_tenant_memory_size) {
      int tmp_ret = EVENT_CALL(EventTable::EN_AMM_WASH_RATIO);
      if (0 != tmp_ret) {
        wash_ratio = -tmp_ret;
      }
      if (0 <= wash_ratio && wash_ratio <=6 && auto_calc) {
        if (OB_FAIL(ObKVGlobalCache::get_instance().get_washable_size(tenant_id_, washable_size, wash_ratio))) {
          LOG_WARN("failed to get washable memory size", K(ret));
        } else {
          max_tenant_memory_size += washable_size;
          ATOMIC_SET(&max_tenant_memory_size_, max_tenant_memory_size);
        }
        // if failed to get washable size, then reset OB_SUCCESS and just use little memory
        ret = OB_SUCCESS;
      } else {
        int64_t tmp_max_tenant_memory_size = ATOMIC_LOAD(&max_tenant_memory_size_);
        if (0 != tmp_max_tenant_memory_size && 0 <= wash_ratio && wash_ratio <=6) {
          // use the value that background thread calculate
          max_tenant_memory_size += tmp_max_tenant_memory_size;
        } else {
          ObTenantResourceMgrHandle resource_handle;
          if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(
              tenant_id_, resource_handle))) {
            ret = OB_SUCCESS;
          } else {
            // TODO: kvcache大概可以淘汰多少内存，目前没有数据，后续寒晖他们会提供接口
            // bug34818894
            // 这里暂时写一个默认比例
            max_tenant_memory_size += resource_handle.get_memory_mgr()->get_cache_hold() * pctg / 100;
            washable_size = -1;
          }
        }
      }
    }
    // 取租户最大可用内存和ctx最大可用内存的最小值
    int64_t remain_memory_size = max_tenant_memory_size > 0
              ? min(max_workarea_memory_size, max_tenant_memory_size)
              : max_tenant_memory_size;
    int64_t total_alloc_size = sql_mem_callback_.get_total_alloc_size();
    double ratio = total_alloc_size * 1.0 / tenant_work_area_memory_hold;
    // 1 - x^3函数，表示随着hold内存越多，可用内存越少，同时alloc越多，可用内存越少
    // 反之，hold越少，可用内存越多，alloc越少，可用内存又会越多
    // 这里采用平方主要是为了内存增长和减少都比较平滑
    // so: formula
    //    hold_ratio = hold / max_size;
    //    tmp_max_wa = (1 - hold_ratio * hold_ratio * hold_ratio) * (max - hold) + alloc
    //    alloc_ratio = alloc / tmp_max_wa
    //    max_wa = tmp_max_wa * (1 - alloc_ratio * alloc_ratio * alloc_ratio)
    int64_t pre_mem_target = mem_target_;
    double hold_ratio = 1. * tenant_work_area_memory_hold / tenant_work_area_max_size;
    int64_t tmp_max_wa_memory_size = (remain_memory_size > 0)
              ? remain_memory_size + total_alloc_size
              : total_alloc_size;
    double alloc_ratio = total_alloc_size * 1.0 / tmp_max_wa_memory_size;
    // if (total_alloc_size >= tmp_max_wa_memory_size) {
    //   // 这里用最近N次的结果来拟合可能比较好，但由于global bound 决定后，内存使用有延迟，比较难决定他们之间的关系
    //   max_wa_memory_size = (tmp_max_wa_memory_size >> 1);
    // } else
    {
      // only use formula (1 - ratio ^ 3)
      max_wa_memory_size = tmp_max_wa_memory_size * (1 - alloc_ratio * alloc_ratio * alloc_ratio);
    }
    max_workarea_size_ = tenant_work_area_max_size;
    workarea_hold_size_ = tenant_work_area_memory_hold;
    max_auto_workarea_size_ = max_wa_memory_size;
    if (0 > max_wa_memory_size) {
      max_wa_memory_size = 0;
      LOG_INFO("max work area is 0", K(tenant_max_memory_limit), K(total_alloc_size),
      K(tenant_work_area_memory_hold), K(tenant_work_area_max_size));
    }
    if (auto_calc) {
      LOG_INFO("trace max work area", K(auto_calc), K(tenant_max_memory_limit), K(total_alloc_size),
        K(tenant_work_area_memory_hold), K(tenant_work_area_max_size), K(max_wa_memory_size),
        K(tmp_max_wa_memory_size), K(pre_mem_target), K(remain_memory_size), K(ratio),
        K(alloc_ratio), K(hold_ratio), K(tenant_memory_hold), K(washable_size),
        K(max_workarea_memory_size), K(max_tenant_memory_size), K(wash_ratio), K_(tenant_id));
    }
  }
  return ret;
}

// total size需要保持一致，在一次处理过程中，需要统一，如果在计算过程中
// 可能被修改，会导致find的interval index和cache size不一致
int ObTenantSqlMemoryManager::find_interval_index(
  const int64_t cache_size,
  int64_t &idx,
  int64_t &out_cache_size)
{
  int ret = OB_SUCCESS;
  bool found = false;
  idx = -1;
  int64_t total_size = cache_size;
  out_cache_size = cache_size;
  if (cache_size <= wa_intervals_[LESS_THAN_100M_CNT - 1].get_interval_cache_size()) {
    idx = (total_size - 1) / LESS_THAN_100M_INTERVAL_SIZE;
    found = true;
  } else {
    idx = LESS_THAN_100M_CNT;
    total_size -= LESS_THAN_100M_INTERVAL_SIZE * LESS_THAN_100M_CNT;
    if (cache_size <= wa_intervals_[LESS_THAN_500M_CNT - 1].get_interval_cache_size()) {
      idx += (total_size - 1) / LESS_THAN_500M_INTERVAL_SIZE;
      found = true;
    } else {
      idx = LESS_THAN_500M_CNT;
      total_size -= LESS_THAN_500M_INTERVAL_SIZE * (LESS_THAN_500M_CNT - LESS_THAN_100M_CNT);
    }
  }

  if (found) {
  } else if (cache_size <= wa_intervals_[LESS_THAN_1G_CNT - 1].get_interval_cache_size()) {
    idx += (total_size - 1) / LESS_THAN_1G_INTERVAL_SIZE;
    found = true;
  } else {
    idx = LESS_THAN_1G_CNT;
    total_size -= LESS_THAN_1G_INTERVAL_SIZE * (LESS_THAN_1G_CNT - LESS_THAN_500M_CNT);
    if (cache_size <= wa_intervals_[LESS_THAN_5G_CNT - 1].get_interval_cache_size()) {
      idx += (total_size - 1) / LESS_THAN_5G_INTERVAL_SIZE;
      found = true;
    } else {
      idx = LESS_THAN_5G_CNT;
      total_size -= LESS_THAN_5G_INTERVAL_SIZE * (LESS_THAN_5G_CNT - LESS_THAN_1G_CNT);
    }
  }

  if (found) {
  } else if (cache_size <= wa_intervals_[LESS_THAN_10G_CNT - 1].get_interval_cache_size()) {
    idx += (total_size - 1) / LESS_THAN_10G_INTERVAL_SIZE;
    found = true;
  } else {
    idx = LESS_THAN_10G_CNT;
    total_size -= LESS_THAN_10G_INTERVAL_SIZE * (LESS_THAN_10G_CNT - LESS_THAN_5G_CNT);
    if (cache_size <= wa_intervals_[LESS_THAN_100G_CNT - 1].get_interval_cache_size()) {
      idx += (total_size - 1) / LESS_THAN_100G_INTERVAL_SIZE;
      found = true;
    } else {
      idx = LESS_THAN_100G_CNT;
      total_size -= LESS_THAN_100G_INTERVAL_SIZE * (LESS_THAN_100G_CNT - LESS_THAN_10G_CNT);
      if (cache_size <= wa_intervals_[LESS_THAN_1T_CNT - 1].get_interval_cache_size()) {
        idx += (total_size - 1) / LESS_THAN_1T_INTERVAL_SIZE;
        found = true;
      } else {
        found = true;
        idx = INTERVAL_NUM - 1;
        LOG_TRACE("too big size", K(cache_size));
      }
    }
  }
  // check
  if (found) {
    if (cache_size < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: cache size is less than 0", K(idx), K(cache_size), K(ret));
    } else if (INTERVAL_NUM - 1 != idx) {
      if (0 == idx) {
        if (cache_size > wa_intervals_[idx].get_interval_cache_size()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to find interval index", K(idx), K(cache_size),
          K(wa_intervals_[idx].get_interval_cache_size()));
        }
      } else {
        if (cache_size <= wa_intervals_[idx - 1].get_interval_cache_size()
          || cache_size > wa_intervals_[idx].get_interval_cache_size()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to find interval index", K(idx), K(cache_size),
            K(wa_intervals_[idx].get_interval_cache_size()));
        }
      }
    } else {
      if (cache_size <= wa_intervals_[idx - 1].get_interval_cache_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to find interval index", K(idx), K(cache_size),
          K(wa_intervals_[idx - 1].get_interval_cache_size()));
      }
    }
  }
  return ret;
}

// sum memory size of all profiles
int ObTenantSqlMemoryManager::count_profile_into_work_area_intervals(
  ObSqlWorkAreaInterval *wa_intervals,
  int64_t &total_memory_size,
  int64_t &cur_profile_cnt)
{
  int ret = OB_SUCCESS;
  int64_t interval_idx = -1;
  int64_t one_pass_idx = -1;
  int64_t cache_size = 0;
  int64_t one_pass_size = 0;
  total_memory_size = 0;
  cur_profile_cnt = 0;

  // count interval stat from all profiles
  if (nullptr == profile_lists_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("profile list is null", K(ret));
  } else {
    for (int64_t i = 0; i < HASH_CNT && OB_SUCC(ret); ++i)  {
      ObLockGuard<ObSpinLock> lock_guard(profile_lists_[i].get_lock());
      ObDList<ObSqlWorkAreaProfile> &profile_list = profile_lists_[i].get_profile_list();
      DLIST_FOREACH_X(profile, profile_list, OB_SUCC(ret)) {
        if (!profile->get_auto_policy()) {
          // 没有使用auto的不作为统计之内
        } else if (OB_FAIL(find_interval_index(profile->get_cache_size(), interval_idx, cache_size))) {
          LOG_WARN("failed to find interval index", K(*profile));
        } else {
          one_pass_size = profile->calc_one_pass_size(cache_size);
          if (OB_FAIL(find_interval_index(
              profile->get_one_pass_size(), one_pass_idx, one_pass_size))) {
            LOG_WARN("failed to find interval index", K(*profile));
          } else if (OB_FAIL(wa_intervals[interval_idx].get_interval_stat().analyze_profile(
                              *profile, cache_size, one_pass_size, MAX_INTERVAL_SIZE))) {
            LOG_WARN("failed to analyze profile", K(*profile));
          } else if (OB_FAIL(wa_intervals[one_pass_idx].get_interval_stat().analyze_profile(
                              *profile, one_pass_size, 0, MAX_INTERVAL_SIZE, true))) {
            LOG_WARN("failed to analyze profile", K(*profile));
          } else {
            total_memory_size += cache_size;
            ++cur_profile_cnt;
          }
        }
      }
    }
  }
  return ret;
}

bool ObTenantSqlMemoryManager::enable_auto_sql_memory_manager()
{
  bool auto_memory_mgr = false;
  ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    const ObString tmp_str(tenant_config->workarea_size_policy.str());
    auto_memory_mgr = !tmp_str.case_compare("AUTO");
    LOG_TRACE("get work area policy config", K(tenant_id_), K(auto_memory_mgr), K(tmp_str),
      K(tenant_config->workarea_size_policy.str()));
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "failed to init tenant config", K(tenant_id_));
  }
  return auto_memory_mgr;
}

// ensure lock outside
void ObTenantSqlMemoryManager::reset()
{
  if (nullptr != profile_lists_) {
    for (int64_t i = 0; i < HASH_CNT; ++i) {
      profile_lists_[i].reset();
    }
  }
  // 统计的内存通过每个operator自己来确定，否则来开与关过程中，存在申请和释放时候，统计不一致
  //sql_mem_callback_.reset();
  drift_size_ = 0;
  profile_cnt_ = 0;
  global_bound_size_ = 0;
}

// try best to push global_bound_size to every profile registered
int ObTenantSqlMemoryManager::try_push_profiles_work_area_size(int64_t global_bound_size)
{
  int ret = OB_SUCCESS;
  if (nullptr == profile_lists_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("profile list is null", K(ret));
  } else {
    for (int64_t i = 0; i < HASH_CNT && OB_SUCC(ret); ++i)  {
      if (OB_SUCC(profile_lists_[i].get_lock().trylock())) {
        ObDList<ObSqlWorkAreaProfile> &profile_list = profile_lists_[i].get_profile_list();
        DLIST_FOREACH_X(profile, profile_list, OB_SUCC(ret)) {
          if (profile->get_auto_policy()
              && OB_FAIL(calc_work_area_size_by_profile(global_bound_size, *profile))) {
            ret = OB_SUCCESS;
            LOG_WARN("failed to calculate worka area size by profile", K(ret), K(*profile),
              K(global_bound_size));
          }
        }
        profile_lists_[i].get_lock().unlock();
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return OB_SUCCESS;
}

int ObTenantSqlMemoryManager::calculate_global_bound_size_by_interval_info(
  ObIAllocator &allocator,
  const int64_t wa_max_memory_size,
  const bool auto_calc)
{
  int ret = OB_SUCCESS;
  ObSqlWorkAreaCalcInfo calc_info;
  if (OB_FAIL(calc_info.init(allocator, wa_intervals_, INTERVAL_NUM))) {
    LOG_WARN("failed to init work area calc info", K(ret));
  } else {
    int64_t pre_profile_cnt = profile_cnt_;
    int64_t total_memory_size = 0;
    int64_t cur_profile_cnt = 0;
    if (OB_FAIL(count_profile_into_work_area_intervals(
      calc_info.get_wa_intervals(), total_memory_size, cur_profile_cnt))) {
      LOG_WARN("failed to count profiles", K(ret));
    } else if (OB_FAIL(calc_info.calculate_global_bound_size(
      wa_max_memory_size, total_memory_size, pre_profile_cnt, auto_calc))) {
      LOG_WARN("failed to find best interval index", K(ret), K(wa_max_memory_size),
        K(total_memory_size));
    } else {
      int64_t pre_drift_size = drift_size_;
      {
        lib::ObMutexGuard guard(mutex_);
        drift_size_ = 0;
        pre_profile_cnt_ = cur_profile_cnt;
        global_bound_size_ = calc_info.get_global_bound_size();
        mem_target_ = calc_info.get_mem_target();
        pre_enable_auto_memory_mgr_ = true;
        // last set enable auto memory manager, so others read the variable to avoiding dirty read
        enable_auto_memory_mgr_ = true;
      }
      if (auto_calc) {
        LOG_INFO("timer to calc global bound size", K(ret), K(global_bound_size_),
          K(manual_calc_cnt_), K(drift_size_), K(pre_drift_size), K(wa_max_memory_size),
          K(sql_mem_callback_.get_total_alloc_size()), K(tenant_id_), K(profile_cnt_),
          K(pre_profile_cnt_), K(pre_profile_cnt), K(calc_info.get_global_bound_size()),
          K(total_memory_size), K(cur_profile_cnt), K(calc_info.get_mem_target()), K(auto_calc),
          K(sql_mem_callback_.get_total_dump_size()));
      }
      if (OB_FAIL(try_push_profiles_work_area_size(calc_info.get_global_bound_size()))) {
        LOG_WARN("failed to push profiles work area size",
          K(ret), K(calc_info.get_global_bound_size()));
      }
    }
  }
  calc_info.destroy(allocator);
  return ret;
}

// 算法步骤：
// 0 切分好间隔点，每个间隔表示一个内存范围
// 1 遍历所有profiles，将profile的cache size找到对应的间隔，遍历结束后
//   则每个区间存放了所有在这区间的所有profile个数（只是估算统计，不是准确的profile信息）
// 2 从后往前遍历间隔，计算每个间隔如果作为bound，需要的mem_target是多少，全部计算结束后,
//   与期望的mem_target对比，返回真正bound大小
int ObTenantSqlMemoryManager::calculate_global_bound_size(ObIAllocator *allocator, bool auto_calc)
{
  int ret = OB_SUCCESS;
  int64_t wa_max_memory_size = 0;
  // set enable_auto_sql_memory_mgr after calculate global bound size
  bool auto_memory_mgr = enable_auto_sql_memory_manager();
  if (!auto_memory_mgr) {
    // manually memory manager
    lib::ObMutexGuard guard(mutex_);
    enable_auto_memory_mgr_ = false;
    if (pre_enable_auto_memory_mgr_) {
      reset();
      pre_enable_auto_memory_mgr_ = false;
    }
  } else {
    {
      lib::ObMutexGuard guard(mutex_);
      if (!pre_enable_auto_memory_mgr_) {
        if (enable_auto_memory_mgr_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status", K(ret));
        } else {
          reset();
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_max_work_area_size(wa_max_memory_size, auto_calc))) {
      LOG_WARN("failed to get percent");
    } else if (0 == wa_max_memory_size) {
      lib::ObMutexGuard guard(mutex_);
      global_bound_size_ = min_bound_size_;
      drift_size_ = 0;
      pre_enable_auto_memory_mgr_ = true;
      // last set enable auto memory manager, so others read the variable to avoiding dirty read
      enable_auto_memory_mgr_ = true;
      if (auto_calc) {
        LOG_INFO("work area memory zero", K(tenant_id_), K(global_bound_size_));
      }
    } else {
      if (OB_ISNULL(allocator)) {
        allocator = &allocator_;
      }
      if (OB_FAIL(calculate_global_bound_size_by_interval_info(
                    *allocator, wa_max_memory_size, auto_calc))) {
        LOG_WARN("failed to calculate global bound size", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantSqlMemoryManager::get_workarea_stat(ObIArray<ObSqlWorkAreaStat> &wa_stats)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard guard(lock_, ObLatchIds::SQL_WA_STAT_MAP_LOCK);
  for (int64_t i = wa_start_; i < wa_start_ + wa_cnt_ && OB_SUCC(ret); ++i) {
    int64_t nth = i % MAX_WORKAREA_STAT_CNT;
    if (OB_FAIL(wa_stats.push_back(workarea_stats_.at(nth)))) {
      LOG_WARN("failed to push back workarea stat", K(ret));
    } else {
      LOG_TRACE("trace workarea history", K(workarea_stats_.at(nth)),
          K(wa_stats.at(wa_stats.count() - 1)));
    }
  }
  return ret;
}

int ObTenantSqlMemoryManager::get_workarea_histogram(
  common::ObIArray<ObWorkareaHistogram> &wa_histograms)
{
  int ret = OB_SUCCESS;
  int64_t cnt = workarea_histograms_.count();
  for (int64_t i = 0; i < cnt && OB_SUCC(ret); ++i) {
    if (OB_FAIL(wa_histograms.push_back(workarea_histograms_.at(i)))) {
      LOG_WARN("failed to push back workarea stat", K(ret));
    } else {
      LOG_TRACE("trace workarea histogram", K(workarea_histograms_.at(i)),
          K(wa_histograms.at(wa_histograms.count() - 1)));
    }
  }
  return ret;
}

int ObTenantSqlMemoryManager::get_all_active_workarea(
  ObIArray<ObSqlWorkareaProfileInfo> &wa_actives)
{
  int ret = OB_SUCCESS;
  if (nullptr == profile_lists_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("profile list is null", K(ret));
  } else {
    for (int64_t i = 0; i < HASH_CNT && OB_SUCC(ret); ++i)  {
      ObLockGuard<ObSpinLock> lock_guard(profile_lists_[i].get_lock());
      ObDList<ObSqlWorkAreaProfile> &profile_list = profile_lists_[i].get_profile_list();
      DLIST_FOREACH_X(profile, profile_list, OB_SUCC(ret)) {
        ObSqlWorkareaProfileInfo profile_info;
        profile_info.profile_ = *profile;
        profile_info.plan_id_ = profile->get_plan_id();
        profile_info.sql_exec_id_ = profile->get_exec_id();
        profile_info.set_sql_id(profile->get_sql_id());
        profile_info.session_id_ = profile->get_session_id();
        if (OB_FAIL(wa_actives.push_back(profile_info))) {
          LOG_WARN("failed to push back profile", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTenantSqlMemoryManager::get_workarea_memory_info(
  ObSqlWorkareaCurrentMemoryInfo &memory_info)
{
  int ret = OB_SUCCESS;
  // 这里暂时仅仅已瞬态方式输出，不考虑并发问题
  memory_info.enable_ = enable_auto_memory_mgr_;
  memory_info.max_workarea_size_ = max_workarea_size_;
  memory_info.workarea_hold_size_ = workarea_hold_size_;
  memory_info.max_auto_workarea_size_ = max_auto_workarea_size_;
  memory_info.mem_target_ = mem_target_;
  memory_info.total_mem_used_ = sql_mem_callback_.get_total_alloc_size();
  memory_info.global_bound_size_ = global_bound_size_;
  memory_info.drift_size_ = drift_size_;
  memory_info.workarea_cnt_ = profile_cnt_;
  memory_info.manual_calc_cnt_ = manual_calc_cnt_;
  return ret;
}

} // sql
} // oceanbase
