/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON

#include "lib/stat/ob_diagnostic_info_summary.h"
#include "lib/stat/ob_diagnostic_info_util.h"
#include "lib/stat/ob_diagnose_info.h"
#include "common/ob_smart_var.h"
#include "lib/allocator/ob_sql_mem_leak_checker.h"

namespace oceanbase
{
namespace common
{

extern int64_t calc_slot_num(int64_t cpu_count);

constexpr int64_t DEFAULT_SUMMARY_TIMEOUT = 50 * 1000;  // in us

ObDiagnosticInfoSlot::ObDiagnosticInfoSlot()
    : events_(), stats_(), latch_stats_(), lock_(ObLatchIds::DI_SUMMARY_LOCK)
{}

void ObDiagnosticInfoSlot::accumulate_diagnostic_info(ObDiagnosticInfo &di)
{
  int ret = OB_SUCCESS;
  lib::ObDisableDiagnoseGuard g;
  if (OB_SUCC(lock_.lock(DEFAULT_SUMMARY_TIMEOUT))) {
    inner_accumuate_diagnostic_info(di);
    lock_.unlock();
  } else {
    LOG_WARN("lock was holding too long, proceed to accumulate diagnostic info without lock.",
        K(lock_.get_wid()), K(di));
    inner_accumuate_diagnostic_info(di);
  }
}

void ObDiagnosticInfoSlot::inner_accumuate_diagnostic_info(ObDiagnosticInfo &di)
{
  di.get_event_stats().accumulate_to(events_);
  stats_.add(di.get_add_stat_stats());
}

ObDiagnosticInfoCollector::ObDiagnosticInfoCollector()
    : tenant_id_(0),
      group_id_(0),
      slot_mask_(0),
      slot_count_(0),
      is_inited_(false),
      di_info_bundle_(nullptr)
{}

ObDiagnosticInfoCollector::~ObDiagnosticInfoCollector()
{
  if (OB_NOT_NULL(di_info_bundle_)) {
    for (int i = 0; i < slot_count_; i++) {
      di_info_bundle_[i].~ObDiagnosticInfoSlot();
    }
    ob_free(di_info_bundle_);
    di_info_bundle_ = nullptr;
    LOG_INFO("destroy current di collector", KPC(this));
  }
}

int ObDiagnosticInfoCollector::init(int cpu_cnt, int64_t tenant_id, int64_t group_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    DISABLE_SQL_MEMLEAK_GUARD;
    tenant_id_ = tenant_id;
    group_id_ = group_id;
    // slot num is power of 2
    int64_t slot_num = calc_slot_num(cpu_cnt);

    slot_mask_ = slot_num - 1;
    slot_count_ = slot_num;
    int64_t size = sizeof(ObDiagnosticInfoSlot) * slot_num;
    ObMemAttr attr(tenant_id, "DiagnosticSum");
    di_info_bundle_ = static_cast<ObDiagnosticInfoSlot *>(ob_malloc(size, attr));
    if (nullptr == di_info_bundle_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate di collector memory", K(ret));
    } else {
      for (int i = 0; i < slot_num; i++) {
        new (&di_info_bundle_[i]) ObDiagnosticInfoSlot();
      }
      is_inited_ = true;
      LOG_INFO(
          "Successfully init diagnostic info collector", K(slot_num), K(tenant_id), K(group_id));
    }
  }
  return ret;
}

int ObDiagnosticInfoCollector::add_diagnostic_info(ObDiagnosticInfo &di)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(is_inited_);
  const int64_t target_slot = di.get_session_id() & slot_mask_;
  OB_ASSERT(target_slot < slot_count_);
  if (OB_NOT_NULL(di_info_bundle_)) {
    di_info_bundle_[target_slot].accumulate_diagnostic_info(di);
  } else {
    LOG_WARN_RET(OB_SUCCESS, "di collector is null", K(is_inited_));
  }
  return ret;
}

void ObDiagnosticInfoCollector::get_all_events(ObWaitEventStatArray &arr) const
{
  OB_ASSERT(is_inited_);
  if (OB_NOT_NULL(di_info_bundle_)) {
    for (int i = 0; i < slot_count_; i++) {
      arr.add(di_info_bundle_[i].get_events());
    }
  } else {
    LOG_WARN_RET(OB_SUCCESS, "di collector is null", K(is_inited_));
  }
}

void ObDiagnosticInfoCollector::get_all_add_stats(ObStatEventAddStatArray &arr) const
{
  OB_ASSERT(is_inited_);
  if (OB_NOT_NULL(di_info_bundle_)) {
    for (int i = 0; i < slot_count_; i++) {
      arr.add(di_info_bundle_[i].get_stats());
    }
  } else {
    LOG_WARN_RET(OB_SUCCESS, "di collector is null", K(is_inited_));
  }
}

void ObDiagnosticInfoCollector::get_all_latch_stat(ObLatchStatArray &arr) const
{
  OB_ASSERT(is_inited_);
  if (OB_NOT_NULL(di_info_bundle_)) {
    for (int i = 0; i < slot_count_; i++) {
      arr.add(di_info_bundle_[i].get_all_latch_stat());
    }
  } else {
    LOG_WARN_RET(OB_SUCCESS, "di collector is null", K(is_inited_));
  }
}

ObDiagnosticInfoSlot *ObDiagnosticInfoCollector::get_slot(int64_t session_id)
{
  OB_ASSERT(is_inited_);
  if (OB_NOT_NULL(di_info_bundle_)) {
    const int64_t target_slot = session_id & slot_mask_;
    OB_ASSERT(target_slot < slot_count_);
    return &di_info_bundle_[target_slot];
  } else {
    LOG_WARN_RET(OB_SUCCESS, "di collector is null", K(is_inited_));
    return nullptr;
  }
}

int ObBaseDiagnosticInfoSummary::add_diagnostic_info(ObDiagnosticInfo &di)
{
  int ret = OB_SUCCESS;
  lib::ObDisableDiagnoseGuard g;
  ObDiagnosticInfoCollector *collector = nullptr;
  if (OB_FAIL(get_di_collector(di.get_tenant_id(), di.get_group_id(), collector))) {
    LOG_WARN("fail to get di collector", K(ret), K(di));
  } else if (OB_NOT_NULL(collector)) {
    if (OB_FAIL(collector->add_diagnostic_info(di))) {
      LOG_WARN("failed to add di info", K(ret), K(di), KPC(collector));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null di collector", K(di));
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::init(int64_t cpu_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("di summary init twice", K(cpu_cnt), KPC(this));
  } else {
    if (OB_FAIL(collectors_.init())) {
      LOG_WARN("failed to init di summary collector", K(ret), K(cpu_cnt));
    } else {
      cpu_cnt_ = cpu_cnt;
      is_inited_ = true;
      LOG_INFO("init di base summary finished", K(cpu_cnt), KPC(this));
    }
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::get_tenant_event(int64_t tenant_id, ObWaitEventStatArray &arr)
{
  int ret = OB_SUCCESS;
  ObDiagnosticInfoCollector *cur = nullptr;
  SummaryMap::Iterator iter(collectors_);
  while (OB_SUCC(ret) && OB_NOT_NULL(iter.next(cur))) {
    if (cur->get_tenant_id() == tenant_id) {
      cur->get_all_events(arr);
    }
    iter.revert(cur);
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::get_tenant_add_stats(
    int64_t tenant_id, ObStatEventAddStatArray &arr)
{
  int ret = OB_SUCCESS;
  ObDiagnosticInfoCollector *cur = nullptr;
  SummaryMap::Iterator iter(collectors_);
  while (OB_SUCC(ret) && OB_NOT_NULL(iter.next(cur))) {
    if (cur->get_tenant_id() == tenant_id) {
      cur->get_all_add_stats(arr);
    }
    iter.revert(cur);
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::get_tenant_latch_stat(int64_t tenant_id, ObLatchStatArray &arr)
{
  int ret = OB_SUCCESS;
  ObDiagnosticInfoCollector *cur = nullptr;
  SummaryMap::Iterator iter(collectors_);
  while (OB_SUCC(ret) && OB_NOT_NULL(iter.next(cur))) {
    if (cur->get_tenant_id() == tenant_id) {
      cur->get_all_latch_stat(arr);
    }
    iter.revert(cur);
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::get_group_event(int64_t group_id, ObWaitEventStatArray &arr)
{
  int ret = OB_SUCCESS;
  ObDiagnosticInfoCollector *cur = nullptr;
  SummaryMap::Iterator iter(collectors_);
  while (OB_SUCC(ret) && OB_NOT_NULL(iter.next(cur))) {
    if (cur->get_group_id() == group_id) {
      cur->get_all_events(arr);
    }
    iter.revert(cur);
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::get_group_add_stats(int64_t group_id, ObStatEventAddStatArray &arr)
{
  int ret = OB_SUCCESS;
  ObDiagnosticInfoCollector *cur = nullptr;
  SummaryMap::Iterator iter(collectors_);
  while (OB_SUCC(ret) && OB_NOT_NULL(iter.next(cur))) {
    if (cur->get_group_id() == group_id) {
      cur->get_all_add_stats(arr);
    }
    iter.revert(cur);
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::get_di_slot(
    int64_t tenant_id, int64_t group_id, int64_t session_id, ObDiagnosticInfoSlot *&slot)
{
  int ret = OB_SUCCESS;
  lib::ObDisableDiagnoseGuard g;
  ObDiagnosticInfoCollector *collector = nullptr;
  if (OB_FAIL(get_di_collector(tenant_id, group_id, collector))) {
    LOG_WARN("fail to get di collector", K(ret), K(tenant_id), K(session_id), K(group_id));
  } else {
    OB_ASSERT(collector != nullptr);
    slot = collector->get_slot(session_id);
    if (OB_ISNULL(slot)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("di slot is null");
    }
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::get_di_collector(
    int64_t tenant_id, int64_t group_id, ObDiagnosticInfoCollector *&collector)
{
  int ret = OB_SUCCESS;
  ObDiagnosticKey key(tenant_id, group_id);
  if (OB_FAIL(collectors_.get(key, collector))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      {
        lib::ObMutexGuard guard(mutex_);
        if (OB_FAIL(collectors_.get(key, collector))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            if (OB_FAIL(collectors_.alloc_value(collector))) {
              LOG_WARN("failed to allocate collector", K(ret));
            } else if (OB_ISNULL(collector)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate collector", K(ret));
            } else {
              // init collector
              if (OB_FAIL(collector->init(cpu_cnt_, tenant_id, group_id))) {
                LOG_WARN(
                    "failed to init diagnostic info collector", K(ret), K(tenant_id), K(group_id));
                collectors_.free_value(collector);
                collector = nullptr;
              } else if (OB_FAIL(collectors_.insert_and_get(key, collector))) {
                // free directly, cause value->hash_node_ is nullptr.
                LOG_WARN("failed to insert and get diagnostic info collector", K(ret), K(tenant_id),
                    K(group_id), K(key), K(this));
                collectors_.free_value(collector);
                collector = nullptr;
              } else {
                LOG_DEBUG("init new diagnostic collector", K(tenant_id), K(group_id), K(this),
                    K(collector), K(lbt()));
              }
            }
          }
        } else {
          // concurrency insert.
        }
      }
    } else {
      LOG_WARN("failed to get di info", K(ret), K(collector));
    }
  }
  if (OB_SUCC(ret)) {
    collectors_.revert(collector);
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::for_each_group(
    int64_t tenant_id, std::function<void(int64_t, const ObDiagnoseTenantInfo &)> fn)
{
  int ret = OB_SUCCESS;
  ObDiagnosticInfoCollector *cur = nullptr;
  SummaryMap::Iterator iter(collectors_);
  ObArenaAllocator arena;
  ObIAllocator *alloc = static_cast<ObIAllocator *>(&arena);
  HEAP_VAR(ObDiagnoseTenantInfo, tmp, alloc)
  {
    while (OB_SUCC(ret) && OB_NOT_NULL(iter.next(cur))) {
      if (cur->get_tenant_id() == tenant_id) {
        cur->get_all_events(tmp.get_event_stats());
        cur->get_all_add_stats(tmp.get_add_stat_stats());
        fn(cur->get_group_id(), tmp);
        tmp.get_event_stats().reset();
        tmp.get_add_stat_stats().reset();
      }
      iter.revert(cur);
    }
  }
  return ret;
}

int ObBaseDiagnosticInfoSummary::remove_if(
    std::function<bool(const ObDiagnosticKey &, ObDiagnosticInfoCollector *)> fn)
{
  return collectors_.remove_if(fn);
}

} /* namespace common */
} /* namespace oceanbase */
