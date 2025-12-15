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

#include "lib/stat/ob_diagnostic_info_container.h"
#include "lib/stat/ob_diagnostic_info_util.h"
#include "lib/ob_lib_config.h"
#include "lib/stat/ob_diagnostic_info_guard.h" // ObLocalDiagnosticInfo

namespace oceanbase
{
namespace common
{

int64_t calc_slot_num(int64_t cpu_count)
{
  constexpr int64_t DEFAULT_MAX_SLOT_NUM = 64;
  constexpr int64_t DEFAULT_MIN_SLOT_NUM = 4;
  // slot num is power of 2
  int64_t slot_num =
      min(DEFAULT_MAX_SLOT_NUM, max(DEFAULT_MIN_SLOT_NUM /*lower bound*/, cpu_count * 2));
  int msb_pos = std::log2(slot_num);
  if (std::pow(2, msb_pos) != slot_num) {
    slot_num = std::pow(2, msb_pos + 1);
  }
  return slot_num;
}

__attribute__((constructor)) void init_global_di_container()
{
  int ret = OB_SUCCESS;
  // make static variable ObFixedClassAllocator construct before get_global_di_container
  // so that it deconstruct after.
  common::ObFixedClassAllocator<common::LinkHashNode<ObDiagnosticKey>> *inst_key =
      common::ObFixedClassAllocator<common::LinkHashNode<ObDiagnosticKey>>::get(
          "LinkHashNode<ObDiagnosticKey>");
  common::ObFixedClassAllocator<common::LinkHashNode<SessionID>> *inst_id =
      common::ObFixedClassAllocator<common::LinkHashNode<SessionID>>::get(
          "LinkHashNode<SessionID>");
  if (OB_FAIL(ObDiagnosticInfoContainer::get_global_di_container()->init(get_cpu_num(), true))) {
    LOG_WARN("failed to init global di container", K(ret));
  } else {
    ObDiagnosticInfoContainer::get_global_di_container()->set_global_container();
    LOG_INFO("init global di container success");
  }
}

#define DI_DIFAULT_SLICE_COUNT 4
#define DI_DEFAULT_ALLOCATOR_NWAY 8

ObDiagnosticInfoContainer::ObDiagnosticInfoContainer(int64_t tenant_id, int64_t di_upper_limit)
    : is_inited_(false),
      stop_(false),
      is_global_container_(false),
      tenant_id_(tenant_id),
      di_allocator_(lib::ObMemAttr(tenant_id, "DiagnosticInfo"), DI_DEFAULT_ALLOCATOR_NWAY,
          DI_DIFAULT_SLICE_COUNT),
      di_collector_allocator_(lib::ObMemAttr(tenant_id, "DICollector"), DI_DEFAULT_ALLOCATOR_NWAY,
          DI_DIFAULT_SLICE_COUNT),
      wait_event_pool_(tenant_id, true, lib::is_mini_mode(), DI_DEFAULT_ALLOCATOR_NWAY),
      summarys_(DiagnosticInfoValueAlloc<ObDiagnosticInfoCollector, ObDiagnosticKey>(
          &di_collector_allocator_)),
      runnings_(tenant_id, DiagnosticInfoValueAlloc<ObDiagnosticInfo, SessionID>(&di_allocator_, di_upper_limit)),
      cache_()
{
  wait_event_pool_.init();
}

ObDiagnosticInfos::ObDiagnosticInfos(
    DiagnosticInfoValueAlloc<ObDiagnosticInfo, SessionID> &value_alloc)
    : di_infos_(value_alloc)
{}

int ObDiagnosticInfos::init(int64_t tenant_id)
{
  return di_infos_.init("DiagnosticInfos", tenant_id);
}

int ObDiagnosticInfos::allocate_diagnostic_info(int64_t tenant_id, int64_t group_id,
    int64_t session_id, ObWaitEventPool &pool, ObDiagnosticInfo *&di_info)
{
  int ret = OB_SUCCESS;
  SessionID sess_id(session_id);
  if (is_rpc_request(session_id)) {
    ATOMIC_INC(&rpc_size_);
  }
  if (OB_FAIL(di_infos_.create(sess_id, di_info))) {
    LOG_WARN("failed to create di info", K(ret), K(session_id), K(tenant_id), K(group_id));
  } else {
    di_info->init(tenant_id, group_id, session_id, pool);
  }
  return ret;
}

int ObDiagnosticInfos::delete_diagnostic_info(const ObDiagnosticInfo *di_info)
{
  int ret = OB_SUCCESS;
  if (is_rpc_request(di_info->get_session_id())) {
    ATOMIC_DEC(&rpc_size_);
  }
  if (OB_NOT_NULL(di_info)) {
    if (di_info->get_uref() - INT32_MAX / 2 < 0) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "di session leak", KPC(di_info), K(di_info));
    }
    if (OB_FAIL(di_infos_.del(SessionID(di_info->get_session_id())))) {
      LOG_WARN("failed to delete diagnostic info", KPC(di_info));
    }
  }
  return ret;
}

int ObDiagnosticInfos::inc_ref(const ObDiagnosticInfo *di_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di_info)) {
    ObDiagnosticInfo *tmp_di = nullptr;
    if (OB_FAIL(di_infos_.get(SessionID(di_info->get_session_id()), tmp_di))) {
      LOG_WARN("failed to inc di ref", K(ret));
    } else {
#ifdef ENABLE_DEBUG_LOG
      // using cache would change group id.
      if (!di_info->is_using_cache() && !(*di_info == *tmp_di)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("inc ref wrongly", K(di_info->get_session_id()), K(tmp_di->get_session_id()),
            KPC(di_info), KPC(tmp_di));
      } else {
        // do noting
      }
#endif
    }
  }
  return ret;
}

void ObDiagnosticInfos::dec_ref(ObDiagnosticInfo *di_info)
{
  if (di_info->get_uref() <= 0) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "di session leak", KPC(di_info), K(di_info));
  }
  di_infos_.revert(di_info);
}

int ObDiagnosticInfos::for_each(
    const std::function<bool(const SessionID &, ObDiagnosticInfo *)> &fn)
{
  return di_infos_.for_each(fn);
}

int ObDiagnosticInfos::get_session_diag_info(int64_t session_id, ObDISessionCollect &diag_info)
{
  int ret = OB_SUCCESS;
  ObDiagnosticInfo *tmp_di = nullptr;
  if (OB_FAIL(di_infos_.get(SessionID(session_id), tmp_di))) {
    LOG_WARN("failed to get session diag info", K(ret), KPC(this), K(session_id));
  } else {
    tmp_di->get_event_stats().accumulate_to(diag_info.base_value_.get_event_stats());
    diag_info.base_value_.get_add_stat_stats().add(tmp_di->get_add_stat_stats());
    diag_info.session_id_ = session_id;
    diag_info.client_sid_ = tmp_di->get_client_sid();
    diag_info.base_value_.set_tenant_id(tmp_di->get_tenant_id());
    diag_info.base_value_.set_curr_wait(tmp_di->get_curr_wait());
    di_infos_.revert(tmp_di);
  }
  return ret;
}

ObRunningDiagnosticInfoContainer::ObRunningDiagnosticInfoContainer(
    int64_t tenant_id, DiagnosticInfoValueAlloc<ObDiagnosticInfo, SessionID> value_alloc)
    : tenant_id_(tenant_id),
      slot_mask_(0),
      slot_count_(0),
      is_inited_(false),
      value_alloc_(value_alloc),
      buffer_(nullptr)
{}

ObRunningDiagnosticInfoContainer::~ObRunningDiagnosticInfoContainer()
{
  if (OB_NOT_NULL(buffer_)) {
    for (int i = 0; i < slot_count_; i++) {
      buffer_[i].~ObDiagnosticInfos();
    }
    ob_free(buffer_);
    buffer_ = nullptr;
  }
}

int ObRunningDiagnosticInfoContainer::init(int cpu_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    int64_t slot_num = calc_slot_num(cpu_cnt);
    slot_mask_ = slot_num - 1;
    slot_count_ = slot_num;
    int64_t size = sizeof(ObDiagnosticInfos) * slot_num;
    ObMemAttr attr(tenant_id_, "DI_CONTAINER");
    buffer_ = static_cast<ObDiagnosticInfos *>(ob_malloc(size, attr));
    if (nullptr == buffer_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate di collector memory", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < slot_num; i++) {
        ObDiagnosticInfos *tmp = new (&buffer_[i]) ObDiagnosticInfos(value_alloc_);
        if (OB_FAIL(tmp->init(tenant_id_))) {
          LOG_WARN("failed to init di infos", K(i), K_(tenant_id));
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
        LOG_INFO("Successfully init running diagnostic info container", K(slot_num));
      } else {
        ob_free(buffer_);
        buffer_ = nullptr;
      }
    }
  }
  return ret;
}

int ObRunningDiagnosticInfoContainer::allocate_diagnostic_info(int64_t tenant_id, int64_t group_id,
    int64_t session_id, ObWaitEventPool &pool, ObDiagnosticInfo *&di_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get running diagnostic info", K(ret), K(is_inited_));
  } else {
    lib::ObDisableDiagnoseGuard g;
    const int64_t target_slot = session_id & slot_mask_;
    OB_ASSERT(target_slot < slot_count_);
    if (OB_FAIL(buffer_[target_slot].allocate_diagnostic_info(
            tenant_id, group_id, session_id, pool, di_info))) {
      LOG_WARN("failed to allocate di info", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObRunningDiagnosticInfoContainer::delete_diagnostic_info(const ObDiagnosticInfo *di_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di_info)) {
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to get running diagnostic info", K(ret), K(is_inited_));
    } else {
      lib::ObDisableDiagnoseGuard g;
      const int64_t target_slot = di_info->get_session_id() & slot_mask_;
      OB_ASSERT(target_slot < slot_count_);
      if (OB_FAIL(buffer_[target_slot].delete_diagnostic_info(di_info))) {
        LOG_ERROR("failed to delete di info", K(ret), KPC(di_info));
      }
    }
  }
  return ret;
}

int ObRunningDiagnosticInfoContainer::inc_ref(const ObDiagnosticInfo *di_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di_info)) {
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to get running diagnostic info", K(ret), K(is_inited_));
    } else {
      const int64_t target_slot = di_info->get_session_id() & slot_mask_;
      OB_ASSERT(target_slot < slot_count_);
      if (OB_FAIL(buffer_[target_slot].inc_ref(di_info))) {
        LOG_WARN("failed to inc di info", K(ret), KPC(di_info));
      }
    }
  }
  return ret;
}

int ObRunningDiagnosticInfoContainer::dec_ref(ObDiagnosticInfo *di_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di_info)) {
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to get running diagnostic info", K(ret), K(is_inited_));
    } else {
      const int64_t target_slot = di_info->get_session_id() & slot_mask_;
      OB_ASSERT(target_slot < slot_count_);
      buffer_[target_slot].dec_ref(di_info);
    }
  }
  return ret;
}

int ObRunningDiagnosticInfoContainer::for_each(
    const std::function<bool(const SessionID &, ObDiagnosticInfo *)> &fn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get running diagnostic info", K(ret), K(is_inited_));
  } else {
    for (int i = 0; i < slot_count_; i++) {
      if (OB_FAIL(buffer_[i].for_each(fn))) {
        LOG_WARN("failed to iter over running di infos", K(ret));
      }
    }
  }
  return ret;
}

void ObRunningDiagnosticInfoContainer::reset()
{
  for (int i = 0; i < slot_count_; i++) {
    buffer_[i].reset();
  }
}

int ObRunningDiagnosticInfoContainer::get_session_diag_info(
    int64_t session_id, ObDISessionCollect &diag_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("get session diagnostic info before init!", K(ret));
  } else if (OB_ISNULL(buffer_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get running diagnostic info", K(ret), K(is_inited_));
  } else {
    const int64_t target_slot = session_id & slot_mask_;
    OB_ASSERT(target_slot < slot_count_);
    if (OB_FAIL(buffer_[target_slot].get_session_diag_info(session_id, diag_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get diagnostic info", K(ret));
      } else {
        LOG_DEBUG("session not exist in current tenant", K(session_id), KPC(this));
      }
    }
  }
  return ret;
}

int64_t ObRunningDiagnosticInfoContainer::size() const
{
  int64_t size = 0;
  if (!is_inited_) {
    LOG_WARN_RET(OB_SUCCESS, "get session diagnostic info size before init!", K(ret));
  } else {
    for (int i = 0; i < slot_count_; i++) {
      size += buffer_[i].size();
    }
  }
  return size;
}

int64_t ObRunningDiagnosticInfoContainer::get_rpc_size() const
{
  int64_t size = 0;
  if (!is_inited_) {
    LOG_WARN_RET(OB_SUCCESS, "get session diagnostic info size before init!", K(ret));
  } else {
    for (int i = 0; i < slot_count_; i++) {
      size += buffer_[i].get_rpc_size();
    }
  }
  return size;
}

int64_t ObRunningDiagnosticInfoContainer::get_value_alloc_count() const
{
  int64_t alloc_count = 0;
  if (!is_inited_) {
    LOG_WARN_RET(OB_SUCCESS, "get session diagnostic info alloc count before init!", K(ret));
  } else {
    for (int i = 0; i < slot_count_; i++) {
      alloc_count += buffer_[i].get_alloc_conut();
    }
  }
  return alloc_count;
}

int ObDiagnosticInfoContainer::init(int64_t cpu_cnt, bool is_global)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K_(tenant_id));
  } else {
    if (OB_FAIL(runnings_.init(cpu_cnt))) {
      LOG_WARN("failed to init running container", K(ret));
    } else if (OB_FAIL(summarys_.init(cpu_cnt))) {
      LOG_WARN("failed to init summary container", K(ret));
    } else if (!is_global) {
      if (OB_FAIL(cache_.init(cpu_cnt))) {
        LOG_WARN("failed to init di cache", K(ret));
      } else if (OB_FAIL(push_element_to_cache())) {
        LOG_WARN("failed to push element to cache", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDiagnosticInfoContainer::acquire_diagnostic_info(int64_t tenant_id, int64_t group_id,
    int64_t session_id, ObDiagnosticInfo *&di_info, bool using_cache)
{
  int ret = OB_SUCCESS;
  ObDiagnosticInfoSlot *slot = nullptr;
  if (stop_) {
    ret = OB_TENANT_NOT_IN_SERVER;
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      LOG_WARN("tenant is stopped", K(ret), K(tenant_id), K(group_id), K(session_id));
    }
  } else if (using_cache && true == lib_enable_diagnostic_info_cache()) {
    if (OB_FAIL(cache_.pop(di_info))) {
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO("no di cache to use", K(ret), K_(cache), K(tenant_id), K(group_id),
            K(session_id), K_(cache));
      }
    } else {
      // notice: if using cache, we shouldn't change its session id. because it is linkhashmap's
      // key.
      di_info->set_group_id(group_id);
      if (OB_FAIL(summarys_.get_di_slot(tenant_id, group_id, di_info->get_session_id(), slot))) {
        LOG_WARN("failed to acquire summary slot for cache di", K(ret), K(tenant_id), K(group_id),
            K(di_info->get_session_id()), K(di_info));
        ret = OB_SUCCESS;
      }
      di_info->set_summary_slot(slot);
    }
  }
  if (OB_NOT_NULL(di_info) || OB_TENANT_NOT_IN_SERVER == ret) {
    // OB_NOT_NULL(di_info) :create using cache success. if using cache failed. we ignore the error code.
    // OB_TENANT_NOT_IN_SERVER == ret : return error directly.
  } else {
    if (using_cache) {
      // if using_cache = true but cache didn't work, we generate new session id for it.
      OB_ASSERT(session_id == 0);
      session_id = ObBackgroundSessionIdGenerator::get_instance().get_next_rpc_session_id();
    }
    if (OB_FAIL(runnings_.allocate_diagnostic_info(tenant_id, group_id, session_id,
            ObDiagnosticInfoContainer::get_global_di_container()->get_wait_event_pool(),
            di_info))) {
      LOG_WARN("failed to allocate new diagnostic info", K(ret), K(tenant_id), K(group_id),
          K(session_id), K(di_info));
      EVENT_INC(ObStatEventIds::DIAGNOSTIC_INFO_ALLOC_FAIL_COUNT);
    } else {
      if (OB_FAIL(summarys_.get_di_slot(tenant_id, group_id, session_id, slot))) {
        LOG_WARN("failed to acquire summary slot", K(ret), K(tenant_id), K(group_id), K(session_id),
            K(di_info));
        ret = OB_SUCCESS;
      }
      di_info->set_summary_slot(slot);
      EVENT_INC(ObStatEventIds::DIAGNOSTIC_INFO_ALLOC_COUNT);
      if (is_global_container()) {
        di_info->set_acquired_from_global();
      }
    }
  }
  return ret;
}

int ObDiagnosticInfoContainer::aggregate_diagnostic_info_summary(ObDiagnosticInfo *di_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di_info)) {
    if (di_info->need_aggregate()) {
      if (OB_FAIL(summarys_.add_diagnostic_info(*di_info))) {
        LOG_WARN("failed to add summary info", KPC(di_info));
      } else {
        di_info->set_aggregated();
      }
    }
  }
  return ret;
}

int ObDiagnosticInfoContainer::return_di_to_cache(ObDiagnosticInfo *di_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cache_.push(di_info))) {
    di_info->unset_using_cache();
    // if push failed. we need to release current di info's ref from cache.
    ObLocalDiagnosticInfo::dec_ref(di_info);
    LOG_ERROR("failed to push di to cache", KPC(this), KPC(di_info));
  }
  return ret;
}

int ObDiagnosticInfoContainer::return_diagnostic_info(ObDiagnosticInfo *di_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di_info)) {
    if (di_info->is_using_cache()) {
      // return di to cache when di ref cnt reduce to 1 (hold by cache)
    } else {
      if (OB_FAIL(runnings_.delete_diagnostic_info(di_info))) {
        EVENT_INC(DIAGNOSTIC_INFO_RETURN_FAIL_COUNT);
        LOG_WARN("failed to return diagnostic info", KPC(di_info));
      } else {
        EVENT_INC(DIAGNOSTIC_INFO_RETURN_COUNT);
      }
    }
  }
  return ret;
}

void ObDiagnosticInfoContainer::dec_ref(ObDiagnosticInfo *di)
{
  const bool using_cache = di->is_using_cache();
  runnings_.dec_ref(di);
  if (OB_UNLIKELY(using_cache)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "cache di object invalid free", KPC(di));
  } else {
    return_diagnostic_info(di);
  }
}

int ObDiagnosticInfoContainer::mtl_new(ObDiagnosticInfoContainer *&container)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  container = nullptr;
  if (is_virtual_tenant_id(LIB_MTL_ID())) {
    // do nothing
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObDiagnosticInfoContainer),
                           ObMemAttr(LIB_MTL_ID(), "DI_CONTAINER")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc di container", K(ret), K(LIB_MTL_ID()));
  } else {
    container = new (buf) ObDiagnosticInfoContainer(LIB_MTL_ID());
  }
  return ret;
}

int ObDiagnosticInfoContainer::mtl_init(ObDiagnosticInfoContainer *&container)
{
  int ret = OB_SUCCESS;
  int64_t tenant_cpu_count = lib_mtl_cpu_count();
  if (OB_NOT_NULL(container)) {
    const uint64_t tenant_id = LIB_MTL_ID();
    if (OB_ISNULL(container)) {
      if (is_virtual_tenant_id(tenant_id)) {
        // do nothing
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    } else if (OB_FAIL(container->init(tenant_cpu_count))) {
      LOG_WARN("mtl init di container failed", K(tenant_id));
    } else {
      container->di_allocator_.set_nway(tenant_cpu_count);
      container->di_collector_allocator_.set_nway(tenant_cpu_count);
      LOG_INFO("mtl init di container success", K(tenant_id), KPC(container));
    }
  }
  return ret;
}

void ObDiagnosticInfoContainer::mtl_wait(ObDiagnosticInfoContainer *&container)
{
  if (container != NULL) {
    container->stop();
    container->clear_di_cache();
    while (container->get_running_size() != 0) {
      ob_usleep(1000 * 1000);
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        LOG_WARN_RET(OB_NEED_RETRY, "tenant di is not empty",
              K(container->get_running_size()), KPC(container));
        // in case there is something left in di cache.
        container->clear_di_cache();
        container->print_all_running_dis();
      }
    }
  }
  LOG_INFO("success to wait tenant diagnostic info container", KPC(container));
}

void ObDiagnosticInfoContainer::mtl_destroy(ObDiagnosticInfoContainer *&container)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(container)) {
    container->~ObDiagnosticInfoContainer();
    ob_free(container);
    container = nullptr;
    LOG_INFO("mtl destroy di container success", K(LIB_MTL_ID()));
  }
}

void ObDiagnosticInfoContainer::print_all_running_dis()
{
  int ret = OB_SUCCESS;
  int current_index = 0;
  std::function<bool(const SessionID &, ObDiagnosticInfo *)> fn =
      [&current_index, this](const SessionID &session_id, ObDiagnosticInfo *di) {
        LOG_INFO("print all running dis", K(current_index), KPC(this), KPC(di));
        current_index++;
        return true;
      };
  if (OB_FAIL(for_each_running_di(fn))) {
    LOG_WARN("failed to iter running diagnostic infos", K(ret), KPC(this));
  }
}

void ObDiagnosticInfoContainer::clear_di_cache()
{
  int ret = OB_SUCCESS;
  ObDiagnosticInfo *di = nullptr;
  if (cache_.is_inited()) {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(cache_.pop(di))) {
        if (ret != OB_ENTRY_NOT_EXIST) {
          COMMON_LOG(ERROR, "failed to clear di", K(ret), K_(cache));
        } else {
          COMMON_LOG(DEBUG, "di cache is empty", K_(cache));
        }
      } else {
        di->unset_using_cache();
        ObLocalDiagnosticInfo::dec_ref(di);
      }
    }
  }
}

int ObDiagnosticInfoContainer::push_element_to_cache()
{
  int ret = OB_SUCCESS;
  int cnt = 0;
  while (OB_SUCC(ret)) {
    if (cache_.get_free()) {
      const int64_t bg_sess_id = ObBackgroundSessionIdGenerator::get_instance().get_next_rpc_session_id();
      ObDiagnosticInfo *di = nullptr;
      if (OB_FAIL(acquire_diagnostic_info(tenant_id_, 0, bg_sess_id, di, false))) {
        LOG_WARN("failed to acquire di for cache", KPC(this), K(bg_sess_id));
      } else if (FALSE_IT(ObLocalDiagnosticInfo::inc_ref(di))) {
      } else if (FALSE_IT(di->set_using_cache())) {
      } else if (OB_FAIL(cache_.push(di))) {
        LOG_ERROR("failed to push di to cache", KPC(this), KPC(di));
        di->unset_using_cache();
        ObLocalDiagnosticInfo::dec_ref(di);
      } else {
        ++cnt;
      }
    } else {
      break;
    }
  }
  LOG_INFO("push di element to cache", K(cnt), KPC(this));
  return ret;
}

int ObDiagnosticInfoContainer::get_session_diag_info(
    int64_t session_id, ObDISessionCollect &diag_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("get session diagnostic info before init", K(ret));
  } else if (OB_FAIL(runnings_.get_session_diag_info(session_id, diag_info))) {
  }
  return ret;
}

void ObDiagnosticInfoContainer::purge_tenant_summary(int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID != tenant_id) {  // sys tenant will not be destroyed
    std::function<bool(const ObDiagnosticKey &, ObDiagnosticInfoCollector *)> fn =
        [tenant_id](const ObDiagnosticKey &key, ObDiagnosticInfoCollector *collector) -> bool {
      bool bret = false;
      if (key.get_tenant_id() == tenant_id) {
        LOG_INFO("target di collector need to be purged", K(tenant_id), K(key), KPC(collector));
        bret = true;
      }
      return bret;
    };
    if (OB_FAIL(summarys_.remove_if(fn))) {
      LOG_WARN("failed to remove summary collects", K(ret), K(tenant_id));
    } else {
      LOG_INFO("success to remove summary collects", K(tenant_id), KPC(this));
    }
  }
}

bool ObDiagnosticInfoContainer::check_element_all_freed() const
{
  bool bret = true;
  const int64_t summary_left =
      summarys_.get_value_alloc_count();  // no need to check summary. it would be removed in
                                          // mtl_destroy
  const int64_t running_left = runnings_.get_value_alloc_count();
  if (running_left) {
    bret = false;
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      LOG_INFO("there are some di element left", K(summary_left), K(running_left), KPC(this));
    }
  }
  return bret;
}

int ObDiagnosticInfoContainer::for_each_and_delay_release_ref(
    std::function<bool(const SessionID &, ObDiagnosticInfo *)> &fn)
{
  int ret = OB_SUCCESS;
  ObArray<ObDiagnosticInfo *> di_array;
  ObRunningDiagnosticInfoContainer &runnings = runnings_;
  std::function<bool(const SessionID &, ObDiagnosticInfo *)> fn_wrapper =
      [&di_array, &runnings, &fn](const SessionID &id, ObDiagnosticInfo *di) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(runnings.inc_ref(di))) {
          LOG_WARN("failed to inc ref di", K(ret));
          //ignore error to continue iteration
          ret = OB_SUCCESS;
        } else if (OB_FAIL(di_array.push_back(di))) {
          LOG_WARN("faield to push back id to di_array", K(ret));
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(runnings.dec_ref(di))) {
            LOG_ERROR("failed to dec ref", K(tmp_ret));
          }

          if (tmp_ret != OB_SUCCESS) {
            ret = tmp_ret;
          }
        } else if (OB_FAIL(fn(id, di))) {
          LOG_WARN("faield to exec fn", K(ret));
        }
        return ret == OB_SUCCESS;
      };

  if (OB_FAIL(for_each_running_di(fn_wrapper))) {
    LOG_WARN("failed to for each running di", K(ret));
  }

  for (int i = 0; i < di_array.count(); i++) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(runnings.dec_ref(di_array[i]))) {
      LOG_ERROR("failed to dec ref", K(tmp_ret));
    }

    if (tmp_ret != OB_SUCCESS) {
      ret = tmp_ret;
    }
  }
  return ret;
}

} /* namespace common */
} /* namespace oceanbase */
