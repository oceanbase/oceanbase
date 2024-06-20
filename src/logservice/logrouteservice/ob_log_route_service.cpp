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

#define USING_LOG_PREFIX OBLOG
#include "ob_log_route_service.h"
#include "lib/thread/thread_mgr.h"  // MTL
#include "share/ob_thread_pool.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_thread_mgr.h"    // TG*
#include "ob_ls_log_stat_info.h"    // LogStatRecordArray
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace logservice
{
ObLogRouteService::ObLogRouteService() :
    is_inited_(false),
    cluster_id_(OB_INVALID_CLUSTER_ID),
    is_tenant_mode_(false),
    source_tenant_id_(OB_INVALID_TENANT_ID),
    self_tenant_id_(OB_INVALID_TENANT_ID),
    is_stopped_(true),
    ls_route_key_set_(),
    ls_router_map_(),
    log_router_allocator_(),
    asyn_task_allocator_(),
    svr_blacklist_(),
    systable_queryer_(),
    all_svr_cache_(),
    ls_route_timer_task_(*this),
    timer_(),
    err_handler_(NULL),
    timer_id_(-1),
    tg_id_(-1),
    background_refresh_time_sec_(0),
    blacklist_survival_time_sec_(0),
    blacklist_survival_time_upper_limit_min_(0),
    blacklist_survival_time_penalty_period_min_(0),
    blacklist_history_overdue_time_min_(0),
    blacklist_history_clear_interval_min_(0)
{
}

ObLogRouteService::~ObLogRouteService()
{
  destroy();
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(LOG_ROUTE_TIMER_INIT_FAIL);
ERRSIM_POINT_DEF(LOG_ROUTE_HANDLER_INIT_FAIL);
ERRSIM_POINT_DEF(LOG_ROUTE_HANDLER_START_FAIL);
#endif
int ObLogRouteService::init(ObISQLClient *proxy,
    const common::ObRegion &prefer_region,
    const int64_t cluster_id,
    const bool is_across_cluster,
    logfetcher::IObLogErrHandler *err_handler,
    const char *external_server_blacklist,
    const int64_t background_refresh_time_sec,
    const int64_t all_server_cache_update_interval_sec,
    const int64_t all_zone_cache_update_interval_sec,
    const int64_t blacklist_survival_time_sec,
    const int64_t blacklist_survival_time_upper_limit_min,
    const int64_t blacklist_survival_time_penalty_period_min,
    const int64_t blacklist_history_overdue_time_min,
    const int64_t blacklist_history_clear_interval_min,
    const bool is_tenant_mode,
    const uint64_t source_tenant_id,
    const uint64_t self_tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t size = sizeof(ObLSRouterValue);
  lib::ObMemAttr log_router_mem_attr(self_tenant_id, "LogRouter");
  lib::ObMemAttr asyn_task_mem_attr(self_tenant_id, "RouterAsynTask");
  timer_.set_run_wrapper(MTL_CTX());

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLogRouteService has been inited twice", KR(ret), K(cluster_id), K(is_across_cluster));
  } else if (OB_ISNULL(proxy) || OB_UNLIKELY(OB_INVALID_CLUSTER_ID == cluster_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(proxy), K(cluster_id));
  } else if (OB_FAIL(ls_route_key_set_.create(DEFAULT_LS_ROUTE_KEY_SET_SIZE))) {
    LOG_ERROR("ls_route_key_set_ init failed", KR(ret));
  } else if (OB_FAIL(ls_router_map_.init("LSRouterMap"))) {
    LOG_WARN("LSRouterMap init failed", KR(ret));
  } else if (OB_FAIL(log_router_allocator_.init(LS_ROUTER_VALUE_SIZE,
          OB_MALLOC_NORMAL_BLOCK_SIZE, common::default_blk_alloc, log_router_mem_attr))) {
    LOG_WARN("log_router_allocator_ init failed", KR(ret));
  } else if (OB_FAIL(asyn_task_allocator_.init(ASYN_TASK_VALUE_SIZE,
          OB_MALLOC_NORMAL_BLOCK_SIZE, common::default_blk_alloc, asyn_task_mem_attr))) {
    LOG_WARN("asyn_task_allocator_ init failed", KR(ret));
  } else if (OB_FAIL(svr_blacklist_.init(external_server_blacklist, false/*is_sql_server*/))) {
    LOG_WARN("ObLogSvrBlacklist init failed", KR(ret), K(cluster_id), K(is_across_cluster),
        K(external_server_blacklist));
  } else if (OB_FAIL(systable_queryer_.init(cluster_id, is_across_cluster, *proxy, err_handler))) {
    LOG_WARN("systable_queryer_ init failed", KR(ret), K(cluster_id), K(is_across_cluster));
  } else if (OB_FAIL(all_svr_cache_.init(systable_queryer_, is_tenant_mode, source_tenant_id, prefer_region,
          all_server_cache_update_interval_sec, all_zone_cache_update_interval_sec))) {
    LOG_WARN("all_svr_cache_ init failed", KR(ret), K(is_tenant_mode), K(prefer_region),
        K(all_server_cache_update_interval_sec), K(all_zone_cache_update_interval_sec));
#ifdef ERRSIM
  } else if (OB_FAIL(LOG_ROUTE_TIMER_INIT_FAIL)) {
    LOG_ERROR("ERRSIM: LOG_ROUTE_TIMER_INIT_FAIL");
#endif
  } else if (OB_FAIL(timer_.init("LogRouter"))) {
    LOG_ERROR("fail to init itable gc timer", K(ret));
#ifdef ERRSIM
  } else if (OB_FAIL(LOG_ROUTE_HANDLER_INIT_FAIL)) {
    LOG_ERROR("ERRSIM: LOG_ROUTE_HANDLER_INIT_FAIL");
#endif
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::LogRouteService, tg_id_))) {
    LOG_ERROR("TG_CREATE failed", KR(ret));
#ifdef ERRSIM
  } else if (OB_FAIL(LOG_ROUTE_HANDLER_START_FAIL)) {
    LOG_ERROR("ERRSIM: LOG_ROUTE_HANDLER_START_FAIL");
#endif
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("TG_SET_HANDLER_AND_START failed", KR(ret), K(tg_id_));
  } else {
    cluster_id_ = cluster_id;
    source_tenant_id_ = source_tenant_id;
    self_tenant_id_ = self_tenant_id;
    log_router_allocator_.set_nway(NWAY);
    asyn_task_allocator_.set_nway(NWAY);
    timer_id_ = lib::TGDefIDs::LogRouterTimer;
    background_refresh_time_sec_ = background_refresh_time_sec * _SEC_;
    blacklist_survival_time_sec_ = blacklist_survival_time_sec * _SEC_;
    blacklist_survival_time_upper_limit_min_ = blacklist_survival_time_upper_limit_min * _MIN_;
    blacklist_survival_time_penalty_period_min_ = blacklist_survival_time_penalty_period_min * _MIN_;
    blacklist_history_overdue_time_min_ = blacklist_history_overdue_time_min * _MIN_;
    blacklist_history_clear_interval_min_ = blacklist_history_clear_interval_min * _MIN_;

    is_stopped_ = true;
    is_tenant_mode_ = is_tenant_mode;
    is_inited_ = true;

    if (OB_TMP_FAIL(update_all_server_and_zone_cache_())) {
      LOG_WARN("update_all_server_and_zone_cache_ failed, will retry", K(tmp_ret));
    }

    LOG_INFO("ObLogRouteService init succ", K(cluster_id), K(is_tenant_mode), K(source_tenant_id_),
        K(prefer_region), K(is_across_cluster),
        K(timer_id_), K(tg_id_));
  }

  return ret;
}

int ObLogRouteService::start()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ls_route_timer_task_.init(lib::TGDefIDs::LogRouterTimer))) {
    LOG_WARN("ObLSRouteTimerTask init failed", KR(ret));
  } else if (OB_FAIL(timer_.schedule_repeate_task_immediately(ls_route_timer_task_, ObLSRouteTimerTask::REFRESH_INTERVAL))) {
    LOG_WARN("fail to schedule min minor sstable gc task", K(ret));
  } else {
    is_stopped_ = false;
    LOG_INFO("ObLogRouteService start succ", K(timer_id_), K(tg_id_));
  }

  return ret;
}

void ObLogRouteService::stop()
{
  LOG_INFO("ObLogRouteService stop begin");
  is_stopped_ = true;
  timer_.stop();
  LOG_INFO("ObLogRouteService stop finish");
}

void ObLogRouteService::wait()
{
  LOG_INFO("ObLogRouteService wait begin");

  int ret = OB_SUCCESS;
  int64_t num = 0;
  if (-1 != tg_id_) {
    while (OB_SUCC(TG_GET_QUEUE_NUM(tg_id_, num)) && num > 0) {
      PAUSE();
    }
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "ObLogRouteService failed to get queue number");
    }
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
  }
  timer_.wait();

  LOG_INFO("ObLogRouteService wait finish");
}

void ObLogRouteService::destroy()
{
  LOG_INFO("ObLogRouteService destroy begin");
  timer_.destroy();
  ls_route_timer_task_.destroy();
  timer_id_ = -1;
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  free_mem_();
  ls_route_key_set_.destroy();
  ls_router_map_.destroy();
  systable_queryer_.destroy();
  all_svr_cache_.destroy();
  svr_blacklist_.destroy();

  log_router_allocator_.destroy();
  asyn_task_allocator_.destroy();

  err_handler_ = NULL;

  cluster_id_ = OB_INVALID_CLUSTER_ID;
  self_tenant_id_ = OB_INVALID_TENANT_ID;
  source_tenant_id_ = OB_INVALID_TENANT_ID;
  background_refresh_time_sec_ = 0;
  blacklist_survival_time_sec_ = 0;
  blacklist_survival_time_upper_limit_min_ = 0;
  blacklist_survival_time_penalty_period_min_ = 0;
  blacklist_history_overdue_time_min_ = 0;
  blacklist_history_clear_interval_min_ = 0;

  is_tenant_mode_ = false;
  is_inited_ = false;
  LOG_INFO("ObLogRouteService destroy finish");
}

void ObLogRouteService::free_mem_()
{
  int ret = OB_SUCCESS;

  ObArray<ObLSRouterValue *> router_values;
  ObLSRouterValueGetter ls_rvalue_getter(*this, router_values);

  if (OB_FAIL(ls_router_map_.for_each(ls_rvalue_getter))) {
    LOG_WARN("ls_router_map_ free_mem for_each failed", KR(ret));
  } else {
    ARRAY_FOREACH_N(router_values, idx, count) {
      ObLSRouterValue *router_value = router_values.at(idx);
      log_router_allocator_.free(router_value);
      router_value = nullptr;
    }
  }

}

void ObLogRouteService::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObLSRouterAsynTask *asyn_task = static_cast<ObLSRouterAsynTask *>(task);

  if (is_stopped_) {
    // ignore handle
    LOG_DEBUG("ignore handle asyn_task while log_route_service is in stop state", KPC(asyn_task));
  } else if (OB_ISNULL(asyn_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("asyn_task is nullptr", KR(ret), KPC(asyn_task));
  } else {
    LOG_DEBUG("ObLogRouteService handle", KPC(asyn_task));
    ObLSRouterKey &router_key = asyn_task->router_key_;
    ObLSRouterValue *router_value = nullptr;

    if (OB_FAIL(ls_router_map_.get(router_key, router_value))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("LSRouterMap get failed", KR(ret), KPC(asyn_task));
      } else {
        if (OB_FAIL(handle_when_ls_route_info_not_exist_(router_key, router_value))) {
          if (OB_ENTRY_EXIST != ret) {
            LOG_WARN("handle_when_ls_route_info_not_exist_ failed", KR(ret), KPC(asyn_task));
          }
        }
      }
    } else {
      // If exist, update
      if (OB_FAIL(update_server_list_(router_key, *router_value))) {
        LOG_WARN("update_server_list_ failed", KR(ret), K(router_key));
      } else {}
    }
  }

  if (nullptr != asyn_task) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ls_route_key_set_.erase_refactored(asyn_task->router_key_))) {
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        ret = OB_SUCCESS;
      } else {
        // won't overwrite ret to tmp_ret
        LOG_WARN("remove router_key from ls_route_key_set_ failed", KR(tmp_ret),
            KPC(asyn_task),
            "current_route_key handle result", ret);
      }
    }

    asyn_task_allocator_.free(asyn_task);
    asyn_task = nullptr;
  }
}

int ObLogRouteService::update_background_refresh_time(const int64_t background_refresh_time_sec)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    const int64_t background_refresh_time = background_refresh_time_sec * _SEC_;
    ATOMIC_SET(&background_refresh_time_sec_, background_refresh_time);
  }

  return ret;
}

int ObLogRouteService::get_background_refresh_time(int64_t &background_refresh_time_sec)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    background_refresh_time_sec = ATOMIC_LOAD(&background_refresh_time_sec_) / _SEC_;
  }

  return ret;
}

int ObLogRouteService::update_preferred_upstream_log_region(const common::ObRegion &prefer_region)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else if (OB_FAIL(all_svr_cache_.update_assign_region(prefer_region))) {
    LOG_WARN("ObLogAllSvrCache update_assign_region failed", KR(ret), K(prefer_region));
  }

  return ret;
}

int ObLogRouteService::get_preferred_upstream_log_region(common::ObRegion &prefer_region)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else if (OB_FAIL(all_svr_cache_.get_assign_region(prefer_region))) {
    LOG_WARN("ObLogAllSvrCache get_assign_region failed", KR(ret), K(prefer_region));
  }

  return ret;
}

int ObLogRouteService::update_cache_update_interval(const int64_t all_server_cache_update_interval_sec,
    const int64_t all_zone_cache_update_interval_sec)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else if (OB_FAIL(all_svr_cache_.update_cache_update_interval(all_server_cache_update_interval_sec,
          all_zone_cache_update_interval_sec))) {
    LOG_WARN("ObLogAllSvrCache update_cache_update_intervala failed", KR(ret),
        K(all_server_cache_update_interval_sec), K(all_zone_cache_update_interval_sec));
  }

  return ret;
}

int ObLogRouteService::get_cache_update_interval(int64_t &all_server_cache_update_interval_sec,
    int64_t &all_zone_cache_update_interval_sec)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else if (OB_FAIL(all_svr_cache_.get_cache_update_interval(all_server_cache_update_interval_sec,
          all_zone_cache_update_interval_sec))) {
    LOG_WARN("ObLogAllSvrCache get_cache_update_intervala failed", KR(ret),
        K(all_server_cache_update_interval_sec), K(all_zone_cache_update_interval_sec));
  }

  return ret;
}

int ObLogRouteService::update_blacklist_parameter(
    const int64_t blacklist_survival_time_sec,
    const int64_t blacklist_survival_time_upper_limit_min,
    const int64_t blacklist_survival_time_penalty_period_min,
    const int64_t blacklist_history_overdue_time_min,
    const int64_t blacklist_history_clear_interval_min)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    const int64_t blacklist_survival_time = blacklist_survival_time_sec * _SEC_;
    const int64_t blacklist_survival_time_upper_limit = blacklist_survival_time_upper_limit_min * _MIN_;
    const int64_t blacklist_survival_time_penalty_period = blacklist_survival_time_penalty_period_min * _MIN_;
    const int64_t blacklist_history_overdue_time = blacklist_history_overdue_time_min * _MIN_;
    const int64_t blacklist_history_clear_interval = blacklist_history_clear_interval_min * _MIN_;

    ATOMIC_SET(&blacklist_survival_time_sec_, blacklist_survival_time);
    ATOMIC_SET(&blacklist_survival_time_upper_limit_min_, blacklist_survival_time_upper_limit);
    ATOMIC_SET(&blacklist_survival_time_penalty_period_min_, blacklist_survival_time_penalty_period);
    ATOMIC_SET(&blacklist_history_overdue_time_min_, blacklist_history_overdue_time);
    ATOMIC_SET(&blacklist_history_clear_interval_min_, blacklist_history_clear_interval);
  }

  return ret;
}

int ObLogRouteService::get_blacklist_parameter(
    int64_t &blacklist_survival_time_sec,
    int64_t &blacklist_survival_time_upper_limit_min,
    int64_t &blacklist_survival_time_penalty_period_min,
    int64_t &blacklist_history_overdue_time_min,
    int64_t &blacklist_history_clear_interval_min)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    blacklist_survival_time_sec = ATOMIC_LOAD(&blacklist_survival_time_sec_) / _SEC_;
    blacklist_survival_time_upper_limit_min = ATOMIC_LOAD(&blacklist_survival_time_upper_limit_min_) / _MIN_;
    blacklist_survival_time_penalty_period_min = ATOMIC_LOAD(&blacklist_survival_time_penalty_period_min_) / _MIN_;
    blacklist_history_overdue_time_min = ATOMIC_LOAD(&blacklist_history_overdue_time_min_) / _MIN_;
    blacklist_history_clear_interval_min = ATOMIC_LOAD(&blacklist_history_clear_interval_min_) / _MIN_;
  }

  return ret;
}

int ObLogRouteService::registered(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObLSRouterKey router_key(cluster_id_, tenant_id, ls_id);
    ObLSRouterAsynTask *asyn_task = nullptr;
    int hashset_flag = 0; // not overwrite if key exist

    if (OB_ISNULL(asyn_task = static_cast<ObLSRouterAsynTask *>(asyn_task_allocator_.alloc()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ObLSRouterAsynTask is nullptr", KR(ret));
    } else if (OB_FAIL(ls_route_key_set_.set_refactored(router_key, hashset_flag))) {
      if (OB_HASH_EXIST == ret) {
        LOG_DEBUG("asnc_task from same tenant_ls_id already waiting to handle", KR(ret), K(router_key));
      } else {
        LOG_WARN("check router_key exist in ls_route_key_set_ failed", KR(ret), K(router_key));
      }
    } else {
      new(asyn_task) ObLSRouterAsynTask();

      asyn_task->router_key_ = router_key;

      if (OB_FAIL(TG_PUSH_TASK(tg_id_, asyn_task))) {
        LOG_WARN("TG_PUSH_TASK failed", KR(ret), K(router_key));
      }
    }

    if (OB_FAIL(ret) && nullptr != asyn_task) {
      asyn_task_allocator_.free(asyn_task);
      asyn_task = nullptr;
    }
  }

  return ret;
}

int ObLogRouteService::remove(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObLSRouterKey router_key(cluster_id_, tenant_id, ls_id);

    if (OB_FAIL(ls_router_map_.erase(router_key))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("ls_router_map_ erase failed", KR(ret), K(router_key));
      } else {
        // If not exist, reset OB_SUCCESS
        ret = OB_SUCCESS;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls_route_key_set_.erase_refactored(router_key))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("ls_route_key_set_ erase_refactored failed", KR(ret), K(router_key));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObLogRouteService::get_all_ls(
    const uint64_t tenant_id,
    ObIArray<share::ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObLSRouterKeyGetter ls_rkey_getter(*this, cluster_id_, tenant_id, ls_ids);

    if (OB_FAIL(ls_router_map_.for_each(ls_rkey_getter))) {
      LOG_WARN("ls_router_map_ get_all_ls for_each failed", KR(ret));
    } else {}
  }

  return ret;
}

int ObLogRouteService::next_server(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const palf::LSN &next_lsn,
    common::ObAddr &svr)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObLSRouterKey router_key(cluster_id_, tenant_id, ls_id);
    ObLSRouterValue *router_value = nullptr;

    if (OB_FAIL(get_ls_router_value_(router_key, router_value))) {
      LOG_WARN("get_ls_router_value_ failed", KR(ret), K(router_key));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(router_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("router_value is nullptr", KR(ret), K(router_key));
      } else if (OB_FAIL(router_value->next_server(router_key, next_lsn, svr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("ObLSRouterValue next_server failed", KR(ret), K(router_key), K(next_lsn), K(svr));
        }
      } else {
        LOG_INFO("LSSvrList next_server succ", KR(ret), K(router_key), K(next_lsn), K(svr));
      }
    }
  }

  return ret;
}

int ObLogRouteService::get_server_array_for_locate_start_lsn(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObIArray<common::ObAddr> &svr_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObLSRouterKey router_key(cluster_id_, tenant_id, ls_id);
    ObLSRouterValue *router_value = nullptr;

    if (OB_FAIL(get_ls_router_value_(router_key, router_value))) {
      LOG_WARN("get_ls_router_value_ failed", KR(ret), K(router_key));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(router_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("router_value is nullptr", KR(ret), K(router_key));
      } else if (OB_FAIL(router_value->get_server_array_for_locate_start_lsn(svr_array))) {
        LOG_WARN("ObLSRouterValue next_server failed", KR(ret), K(router_key));
      } else {
        LOG_INFO("LSSvrList get_server_array_for_locate_start_lsn succ", KR(ret), K(router_key));
      }
    }
  }

  return ret;
}

int ObLogRouteService::get_leader(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    common::ObAddr &leader)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObLSRouterKey router_key(cluster_id_, tenant_id, ls_id);
    ObLSRouterValue *router_value = nullptr;

    if (OB_FAIL(get_ls_router_value_(router_key, router_value))) {
      LOG_WARN("get_ls_router_value_ failed", KR(ret), K(router_key));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(router_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("router_value is nullptr", KR(ret), K(router_key));
      } else if (OB_FAIL(router_value->get_leader(router_key, leader))) {
        if (OB_NOT_MASTER != ret) {
          LOG_WARN("ObLSRouterValue get_leader failed", KR(ret), K(router_key));
        }
      } else {}
    }
  }

  return ret;
}

bool ObLogRouteService::need_switch_server(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const palf::LSN &next_lsn,
    const common::ObAddr &cur_svr)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObLSRouterKey router_key(cluster_id_, tenant_id, ls_id);
    ObLSRouterValue *router_value = nullptr;

    if (OB_FAIL(ls_router_map_.get(router_key, router_value))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("LSRouterMap get failed", KR(ret), K(router_key));
      } else {
        bool_ret = false;
      }
    } else if (OB_ISNULL(router_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("router_value is nullptr", KR(ret));
    } else {
      bool_ret = router_value->need_switch_server(router_key, next_lsn, cur_svr);
    }
  }

  return bool_ret;
}

int ObLogRouteService::get_server_count(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    int64_t &avail_svr_count) const
{
  int ret = OB_SUCCESS;
  avail_svr_count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObLSRouterKey router_key(cluster_id_, tenant_id, ls_id);
    ObLSRouterValue *router_value = nullptr;

    if (OB_FAIL(ls_router_map_.get(router_key, router_value))) {
      LOG_WARN("LSRouterMap get failed", KR(ret), K(router_key));
    } else if (OB_ISNULL(router_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("router_value is nullptr", KR(ret), K(router_key));
    } else {
      avail_svr_count =  router_value->get_server_count();
    }
  }

  return ret;
}

int ObLogRouteService::add_into_blacklist(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObAddr &svr,
    const int64_t svr_service_time,
    int64_t &survival_time)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObLSRouterKey router_key(cluster_id_, tenant_id, ls_id);
    ObLSRouterValue *router_value = nullptr;

    if (OB_FAIL(ls_router_map_.get(router_key, router_value))) {
      LOG_WARN("LSRouterMap get failed", KR(ret), K(router_key));
    } else if (OB_ISNULL(router_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("router_value is nullptr", KR(ret));
    } else if (OB_FAIL(router_value->add_into_blacklist(router_key, svr, svr_service_time,
          ATOMIC_LOAD(&blacklist_survival_time_sec_),
          ATOMIC_LOAD(&blacklist_survival_time_upper_limit_min_),
          ATOMIC_LOAD(&blacklist_survival_time_penalty_period_min_),
          ATOMIC_LOAD(&blacklist_history_overdue_time_min_),
          ATOMIC_LOAD(&blacklist_history_clear_interval_min_),
          survival_time))) {
      LOG_WARN("router_value add_into_blacklist failed", KR(ret), K(router_key), K(svr),
          K(svr_service_time), K(survival_time));
    } else {}
  }

  return ret;
}

int ObLogRouteService::set_external_svr_blacklist(const char *server_blacklist)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else if (OB_ISNULL(server_blacklist)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(server_blacklist));
  } else {
    svr_blacklist_.refresh(server_blacklist);
  }

  return ret;
}

int ObLogRouteService::async_server_query_req(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else if (OB_FAIL(registered(tenant_id, ls_id))) {
    if (OB_EAGAIN == ret) {
      LOG_WARN("handler thread_pool may alraedy full, please retry later or adjust thread_num of log_route_service",
          KR(ret), K_(cluster_id), K(tenant_id), K(ls_id));
    } else if (OB_HASH_EXIST == ret) {
      LOG_DEBUG("async_server_query_req from same tenant_id and ls_id still waiting to handle, \
          won't registe before last task is handled", KR(ret), K_(cluster_id), K(tenant_id), K(ls_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("registered task failed", KR(ret), K(cluster_id_), K(tenant_id), K(ls_id));
    }
  } else {}

  return ret;
}

int ObLogRouteService::get_ls_svr_list_(const ObLSRouterKey &router_key,
    LSSvrList &svr_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(query_ls_log_info_and_update_(router_key, svr_list))) {
    LOG_WARN("failed to query_ls_log_info_and_update_", K(router_key), K(svr_list));
  } else if (0 == svr_list.count()) {
    // 1. Log Stream quickly GC in the transfer scenario, so the Log Stream can not get server list from GV$OB_LOG_STAT
    // 2. We employ the complementary mechanism of querying the server list from GV$OB_UNITS
    if (OB_FAIL(query_units_info_and_update_(router_key, svr_list))) {
      LOG_WARN("failed to query_units_info_and_update", K(router_key), K(svr_list));
    }
  }

  if (OB_SUCC(ret)) {
    // Sort by Fetch log priority when add_server_or_update completed
    svr_list.sort_by_priority();
  }

  if (OB_SUCC(ret)) {
    const int64_t svr_count_before_filter = svr_list.count();
    ObArray<ObAddr> remove_svrs;

    if (OB_FAIL(svr_list.filter_by_svr_blacklist(svr_blacklist_, remove_svrs))) {
      LOG_ERROR("ls_svr_list filter_by_svr_blacklist fail", KR(ret), K(remove_svrs));
    } else {
      const int64_t svr_count_after_filter = svr_list.count();

      // print if has svr filtered
      if (svr_count_before_filter > svr_count_after_filter) {
        _LOG_INFO("[SERVER_BLACKLIST] [FILTER] [KEY=%s] [FILTER_SVR_CNT=%ld(%ld/%ld)] [REMOVE_SVR=%s]",
            to_cstring(router_key), svr_count_before_filter - svr_count_after_filter,
            svr_count_before_filter, svr_count_after_filter, to_cstring(remove_svrs));
      }
    }
  }

  LOG_INFO("get_ls_svr_list_ done", KR(ret), K(router_key), K(svr_list), K(tg_id_));

  return ret;
}

int ObLogRouteService::query_ls_log_info_and_update_(const ObLSRouterKey &router_key,
    LSSvrList &svr_list)
{
  int ret = OB_SUCCESS;

  ObLSLogInfo ls_log_info;
  if (OB_FAIL(systable_queryer_.get_ls_log_info(router_key.get_tenant_id(),
      router_key.get_ls_id(), ls_log_info))) {
    LOG_WARN("failed to get_ls_log_info", K(router_key));
  } else {
    svr_list.reset();
    const ObLSLogInfo::LogStatRecordArray &log_stat_records = ls_log_info.get_log_stat_array();
    ARRAY_FOREACH_NORET(log_stat_records, idx) {
      int tmp_ret = OB_SUCCESS;
      const LogStatRecord &rec = log_stat_records.at(idx);
      const ObAddr& svr = rec.server_;
      RegionPriority region_priority = RegionPriority::REGION_PRIORITY_UNKNOWN;
      if (! all_svr_cache_.is_svr_avail(svr, region_priority)) {
        // ignore
      } else if (OB_TMP_FAIL(svr_list.add_server_or_update(rec.server_, rec.begin_lsn_, rec.end_lsn_,
          region_priority, ObRole::LEADER == rec.role_))) {
        LOG_WARN_RET(tmp_ret, "failed to add_server_or_update after get_ls_log_info", K(router_key), K(rec));
      }
    }
  }

  return ret;
}

int ObLogRouteService::query_units_info_and_update_(const ObLSRouterKey &router_key,
    LSSvrList &svr_list)
{
  int ret = OB_SUCCESS;

  ObUnitsRecordInfo units_record_info;
  if (OB_FAIL(systable_queryer_.get_all_units_info(router_key.get_tenant_id(), units_record_info))) {
    LOG_WARN("failed to get_all_units_info", K(router_key));
  } else {
    const ObUnitsRecordInfo::ObUnitsRecordArray &units_record_array = units_record_info.get_units_record_array();
    ARRAY_FOREACH_NORET(units_record_array, idx) {
      int tmp_ret = OB_SUCCESS;
      const ObUnitsRecord &record = units_record_array.at(idx);
      if (OB_TMP_FAIL(svr_list.add_server_or_update(record.server_, palf::LSN(palf::PALF_INITIAL_LSN_VAL),
          palf::LSN(palf::LOG_MAX_LSN_VAL), RegionPriority::REGION_PRIORITY_UNKNOWN, false))) {
        LOG_WARN_RET(tmp_ret, "failed to add_server_or_update after get_all_units_info", K(record), K(router_key));
      }
    }
  }

  return ret;
}

int ObLogRouteService::get_ls_router_value_(
    const ObLSRouterKey &router_key,
    ObLSRouterValue *&router_value)
{
  int ret = OB_SUCCESS;
  router_value = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else if (OB_FAIL(ls_router_map_.get(router_key, router_value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("LSRouterMap get failed", KR(ret), K(router_key));
    } else {
      if (OB_FAIL(handle_when_ls_route_info_not_exist_(router_key, router_value))) {
        if (OB_ENTRY_EXIST != ret) {
          LOG_WARN("handle_when_ls_route_info_not_exist_ failed", KR(ret), K(router_key));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(ls_router_map_.get(router_key, router_value))) {
            LOG_WARN("LSRouterMap get failed", KR(ret), K(router_key));
          }
        }
      } // handle_when_ls_route_info_not_exist_
    }
  }

  return ret;
}

int ObLogRouteService::handle_when_ls_route_info_not_exist_(
    const ObLSRouterKey &router_key,
    ObLSRouterValue *&router_value)
{
  int ret = OB_SUCCESS;
  router_value = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else if (OB_ISNULL(router_value = static_cast<ObLSRouterValue *>(log_router_allocator_.alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("ObLSRouterValue is nullptr", KR(ret));
  } else {
    new(router_value) ObLSRouterValue();

    if (OB_FAIL(update_server_list_(router_key, *router_value))) {
      LOG_WARN("update_server_list_ failed", KR(ret), K(router_key));
      // SQL execution may fail, reset ret is OB_SUCCESS to ensure that the key is inserted into the map
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ls_router_map_.insert(router_key, router_value))) {
        if (OB_ENTRY_EXIST != ret) {
          LOG_WARN("LSRouterMap insert failed", KR(ret), K(router_key));
        }
      }
    }

    if (OB_FAIL(ret)) {
      log_router_allocator_.free(router_value);
      router_value = nullptr;
    }
  }

  return ret;
}

bool ObLogRouteService::ObLSRouterKeyGetter::operator()(const ObLSRouterKey &key, ObLSRouterValue *value)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_ids is nullptr", KR(ret));
  } else if (cluster_id_ == key.get_cluster_id() && tenant_id_ == key.get_tenant_id()) {
    if (OB_FAIL(ls_ids_->push_back(key.get_ls_id()))) {
      LOG_WARN("ls_ids_ push_back failed", KR(ret), K(key));
    }
  }

  return OB_SUCCESS == ret;
}

bool ObLogRouteService::ObAllLSRouterKeyGetter::operator()(const ObLSRouterKey &key, ObLSRouterValue *value)
{
  int ret = OB_SUCCESS;

  UNUSED(value);

  if (OB_FAIL(router_keys_.push_back(key))) {
    LOG_WARN("failed to push_back key into router_keys_", K(key), "count", router_keys_.count());
  }

  return OB_SUCCESS == ret;
}

bool ObLogRouteService::ObLSRouterValueUpdater::operator()(const ObLSRouterKey &key, ObLSRouterValue *value)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(value)) {
    // ignore
  } else {
    value->refresh_ls_svr_list(svr_list_);
  }

  return OB_SUCCESS == ret;
}

bool ObLogRouteService::ObLSRouterValueGetter::operator()(const ObLSRouterKey &key, ObLSRouterValue *value)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(router_values_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("router_values_ is nullptr", KR(ret));
  } else {
    if (nullptr != value) {
      if (OB_FAIL(router_values_->push_back(value))) {
        LOG_WARN("router_values_ push_back failed", KR(ret), K(key));
      }
    }
  }

  return OB_SUCCESS == ret;
}

int ObLogRouteService::update_all_ls_server_list_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    ObAllLSRouterKeyGetter all_ls_routerkey_getter;
    if (OB_FAIL(ls_router_map_.for_each(all_ls_routerkey_getter))) {
      LOG_WARN("ls_router_map_ update_all_ls_server_list_ for_each failed", KR(ret));
    } else {
      const ObIArray<ObLSRouterKey> &router_keys = all_ls_routerkey_getter.router_keys_;
      LSSvrList tmp_svr_list;
      ObLSRouterValueUpdater updater(tmp_svr_list);

      ARRAY_FOREACH_NORET(router_keys, idx) {
        tmp_svr_list.reset();
        const ObLSRouterKey &key = router_keys.at(idx);
        if (OB_FAIL(get_ls_svr_list_(key, tmp_svr_list))) {
          LOG_WARN("failed to get_ls_svr_list when update_all_ls_server_list", K(key), K(idx),
            "count", router_keys.count());
        } else if (OB_FAIL(ls_router_map_.operate(key, updater))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("failed to update router_value for key", K(key));
          }
        }
      }
    }
  }

  return ret;
}

int ObLogRouteService::update_server_list_(
    const ObLSRouterKey &router_key,
    ObLSRouterValue &router_value)
{
  int ret = OB_SUCCESS;
  ObLSLogInfo ls_log_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    LSSvrList tmp_svr_list;
    if (OB_FAIL(get_ls_svr_list_(router_key, tmp_svr_list))) {
      LOG_WARN("failed to get_ls_svr_list when update_server_list", K(router_key));
    } else {
      router_value.refresh_ls_svr_list(tmp_svr_list);
    }

    LOG_INFO("update server list done", KR(ret), K(router_key), K(ls_log_info), K(tmp_svr_list), K(tg_id_));
  }

  return ret;
}

int ObLogRouteService::query_units_info_and_update_(
    const ObLSRouterKey &router_key,
    ObLSRouterValue &router_value)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    LSSvrList &ls_svr_list = router_value.get_ls_svr_list();
    ObUnitsRecordInfo units_record_info;

    if (OB_FAIL(systable_queryer_.get_all_units_info(router_key.get_tenant_id(), units_record_info))) {
      if (OB_NEED_RETRY == ret) {
        LOG_WARN("query the GV$OB_UNITS failed, need retry", KR(ret));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("query the GV$OB_UNITS failed, will be retried later", KR(ret));
      }
    } else {
      ObUnitsRecordInfo::ObUnitsRecordArray &units_record_array = units_record_info.get_units_record_array();

      ARRAY_FOREACH_N(units_record_array, idx, count) {
        ObUnitsRecord &record = units_record_array.at(idx);
        ObAddr &server = record.server_;
        RegionPriority region_priority = REGION_PRIORITY_UNKNOWN;

        if (OB_FAIL(ls_svr_list.add_server_or_update(server,
                palf::LSN(0), palf::LSN(palf::LOG_MAX_LSN_VAL), region_priority, false/*is_leader*/))) {
          LOG_WARN("ObLogRouteService add_server_or_update failed", KR(ret), K(router_key),
              K(router_value));
        }
      }
    }
  }

  return ret;
}

int ObLogRouteService::update_all_server_and_zone_cache_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRouteService has not been inited", KR(ret));
  } else {
    all_svr_cache_.query_and_update();
  }

  return ret;
}

ObLogRouteService::ObLSRouteTimerTask::ObLSRouteTimerTask(ObLogRouteService &log_route_service) :
    is_inited_(false),
    log_route_service_(log_route_service)
{}

int ObLogRouteService::ObLSRouteTimerTask::init(int tg_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLSRouteTimerTask has already been inited", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObLogRouteService::ObLSRouteTimerTask::destroy()
{
  is_inited_ = false;
}

void ObLogRouteService::ObLSRouteTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLSRouteTimerTask has not been inited", KR(ret));
  } else if (OB_FAIL(log_route_service_.update_all_server_and_zone_cache_())) {
    LOG_WARN("ObLogRouteService update_all_server_and_zone_cache_ failed", KR(ret));
  } else if (OB_FAIL(log_route_service_.update_all_ls_server_list_())) {
    LOG_WARN("ObLogRouteService update_all_ls_server_list_ failed", KR(ret));
  } else {}

  // ignore ret
  if (OB_FAIL(log_route_service_.schedule_ls_timer_task_())) {
    LOG_WARN("schedule_ls_timer_task_ failed", KR(ret));
  }
}

int ObLogRouteService::schedule_ls_timer_task_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLSRouteTimerTask has not been inited", KR(ret));
  } else {
    // do nothing
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase

