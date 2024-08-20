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

#define USING_LOG_PREFIX SHARE_LOCATION
#include "share/location_cache/ob_tablet_location_refresh_service.h"
#include "share/location_cache/ob_tablet_ls_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/transfer/ob_transfer_task_operator.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase
{
namespace share
{

ObTabletLocationRefreshMgr::ObTabletLocationRefreshMgr(
   const uint64_t tenant_id,
   const ObTransferTaskID &base_task_id)
  : mutex_(),
    tenant_id_(tenant_id),
    base_task_id_(base_task_id),
    tablet_ids_(),
    inc_task_infos_()
{
  tablet_ids_.set_attr(SET_USE_500("TbltRefIDS"));
  inc_task_infos_.set_attr(SET_USE_500("TbltRefTasks"));
}

ObTabletLocationRefreshMgr::~ObTabletLocationRefreshMgr()
{
}

void ObTabletLocationRefreshMgr::set_base_task_id(
     const ObTransferTaskID &base_task_id)
{
  lib::ObMutexGuard guard(mutex_);
  base_task_id_ = base_task_id;
}

void ObTabletLocationRefreshMgr::get_base_task_id(
     ObTransferTaskID &base_task_id)
{
  lib::ObMutexGuard guard(mutex_);
  base_task_id = base_task_id_;
}

int ObTabletLocationRefreshMgr::set_tablet_ids(
    const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("fail to assign tablet_ids", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObTabletLocationRefreshMgr::get_tablet_ids(
    common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(tablet_ids.assign(tablet_ids_))) {
    LOG_WARN("fail to get tablet_ids", KR(ret), K_(tenant_id));
  }
  return ret;
}

// update & add inc_task_infos_
int ObTabletLocationRefreshMgr::merge_inc_task_infos(
    common::ObArray<ObTransferRefreshInfo> &inc_task_infos_to_merge)
{
  int ret = OB_SUCCESS;
  if (inc_task_infos_to_merge.count() <= 0) {
    // do nothing
  } else {
    lib::ob_sort(inc_task_infos_to_merge.begin(), inc_task_infos_to_merge.end(), ObTransferRefreshInfo::less_than);
    ObArray<ObTransferRefreshInfo> new_tasks;
    ObArray<ObTransferRefreshInfo> changed_tasks;

    lib::ObMutexGuard guard(mutex_);
    int64_t local_pos = 0;
    int64_t inc_pos = 0;
    for ( ;
          OB_SUCC(ret)
          && local_pos < inc_task_infos_.count()
          && inc_pos < inc_task_infos_to_merge.count()
          ;) {
      ObTransferRefreshInfo &local_task = inc_task_infos_.at(local_pos);
      const ObTransferRefreshInfo &inc_task = inc_task_infos_to_merge.at(inc_pos);
      if (local_task.get_task_id() == inc_task.get_task_id()) {
        bool changed = false;
        (void) local_task.get_status().update(inc_task.get_status(), changed);
        if (changed && OB_FAIL(changed_tasks.push_back(inc_task))) {
          LOG_WARN("fail to push back task", KR(ret), K(inc_task));
        }
        local_pos++;
        inc_pos++;
      } else if (local_task.get_task_id() < inc_task.get_task_id()) {
        local_pos++;
      } else if (local_task.get_task_id() > inc_task.get_task_id()) {
        if (OB_FAIL(new_tasks.push_back(inc_task))) {
          LOG_WARN("fail to push back task", KR(ret), K(inc_task));
        }
        inc_pos++;
      }
    } // end for

    if (changed_tasks.count() > 0) {
      FLOG_INFO("[REFRESH_TABLET_LOCATION] change tasks",
                KR(ret), K_(tenant_id), K(changed_tasks.count()), K(changed_tasks));
    }

    for (int64_t i = inc_pos; OB_SUCC(ret) && i < inc_task_infos_to_merge.count(); i++) {
      const ObTransferRefreshInfo &inc_task = inc_task_infos_to_merge.at(i);
      if (OB_FAIL(new_tasks.push_back(inc_task))) {
        LOG_WARN("fail to push back task", KR(ret), K(inc_task));
      }
    } // end dor

    if (OB_SUCC(ret) && new_tasks.count() > 0) {
      int64_t new_count = inc_task_infos_.count() + new_tasks.count();
      if (OB_FAIL(inc_task_infos_.reserve(new_count))) {
        LOG_WARN("fail to reserve array", KR(ret), K(new_count));
      } else if (OB_FAIL(append(inc_task_infos_, new_tasks))) {
        LOG_WARN("fail to append array", KR(ret), K(new_count));
      } else {
        lib::ob_sort(inc_task_infos_.begin(), inc_task_infos_.end(), ObTransferRefreshInfo::less_than);
        FLOG_INFO("[REFRESH_TABLET_LOCATION] add tasks",
                  KR(ret), K_(tenant_id), K(new_tasks.count()), K(new_tasks));
      }
    }
  }
  return ret;
}

// return 128 task ids at most in one time
int ObTabletLocationRefreshMgr::get_doing_task_ids(
    common::ObIArray<ObTransferTaskID> &task_ids)
{
  int ret = OB_SUCCESS;
  task_ids.reset();
  lib::ObMutexGuard guard(mutex_);
  for (int64_t i = 0;
       OB_SUCC(ret)
       && i < inc_task_infos_.count()
       && task_ids.count() < BATCH_TASK_COUNT
       ; i++) {
    const ObTransferRefreshInfo &inc_task = inc_task_infos_.at(i);
    if (inc_task.get_status().is_doing_status()) {
      if (OB_FAIL(task_ids.push_back(inc_task.get_task_id()))) {
        LOG_WARN("fail to push back task", KR(ret), K(inc_task));
      }
    }
  } // end for
  return ret;
}

// clear inc_task_infos_ & update base_task_id_
int ObTabletLocationRefreshMgr::clear_task_infos(bool &has_doing_task)
{
  int ret = OB_SUCCESS;
  has_doing_task = false;
  lib::ObMutexGuard guard(mutex_);

  int64_t continuous_done_status_pos = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_task_infos_.count(); i++) {
    if (inc_task_infos_.at(i).get_status().is_done_status()) {
      continuous_done_status_pos = i;
    } else {
      break;
    }
  } // end for

  if (OB_SUCC(ret)
      && continuous_done_status_pos >= 0
      && continuous_done_status_pos < inc_task_infos_.count()) {
    ObTransferTaskID from_task_id = inc_task_infos_.at(0).get_task_id();
    ObTransferTaskID to_task_id = inc_task_infos_.at(continuous_done_status_pos).get_task_id();
    int64_t clear_tasks_cnt = continuous_done_status_pos + 1;
    int64_t remain_tasks_cnt = inc_task_infos_.count() - 1 - continuous_done_status_pos;
    if (continuous_done_status_pos == inc_task_infos_.count() - 1) {
      inc_task_infos_.reset();
    } else {
      ObArray<ObTransferRefreshInfo> tmp_task_infos;
      if (OB_FAIL(rootserver::ObRootUtils::copy_array(
                 inc_task_infos_, continuous_done_status_pos + 1,
                 inc_task_infos_.count(), tmp_task_infos))) {
        LOG_WARN("fail to copy array", KR(ret), K(continuous_done_status_pos), K(inc_task_infos_.count()));
      } else if (OB_FAIL(inc_task_infos_.assign(tmp_task_infos))) {
        LOG_WARN("fail to assign array", KR(ret), K(tmp_task_infos.count()));
      }
    }

    if (OB_SUCC(ret) && base_task_id_ < to_task_id) {
      FLOG_INFO("[REFRESH_TABLET_LOCATION] update base_task_id when fetch inc tasks",
                KR(ret), K_(tenant_id), "from_base_task_id", base_task_id_, "to_base_task_id", to_task_id);
      base_task_id_ = to_task_id;
    }
    FLOG_INFO("[REFRESH_TABLET_LOCATION] clear tasks", KR(ret), K_(tenant_id),
              K(from_task_id), K(to_task_id), K(clear_tasks_cnt), K(remain_tasks_cnt));
  }

  for (int64_t i = 0; OB_SUCC(ret) && !has_doing_task && i < inc_task_infos_.count(); i++) {
    if (inc_task_infos_.at(i).get_status().is_doing_status()) {
      has_doing_task = true;
    }
  } // end for
  return ret;
}

void ObTabletLocationRefreshMgr::dump_statistic()
{
  lib::ObMutexGuard guard(mutex_);
  int64_t unknown_task_cnt = 0;
  int64_t doing_task_cnt = 0;
  int64_t done_task_cnt = 0;
  for (int64_t i = 0; i < inc_task_infos_.count(); i++) {
    const ObTransferRefreshInfo &transfer_task = inc_task_infos_.at(i);
    if (transfer_task.get_status().is_unknown_status()) {
      unknown_task_cnt++;
    } else if (transfer_task.get_status().is_done_status()) {
      doing_task_cnt++;
    } else if (transfer_task.get_status().is_done_status()) {
      done_task_cnt++;
    } else {}
  } // end for
  FLOG_INFO("[REFRESH_TABLET_LOCATION] dump statistic",
            K_(tenant_id), K_(base_task_id),
            "tablet_list_cnt", tablet_ids_.count(),
            "total_task_cnt", inc_task_infos_.count(),
            K(unknown_task_cnt), K(doing_task_cnt), K(done_task_cnt));
}

int64_t ObTabletLocationRefreshServiceIdling::get_idle_interval_us()
{
  int64_t idle_time = DEFAULT_TIMEOUT_US;
  if (0 != GCONF._auto_refresh_tablet_location_interval) {
    idle_time = GCONF._auto_refresh_tablet_location_interval;
  }
  return idle_time;
}

int ObTabletLocationRefreshServiceIdling::fast_idle()
{
  return idle(FAST_TIMEOUT_US);
}

ObTabletLocationRefreshService::ObTabletLocationRefreshService()
  : ObRsReentrantThread(true),
    inited_(false),
    has_task_(false),
    idling_(stop_),
    tablet_ls_service_(NULL),
    schema_service_(NULL),
    sql_proxy_(NULL),
    allocator_(SET_USE_500("TbltReSrv")),
    rwlock_(),
    tenant_mgr_map_()
{
}

ObTabletLocationRefreshService::~ObTabletLocationRefreshService()
{
  destroy();
}

int ObTabletLocationRefreshService::init(
    ObTabletLSService &tablet_ls_service,
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const int64_t THREAD_CNT = 1;
  const int64_t BUCKET_NUM = 1024;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (OB_FAIL(create(THREAD_CNT, "TbltRefreshSer"))) {
    LOG_WARN("create thread failed", KR(ret));
  } else if (OB_FAIL(tenant_mgr_map_.create(BUCKET_NUM,
             SET_USE_500("TbltRefreshMap"), SET_USE_500("TbltRefreshMap")))) {
    LOG_WARN("fail to create hash map", KR(ret));
  } else {
    tablet_ls_service_ = &tablet_ls_service;
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    has_task_ = false;
    inited_ = true;
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] init service", KR(ret));
  return ret;
}

int ObTabletLocationRefreshService::start()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[REFRESH_TABLET_LOCATION] start service begin");
  if (OB_FAIL(ObRsReentrantThread::start())) {
    LOG_WARN("fail to start thread", KR(ret));
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] start service end", KR(ret));
  return ret;
}

void ObTabletLocationRefreshService::stop()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] stop service begin");
  ObReentrantThread::stop();
  idling_.wakeup();
  FLOG_INFO("[REFRESH_TABLET_LOCATION] stop service end");
}

void ObTabletLocationRefreshService::wait()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] wait service begin");
  ObRsReentrantThread::wait();
  ObReentrantThread::wait();
  FLOG_INFO("[REFRESH_TABLET_LOCATION] wait service end");
}

void ObTabletLocationRefreshService::destroy()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] destroy service begin");
  (void) stop();
  (void) wait();
  SpinWLockGuard guard(rwlock_);
  if (inited_) {
    FOREACH(it, tenant_mgr_map_) {
      if (OB_NOT_NULL(it->second)) {
        (it->second)->~ObTabletLocationRefreshMgr();
        it->second = NULL;
      }
    }
    tablet_ls_service_ = NULL;
    schema_service_ = NULL;
    sql_proxy_ = NULL;
    tenant_mgr_map_.destroy();
    allocator_.reset();
    has_task_ = false;
    inited_ = false;
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] destroy service end");
}

void ObTabletLocationRefreshService::run3()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] run service begin");
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", KR(ret));
  } else {
    while(!stop_) {
      ObCurTraceId::init(GCTX.self_addr());
      if (OB_FAIL(refresh_cache_())) {
        LOG_WARN("fail to refresh tablet location", KR(ret));
      }
      // retry until stopped, reset ret to OB_SUCCESS
      ret = OB_SUCCESS;
      idle_();
    }
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] run service end");
}

void ObTabletLocationRefreshService::idle_()
{
  if (OB_UNLIKELY(stop_)) {
    // skip
  } else if (OB_UNLIKELY(has_task_ && 0 != GCONF._auto_refresh_tablet_location_interval)) {
    (void) idling_.fast_idle();
  } else {
    (void) idling_.idle();
  }
  has_task_ = false;
}

int ObTabletLocationRefreshService::check_stop_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", KR(ret));
  } else if (OB_UNLIKELY(stop_)) {
    ret = OB_CANCELED;
    LOG_WARN("thread has been stopped", KR(ret));
  } else if (OB_UNLIKELY(0 == GCONF._auto_refresh_tablet_location_interval)) {
    ret = OB_CANCELED;
    LOG_WARN("service is shut down by config", KR(ret));
  }
  return ret;
}

int ObTabletLocationRefreshService::check_tenant_can_refresh_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to check data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_2_1_2) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant's data_version is less than 4.2.1.2, try later",
             KR(ret), K(tenant_id), K(data_version));
  }
  return ret;
}

int ObTabletLocationRefreshService::get_base_task_id_(
    const uint64_t tenant_id,
    ObTransferTaskID &base_task_id)
{
  int ret = OB_SUCCESS;
  base_task_id.reset();
  SpinRLockGuard guard(rwlock_);
  ObTabletLocationRefreshMgr *mgr = NULL;
  if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
    LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
  } else {
    (void) mgr->get_base_task_id(base_task_id);
  }
  return ret;
}

int ObTabletLocationRefreshService::try_init_base_point(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_user_tenant(tenant_id)) {
    bool should_init = false;
    {
      SpinRLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          should_init = true;
        } else {
          LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
        }
      }
    }
    if (OB_SUCC(ret) && should_init) {
      if (OB_FAIL(try_init_base_point_(tenant_id))) {
        LOG_WARN("fail to init base point", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

// 1. mgr is null means tenant has been dropped.
// 2. OB_HASH_NOT_EXIST means tenant has not been inited.
int ObTabletLocationRefreshService::inner_get_mgr_(
    const int64_t tenant_id,
    ObTabletLocationRefreshMgr *&mgr)
{
  int ret = OB_SUCCESS;
  mgr = NULL;
  int hash_ret = tenant_mgr_map_.get_refactored(tenant_id, mgr);
  if (OB_SUCCESS == hash_ret) {
    // success
  } else {
    mgr = NULL;
    ret = hash_ret;
    LOG_WARN("fail to get mgr from map", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTabletLocationRefreshService::get_tenant_ids_(
    common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_FAIL(tenant_ids.reserve(tenant_mgr_map_.size()))) {
    LOG_WARN("fail to reserved array", KR(ret));
  } else {
    FOREACH_X(it, tenant_mgr_map_, OB_SUCC(ret)) {
      const uint64_t tenant_id = it->first;
      if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("fail to push back tenant_id", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObTabletLocationRefreshService::try_clear_mgr_(const uint64_t tenant_id, bool &clear)
{
  int ret = OB_SUCCESS;
  bool has_been_dropped = false;
  clear = false;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", KR(ret));
  } else if (OB_FAIL(schema_service_->check_if_tenant_has_been_dropped(tenant_id, has_been_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", KR(ret), K(tenant_id));
  } else if (!has_been_dropped) {
    // skip
  } else {
    bool should_clear = false;
    {
      SpinRLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      } else if (OB_NOT_NULL(mgr)) {
        should_clear = true;
      } else {
        clear = true;
      }
    }

    if (OB_SUCC(ret) && should_clear) {
      SpinWLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      } else if (OB_NOT_NULL(mgr)) {
        ObTabletLocationRefreshMgr *tmp_mgr = NULL;
        int overwrite = 1;
        if (OB_FAIL(tenant_mgr_map_.set_refactored(tenant_id, tmp_mgr, overwrite))) {
          LOG_WARN("fail to overwrite mgr", KR(ret), K(tenant_id));
        } else {
          FLOG_INFO("[REFRESH_TABLET_LOCATION] destroy struct because tenant has been dropped", K(tenant_id));
          mgr->~ObTabletLocationRefreshMgr();
          mgr = NULL;
        }
      }
      if (OB_SUCC(ret)) {
        clear = true;
      }
    }
  }
  return ret;
}

int ObTabletLocationRefreshService::try_init_base_point_(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  ObTransferTaskID base_task_id;

  // try get data_version
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    // tenant's compat version may be not persisted in local in the following cases:
    // 1. unit has been dropped
    // 2. unit is added/migrated
    data_version = LAST_BARRIER_DATA_VERSION;
    LOG_WARN("fail to get min data version, use last barrier version instead",
             KR(tmp_ret), K(tenant_id), K(data_version));
  }

  // try get base_task_id
  if (data_version >= DATA_VERSION_4_2_1_2) {
    if (OB_ISNULL(sql_proxy_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql_proxy_ is null", KR(ret));
    } else if (OB_FAIL(ObTransferTaskOperator::fetch_initial_base_task_id(
               *sql_proxy_, tenant_id, base_task_id))) {
      LOG_WARN("fail to fetch initial base task id", KR(ret), K(tenant_id));
    }
  } else {
    base_task_id.reset();
  }

  // try init struct
  if (OB_SUCC(ret)) {
    SpinWLockGuard guard(rwlock_);
    ObTabletLocationRefreshMgr *mgr = NULL;
    if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        void *buf = NULL;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTabletLocationRefreshMgr)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (FALSE_IT(mgr = new (buf) ObTabletLocationRefreshMgr(tenant_id, base_task_id))) {
          LOG_WARN("fail to new ObTabletLocationRefreshMgr",
                   KR(ret), K(tenant_id), K(data_version), K(base_task_id));
        } else if (OB_FAIL(tenant_mgr_map_.set_refactored(tenant_id, mgr))) {
          LOG_WARN("fail to set ObTabletLocationRefreshMgr",
                   KR(ret), K(tenant_id), K(data_version), K(base_task_id));
        }
        FLOG_INFO("[REFRESH_TABLET_LOCATION] init struct",
                  KR(ret), K(tenant_id), K(data_version), K(base_task_id));
      } else {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObTabletLocationRefreshService::refresh_cache_()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] refresh cache start");
  int64_t start_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(get_tenant_ids_(tenant_ids))) {
    LOG_WARN("fail to get tenant_ids", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;

    // dump statistic
    const int64_t DUMP_INTERVAL = 10 * 60 * 1000 * 1000L; // 10min
    if (TC_REACH_TIME_INTERVAL(DUMP_INTERVAL)) {
      for (int64_t i = 0; i < tenant_ids.count(); i++) { // ignore different tenant's failure
        const uint64_t tenant_id = tenant_ids.at(i);
        SpinRLockGuard guard(rwlock_);
        ObTabletLocationRefreshMgr *mgr = NULL;
        if (OB_TMP_FAIL(inner_get_mgr_(tenant_id, mgr))) {
          LOG_WARN("fail to get mgr", KR(tmp_ret), K(tenant_id));
        } else if (OB_NOT_NULL(mgr)) {
          (void) mgr->dump_statistic();
        }
      } // end for
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(check_stop_())) {
        LOG_WARN("fail to check stop", KR(ret));
      } else { // ignore different tenant's failure
        bool clear = false; // will be true when tenant has been dropped
        if (OB_TMP_FAIL(try_clear_mgr_(tenant_id, clear))) {
          LOG_WARN("fail to clear mgr", KR(tmp_ret), K(tenant_id));
        }

        if (!clear && OB_TMP_FAIL(refresh_cache_(tenant_id))) {
          LOG_WARN("fail to refresh cache", KR(tmp_ret), K(tenant_id));
        }
      }
    } // end for
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] refresh cache end",
            KR(ret), "cost_us", ObTimeUtility::current_time() - start_time,
            "tenant_cnt", tenant_mgr_map_.size());
  return ret;
}

int ObTabletLocationRefreshService::refresh_cache_(const uint64_t tenant_id)
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] refresh cache start", K(tenant_id));
  int64_t start_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop_())) {
    LOG_WARN("fail to check stop", KR(ret));
  } else if (OB_FAIL(check_tenant_can_refresh_(tenant_id))) {
    LOG_WARN("fail to check tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(try_runs_for_compatibility_(tenant_id))) {
    LOG_WARN("fail to runs for compatibility", KR(ret), K(tenant_id));
  } else if (OB_FAIL(fetch_inc_task_infos_and_update_(tenant_id))) {
    LOG_WARN("fail to fetch inc task infos", KR(ret), K(tenant_id));
  } else if (OB_FAIL(process_doing_task_infos_(tenant_id))) {
    LOG_WARN("fail to process tasks", KR(ret), K(tenant_id));
  } else if (OB_FAIL(clear_task_infos_(tenant_id))) {
    LOG_WARN("fail to clear tasks", KR(ret), K(tenant_id));
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] refresh cache end",
            KR(ret), K(tenant_id), "cost_us", ObTimeUtility::current_time() - start_time);
  return ret;
}

// try init base_task_id_ & get tablet_ids from local cache
int ObTabletLocationRefreshService::try_runs_for_compatibility_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransferTaskID base_task_id;
  if (OB_FAIL(get_base_task_id_(tenant_id, base_task_id))) {
    LOG_WARN("fail to get base_task_id", KR(ret), K(tenant_id));
  } else if (base_task_id.is_valid()) {
    // non compatibility scene, do nothing
  } else {
    ObArray<ObTabletID> tablet_ids;
    tablet_ids.set_attr(SET_USE_500("TbltRefIDS"));
    if (OB_FAIL(check_stop_())) {
      LOG_WARN("fail to check stop", KR(ret));
    } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tablet_ls_service_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql_proxy_ or tablet_ls_service_ is null",
               KR(ret), KP_(sql_proxy), KP_(tablet_ls_service));
    } else if (OB_FAIL(ObTransferTaskOperator::fetch_initial_base_task_id(
               *sql_proxy_, tenant_id, base_task_id))) {
      LOG_WARN("fail to fetch initial base task id", KR(ret), K(tenant_id));
    } else if (OB_FAIL(tablet_ls_service_->get_tablet_ids_from_cache(tenant_id, tablet_ids))) {
      LOG_WARN("fail to get tablet_ids", KR(ret), K(tenant_id));
    } else {
      SpinWLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      ObTransferTaskID existed_base_task_id;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(mgr)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
      } else if (FALSE_IT(mgr->get_base_task_id(existed_base_task_id))) {
      } else if (existed_base_task_id.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("base_task_id should be invalid",
                 KR(ret), K(tenant_id), K(existed_base_task_id));
      } else if (OB_FAIL(mgr->set_tablet_ids(tablet_ids))) {
        LOG_WARN("fail to set tablet_ids", KR(ret));
      } else {
        (void) mgr->set_base_task_id(base_task_id);
        FLOG_INFO("[REFRESH_TABLET_LOCATION] update base_task_id for compatibility",
                  K(tenant_id), K(base_task_id), "tablet_ids_cnt", tablet_ids.count());
      }
    }
  }

  // try reload tablet-ls caches according to tablet_ids_
  if (FAILEDx(try_reload_tablet_cache_(tenant_id))) {
    LOG_WARN("fail to reload tablet cache", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTabletLocationRefreshService::try_reload_tablet_cache_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> store_tablet_ids;
  store_tablet_ids.set_attr(SET_USE_500("TbltRefIDS"));
  ObTransferTaskID base_task_id;
  {
    SpinRLockGuard guard(rwlock_);
    ObTabletLocationRefreshMgr *mgr = NULL;
    if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
      LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(mgr)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->get_tablet_ids(store_tablet_ids))) {
      LOG_WARN("fail to get tablet_ids", KR(ret), K(tenant_id));
    } else {
      (void) mgr->get_base_task_id(base_task_id);
    }
  }

  if (OB_SUCC(ret) && base_task_id.is_valid() && store_tablet_ids.count() > 0) {

    const int64_t MAX_RELOAD_TABLET_NUM_IN_BATCH = 128;
    int64_t end_pos = min(store_tablet_ids.count(), MAX_RELOAD_TABLET_NUM_IN_BATCH);
    ObArenaAllocator allocator;
    ObList<ObTabletID, ObIAllocator> process_tablet_ids(allocator);
    for (int64_t i = 0; OB_SUCC(ret) && i < end_pos; i++) {
      if (OB_FAIL(check_stop_())) {
        LOG_WARN("fail to check stop", KR(ret));
      } else if (OB_FAIL(process_tablet_ids.push_back(store_tablet_ids.at(i)))) {
        LOG_WARN("fail to push back", KR(ret), K(store_tablet_ids.at(i)));
      }
    } // end for

    if (OB_SUCC(ret)) {
      ObArray<ObTabletLSCache> tablet_ls_caches; // not used
      if (OB_FAIL(check_stop_())) {
        LOG_WARN("fail to check stop", KR(ret));
      } else if (OB_ISNULL(tablet_ls_service_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("tablet_ls_service_ is null", KR(ret));
      } else if (OB_FAIL(tablet_ls_service_->batch_renew_tablet_ls_cache(
                 tenant_id, process_tablet_ids, tablet_ls_caches))) {
        LOG_WARN("fail to batch renew tablet ls cache",
                 KR(ret), K(tenant_id), "tablet_ids_cnt", process_tablet_ids.size());
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<ObTabletID> remain_tablet_ids;
      remain_tablet_ids.set_attr(SET_USE_500("TbltRefIDS"));
      if (OB_FAIL(rootserver::ObRootUtils::copy_array(store_tablet_ids,
          end_pos, store_tablet_ids.count(), remain_tablet_ids))) {
        LOG_WARN("fail to copy array", KR(ret), K(end_pos), K(store_tablet_ids.count()));
      } else {
        SpinRLockGuard guard(rwlock_);
        ObTabletLocationRefreshMgr *mgr = NULL;
        if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
          LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(mgr)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
        } else if (OB_FAIL(mgr->set_tablet_ids(remain_tablet_ids))) {
          LOG_WARN("fail to set tablet_ids", KR(ret), K(tenant_id), K(remain_tablet_ids.count()));
        } else {
          has_task_ = (remain_tablet_ids.count() > 0);
        }
      }
    }

    FLOG_INFO("[REFRESH_TABLET_LOCATION] update tablet-ls caches for compatibility", KR(ret),
              K(tenant_id), "process_tablet_cnt", process_tablet_ids.size(),
              "remain_tablet_cnt", store_tablet_ids.count() - process_tablet_ids.size());
  }
  return ret;
}

int ObTabletLocationRefreshService::fetch_inc_task_infos_and_update_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransferTaskID base_task_id;
  if (OB_FAIL(get_base_task_id_(tenant_id, base_task_id))) {
    LOG_WARN("fail to get base_task_id", KR(ret), K(tenant_id));
  } else if (!base_task_id.is_valid()) {
    // skip
  } else {
    ObArray<ObTransferRefreshInfo> inc_task_infos;
    if (OB_FAIL(check_stop_())) {
      LOG_WARN("fail to check stop", KR(ret));
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql_proxy_ is null", KR(ret), KP_(sql_proxy));
    } else if (OB_FAIL(ObTransferTaskOperator::fetch_inc_task_infos(
               *sql_proxy_, tenant_id, base_task_id, inc_task_infos))) {
      LOG_WARN("fail to fetch inc task infos", KR(ret), K(tenant_id), K(base_task_id));
    } else {
      SpinRLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(mgr)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
      } else if (OB_FAIL(mgr->merge_inc_task_infos(inc_task_infos))) {
        LOG_WARN("fail to merge inc task infos", KR(ret), K(tenant_id), K(inc_task_infos.count()));
      }
    }
  }
  return ret;
}

int ObTabletLocationRefreshService::process_doing_task_infos_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransferTaskID base_task_id;
  if (OB_FAIL(get_base_task_id_(tenant_id, base_task_id))) {
    LOG_WARN("fail to get base_task_id", KR(ret), K(tenant_id));
  } else if (!base_task_id.is_valid()) {
    // skip
  } else {
    ObArray<ObTransferTaskID> task_ids;
    {
      SpinRLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(mgr)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
      } else if (OB_FAIL(mgr->get_doing_task_ids(task_ids))) {
        LOG_WARN("fail to get doing task ids", KR(ret), K(tenant_id));
      }
    }

    if (OB_SUCC(ret) && task_ids.count() > 0) {
      common::ObArray<ObTabletLSCache> tablet_ls_caches;
      tablet_ls_caches.set_attr(SET_USE_500("TbltRefCaches"));
      if (OB_FAIL(check_stop_())) {
        LOG_WARN("fail to check stop", KR(ret));
      } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tablet_ls_service_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("sql_proxy_ or tablet_ls_service_ is null",
                 KR(ret), KP_(sql_proxy), KP_(tablet_ls_service));
      } else if (OB_FAIL(ObTransferTaskOperator::batch_get_tablet_ls_cache(
                 *sql_proxy_, tenant_id, task_ids, tablet_ls_caches))) {
        LOG_WARN("fail to get tablet ls cache", KR(ret), K(tenant_id), K(task_ids));
      } else {
        int64_t start_time = ObTimeUtility::current_time();
        const bool update_only = true;
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ls_caches.count(); i++) {
          const ObTabletLSCache &tablet_ls = tablet_ls_caches.at(i);
          if (OB_FAIL(check_stop_())) {
            LOG_WARN("fail to check stop", KR(ret));
          } else if (OB_FAIL(tablet_ls_service_->update_cache(tablet_ls, update_only))) {
            LOG_WARN("update cache failed", KR(ret), K(tablet_ls));
          }
        } // end for
        FLOG_INFO("[REFRESH_TABLET_LOCATION] update tablet-ls caches when process tasks",
                  KR(ret), K(tenant_id), K(tablet_ls_caches.count()),
                  "cost_us", ObTimeUtility::current_time() - start_time,
                  K(task_ids));
      }

      if (OB_SUCC(ret)) {
        ObArray<ObTransferRefreshInfo> done_task_infos;
        ObTransferRefreshStatus done_status(ObTransferRefreshStatus::DONE);
        if (OB_FAIL(done_task_infos.reserve(task_ids.count()))) {
          LOG_WARN("fail to reserve array", KR(ret), K(task_ids.count()));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < task_ids.count(); i++) {
          ObTransferRefreshInfo task_info;
          if (OB_FAIL(check_stop_())) {
            LOG_WARN("fail to check stop", KR(ret));
          } else if (OB_FAIL(task_info.init(task_ids.at(i), done_status))) {
            LOG_WARN("fail to init refresh info", KR(ret), K(tenant_id), K(task_ids.at(i)));
          } else if (OB_FAIL(done_task_infos.push_back(task_info))) {
            LOG_WARN("fail to push back task info", KR(ret), K(tenant_id), K(task_info));
          }
        } // end for
        if (OB_SUCC(ret)) {
          SpinRLockGuard guard(rwlock_);
          ObTabletLocationRefreshMgr *mgr = NULL;
          if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
            LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
          } else if (OB_ISNULL(mgr)) {
            ret = OB_TENANT_NOT_EXIST;
            LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
          } else if (OB_FAIL(mgr->merge_inc_task_infos(done_task_infos))) {
            LOG_WARN("fail to merge inc task infos", KR(ret), K(tenant_id), K(done_task_infos));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletLocationRefreshService::clear_task_infos_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransferTaskID base_task_id;
  if (OB_FAIL(get_base_task_id_(tenant_id, base_task_id))) {
    LOG_WARN("fail to get base_task_id", KR(ret), K(tenant_id));
  } else if (!base_task_id.is_valid()) {
    // skip
  } else {
    SpinRLockGuard guard(rwlock_);
    ObTabletLocationRefreshMgr *mgr = NULL;
    bool has_doing_task = false;
    if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
      LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(mgr)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->clear_task_infos(has_doing_task))) {
      LOG_WARN("fail to clear task", KR(ret), K(tenant_id));
    } else if (has_doing_task) {
      has_task_ = true;
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
