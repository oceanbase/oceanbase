/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER

#include <algorithm>
#include "lib/allocator/ob_malloc.h"
#include "share/vector_index/ob_vec_idx_async_task_scheduler.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vec_index_priority_queue_manager.h"
#include "lib/thread/thread_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_cluster_version.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_debug_sync.h"
#include "share/table/ob_ttl_util.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace share
{

// ---------------------------------- ObVecIdxAsyncTaskScheduler ----------------------------------//

struct ObStartupCleanupTaskKey
{
  ObStartupCleanupTaskKey()
    : tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(OB_INVALID_ID),
      tablet_id_(0),
      task_id_(0)
  {}
  ObStartupCleanupTaskKey(uint64_t tenant_id, uint64_t table_id, uint64_t tablet_id, int64_t task_id)
    : tenant_id_(tenant_id),
      table_id_(table_id),
      tablet_id_(tablet_id),
      task_id_(task_id)
  {}
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(tablet_id), K_(task_id));
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t tablet_id_;
  int64_t task_id_;
};

ObVecIdxAsyncTaskScheduler::~ObVecIdxAsyncTaskScheduler()
{
  ob_free(disabled_all_);
  disabled_all_ = nullptr;
  ob_free(disable_entries_);
  disable_entries_ = nullptr;
}

int ObVecIdxAsyncTaskScheduler::init(uint64_t tenant_id, ObPluginVectorIndexService *service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("vec idx async task scheduler init twice", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(service));
  } else if (OB_FAIL(ls_leader_executor_map_.create(
      common::hash::cal_next_prime(DEFAULT_LS_EXECUTOR_MAP_SIZE),
      ObMemAttr(tenant_id, "VecIdxLdrExMap")))) {
    LOG_WARN("fail to create ls leader executor map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ls_follower_executor_map_.create(
      common::hash::cal_next_prime(DEFAULT_LS_EXECUTOR_MAP_SIZE),
      ObMemAttr(tenant_id, "VecIdxFlwExMap")))) {
    LOG_WARN("fail to create ls follower executor map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::VectorIndexScheduleTimer, schedule_tg_id_))) {
    LOG_WARN("fail to create vec idx schedule timer tg", KR(ret), K(tenant_id));
  } else {
    const ObMemAttr disable_mem_attr(tenant_id, "VecIdxDisable");
    const int64_t disable_all_cnt = static_cast<int64_t>(OB_VECTOR_ASYNC_TASK_TYPE_INVALID);
    bool *disabled_buf = static_cast<bool *>(
        ob_malloc(disable_all_cnt * static_cast<int64_t>(sizeof(bool)), disable_mem_attr));
    DisableEntry *entries_buf = nullptr;
    if (OB_ISNULL(disabled_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc vec idx disable_all flags", KR(ret), K(tenant_id), K(disable_all_cnt));
      TG_DESTROY(schedule_tg_id_);
      schedule_tg_id_ = -1;
    } else if (OB_ISNULL(entries_buf = static_cast<DisableEntry *>(
                   ob_malloc(static_cast<int64_t>(MAX_DISABLE_ENTRIES)
                                 * static_cast<int64_t>(sizeof(DisableEntry)),
                             disable_mem_attr)))) {
      ob_free(disabled_buf);
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc vec idx disable_entries", KR(ret), K(tenant_id));
      TG_DESTROY(schedule_tg_id_);
      schedule_tg_id_ = -1;
    } else {
      disabled_all_ = disabled_buf;
      disable_entries_ = entries_buf;
      for (int64_t i = 0; i < disable_all_cnt; ++i) {
        disabled_all_[i] = false;
      }
      for (int64_t i = 0; i < MAX_DISABLE_ENTRIES; ++i) {
        disable_entries_[i].task_type_ = OB_VECTOR_ASYNC_TASK_TYPE_INVALID;
        disable_entries_[i].tablet_id_ = 0;
      }
      tenant_id_ = tenant_id;
      service_ = service;
      startup_cleanup_cutoff_ts_ = ObTimeUtility::current_time();
      executor_allocator_.set_attr(ObMemAttr(tenant_id, "VecIdxExecAlloc"));
      for (int i = 0; i < ObVectorTaskScheduleType::SCHEDULE_MAX; i++) {
        last_schedule_time_[i] = ObTimeUtility::fast_current_time();
        can_schedule_[i] = false;
      }
      is_inited_ = true;
      FLOG_INFO("vec idx async task scheduler inited", K(tenant_id_));
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vec idx async task scheduler not init", KR(ret));
  } else if (OB_FAIL(TG_START(schedule_tg_id_))) {
    LOG_WARN("fail to start vec idx schedule timer tg", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(TG_SCHEDULE(schedule_tg_id_, *this, SCHEDULE_PERIOD_US, true))) {
    LOG_WARN("fail to schedule vec idx async task scheduler", KR(ret), K_(tenant_id));
  } else {
    is_stopped_ = false;
    FLOG_INFO("vec idx async task scheduler started", K_(tenant_id));
  }
  return ret;
}

void ObVecIdxAsyncTaskScheduler::stop()
{
  FLOG_INFO("vec idx async task scheduler stop", K_(tenant_id));
  if (IS_INIT && schedule_tg_id_ >= 0) {
    TG_STOP(schedule_tg_id_);
    is_stopped_ = true;
  }
  FLOG_INFO("vec idx async task scheduler stopped", K_(tenant_id));
}

void ObVecIdxAsyncTaskScheduler::destroy()
{
  FLOG_INFO("vec idx async task scheduler destroy", K_(tenant_id));
  if (schedule_tg_id_ >= 0) {
    TG_DESTROY(schedule_tg_id_);
    schedule_tg_id_ = -1;
  }
  // Explicitly call destructors for all executor sets before the arena is freed.
  // LS removals are rare; memory is reclaimed when the allocator is destroyed.
  {
    common::ObSpinLockGuard guard(ls_executor_lock_);
    auto destroy_leader_func = [](common::hash::HashMapPair<share::ObLSID, ObVecIdxLeaderExecutors *> &entry) -> int {
      if (OB_NOT_NULL(entry.second) && !entry.second->is_removed_) {
        entry.second->~ObVecIdxLeaderExecutors();
        entry.second = nullptr;
      }
      return OB_SUCCESS;
    };
    (void)ls_leader_executor_map_.foreach_refactored(destroy_leader_func);
    ls_leader_executor_map_.destroy();

    auto destroy_follower_func = [](common::hash::HashMapPair<share::ObLSID, ObVecIdxFollowerExecutors *> &entry) -> int {
      if (OB_NOT_NULL(entry.second) && !entry.second->is_removed_) {
        entry.second->~ObVecIdxFollowerExecutors();
        entry.second = nullptr;
      }
      return OB_SUCCESS;
    };
    (void)ls_follower_executor_map_.foreach_refactored(destroy_follower_func);
    ls_follower_executor_map_.destroy();
  }
  if (OB_NOT_NULL(disabled_all_)) {
    ob_free(disabled_all_);
    disabled_all_ = nullptr;
  }
  if (OB_NOT_NULL(disable_entries_)) {
    ob_free(disable_entries_);
    disable_entries_ = nullptr;
  }
  is_inited_ = false;
  FLOG_INFO("vec idx async task scheduler destroyed", K_(tenant_id));
}

void ObVecIdxAsyncTaskScheduler::runTimerTask()
{
  ObCurTraceId::init(GCONF.self_addr_);
  run_timer_task();
}

bool ObVecIdxAsyncTaskScheduler::check_tenant_can_schedule_()
{
  bool bret = false;
  if (OB_INVALID_TENANT_ID == tenant_id_) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid tenant id");
  } else {
    int ret = OB_SUCCESS;
    bool is_restore = true;
    if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().check_tenant_is_restore(
            NULL, tenant_id_, is_restore))) {
      LOG_WARN("fail to check tenant is restore", KR(ret), K_(tenant_id), K(common::lbt()));
    } else {
      bret = !is_restore;
    }
  }
  return bret;
}

void ObVecIdxAsyncTaskScheduler::run_timer_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vec idx async task scheduler not inited", KR(ret));
  } else if (is_stopped_) {
    // stopped, do nothing
  } else if (!check_tenant_can_schedule_()) {
    if (REACH_TIME_INTERVAL(10L * 1000000)) {
      LOG_INFO("tenant is in restore or not ready, skip vec idx async task scheduling", K_(tenant_id));
    }
  } else if (check_can_do_work()) {
    ObTimeGuard tg("VecIdxScheduler::run_timer_task", VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD);
    if (!is_first_round_done_) {
      const int64_t now = ObTimeUtility::fast_current_time();
      if (0 == last_startup_cleanup_retry_ts_
          || now - last_startup_cleanup_retry_ts_ >= STARTUP_CLEANUP_RETRY_INTERVAL_US) {
        bool cleanup_done = false;
        int tmp_ret = cleanup_stale_tasks_on_startup_(cleanup_done);
        if (OB_SUCCESS != tmp_ret) {
          last_startup_cleanup_retry_ts_ = now;
          LOG_WARN("fail to cleanup stale tasks on startup", KR(tmp_ret), K_(tenant_id));
        } else if (cleanup_done) {
          is_first_round_done_ = true;
          LOG_INFO("[VEC_ASYNC_TASK] startup cleanup done",
                   K_(tenant_id), K_(startup_cleanup_cutoff_ts), K_(startup_cleanup_total_rows));
        } else {
          last_startup_cleanup_retry_ts_ = 0;
          LOG_INFO("[VEC_ASYNC_TASK] startup cleanup has more rows",
                   K_(tenant_id), K_(startup_cleanup_cutoff_ts), K_(startup_cleanup_total_rows));
        }
      }
      if (!is_first_round_done_) {
        // Do not load/schedule inner-table tasks until stale self-owned rows are swept;
        // otherwise a later retry might cancel a task that this observer just resumed.
        return;
      }
    }
    check_can_schedule();
    refresh_disable_list_config_();
    tg.click("refresh config");
    ObSEArray<std::pair<ObLSID, ObVecIdxLeaderExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> leader_executors_to_run;
    ObSEArray<std::pair<ObLSID, ObVecIdxFollowerExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> follower_executors_to_run;
    // sync_ls_executors is unconditionally executed each round to keep LS map up to date
    if (OB_FAIL(sync_ls_executors())) {
      LOG_WARN("fail to sync ls executors", KR(ret), K_(tenant_id));
    }
    tg.click("sync ls");
    if (OB_SUCC(ret) && OB_FAIL(load_triggered_tasks_())) {
      LOG_WARN("fail to load triggered tasks from inner table", KR(ret), K_(tenant_id));
    }
    tg.click("load triggered");
    if (OB_SUCC(ret) && OB_FAIL(collect_leader_executors_to_run(leader_executors_to_run))) {
      LOG_WARN("fail to collect leader executors to run", KR(ret), K_(tenant_id));
    }
    tg.click("collect leader executors");
    if (OB_SUCC(ret) && OB_FAIL(collect_follower_executors_to_run(follower_executors_to_run))) {
      LOG_WARN("fail to collect follower executors to run", KR(ret), K_(tenant_id));
    }
    tg.click("collect follower executors");
    uint64_t task_trace_base_num = 0;
    if (OB_SUCC(ret) && OB_FAIL(load_leader_tasks(leader_executors_to_run, task_trace_base_num))) {
      LOG_WARN("fail to load leader ls tasks", KR(ret), K_(tenant_id));
    }
    tg.click("load leader tasks");
    if (OB_SUCC(ret) && OB_FAIL(load_follower_tasks(follower_executors_to_run, task_trace_base_num))) {
      LOG_WARN("fail to load follower ls tasks", KR(ret), K_(tenant_id));
    }
    tg.click("load follower tasks");
    if (OB_SUCC(ret) && OB_FAIL(check_and_schedule_leader_ls_tasks(leader_executors_to_run))) {
      LOG_WARN("fail to schedule leader ls tasks", KR(ret), K_(tenant_id));
    }
    tg.click("schedule leader tasks");
    if (OB_SUCC(ret) && OB_FAIL(check_and_schedule_follower_ls_tasks(follower_executors_to_run))) {
      LOG_WARN("fail to schedule follower ls tasks", KR(ret), K_(tenant_id));
    }
    tg.click("schedule follower tasks");
    // Release executor refs acquired by collect_*_executors_to_run() before proceeding.
    // Must run unconditionally (even on error) to prevent deferred-destruction stalls.
    if (!leader_executors_to_run.empty() || !follower_executors_to_run.empty()) {
      common::ObSpinLockGuard guard(ls_executor_lock_);
      for (int64_t i = 0; i < leader_executors_to_run.count(); i++) {
        dec_leader_executor_ref_(*leader_executors_to_run.at(i).second);
      }
      for (int64_t i = 0; i < follower_executors_to_run.count(); i++) {
        dec_follower_executor_ref_(*follower_executors_to_run.at(i).second);
      }
    }
    tg.click("release executor refs");
    if (OB_SUCC(ret) && can_schedule(ObVectorTaskScheduleType::ADAPTER_MAINTENANCE)
        && OB_FAIL(check_and_execute_adapter_maintenance_tasks())) {
      LOG_WARN("fail to execute adapter maintenance tasks", KR(ret), K_(tenant_id));
    }
    tg.click("adapter maintenance");
    if (OB_SUCC(ret) && OB_FAIL(pop_tasks_to_work())) {
      LOG_WARN("fail to pop tasks to work", KR(ret), K_(tenant_id));
    }
    tg.click("pop to work");
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(check_and_finish_orphan_self_tasks_())) {
        LOG_WARN("fail to check orphan self vector async tasks", KR(tmp_ret), K_(tenant_id));
      }
    }
    tg.click("orphan task check");
    if (can_schedule(ObVectorTaskScheduleType::HISTORY_CLEANUP)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(do_history_cleanup_())) {
        LOG_WARN("fail to do history cleanup", KR(tmp_ret), K_(tenant_id));
      }
      tg.click("history cleanup");
    }
    LOG_TRACE("vec idx async task scheduler run_timer_task finish", KR(ret), K_(tenant_id), K(tg));
    schedule_finish();
  }
}

bool ObVecIdxAsyncTaskScheduler::check_can_do_work()
{
  const int64_t now = ObTimeUtility::fast_current_time();
  if (last_can_do_work_check_ts_ > 0
      && now - last_can_do_work_check_ts_ < CAN_DO_WORK_CACHE_INTERVAL_US) {
    return cached_can_do_work_;
  }
  bool bret = true;
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool is_oracle_mode = false;
  if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K_(tenant_id));
    // do not update cache timestamp on failure, retry next tick
    return false;
  } else if (is_oracle_mode) {
    bret = false;
    LOG_DEBUG("vector index not support oracle mode", K_(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
    return false;
    // Mixed-version compat: keep the lower bound aligned with the legacy
    // ObPluginVectorIndexLoadScheduler (4_3_3_0). When data_version < 4_6_0_1, the new
    // framework still drives in-memory scheduling on the new observer, but inner-table
    // reads/writes are short-circuited (see insert_new_task / update_vec_task /
    // load_triggered_tasks_ etc). This way old observers (running 460 binary with the
    // legacy scheduler) and new observers (running this binary with the new scheduler)
    // can co-exist during the upgrade window without conflicting on the task table.
  } else if (tenant_data_version < DATA_VERSION_4_3_3_0) {
    bret = false;
    LOG_DEBUG("vector async task scheduler can not work with data version less than 4_3_3_0", K(tenant_data_version));
  } else if (is_user_tenant(tenant_id_)) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id_), tenant_data_version))) {
      bret = false;
      LOG_WARN("get tenant data version failed", K(ret));
      return false;
    } else if (tenant_data_version < DATA_VERSION_4_3_3_0) {
      bret = false;
      LOG_DEBUG("vector async task scheduler can not work with data version less than 4_3_3_0", K(tenant_data_version));
    }
  }
  cached_can_do_work_ = bret;
  last_can_do_work_check_ts_ = now;
  return bret;
}

void ObVecIdxAsyncTaskScheduler::check_can_schedule()
{
  for (int i = 0; i < ObVectorTaskScheduleType::SCHEDULE_MAX; i++) {
    can_schedule_[i] = (ObTimeUtility::fast_current_time() - last_schedule_time_[i] > schedule_interval[i]);
  }
}

void ObVecIdxAsyncTaskScheduler::schedule_finish()
{
  for (int i = 0; i < ObVectorTaskScheduleType::SCHEDULE_MAX; i++) {
    if (can_schedule_[i]) {
      last_schedule_time_[i] = ObTimeUtility::fast_current_time();
      can_schedule_[i] = false;
    }
  }
}

int ObVecIdxAsyncTaskScheduler::cleanup_stale_tasks_on_startup_(bool &cleanup_done)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  cleanup_done = false;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K_(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_0_1) {
    // just print_log clean task is skip this time, need to recheck in next timer tick
    LOG_INFO("skip cleanup stale tasks, data version < 4.6.0.1", K_(tenant_id), K(data_version));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else {
    char addr_buf[MAX_IP_PORT_LENGTH] = { 0 };
    GCTX.self_addr().ip_port_to_string(addr_buf, sizeof(addr_buf));
    if (startup_cleanup_cutoff_ts_ <= 0) {
      startup_cleanup_cutoff_ts_ = ObTimeUtility::current_time();
    }
    const int64_t now = ObTimeUtility::current_time();
    ObSqlString sql;
    ObSEArray<ObStartupCleanupTaskKey, STARTUP_CLEANUP_BATCH_SIZE> task_keys;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT tenant_id, table_id, tablet_id, task_id FROM %s"
            " WHERE tenant_id = %ld AND exec_addr = '%s'"
            " AND status IN (%ld, %ld, %ld, %ld, %ld, %ld)"
            " AND gmt_modified < usec_to_time(%ld)"
            " ORDER BY tenant_id, table_id, tablet_id, task_id LIMIT %ld",
            OB_ALL_VECTOR_INDEX_TASK_TNAME,
            ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_),
            addr_buf,
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_QUEUE),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL),
            startup_cleanup_cutoff_ts_,
            STARTUP_CLEANUP_BATCH_SIZE))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
          LOG_WARN("fail to query startup cleanup tasks", KR(ret), K(sql), K_(tenant_id));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query result is null", KR(ret), K_(tenant_id));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END != ret) {
                LOG_WARN("fail to get next startup cleanup row", KR(ret), K(sql));
              }
            } else {
              uint64_t task_tenant_id = OB_INVALID_TENANT_ID;
              uint64_t table_id = OB_INVALID_ID;
              uint64_t tablet_id = 0;
              int64_t task_id = 0;
              EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", task_tenant_id, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", tablet_id, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "task_id", task_id, int64_t);
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(task_keys.push_back(
                          ObStartupCleanupTaskKey(task_tenant_id, table_id, tablet_id, task_id)))) {
                LOG_WARN("fail to push startup cleanup task key", KR(ret),
                         K(task_tenant_id), K(table_id), K(tablet_id), K(task_id));
              }
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }
    int64_t batch_cleanup_rows = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < task_keys.count(); ++i) {
      int64_t affect_rows = 0;
      const ObStartupCleanupTaskKey &key = task_keys.at(i);
      if (OB_FAIL(sql.assign_fmt(
              "UPDATE %s SET status = %ld, ret_code = %d,"
              " err_msg = 'task cancelled due to observer restart',"
              " end_time = usec_to_time(%ld)"
              " WHERE tenant_id = %ld AND table_id = %ld AND tablet_id = %ld AND task_id = %ld"
              " AND exec_addr = '%s'"
              " AND status IN (%ld, %ld, %ld, %ld, %ld, %ld)"
              " AND gmt_modified < usec_to_time(%ld)",
              OB_ALL_VECTOR_INDEX_TASK_TNAME,
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH),
              OB_CANCELED,
              now,
              key.tenant_id_,
              key.table_id_,
              key.tablet_id_,
              key.task_id_,
              addr_buf,
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_QUEUE),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL),
              startup_cleanup_cutoff_ts_))) {
        LOG_WARN("fail to assign cleanup update sql", KR(ret), K(key));
      } else if (OB_FAIL(GCTX.sql_proxy_->write(tenant_id_, sql.ptr(), affect_rows))) {
        LOG_WARN("fail to execute startup cleanup update sql", KR(ret), K(sql), K(key));
      } else {
        batch_cleanup_rows += affect_rows;
      }
    }
    if (OB_SUCC(ret)) {
      startup_cleanup_total_rows_ += batch_cleanup_rows;
      cleanup_done = (task_keys.count() < STARTUP_CLEANUP_BATCH_SIZE);
      LOG_INFO("[VEC_ASYNC_TASK] cleanup stale tasks on startup",
               K_(tenant_id), K(addr_buf), K(task_keys.count()), K(batch_cleanup_rows),
               K_(startup_cleanup_cutoff_ts));
    }
    // Stage 2 (mixed-version compat): cancel rows written by the legacy 460
    // ObPluginVectorIndexLoadScheduler that were left orphaned after the upgrade.
    // Those rows are identifiable by exec_addr IS NULL because the legacy INSERT
    // never populated this column (introduced in 4_6_0_1). Skip STANDBY rows: a
    // freshly inserted MANUAL task by DBMS_VECTOR also has exec_addr IS NULL until
    // claim_triggered_task_ promotes it to RUNNING — it must be left alone so
    // process_triggered_tasks_ can pick it up.
    // Only proceed once stage 1 has finished draining; otherwise we may interleave
    // with self-addr cleanup paging.
    if (OB_SUCC(ret) && cleanup_done) {
      int64_t legacy_cleanup_rows = 0;
      if (OB_FAIL(sql.assign_fmt(
              "UPDATE %s SET status = %ld, ret_code = %d,"
              " err_msg = 'legacy task cancelled after upgrade to 4.6.0.1',"
              " end_time = usec_to_time(%ld)"
              " WHERE tenant_id = %ld AND exec_addr IS NULL"
              " AND status IN (%ld, %ld, %ld, %ld, %ld, %ld)"
              " AND gmt_modified < usec_to_time(%ld)"
              " LIMIT %ld",
              OB_ALL_VECTOR_INDEX_TASK_TNAME,
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH),
              OB_CANCELED,
              now,
              ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_QUEUE),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN),
              static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL),
              startup_cleanup_cutoff_ts_,
              STARTUP_CLEANUP_BATCH_SIZE))) {
        LOG_WARN("fail to assign legacy cleanup update sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->write(tenant_id_, sql.ptr(), legacy_cleanup_rows))) {
        LOG_WARN("fail to execute legacy cleanup update sql", KR(ret), K(sql));
      } else {
        startup_cleanup_total_rows_ += legacy_cleanup_rows;
        // If this batch hit the LIMIT, more legacy rows may remain — defer cleanup_done
        // so the next timer tick continues the loop. Concurrent observers running the
        // same UPDATE are idempotent: rows already advanced to FINISH no longer match
        // the WHERE clause.
        if (legacy_cleanup_rows >= STARTUP_CLEANUP_BATCH_SIZE) {
          cleanup_done = false;
        }
        LOG_INFO("[VEC_ASYNC_TASK] cleanup legacy 460 stale tasks on startup",
                 K_(tenant_id), K(legacy_cleanup_rows), K(cleanup_done),
                 K_(startup_cleanup_cutoff_ts));
      }
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::do_history_cleanup_()
{
  int ret = OB_SUCCESS;
  bool has_leader = false;
  {
    common::ObSpinLockGuard guard(ls_executor_lock_);
    has_leader = (ls_leader_executor_map_.size() > 0);
  }
  if (!has_leader) {
  } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
  } else if (OB_FAIL(move_task_to_history_table_())) {
    LOG_WARN("fail to move task to history table", KR(ret), K_(tenant_id));
  } else if (is_stopped_) {
  } else if (OB_FAIL(clear_history_task_())) {
    LOG_WARN("fail to clear history task", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::move_task_to_history_table_()
{
  int ret = OB_SUCCESS;
  int64_t move_rows = HISTORY_MOVE_BATCH_SIZE;
  int64_t total_moved = 0;
  while (OB_SUCC(ret) && move_rows != 0 && total_moved < HISTORY_MOVE_MAX_ROWS_PER_ROUND) {
    ObMySQLTransaction trans;
    if (is_stopped_) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::move_task_to_history_table(
                   tenant_id_, HISTORY_MOVE_BATCH_SIZE, trans, move_rows))) {
      LOG_WARN("fail to move task to history table", KR(ret), K_(tenant_id));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      total_moved += move_rows;
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::clear_history_task_()
{
  int ret = OB_SUCCESS;
  int64_t clear_rows = 0;
  ObMySQLTransaction trans;
  if (is_stopped_) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::clear_history_expire_task_record(
                 tenant_id_, HISTORY_CLEAR_BATCH_SIZE, trans, clear_rows))) {
    LOG_WARN("fail to clear history task", KR(ret), K_(tenant_id));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_tenant_memory()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id_, current_memory_config_))) {
    LOG_WARN("failed to get vector mem limit size", K(ret), K_(tenant_id));
    ret = OB_SUCCESS;
    current_memory_config_ = 0;
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_has_vector_index(common::ObIArray<uint64_t> &vec_table_id_array)
{
  int ret = OB_SUCCESS;
  bool has_ivf_index = false;
  if (OB_FAIL(ObPluginVectorIndexUtils::get_tenant_vector_index_ids(
          tenant_id_, has_ivf_index, vec_table_id_array))) {
    LOG_WARN("fail to get tenant vector index ids", K(ret), K_(tenant_id));
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_schema_version(bool &schema_version_changed)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  int64_t schema_version = 0;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret), K_(tenant_id));
  } else if (!ObSchemaService::is_formal_version(schema_version)) {
    ret = OB_EAGAIN;
    LOG_INFO("is not a formal_schema_version", KR(ret), K(schema_version));
  } else if (local_schema_version_ == OB_INVALID_VERSION ||  local_schema_version_ < schema_version) {
    FLOG_INFO("schema changed", KR(ret), K_(local_schema_version), K(schema_version));
    local_schema_version_ = schema_version;
    schema_version_changed = true;
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_index_adapter_exist(ObPluginVectorIndexMgr *mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(mgr));
  } else if (!mgr->get_partial_adapter_map().empty() || !mgr->get_complete_adapter_map().empty()) {
    // partial map not empty, exist adapter create by dml/ddl data complement/query
    // complete adapter not empty, also need check for transfer
    set_need_maintenance(mgr);
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_need_maintenance_ls_follower(ObPluginVectorIndexMgr *mgr)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", K(ret), K(MTL_ID()));
  } else if ((!mgr->get_ls_leader() && tenant_config->load_vector_index_on_follower) || mgr->need_refresh_memdata()) {
    set_need_maintenance(mgr);
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_need_maintenance(ObPluginVectorIndexMgr *mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_index_adapter_exist(mgr))) {
    LOG_WARN("fail to check exist paritial index adapter", KR(ret));
  } else if (OB_FAIL(check_need_maintenance_ls_follower(mgr))) {
    LOG_WARN("fail to check need maintenance ls follower", KR(ret));
  }
  return ret;
}

void ObVecIdxAsyncTaskScheduler::set_need_maintenance(ObPluginVectorIndexMgr *mgr)
{
  int ret = OB_SUCCESS;
  if (common::ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
    mgr->set_need_maintenance(true);
    FLOG_INFO("finish set need maintenance", K(mgr->get_ls_id()));
  }
}

int ObVecIdxAsyncTaskScheduler::check_is_vector_index_table(const ObSimpleTableSchemaV2 &table_schema,
                                                                  bool &is_vector_index_table,
                                                                  bool &is_shared_index_table)
{
  int ret = OB_SUCCESS;
  is_vector_index_table = false;
  is_shared_index_table = false;
  if (table_schema.is_index_table() && !table_schema.is_in_recyclebin()) {
    if (table_schema.is_vec_delta_buffer_type()
        || table_schema.is_vec_index_id_type()
        || table_schema.is_vec_index_snapshot_data_type()
        || table_schema.is_hybrid_vec_index_log_type()
        || table_schema.is_hybrid_vec_index_embedded_type()) {
      is_vector_index_table = true;
    } else if (table_schema.is_vec_rowkey_vid_type()
        || table_schema.is_vec_vid_rowkey_type()) {
      is_shared_index_table = true;
    }
  }
  return ret;
}

void ObVecIdxAsyncTaskScheduler::clean_deprecated_adapters(
    storage::ObLS *ls,
    ObPluginVectorIndexMgr *mgr,
    const bool is_ls_leader)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, DEFAULT_TABLE_ARRAY_SIZE> delete_tablet_id_array;
  bool clear_ls_follower_adapter = false;
  if (OB_ISNULL(ls) || OB_ISNULL(mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls), KP(mgr));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail get tenant_config", K(ret), K_(tenant_id));
    } else {
      clear_ls_follower_adapter = !is_ls_leader && !tenant_config->load_vector_index_on_follower;
    }
  }

  // get schema_guard once outside the loop
  ObSchemaGetterGuard schema_guard;
  if (OB_SUCC(ret) && OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                           tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K_(tenant_id));
  }

  if (OB_SUCC(ret)) {
    {
      common::RWLock::RLockGuard lock_guard(mgr->get_adapter_map_lock());
      FOREACH_X(iter, mgr->get_complete_adapter_map(), OB_SUCC(ret)) {
        ObPluginVectorIndexAdaptor *adapter = iter->second;
        const ObSimpleTableSchemaV2 *table_schema = nullptr;
        ObTabletID tablet_id = iter->first;
        ObTabletHandle tablet_handle;
        bool need_delete = false;
        if (OB_ISNULL(adapter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("adapter is null", K(ret), K(tablet_id));
        } else if (OB_FAIL(schema_guard.get_simple_table_schema(
                       tenant_id_, adapter->get_vbitmap_table_id(), table_schema))) {
          if (OB_TABLE_NOT_EXIST == ret || OB_TENANT_NOT_EXIST == ret) {
            ret = OB_SUCCESS; // table/tenant gone, treat as missing schema
          } else {
            LOG_WARN("fail to get vbitmap table schema", KR(ret), K_(tenant_id),
                     K(adapter->get_vbitmap_table_id()));
          }
        } else if (OB_ISNULL(table_schema) || table_schema->is_in_recyclebin()) {
          // schema not exist / in recyclebin  -> mark for deletion
          need_delete = true;
          adapter->set_need_cancel_task();
          if (OB_FAIL(delete_tablet_id_array.push_back(adapter->get_inc_tablet_id()))) {
            LOG_WARN("push back table id failed",
              K(delete_tablet_id_array.count()), K(adapter->get_inc_tablet_id()), KR(ret));
          } else if (OB_FAIL(delete_tablet_id_array.push_back(adapter->get_vbitmap_tablet_id()))) {
            LOG_WARN("push back table id failed",
              K(delete_tablet_id_array.count()), K(adapter->get_vbitmap_tablet_id()), KR(ret));
          } else if (OB_FAIL(delete_tablet_id_array.push_back(adapter->get_snap_tablet_id()))) {
            LOG_WARN("push back table id failed",
              K(delete_tablet_id_array.count()), K(adapter->get_snap_tablet_id()), KR(ret));
          } else if (OB_FAIL(delete_tablet_id_array.push_back(adapter->get_embedded_tablet_id()))) {
            LOG_WARN("push back table id failed",
              K(delete_tablet_id_array.count()), K(adapter->get_embedded_tablet_id()), KR(ret));
          }
        } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(tablet_id, tablet_handle))) {
          if (OB_TABLET_NOT_EXIST != ret) {
            LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
          } else {
            ret = OB_SUCCESS; // not found, moved from this ls
            need_delete = true;
            adapter->set_need_cancel_task();
            if (OB_FAIL(delete_tablet_id_array.push_back(tablet_id))) {
              LOG_WARN("push tablet id failed", K(ret), K(tablet_id));
            }
          }
        } else {
          // Tablet still exists. If its schema truncate_version moved forward,
          // cancel any in-flight async task ctx whose truncate_version_ is older
          // so a fresh task can be scheduled against the truncated data.
          const int64_t truncate_version = table_schema->get_truncate_version();
          int tmp_ret = cancel_tablet_async_tasks_for_truncated_(
              tablet_id, truncate_version, mgr->get_async_task_opt());
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to cancel async tasks for truncated tablet",
                     KR(tmp_ret), K(tablet_id), K(truncate_version));
          }
          if (clear_ls_follower_adapter) {
            need_delete = true;
            adapter->set_need_cancel_task();
            if (OB_FAIL(delete_tablet_id_array.push_back(tablet_id))) {
              LOG_WARN("push tablet id failed", K(ret), K(tablet_id));
            }
          }
        }
      }
    }

    // cancel async tasks before erasing adapters
    if (OB_SUCC(ret) && !delete_tablet_id_array.empty()) {
      LOG_INFO("try erase complete vector index adapter",
        K(mgr->get_ls_id()), K(delete_tablet_id_array.count()));
      ObVecIndexAsyncTaskOption &task_opt = mgr->get_async_task_opt();
      for (int64_t i = 0; i < delete_tablet_id_array.count(); ++i) {
        int tmp_ret = cancel_tablet_async_tasks_(delete_tablet_id_array.at(i), task_opt);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to cancel async tasks for tablet", KR(tmp_ret), K(delete_tablet_id_array.at(i)));
        }
      }
    }
    {
      common::RWLock::WLockGuard lock_guard(mgr->get_adapter_map_lock());
      for (int64_t i = 0; OB_SUCC(ret) && i < delete_tablet_id_array.count(); ++i) {
        if (OB_FAIL(mgr->erase_complete_adapter(delete_tablet_id_array.at(i)))) {
          if (ret == OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to erase complete vector index adapter", K(mgr->get_ls_id()), K(ret), K(delete_tablet_id_array.at(i)));
          }
        }
      }
    }

    delete_tablet_id_array.reset();
    if (OB_SUCC(ret)) {
      common::hash::ObHashSet<uintptr_t> full_partial_adaptor_hash_set;
      common::RWLock::RLockGuard lock_guard(mgr->get_adapter_map_lock());
      if (OB_SUCC(ret) && mgr->get_partial_adapter_map().size() > 0) {
        if (OB_FAIL(full_partial_adaptor_hash_set.create(mgr->get_partial_adapter_map().size()))) {
          LOG_WARN("fail to create partial adaptor hash set", K(ret), K(mgr->get_partial_adapter_map().size()));
        } else {
          FOREACH_X(iter, mgr->get_partial_adapter_map(), OB_SUCC(ret)) {
            ObPluginVectorIndexAdaptor *adapter = iter->second;
            ObTabletID tablet_id = iter->first;
            ObTabletHandle tablet_handle;
            if (OB_ISNULL(adapter)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("adapter is null", K(ret), K(tablet_id));
            } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(tablet_id, tablet_handle))) {
              if (OB_TABLET_NOT_EXIST != ret) {
                LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
              } else {
                ret = OB_SUCCESS;
                adapter->set_need_cancel_task();
                if (OB_FAIL(delete_tablet_id_array.push_back(tablet_id))) {
                  LOG_WARN("push tablet id failed", K(ret), K(tablet_id));
                }
              }
            } else if (adapter->get_create_type() == CreateTypeFullPartial) {
              uintptr_t adapter_addr = reinterpret_cast<uintptr_t>(adapter);
              int tmp_ret = full_partial_adaptor_hash_set.exist_refactored(adapter_addr);
              if (OB_HASH_EXIST == tmp_ret) {
              } else if (OB_HASH_NOT_EXIST == tmp_ret) {
                if (OB_FAIL(full_partial_adaptor_hash_set.set_refactored(adapter_addr))) {
                  LOG_WARN("fail to set adapter address to hashset", K(ret), K(adapter_addr));
                } else {
                  adapter->inc_idle();
                  if (adapter->is_deprecated()) {
                    adapter->set_need_cancel_task();
                    if (OB_FAIL(delete_tablet_id_array.push_back(tablet_id))) {
                      LOG_WARN("push tablet id failed", K(ret), K(tablet_id));
                    }
                  }
                }
              } else {
                ret = tmp_ret;
                LOG_WARN("fail to check adapter address", K(ret), K(adapter_addr));
              }
            } else {
              adapter->inc_idle();
              if (adapter->is_deprecated()) {
                adapter->set_need_cancel_task();
                if (OB_FAIL(delete_tablet_id_array.push_back(tablet_id))) {
                  LOG_WARN("push tablet id failed", K(ret), K(tablet_id));
                }
              }
            }
          }
        }
      }
    }

    // cancel async tasks before erasing partial adapters
    if (OB_SUCC(ret) && !delete_tablet_id_array.empty()) {
      LOG_INFO("try erase partial vector index adapter",
        K(mgr->get_ls_id()), K(delete_tablet_id_array.count()));
      ObVecIndexAsyncTaskOption &task_opt = mgr->get_async_task_opt();
      for (int64_t i = 0; i < delete_tablet_id_array.count(); ++i) {
        int tmp_ret = cancel_tablet_async_tasks_(delete_tablet_id_array.at(i), task_opt);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to cancel async tasks for partial tablet", KR(tmp_ret), K(delete_tablet_id_array.at(i)));
        }
      }
    }
    {
      common::RWLock::WLockGuard lock_guard(mgr->get_adapter_map_lock());
      for (int64_t i = 0; OB_SUCC(ret) && i < delete_tablet_id_array.count(); ++i) {
        if (OB_FAIL(mgr->erase_partial_adapter(delete_tablet_id_array.at(i)))) {
          if (ret == OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to erase partial vector index adapter", K(ret), K(delete_tablet_id_array.at(i)));
          }
        }
      }
    }

    delete_tablet_id_array.reset();
  }
}

int ObVecIdxAsyncTaskScheduler::acquire_adapter_in_maintenance(
    storage::ObLS *ls,
    const share::ObLSID &ls_id,
    const int64_t table_id,
    const ObTableSchema *table_schema,
    ObVecIdxSharedTableInfoMap &shared_table_info_map)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type;
  ObArray<ObTabletID> tablet_ids;
  if (OB_ISNULL(ls) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls), KP(table_schema));
  } else if (OB_FALSE_IT(index_type = table_schema->get_index_type())) {
    // do nothing
  } else if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("fail to get tablet ids", K(ret), K(table_id));
  } else {
    ObTabletHandle tablet_handle;
    ObVectorIndexSharedTableInfo info;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      if (OB_FAIL(ls->get_tablet_svr()->get_tablet(tablet_ids.at(i), tablet_handle))) {
        if (ret == OB_TABLET_NOT_EXIST) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get tablet", K(ret), K(tablet_ids.at(i)));
        }
      } else {
        ObPluginVectorIndexAdapterGuard adapter_guard;
        ObString index_identity;
        // Notice:only no.3 aux table has vec_idx_params
        ObString vec_idx_params = table_schema->get_index_params();
        int64_t dim = 0;
        ObTabletID data_tablet_id = tablet_handle.get_obj()->get_data_tablet_id();
        if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(*table_schema, dim))) {
          LOG_WARN("fail to get vector index dim", K(ret), K(table_id));
        } else if (OB_FAIL(service_->acquire_adapter_guard(
                       ls_id, tablet_ids.at(i), index_type, adapter_guard, &vec_idx_params, dim))) {
          LOG_WARN("fail to acquire adapter guard", K(ret), K(ls_id), K(tablet_ids.at(i)));
        } else if (adapter_guard.get_adatper()->is_complete()) {
          // already exist full adapter, bypass
        } else if (OB_FAIL(adapter_guard.get_adatper()->set_table_id(
                       ObPluginVectorIndexUtils::index_type_to_record_type(index_type), table_id))) {
          LOG_WARN("fail to set table id", K(ret), K(table_id));
        } else if (OB_FAIL(adapter_guard.get_adatper()->set_tablet_id(VIRT_DATA, data_tablet_id))) {
          LOG_WARN("fail to set data tablet id", K(ret), K(data_tablet_id));
        } else if (OB_FAIL(ObPluginVectorIndexUtils::get_vector_index_prefix(*table_schema, index_identity))) {
          LOG_WARN("fail to get index identity", K(ret), K(table_id));
        } else if (OB_FAIL(adapter_guard.get_adatper()->set_index_identity(index_identity))) {
          LOG_WARN("fail to set index identity", K(ret), K(index_identity));
        } else {
          adapter_guard.get_adatper()->reset_idle();
        }

        if (OB_SUCC(ret)) {
          int tmp_ret = shared_table_info_map.get_refactored(data_tablet_id, info);
          if (OB_HASH_NOT_EXIST == tmp_ret) {
            info.data_table_id_ = table_schema->get_data_table_id();
            if (OB_FAIL(shared_table_info_map.set_refactored(data_tablet_id, info))) {
              LOG_WARN("fail to set shared table info", K(ret), K(data_tablet_id));
            }
          } else if (OB_SUCCESS != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("fail to get shared table info", K(ret), K(data_tablet_id));
          } else {
            info.data_table_id_ = table_schema->get_data_table_id();
            if (OB_FAIL(shared_table_info_map.set_refactored(data_tablet_id, info, 1))) {
              LOG_WARN("fail to overwrite shared table info", K(ret), K(data_tablet_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::set_shared_table_info_in_maintenance(
    storage::ObLS *ls,
    const int64_t table_id,
    const ObSimpleTableSchemaV2 *table_schema,
    ObVecIdxSharedTableInfoMap &shared_table_info_map)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type;
  ObArray<ObTabletID> tablet_ids;
  if (OB_ISNULL(ls) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls), KP(table_schema));
  } else if (OB_FALSE_IT(index_type = table_schema->get_index_type())) {
    // do nothing
  } else if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("fail to get tablet ids", K(ret), K(table_id));
  } else {
    ObTabletHandle tablet_handle;
    ObVectorIndexSharedTableInfo info;
    ObTabletID data_tablet_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      if (OB_FAIL(ls->get_tablet_svr()->get_tablet(tablet_ids.at(i), tablet_handle))) {
        if (ret == OB_TABLET_NOT_EXIST) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get tablet", K(ret), K(tablet_ids.at(i)));
        }
      } else if (FALSE_IT(data_tablet_id = tablet_handle.get_obj()->get_data_tablet_id())) {
      } else {
        int tmp_ret = shared_table_info_map.get_refactored(data_tablet_id, info);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          if (index_type == INDEX_TYPE_VEC_ROWKEY_VID_LOCAL) {
            info.rowkey_vid_table_id_ = table_id;
            info.rowkey_vid_tablet_id_ = tablet_ids.at(i);
          } else {
            info.vid_rowkey_table_id_ = table_id;
            info.vid_rowkey_tablet_id_ = tablet_ids.at(i);
          }
          info.data_table_id_ = table_schema->get_data_table_id();
          if (OB_FAIL(shared_table_info_map.set_refactored(data_tablet_id, info))) {
            LOG_WARN("fail to set shared table info", K(ret), K(data_tablet_id));
          }
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to get shared table info", K(ret), K(data_tablet_id));
        } else {
          if (index_type == INDEX_TYPE_VEC_ROWKEY_VID_LOCAL) {
            info.rowkey_vid_table_id_ = table_id;
            info.rowkey_vid_tablet_id_ = tablet_ids.at(i);
          } else {
            info.vid_rowkey_table_id_ = table_id;
            info.vid_rowkey_tablet_id_ = tablet_ids.at(i);
          }
          info.data_table_id_ = table_schema->get_data_table_id();
          if (OB_FAIL(shared_table_info_map.set_refactored(data_tablet_id, info, 1))) {
            LOG_WARN("fail to overwrite shared table info", K(ret), K(data_tablet_id));
          }
        }
      }
    }
  }
  return ret;
}

// scan all vector tablet in current LS
int ObVecIdxAsyncTaskScheduler::execute_adapter_maintenance_for_ls(
    storage::ObLS *ls,
    ObPluginVectorIndexMgr *mgr,
    const common::ObIArray<uint64_t> &vec_table_id_array,
    const bool is_ls_leader)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObPluginVectorIndexLoadScheduler::check_and_generate_tablet_tasks",
    VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD);
  ObVecIdxSharedTableInfoMap shared_table_info_map;
  ObMemAttr memattr(tenant_id_, "VecIdxInfo");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet vector index scheduler not init", KR(ret));
  } else if (OB_ISNULL(ls) || OB_ISNULL(mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls), KP(mgr));
  } else {
    clean_deprecated_adapters(ls, mgr, is_ls_leader);
  }
  if (OB_FAIL(ret)) {
  } else if (current_memory_config_ == 0) {
  } else if (!vec_table_id_array.empty()
             && OB_FAIL(shared_table_info_map.create(DEFAULT_TABLE_ARRAY_SIZE, memattr, memattr))) {
    LOG_WARN("fail to create shared table info map", K(ret), K(ls->get_ls_id()));
  } else {
    bool need_maintenance = true;
    if (OB_SUCC(ret)) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      if (!tenant_config.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail get tenant_config", K(ret), K(MTL_ID()));
      } else if (!is_ls_leader && !tenant_config->load_vector_index_on_follower) {
        need_maintenance = false;
        LOG_INFO("do not need maintenance ls follower", K(ret), K(tenant_config->load_vector_index_on_follower), K(is_ls_leader));
      }
    }

    int64_t start_idx = 0;
    int64_t end_idx = 0;
    while (OB_SUCC(ret) && need_maintenance && start_idx < vec_table_id_array.count()) {
      ObSchemaGetterGuard schema_guard;
      start_idx = end_idx;
      end_idx = MIN(vec_table_id_array.count(), start_idx + TABLE_GENERATE_BATCH_SIZE);
      bool is_vector_index = false;
      bool is_shared_index = false;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret), K_(tenant_id));
      }
      for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx; ++idx) {
        const int64_t table_id = vec_table_id_array.at(idx);
        const ObSimpleTableSchemaV2 *table_schema = nullptr;
        if (is_sys_table(table_id)) {
          // do nothing
        } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id, table_schema))) {
          LOG_WARN("failed to get simple schema", K(ret), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table schema is null", K(ret), K(table_id), K_(tenant_id));
        } else if (table_schema->is_in_recyclebin()) {
          // do nothing
        } else if (OB_FAIL(check_is_vector_index_table(*table_schema,
                                                       is_vector_index,
                                                       is_shared_index))) {
          LOG_WARN("fail to check is vector index", K(ret), K(table_id));
        } else if (is_vector_index) {
          const ObTableSchema *tmp_table_schema = nullptr;
          if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, tmp_table_schema))) {
            LOG_WARN("failed to get table schema", K(ret), K(table_id));
          } else if (OB_ISNULL(tmp_table_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("table schema is null", K(ret), K(table_id), K_(tenant_id));
          } else if (OB_FAIL(acquire_adapter_in_maintenance(
                         ls, ls->get_ls_id(), table_id, tmp_table_schema, shared_table_info_map))) {
            LOG_WARN("fail to acquire adapter in maintenance", K(ret), K(table_id));
          }
        } else if (is_shared_index) {
          if (OB_FAIL(set_shared_table_info_in_maintenance(
                         ls, table_id, table_schema, shared_table_info_map))) {
            LOG_WARN("fail to set shared table info", K(ret), K(table_id));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(service_->check_and_merge_adapter(ls->get_ls_id(), shared_table_info_map))) {
      LOG_WARN("fail to merge partial adapter task", K(ret), K(ls->get_ls_id()));
    } else {
      mgr->set_need_maintenance(false);
    }
  }
  LOG_INFO("finish generate tenant tablet tasks", KR(ret), K_(tenant_id), KP(ls));
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_and_execute_adapter_maintenance_tasks()
{
  int ret = OB_SUCCESS;
  bool schema_version_changed = false;
  ObSEArray<uint64_t, DEFAULT_TABLE_ARRAY_SIZE> vec_table_id_array;
  // if schema version change, or exist partial adapter(create by access) need do maintenance
  if (OB_FAIL(check_tenant_memory())) {
    LOG_WARN("check vector index resource failed", K(ret), K_(tenant_id));
  } else if (OB_FAIL(check_has_vector_index(vec_table_id_array))) {
    LOG_WARN("check vector index schema failed", K(ret), K_(tenant_id));
  } else if (OB_FAIL(check_schema_version(schema_version_changed))) {
    LOG_WARN("fail to check schema version", K(ret), K_(tenant_id));
  } else {
    ObSEArray<share::ObLSID, DEFAULT_LS_EXECUTOR_MAP_SIZE> ls_ids_to_maintain;
    {
      common::ObSpinLockGuard guard(ls_executor_lock_);
      LSIndexMgrMap &mgr_map = service_->get_ls_index_mgr_map();
      FOREACH_X(iter, ls_leader_executor_map_, OB_SUCC(ret)) {
        ObPluginVectorIndexMgr *mgr = nullptr;
        if (OB_FAIL(mgr_map.get_refactored(iter->first, mgr))) {
          LOG_WARN("fail to get ls mgr", K(ret), K(iter->first));
        } else if (OB_ISNULL(mgr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mgr is null", K(ret), K(iter->first));
        } else if (schema_version_changed) {
          set_need_maintenance(mgr);
        } else if(OB_FAIL(check_need_maintenance(mgr))) {
          LOG_WARN("fail to check need maintenance", K(ret), K(iter->first));
        }
        if (OB_SUCC(ret) && mgr->need_maintenance()) {
          if (OB_FAIL(ls_ids_to_maintain.push_back(iter->first))) {
            LOG_WARN("fail to push ls id to maintenance list", K(ret), K(iter->first));
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids_to_maintain.count(); ++i) {
      ObPluginVectorIndexMgr *mgr = nullptr;
      ObLSHandle ls_handle;
      ObLSService *ls_svr = MTL(ObLSService *);
      const share::ObLSID &ls_id = ls_ids_to_maintain.at(i);
      if (OB_ISNULL(ls_svr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls service is null", K(ret));
      } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
        LOG_WARN("fail to get ls", K(ret), K(ls_id));
      } else if (OB_ISNULL(ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null ls", K(ret), K(ls_id));
      } else if (OB_FAIL(service_->acquire_vector_index_mgr(ls_id, mgr))) {
        LOG_WARN("fail to acquire vector index mgr", K(ret), K(ls_id));
      } else if (OB_ISNULL(mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mgr is null", K(ret), K(ls_id));
      } else if (OB_FAIL(execute_adapter_maintenance_for_ls(
                     ls_handle.get_ls(), mgr, vec_table_id_array, mgr->get_ls_leader()))) {
        LOG_WARN("fail to execute adapter maintenance for ls", K(ret), K(ls_id));
      }
      if (OB_NOT_NULL(mgr)) {
        mgr->dump_all_inst();
      }
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::sync_ls_executors()
{
  int ret = OB_SUCCESS;
  ObSEArray<share::ObLSID, 16> stale_ls_ids;
  ObSEArray<share::ObLSID, 16> new_ls_ids;

  // Step 1 & 2: under lock, collect stale IDs (from leader map, follower map has the same key set),
  // remove them from both maps, then collect new LS IDs.
  {
    common::ObSpinLockGuard guard(ls_executor_lock_);
    LSIndexMgrMap &mgr_map = service_->get_ls_index_mgr_map();

    auto collect_stale_func = [&](common::hash::HashMapPair<share::ObLSID, ObVecIdxLeaderExecutors *> &entry) -> int {
      int inner_ret = OB_SUCCESS;
      ObPluginVectorIndexMgr *mgr = nullptr;
      if (OB_SUCCESS != (inner_ret = mgr_map.get_refactored(entry.first, mgr))) {
        if (OB_HASH_NOT_EXIST == inner_ret) {
          inner_ret = OB_SUCCESS;
          if (OB_SUCCESS != (inner_ret = stale_ls_ids.push_back(entry.first))) {
            LOG_WARN("fail to push stale ls id", K(inner_ret), K(entry.first));
          }
        } else {
          LOG_WARN("fail to get ls mgr", K(inner_ret), K(entry.first));
        }
      }
      return inner_ret;
    };
    if (OB_FAIL(ls_leader_executor_map_.foreach_refactored(collect_stale_func))) {
      LOG_WARN("fail to collect stale ls executors", KR(ret), K_(tenant_id));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < stale_ls_ids.count(); i++) {
      const share::ObLSID &stale_id = stale_ls_ids.at(i);
      ObVecIdxLeaderExecutors *leader_exec = nullptr;
      ObVecIdxFollowerExecutors *follower_exec = nullptr;
      if (OB_FAIL(ls_leader_executor_map_.erase_refactored(stale_id, &leader_exec))) {
        LOG_WARN("fail to erase stale leader executor", KR(ret), K(stale_id));
      } else if (OB_NOT_NULL(leader_exec)) {
        retire_leader_executor_(leader_exec);
      }
      int tmp_ret = ls_follower_executor_map_.erase_refactored(stale_id, &follower_exec);
      if (OB_SUCCESS != tmp_ret && OB_HASH_NOT_EXIST != tmp_ret) {
        LOG_WARN("fail to erase stale follower executor", K(tmp_ret), K(stale_id));
      } else if (OB_NOT_NULL(follower_exec)) {
        retire_follower_executor_(follower_exec);
      }
    }

    auto collect_new_func = [&](common::hash::HashMapPair<share::ObLSID, ObPluginVectorIndexMgr *> &entry) -> int {
      int inner_ret = OB_SUCCESS;
      ObVecIdxLeaderExecutors *existing = nullptr;
      if (!entry.first.is_user_ls()) {
        // Non-user LS (e.g. SYS LS) does not carry vector index data; skip to avoid
        // leaking SHARE_MOD ObLSHandle refs that block LS safe-destroy on tenant GC.
      } else if (OB_SUCCESS == ls_leader_executor_map_.get_refactored(entry.first, existing)) {
        // already exists, skip
      } else if (OB_NOT_NULL(entry.second) && entry.second->is_vec_idx_async_executor_bind_stopped()) {
        // LoadScheduler::stop(): do not queue ls_id for new SHARE_MOD executor binds.
      } else if (OB_SUCCESS != (inner_ret = new_ls_ids.push_back(entry.first))) {
        LOG_WARN("fail to push new ls id", K(inner_ret), K(entry.first));
      }
      return inner_ret;
    };
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(mgr_map.foreach_refactored(collect_new_func))) {
      LOG_WARN("fail to collect new ls ids", KR(ret), K_(tenant_id));
    }
  }

  // Step 3: outside lock, init executors for new LSes (heavy: get_ls, alloc, init)
  ObSEArray<std::pair<share::ObLSID, ObVecIdxLeaderExecutors *>, 16> leader_executors_to_insert;
  ObSEArray<std::pair<share::ObLSID, ObVecIdxFollowerExecutors *>, 16> follower_executors_to_insert;
  for (int64_t i = 0; OB_SUCC(ret) && i < new_ls_ids.count(); i++) {
    const share::ObLSID &ls_id = new_ls_ids.at(i);
    ObLSHandle ls_handle;
    ObLSService *ls_svr = MTL(ObLSService *);
    void *leader_buf = nullptr;
    void *follower_buf = nullptr;
    ObVecIdxLeaderExecutors *leader_exec = nullptr;
    ObVecIdxFollowerExecutors *follower_exec = nullptr;
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null ls", KR(ret), K(ls_id));
    } else {
      ObPluginVectorIndexMgr *ls_mgr = nullptr;
      const int mgr_ret = service_->get_ls_index_mgr_map().get_refactored(ls_id, ls_mgr);
      if (OB_SUCCESS == mgr_ret && OB_NOT_NULL(ls_mgr) && ls_mgr->is_vec_idx_async_executor_bind_stopped()) {
        continue;
      }
      if (OB_ISNULL(leader_buf = executor_allocator_.alloc(sizeof(ObVecIdxLeaderExecutors)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc leader executor set", KR(ret));
      } else if (OB_ISNULL(follower_buf = executor_allocator_.alloc(sizeof(ObVecIdxFollowerExecutors)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc follower executor set", KR(ret));
      } else {
        leader_exec = new (leader_buf) ObVecIdxLeaderExecutors();
        follower_exec = new (follower_buf) ObVecIdxFollowerExecutors();
        if (OB_FAIL(leader_exec->async_task_exec_.init(tenant_id_, ls_handle))) {
          LOG_WARN("fail to init async task executor", KR(ret), K(ls_id));
        } else if (OB_FAIL(leader_exec->embedding_task_exec_.init(tenant_id_, ls_handle))) {
          LOG_WARN("fail to init embedding task executor", KR(ret), K(ls_id));
        } else if (OB_FAIL(leader_exec->ivf_task_exec_.init(tenant_id_, ls_handle))) {
          LOG_WARN("fail to init ivf task executor", KR(ret), K(ls_id));
        } else if (OB_FAIL(leader_exec->freeze_exec_.init(tenant_id_, ls_handle))) {
          LOG_WARN("fail to init freeze executor", KR(ret), K(ls_id));
        } else if (OB_FAIL(leader_exec->merge_exec_.init(tenant_id_, ls_handle))) {
          LOG_WARN("fail to init merge executor", KR(ret), K(ls_id));
        } else if (OB_FAIL(leader_exec->mem_sync_exec_.init(tenant_id_, ls_handle))) {
          LOG_WARN("fail to init mem sync executor", KR(ret), K(ls_id));
        } else if (OB_FAIL(follower_exec->mem_sync_exec_.init(tenant_id_, ls_handle))) {
          LOG_WARN("fail to init follower mem sync executor", KR(ret), K(ls_id));
        } else if (OB_FAIL(follower_exec->ivf_task_exec_.init(tenant_id_, ls_handle))) {
          LOG_WARN("fail to init ivf task executor for follower", KR(ret), K(ls_id));
        } else if (OB_FAIL(leader_executors_to_insert.push_back(std::make_pair(ls_id, leader_exec)))) {
          LOG_WARN("fail to push leader executor to insert list", KR(ret), K(ls_id));
        } else if (OB_FAIL(follower_executors_to_insert.push_back(std::make_pair(ls_id, follower_exec)))) {
          LOG_WARN("fail to push follower executor to insert list", KR(ret), K(ls_id));
        } else {
          leader_exec->is_inited_ = true;
          follower_exec->is_inited_ = true;
          LOG_INFO("vec idx ls executors inited", K_(tenant_id), K(ls_id));
        }
        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(leader_exec)) {
            leader_exec->~ObVecIdxLeaderExecutors();
            executor_allocator_.free(leader_buf);
            leader_buf = nullptr;
            leader_exec = nullptr;
          }
          if (OB_NOT_NULL(follower_exec)) {
            follower_exec->~ObVecIdxFollowerExecutors();
            executor_allocator_.free(follower_buf);
            follower_buf = nullptr;
            follower_exec = nullptr;
          }
        }
      }
    }
  }

  // Step 4: under lock briefly, insert new executors (re-check LS still in mgr_map)
  if (OB_SUCC(ret) && !leader_executors_to_insert.empty()) {
    common::ObSpinLockGuard guard(ls_executor_lock_);
    LSIndexMgrMap &mgr_map = service_->get_ls_index_mgr_map();
    for (int64_t i = 0; OB_SUCC(ret) && i < leader_executors_to_insert.count(); i++) {
      const share::ObLSID &ls_id = leader_executors_to_insert.at(i).first;
      ObVecIdxLeaderExecutors *leader_exec = leader_executors_to_insert.at(i).second;
      ObVecIdxFollowerExecutors *follower_exec = follower_executors_to_insert.at(i).second;
      ObPluginVectorIndexMgr *mgr = nullptr;
      int tmp_ret = mgr_map.get_refactored(ls_id, mgr);
      if (OB_SUCCESS != tmp_ret) {
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          leader_exec->~ObVecIdxLeaderExecutors();
          follower_exec->~ObVecIdxFollowerExecutors();
          LOG_INFO("ls removed before executor insert, skip", K(ls_id));
        } else {
          ret = tmp_ret;
          LOG_WARN("fail to get ls mgr", KR(ret), K(ls_id));
          leader_exec->~ObVecIdxLeaderExecutors();
          follower_exec->~ObVecIdxFollowerExecutors();
        }
      } else if (OB_NOT_NULL(mgr) && mgr->is_vec_idx_async_executor_bind_stopped()) {
        leader_exec->~ObVecIdxLeaderExecutors();
        follower_exec->~ObVecIdxFollowerExecutors();
        LOG_INFO("vec idx async bind stopped before executor insert, skip", K(ls_id));
      } else if (OB_FAIL(ls_leader_executor_map_.set_refactored(ls_id, leader_exec))) {
        LOG_WARN("fail to set leader executor into map", KR(ret), K(ls_id));
        leader_exec->~ObVecIdxLeaderExecutors();
        follower_exec->~ObVecIdxFollowerExecutors();
      } else if (OB_FAIL(ls_follower_executor_map_.set_refactored(ls_id, follower_exec))) {
        LOG_WARN("fail to set follower executor into map", KR(ret), K(ls_id));
        follower_exec->~ObVecIdxFollowerExecutors();
        int tmp_ret = ls_leader_executor_map_.erase_refactored(ls_id);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to rollback leader executor from map", K(tmp_ret), K(ls_id));
        }
        leader_exec->~ObVecIdxLeaderExecutors();
      } else if (OB_NOT_NULL(mgr) && mgr->get_ls_leader()) {
        // Compensate for mark_ls_need_resume() that fired before this executor existed.
        // switch_to_leader() sets mgr->ls_leader_=true BEFORE mark_ls_need_resume(),
        // so if ls_leader is true here, the resume flag was lost and needs compensation.
        leader_exec->set_need_resume(true);
        LOG_INFO("new executor for already-leader LS, auto-set need_resume",
                 K(ls_id), K_(tenant_id));
      } else if (OB_NOT_NULL(mgr) && !mgr->get_ls_leader()) {
        // Symmetric compensation for the follower side: if mark_ls_need_resume_for_follower()
        // fired before this executor was created, the flag would be lost. Set it here so
        // the next timer round runs the R1 sweep on the follower load path.
        follower_exec->set_need_resume(true);
        LOG_INFO("new executor for already-follower LS, auto-set follower need_resume",
                 K(ls_id), K_(tenant_id));
      }
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::collect_leader_executors_to_run(
    ObSEArray<std::pair<ObLSID, ObVecIdxLeaderExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &leader_executors_to_run)
{
  int ret = OB_SUCCESS;
  leader_executors_to_run.reset();
  common::ObSpinLockGuard guard(ls_executor_lock_);
  LSIndexMgrMap &mgr_map = service_->get_ls_index_mgr_map();
  auto collect_func = [&](common::hash::HashMapPair<share::ObLSID, ObVecIdxLeaderExecutors *> &entry) -> int {
    int inner_ret = OB_SUCCESS;
    ObPluginVectorIndexMgr *mgr = nullptr;
    ObVecIdxLeaderExecutors *exec = entry.second;
    if (OB_ISNULL(exec) || !exec->is_inited_ || exec->is_removed_) {
      // skip uninited or retiring executors
    } else if (OB_SUCCESS != (inner_ret = mgr_map.get_refactored(entry.first, mgr))) {
      if (OB_HASH_NOT_EXIST == inner_ret) {
        inner_ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get ls mgr", K(inner_ret), K(entry.first));
      }
    } else if (OB_ISNULL(mgr) || !mgr->get_ls_leader()) {
      // not leader, skip
    } else {
      ++exec->ref_cnt_;
      if (OB_SUCCESS != (inner_ret = leader_executors_to_run.push_back(
          std::make_pair(entry.first, exec)))) {
        --exec->ref_cnt_;
        LOG_WARN("fail to push leader executor to run list", K(inner_ret), K(entry.first));
      }
    }
    return inner_ret;
  };
  if (OB_FAIL(ls_leader_executor_map_.foreach_refactored(collect_func))) {
    LOG_WARN("fail to collect leader executors to run", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::collect_follower_executors_to_run(
    ObSEArray<std::pair<ObLSID, ObVecIdxFollowerExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &follower_executors_to_run)
{
  int ret = OB_SUCCESS;
  follower_executors_to_run.reset();
  common::ObSpinLockGuard guard(ls_executor_lock_);
  LSIndexMgrMap &mgr_map = service_->get_ls_index_mgr_map();
  auto collect_func = [&](common::hash::HashMapPair<share::ObLSID, ObVecIdxFollowerExecutors *> &entry) -> int {
    int inner_ret = OB_SUCCESS;
    ObPluginVectorIndexMgr *mgr = nullptr;
    ObVecIdxFollowerExecutors *exec = entry.second;
    if (OB_ISNULL(exec) || !exec->is_inited_ || exec->is_removed_) {
      // skip uninited or retiring executors
    } else if (OB_SUCCESS != (inner_ret = mgr_map.get_refactored(entry.first, mgr))) {
      if (OB_HASH_NOT_EXIST == inner_ret) {
        inner_ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get ls mgr", K(inner_ret), K(entry.first));
      }
    } else if (OB_NOT_NULL(mgr) && mgr->get_ls_leader()) {
      // leader, not follower path
    } else {
      ++exec->ref_cnt_;
      if (OB_SUCCESS != (inner_ret = follower_executors_to_run.push_back(
          std::make_pair(entry.first, exec)))) {
        --exec->ref_cnt_;
        LOG_WARN("fail to push follower executor to run list", K(inner_ret), K(entry.first));
      }
    }
    return inner_ret;
  };
  if (OB_FAIL(ls_follower_executor_map_.foreach_refactored(collect_func))) {
    LOG_WARN("fail to collect follower executors to run", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::find_ls_holding_tablet_(const ObTabletID &tablet_id, ObLSHandle &ls_handle_out)
{
  int ret = OB_ENTRY_NOT_EXIST;
  ObLSService *ls_svc = MTL(ObLSService *);
  if (OB_ISNULL(ls_svc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret));
  } else {
    struct TabletLsFinder {
      const ObTabletID &tid_;
      ObLSHandle &out_;
      ObLSService *svc_;
      bool found_;
      int last_err_;  // most recent per-LS error; surfaced only when no LS holds tablet
      int operator()(ObLS &ls)
      {
        if (!found_ && ls.get_ls_id().is_user_ls()) {
          ObTabletHandle th;
          int gt_ret = ls.get_tablet_svr()->get_tablet(tid_, th);
          if (OB_TABLET_NOT_EXIST == gt_ret) {
            // tablet not on this LS, continue scanning
          } else if (OB_SUCCESS == gt_ret) {
            int gl_ret = svc_->get_ls(ls.get_ls_id(), out_, ObLSGetMod::SHARE_MOD);
            if (OB_SUCCESS == gl_ret) {
              found_ = true;
            } else {
              SERVER_LOG_RET(WARN, gl_ret, "fail to get ls handle for tablet",
                             KR(gl_ret), K(ls.get_ls_id()), K(tid_));
              last_err_ = gl_ret;
            }
          } else {
            SERVER_LOG_RET(WARN, gt_ret, "fail to get tablet when scanning ls",
                           KR(gt_ret), K(ls.get_ls_id()), K(tid_));
            last_err_ = gt_ret;
          }
        }
        // Always return success so foreach_ls keeps scanning the remaining LSes —
        // a transient error on one LS must not abort the search across other LSes.
        return OB_SUCCESS;
      }
    } finder{tablet_id, ls_handle_out, ls_svc, false, OB_SUCCESS};
    if (OB_FAIL(ls_svc->foreach_ls(finder))) {
      LOG_WARN("foreach_ls failed", KR(ret));
    } else if (finder.found_) {
      ret = OB_SUCCESS;
    } else if (OB_SUCCESS != finder.last_err_) {
      // No LS holds the tablet, but some LS errored during scan. Propagate the
      // transient error so the caller distinguishes it from a clean NOT_EXIST.
      ret = finder.last_err_;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::resolve_leader_triggered_executor_(
    const int64_t task_type,
    ObVecIdxLeaderExecutors &exec,
    ObVecITaskExecutor *&out)
{
  int ret = OB_SUCCESS;
  out = nullptr;
  switch (task_type) {
    case ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_OPTINAL:
      out = &exec.async_task_exec_;
      break;
    case ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING:
      out = &exec.embedding_task_exec_;
      break;
    case ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_FREEZE:
      out = &exec.freeze_exec_;
      break;
    case ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_MERGE:
      out = &exec.merge_exec_;
      break;
    case ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_MEM_SYNC_TASK:
      out = &exec.mem_sync_exec_;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("manual trigger not supported for leader task type", KR(ret), K(task_type));
      break;
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::resolve_follower_triggered_executor_(
    const int64_t task_type,
    ObVecIdxFollowerExecutors &exec,
    ObVecITaskExecutor *&out)
{
  int ret = OB_NOT_SUPPORTED;
  out = nullptr;
  UNUSED(exec);
  // No task type supports manual trigger on follower yet; enable branches one by one as support is added.
  LOG_INFO("manual trigger not supported for follower task type", KR(ret), K(task_type));
  return ret;
}

int ObVecIdxAsyncTaskScheduler::handle_cancelled_task_(
    ObVecIndexAsyncTaskCtx &task_ctx,
    const share::ObLSID &ls_id,
    ObVecIndexTaskCtxArray &cancelled_ctx_array)
{
  int ret = OB_SUCCESS;
  bool need_cleanup = false;
  bool drain_pending = false;
  ObVecIndexAsyncTaskCtx *task_ctx_ptr = &task_ctx;
  // Drain DB sync + kill_inner_sql deferred from the lightweight switch path.
  // Idempotent: only does work when cancel_post_work_pending_ is true.
  {
    int tmp_ret = task_ctx.drain_cancel_post_work_if_pending();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to drain deferred cancel post-work, will retry next tick",
               K(tmp_ret), K(ls_id), KPC(task_ctx_ptr));
      drain_pending = true;
    }
  }
  {
    common::ObSpinLockGuard ctx_guard(task_ctx.lock_);
    // If drain failed, leave the ctx in CANCEL so the next tick retries DB sync
    // before promoting to FINISH; otherwise the in-memory state would race ahead
    // of the inner table and the cancel would never land.
    if (drain_pending) {
      LOG_INFO("skip FINISH transition for cancelled task because drain is still pending",
               K(ls_id), KPC(task_ctx_ptr));
    } else if (!task_ctx.in_thread_pool_ && !task_ctx.in_queue_) {
      task_ctx.task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
      if (task_ctx.task_status_.ret_code_ == VEC_ASYNC_TASK_DEFAULT_ERR_CODE) {
        task_ctx.task_status_.ret_code_ = OB_CANCELED;
      }
      task_ctx.task_status_.all_finished_ = true;
      need_cleanup = true;
      LOG_INFO("cancelled task thread exited, transition to FINISH for cleanup", K(ls_id), KPC(task_ctx_ptr));
    } else {
      LOG_INFO("cancelled task waiting for thread pool exit", K(ls_id), KPC(task_ctx_ptr));
    }
  }
  if (need_cleanup) {
    int tmp_ret = cancelled_ctx_array.push_back(task_ctx_ptr);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to push back cancelled_ctx_array for cancelled task", K(tmp_ret), KPC(task_ctx_ptr));
    }
  }
  return ret;
}

void ObVecIdxAsyncTaskScheduler::inc_leader_executor_ref_(ObVecIdxLeaderExecutors &exec)
{
  ++exec.ref_cnt_;
}

void ObVecIdxAsyncTaskScheduler::dec_leader_executor_ref_(ObVecIdxLeaderExecutors &exec)
{
  if (OB_UNLIKELY(exec.ref_cnt_ <= 0)) {
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid leader executor ref count",
             KR(ret),
             "ref_cnt", exec.ref_cnt_,
             "is_removed", exec.is_removed_,
             "is_inited", exec.is_inited_);
  } else {
    --exec.ref_cnt_;
    if (exec.is_removed_ && 0 == exec.ref_cnt_) {
      exec.~ObVecIdxLeaderExecutors();
    }
  }
}

void ObVecIdxAsyncTaskScheduler::retire_leader_executor_(ObVecIdxLeaderExecutors *leader_exec)
{
  if (OB_NOT_NULL(leader_exec)) {
    leader_exec->is_removed_ = true;
    if (0 == leader_exec->ref_cnt_) {
      leader_exec->~ObVecIdxLeaderExecutors();
    }
  }
}

void ObVecIdxAsyncTaskScheduler::inc_follower_executor_ref_(ObVecIdxFollowerExecutors &exec)
{
  ++exec.ref_cnt_;
}

void ObVecIdxAsyncTaskScheduler::dec_follower_executor_ref_(ObVecIdxFollowerExecutors &exec)
{
  if (OB_UNLIKELY(exec.ref_cnt_ <= 0)) {
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid follower executor ref count",
             KR(ret),
             "ref_cnt", exec.ref_cnt_,
             "is_removed", exec.is_removed_,
             "is_inited", exec.is_inited_);
  } else {
    --exec.ref_cnt_;
    if (exec.is_removed_ && 0 == exec.ref_cnt_) {
      exec.~ObVecIdxFollowerExecutors();
    }
  }
}

void ObVecIdxAsyncTaskScheduler::retire_follower_executor_(ObVecIdxFollowerExecutors *follower_exec)
{
  if (OB_NOT_NULL(follower_exec)) {
    follower_exec->is_removed_ = true;
    if (0 == follower_exec->ref_cnt_) {
      follower_exec->~ObVecIdxFollowerExecutors();
    }
  }
}

int ObVecIdxAsyncTaskScheduler::claim_triggered_task_(const ObVecIndexTaskStatus &row, bool &claimed)
{
  int ret = OB_SUCCESS;
  claimed = false;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObVecIndexTaskKey key(row.tenant_id_, row.table_id_, row.tablet_id_.id(), row.task_id_);
    ObVecIndexFieldArray update_fields;
    ObVecIndexTaskStatusField f_status;
    f_status.field_name_ = "status";
    f_status.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
    f_status.data_.uint_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
    ObVecIndexTaskStatusField f_ret_code;
    f_ret_code.field_name_ = "ret_code";
    f_ret_code.type_ = ObVecIndexTaskStatusField::INT_TYPE;
    f_ret_code.data_.int_ = VEC_ASYNC_TASK_DEFAULT_ERR_CODE;
    ObSEArray<int64_t, 1> expected_statuses;
    if (OB_FAIL(update_fields.push_back(f_status))) {
      LOG_WARN("fail to push back status field", KR(ret));
    } else if (OB_FAIL(update_fields.push_back(f_ret_code))) {
      LOG_WARN("fail to push back ret_code field", KR(ret));
    } else if (OB_FAIL(expected_statuses.push_back(
                   ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_STANDBY))) {
      LOG_WARN("fail to push expected status", KR(ret));
    } else {
      ObVecIndexTaskProgressInfo progress_info = row.progress_info_;
      ObVecIndexTaskInfo task_info = row.task_info_;
      ObMySQLTransaction trans;
      const int64_t priority = static_cast<int64_t>(get_priority_by_task_type(
          static_cast<ObVecIndexAsyncTaskType>(row.task_type_), row.trigger_type_));
      if (OB_FAIL(trans.start(sql_proxy, tenant_id_))) {
        LOG_WARN("fail to start transaction", KR(ret));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_vec_task(
              tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, trans, key,
              update_fields, progress_info, task_info,
              GCTX.self_addr(), priority, row.start_time_, row.end_time_,
              row.err_msg_, &expected_statuses, &claimed))) {
        LOG_WARN("fail to claim triggered task", KR(ret), K(row));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
    LOG_INFO("claim triggered task", KR(ret), K(claimed), K(row));
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::finish_triggered_task_(
    const ObVecIndexTaskStatus &row,
    const int ret_code,
    const int64_t expected_status)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObVecIndexTaskKey key(row.tenant_id_, row.table_id_, row.tablet_id_.id(), row.task_id_);
    ObVecIndexFieldArray update_fields;
    ObVecIndexTaskStatusField f_status;
    f_status.field_name_ = "status";
    f_status.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
    f_status.data_.uint_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
    ObVecIndexTaskStatusField f_ret_code;
    f_ret_code.field_name_ = "ret_code";
    f_ret_code.type_ = ObVecIndexTaskStatusField::INT_TYPE;
    f_ret_code.data_.int_ = ret_code;
    ObSEArray<int64_t, 1> expected_statuses;
    bool matched = false;
    if (OB_FAIL(update_fields.push_back(f_status))) {
      LOG_WARN("fail to push back status field", KR(ret));
    } else if (OB_FAIL(update_fields.push_back(f_ret_code))) {
      LOG_WARN("fail to push back ret_code field", KR(ret));
    } else if (OB_FAIL(expected_statuses.push_back(expected_status))) {
      LOG_WARN("fail to push expected status", KR(ret));
    } else {
      ObVecIndexTaskProgressInfo progress_info = row.progress_info_;
      ObVecIndexTaskInfo task_info = row.task_info_;
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(sql_proxy, tenant_id_))) {
        LOG_WARN("fail to start transaction", KR(ret));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_vec_task(
              tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, trans, key,
              update_fields, progress_info, task_info,
              row.exec_addr_, row.priority_, row.start_time_, row.end_time_,
              row.err_msg_, &expected_statuses, &matched))) {
        LOG_WARN("fail to update triggered task to finish",
                 KR(ret), K(row), K(ret_code), K(expected_status));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
    LOG_INFO("finish triggered task", KR(ret), K(matched), K(row), K(ret_code), K(expected_status));
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_and_finish_orphan_self_tasks_()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::fast_current_time();
  if (last_orphan_task_check_ts_ > 0
      && now - last_orphan_task_check_ts_ < ORPHAN_TASK_CHECK_INTERVAL_US) {
    return OB_SUCCESS;
  }

  uint64_t data_version = 0;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vec idx async task scheduler not inited", KR(ret));
  } else if (is_stopped_) {
    // skip
  } else if (OB_ISNULL(service_) || OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), KP(service_), KP(sql_proxy));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K_(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_0_1) {
    LOG_TRACE("skip orphan self task check, data version < 4.6.0.1",
              K_(tenant_id), K(data_version));
    last_orphan_task_check_ts_ = now;
  } else {
    char addr_buf[MAX_IP_PORT_LENGTH] = { 0 };
    GCTX.self_addr().ip_port_to_string(addr_buf, sizeof(addr_buf));
    const int64_t cutoff_ts = ObTimeUtility::current_time() - ORPHAN_TASK_MIN_AGE_US;
    ObSqlString sql;
    ObVecIndexTaskStatusArray rows;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT * FROM %s"
            " WHERE tenant_id = %ld AND exec_addr = '%s'"
            " AND tablet_id != -1"
            " AND status IN (%ld, %ld, %ld, %ld, %ld, %ld)"
            " AND gmt_modified < usec_to_time(%ld)"
            " ORDER BY tenant_id, table_id, tablet_id, task_id LIMIT %ld",
            OB_ALL_VECTOR_INDEX_TASK_TNAME,
            ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_),
            addr_buf,
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_QUEUE),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL),
            cutoff_ts,
            STARTUP_CLEANUP_BATCH_SIZE))) {
      LOG_WARN("fail to assign orphan self task sql", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(sql_proxy->read(res, tenant_id_, sql.ptr()))) {
          LOG_WARN("fail to query orphan self tasks", KR(ret), K(sql), K_(tenant_id));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query result is null", KR(ret), K_(tenant_id));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END != ret) {
                LOG_WARN("fail to get next orphan self task row", KR(ret), K(sql));
              }
            } else {
              ObVecIndexTaskStatus row;
              if (OB_FAIL(ObVecIndexAsyncTaskUtil::extract_one_task_sql_result(result, row))) {
                LOG_WARN("fail to extract orphan self task row", KR(ret));
              } else if (OB_FAIL(rows.push_back(row))) {
                LOG_WARN("fail to push orphan self task row", KR(ret), K(row));
              }
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }

    int64_t finish_rows = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      const ObVecIndexTaskStatus &row = rows.at(i);
      bool alive = true;
      int orphan_ret_code = OB_ERR_UNEXPECTED;
      int tmp_ret = OB_SUCCESS;
      if (row.task_type_ == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_MEM_SYNC_TASK
          && row.trigger_type_ == ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_MANUAL) {
        LOG_TRACE("skip manual mem sync task in orphan self task check", K(row));
      } else if (OB_TMP_FAIL(check_task_alive_in_memory_(row, alive, orphan_ret_code))) {
        LOG_WARN("fail to check task alive in memory, skip orphan self task row",
                 KR(tmp_ret), K(row));
      } else if (alive) {
        LOG_TRACE("self task row is still alive in memory", K(row));
      } else if (OB_TMP_FAIL(finish_orphan_self_task_(row))) {
        LOG_WARN("fail to finish orphan self task", KR(tmp_ret), K(row));
      } else {
        ++finish_rows;
      }
    }
    if (OB_SUCC(ret)) {
      last_orphan_task_check_ts_ = now;
      if (rows.count() > 0 || finish_rows > 0) {
        LOG_INFO("[VEC_ASYNC_TASK] orphan self task check finish",
                 K_(tenant_id), K(addr_buf), K(rows.count()), K(finish_rows));
      } else {
        LOG_TRACE("[VEC_ASYNC_TASK] orphan self task check finish",
                  K_(tenant_id), K(addr_buf), K(rows.count()), K(finish_rows));
      }
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_task_alive_in_memory_(
  const ObVecIndexTaskStatus &row,
  bool &alive,
  int &orphan_ret_code)
{
int ret = OB_SUCCESS;
alive = false;
orphan_ret_code = OB_ERR_UNEXPECTED;
if (OB_ISNULL(service_)
    || !row.tablet_id_.is_valid()
    || row.task_type_ < 0
    || row.task_type_ >= ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_TASK_TYPE_INVALID) {
  ret = OB_INVALID_ARGUMENT;
  LOG_WARN("invalid argument", KR(ret), KP(service_), K(row));
} else {
  ObLSHandle ls_handle;
  ObPluginVectorIndexMgr *mgr = nullptr;
  int tmp_ret = find_ls_holding_tablet_(row.tablet_id_, ls_handle);
  if (OB_ENTRY_NOT_EXIST == tmp_ret) {
    alive = false;
    orphan_ret_code = OB_TABLET_NOT_EXIST;
    LOG_INFO("tablet is not held by this observer, treat self task row as orphan",
             KR(tmp_ret), K(row));
  } else if (OB_SUCCESS != tmp_ret) {
    alive = true;
    LOG_WARN("fail to locate ls holding tablet, skip orphan self task check",
             KR(tmp_ret), K(row));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    alive = true;
    LOG_WARN("ls is null while checking orphan self task, skip", K(row));
  } else if (OB_SUCCESS != (tmp_ret = service_->get_ls_index_mgr_map().get_refactored(
                 ls_handle.get_ls()->get_ls_id(), mgr))) {
    alive = true;
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      LOG_TRACE("ls vector index mgr not ready, skip orphan self task check",
                K(ls_handle.get_ls()->get_ls_id()), K(row));
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get ls vector index mgr", KR(ret), K(ls_handle.get_ls()->get_ls_id()), K(row));
    }
  } else if (OB_ISNULL(mgr) || mgr->get_async_task_opt().is_stop()) {
    alive = true;
    LOG_TRACE("ls vector index mgr is null or stopped, skip orphan self task check",
              KP(mgr), K(row));
  } else {
    ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
    ObVecIndexAsyncTaskKey key(row.tablet_id_, static_cast<uint32_t>(row.task_type_));
    ObVecIndexAsyncTaskOption &task_opt = mgr->get_async_task_opt();
    common::ObSpinLockGuard task_ctx_map_guard(mgr->task_ctx_lock_);
    if (OB_FAIL(task_opt.get_async_task_map().get_refactored(key, task_ctx))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        alive = false;
      } else {
        LOG_WARN("fail to get task ctx from map", KR(ret), K(key), K(row));
      }
    } else if (OB_ISNULL(task_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null task ctx", KR(ret), K(key), K(row));
    } else {
      common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
      alive = (task_ctx->task_status_.task_id_ == row.task_id_);
      LOG_TRACE("checked self task row against memory task ctx",
                K(alive), K(row), KPC(task_ctx));
    }
  }
}
return ret;
}

int ObVecIdxAsyncTaskScheduler::finish_orphan_self_task_(const ObVecIndexTaskStatus &row)
{
  int ret = OB_SUCCESS;
  bool alive = true;
  int orphan_ret_code = OB_ERR_UNEXPECTED;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(check_task_alive_in_memory_(row, alive, orphan_ret_code))) {
    LOG_WARN("fail to recheck task alive in memory", KR(ret), K(row));
  } else if (alive) {
    LOG_TRACE("skip finishing orphan self task, task ctx reappeared in memory", K(row));
  } else {
    char addr_buf[MAX_IP_PORT_LENGTH] = { 0 };
    GCTX.self_addr().ip_port_to_string(addr_buf, sizeof(addr_buf));
    const int64_t now = ObTimeUtility::current_time();
    const int64_t cutoff_ts = now - ORPHAN_TASK_MIN_AGE_US;
    const char *err_msg = (OB_TABLET_NOT_EXIST == orphan_ret_code)
        ? "tablet not held by this observer during orphan check"
        : "task ctx not found in memory map during orphan check";
    ObSqlString sql;
    int64_t affect_rows = 0;
    if (OB_FAIL(sql.assign_fmt(
            "UPDATE %s SET status = %ld, ret_code = %d,"
            " err_msg = '%s', end_time = usec_to_time(%ld)"
            " WHERE tenant_id = %ld AND table_id = %ld AND tablet_id = %ld AND task_id = %ld"
            " AND exec_addr = '%s'"
            " AND status IN (%ld, %ld, %ld, %ld, %ld, %ld)"
            " AND gmt_modified < usec_to_time(%ld)",
            OB_ALL_VECTOR_INDEX_TASK_TNAME,
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH),
            orphan_ret_code,
            err_msg,
            now,
            ObSchemaUtils::get_extract_tenant_id(row.tenant_id_, row.tenant_id_),
            row.table_id_,
            row.tablet_id_.id(),
            row.task_id_,
            addr_buf,
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_QUEUE),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN),
            static_cast<int64_t>(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL),
            cutoff_ts))) {
      LOG_WARN("fail to assign finish orphan self task sql", KR(ret), K(row));
    } else if (OB_FAIL(sql_proxy->write(tenant_id_, sql.ptr(), affect_rows))) {
      LOG_WARN("fail to execute finish orphan self task sql", KR(ret), K(sql), K(row));
    } else if (affect_rows > 0) {
      LOG_INFO("[VEC_ASYNC_TASK] finish orphan self task",
               K_(tenant_id), K(row), K(affect_rows), K(sql));
    } else {
      LOG_TRACE("[VEC_ASYNC_TASK] orphan self task was not updated",
                K_(tenant_id), K(row), K(affect_rows), K(sql));
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_table_dropped_(uint64_t table_id, bool &dropped)
{
  int ret = OB_SUCCESS;
  dropped = false;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
          tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard for orphan task check", KR(ret), K_(tenant_id), K(table_id));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("fail to get table schema for orphan task check", KR(ret), K_(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema) || table_schema->is_in_recyclebin()) {
    dropped = true;
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::load_triggered_tasks_()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::fast_current_time();
  if (last_triggered_task_check_ts_ > 0
      && now - last_triggered_task_check_ts_ < TRIGGERED_TASK_CHECK_INTERVAL_US) {
    return OB_SUCCESS;
  }
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vec idx async task scheduler not inited", KR(ret));
  } else if (is_stopped_) {
    // skip
  } else if (OB_ISNULL(service_) || OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), KP(service_), KP(sql_proxy));
  } else {
    ObVecIndexFieldArray filters;
    ObVecIndexTaskStatusField f_tenant;
    f_tenant.field_name_ = "tenant_id";
    f_tenant.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
    f_tenant.data_.uint_ = ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_);
    ObVecIndexTaskStatusField f_trigger;
    f_trigger.field_name_ = "trigger_type";
    f_trigger.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
    f_trigger.data_.uint_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_MANUAL;
    ObVecIndexTaskStatusField f_status;
    f_status.field_name_ = "status";
    f_status.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
    f_status.data_.uint_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_STANDBY;
    if (OB_FAIL(filters.push_back(f_tenant))) {
      LOG_WARN("fail to push filter", KR(ret));
    } else if (OB_FAIL(filters.push_back(f_trigger))) {
      LOG_WARN("fail to push filter", KR(ret));
    } else if (OB_FAIL(filters.push_back(f_status))) {
      LOG_WARN("fail to push filter", KR(ret));
    } else {
      ObVecIndexTaskStatusArray rows;
      if (OB_FAIL(ObVecIndexAsyncTaskUtil::read_ls_scope_vec_tasks_from_inner_table(
              tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, false, filters, *sql_proxy, rows))) {
        LOG_WARN("fail to read triggered tasks", KR(ret), K_(tenant_id));
      } else {
        LSIndexMgrMap &mgr_map = service_->get_ls_index_mgr_map();
        for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
          const ObVecIndexTaskStatus &row = rows.at(i);
          const ObTabletID tablet_id(row.tablet_id_);
          ObLSHandle ls_handle;
          int tmp_ret = find_ls_holding_tablet_(tablet_id, ls_handle);
          if (OB_SUCCESS != tmp_ret) {
            // Tablet not located on any LS on this observer (OB_ENTRY_NOT_EXIST), or a
            // transient error occurred during the scan. We must NOT finish the task
            // unconditionally: "tablet not on this observer" is very different from
            // "table truly dropped" — for the former, the LS leader on another observer
            // is responsible for the task. Verify with schema before finishing.
            bool table_dropped = false;
            int chk_ret = check_table_dropped_(row.table_id_, table_dropped);
            if (OB_SUCCESS == chk_ret && table_dropped) {
              // Orphan task: table is gone, finish so it does not linger.
              int fail_ret = finish_triggered_task_(
                  row, OB_TABLE_NOT_EXIST, ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_STANDBY);
              if (OB_SUCCESS != fail_ret) {
                LOG_WARN("fail to finish orphan triggered task for dropped table",
                         KR(fail_ret), KR(tmp_ret), K(row));
              } else {
                LOG_INFO("finished orphan triggered task, table dropped",
                         KR(tmp_ret), K(row.table_id_), K(row.tablet_id_), K(row.task_type_));
              }
            } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
              // Table still exists; tablet just isn't on this observer. The LS leader on
              // another node will pick it up. Skip silently to avoid log spam.
              LOG_TRACE("tablet not on this observer, skip manual trigger task",
                        KR(tmp_ret), KR(chk_ret), K(row));
            } else {
              // Transient error locating LS (e.g., snapshot discarded). Skip and let
              // the next scheduler tick retry — better than silently swallowing forever.
              LOG_WARN("fail to locate ls for tablet, skip this row and retry next round",
                       KR(tmp_ret), KR(chk_ret), K(row));
            }
          } else if (OB_ISNULL(ls_handle.get_ls())) {
            int fail_ret = finish_triggered_task_(
                row, OB_ERR_UNEXPECTED, ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_STANDBY);
            if (OB_SUCCESS != fail_ret) {
              LOG_WARN("fail to finish triggered task for null ls", KR(fail_ret), K(row));
            }
          } else {
            const ObLSID ls_id = ls_handle.get_ls()->get_ls_id();
            // Use palf role as the authoritative leader/follower check.
            // This avoids depending on mgr_map which may not be populated yet.
            common::ObRole ls_role = common::FOLLOWER;
            int64_t proposal_id = -1;
            int role_ret = ls_handle.get_ls()->get_log_handler()->get_role(ls_role, proposal_id);
            if (OB_SUCCESS != role_ret) {
              LOG_WARN("fail to get ls role for manual trigger task", KR(role_ret), K(ls_id), K(row));
              continue;
            } else if (!is_strong_leader(ls_role)) {
              // Not leader for this LS, skip without finishing.
              // The leader node's scheduler will pick up and execute this task.
              LOG_TRACE("skip manual trigger task, not leader for ls",
                        K(ls_id), K(ls_role), K(row));
              continue;
            }
            // From here on, this node is the leader for ls_id.
            ObPluginVectorIndexMgr *mgr = nullptr;
            bool mgr_found = false;
            ObVecITaskExecutor *exec_to_run = nullptr;
            ObVecIdxLeaderExecutors *leader_exec_to_run = nullptr;
            int resolve_ret = OB_SUCCESS;
            int mgr_ret = OB_SUCCESS;
            int executor_ret = OB_SUCCESS;
            bool executor_inited = false;
            {
              common::ObSpinLockGuard guard(ls_executor_lock_);
              mgr_ret = mgr_map.get_refactored(ls_id, mgr);
              if (OB_SUCCESS == mgr_ret && OB_NOT_NULL(mgr)) {
                mgr_found = true;
              }
              if (mgr_found) {
                ObVecIdxLeaderExecutors *lex = nullptr;
                executor_ret = ls_leader_executor_map_.get_refactored(ls_id, lex);
                if (OB_SUCCESS == executor_ret && OB_NOT_NULL(lex)) {
                  executor_inited = lex->is_inited_;
                  if (lex->is_inited_ && !lex->is_removed_) {
                    resolve_ret = resolve_leader_triggered_executor_(row.task_type_, *lex, exec_to_run);
                    if (OB_SUCCESS == resolve_ret && OB_NOT_NULL(exec_to_run)) {
                      inc_leader_executor_ref_(*lex);
                      leader_exec_to_run = lex;
                    }
                  }
                }
              }
            }
            if (!mgr_found) {
              if (OB_HASH_NOT_EXIST != mgr_ret && OB_SUCCESS != mgr_ret) {
                LOG_WARN("fail to get ls mgr for manual trigger task", KR(mgr_ret), K(ls_id), K(row));
                continue;
              }
              // Leader LS but mgr not yet registered (lazy init). Proactively create it
              // so sync_ls_executors discovers it next round and creates executor.
              ObPluginVectorIndexMgr *tmp_mgr = nullptr;
              int acquire_ret = service_->acquire_vector_index_mgr(ls_id, tmp_mgr);
              if (OB_SUCCESS != acquire_ret) {
                LOG_WARN("manual trigger task: fail to acquire leader ls mgr", KR(acquire_ret), K(ls_id), K(row));
              } else {
                LOG_INFO("manual trigger task: leader ls not in mgr map, acquired mgr",
                         K(ls_id), K(row));
              }
              continue;  // wait for next round to create executor and execute
            }
            if (OB_SUCCESS != executor_ret) {
              if (OB_HASH_NOT_EXIST == executor_ret) {
                LOG_INFO("skip manual trigger task, leader executor not ready in map", K(ls_id), K(row));
              } else {
                LOG_WARN("fail to get leader executor for manual trigger task", KR(executor_ret), K(ls_id), K(row));
              }
              continue;
            }
            if (OB_SUCCESS != resolve_ret) {
              // Fail-safe: mark task as FINISH with the resolve error code
              // so it does not stay in STANDBY forever.
              int fail_ret = finish_triggered_task_(
                  row, resolve_ret, ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_STANDBY);
              if (OB_SUCCESS != fail_ret) {
                LOG_WARN("fail to finish triggered task", KR(fail_ret), K(resolve_ret), K(row));
              }
            } else if (OB_ISNULL(exec_to_run) || OB_ISNULL(leader_exec_to_run)) {
              // Leader LS, mgr exists, but executor not yet initialized.
              // Wait for sync_ls_executors to create executor next round.
              LOG_INFO("skip manual trigger task, executor not ready yet", K(ls_id), K(executor_inited), K(row));
              continue;
            } else {
              bool claimed = false;
              tmp_ret = claim_triggered_task_(row, claimed);
              if (OB_SUCCESS != tmp_ret) {
                common::ObSpinLockGuard guard(ls_executor_lock_);
                dec_leader_executor_ref_(*leader_exec_to_run);
                LOG_WARN("fail to claim manual trigger task", KR(tmp_ret), K(ls_id), K(row));
              } else if (!claimed) {
                common::ObSpinLockGuard guard(ls_executor_lock_);
                dec_leader_executor_ref_(*leader_exec_to_run);
                LOG_INFO("manual trigger task already claimed or advanced, skip loading", K(ls_id), K(row));
              } else {
                ObVecIndexTaskStatus claimed_row = row;
                claimed_row.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
                claimed_row.ret_code_ = VEC_ASYNC_TASK_DEFAULT_ERR_CODE;
                claimed_row.exec_addr_ = GCTX.self_addr();
                claimed_row.priority_ = static_cast<int64_t>(get_priority_by_task_type(
                    static_cast<ObVecIndexAsyncTaskType>(row.task_type_), row.trigger_type_));
                tmp_ret = exec_to_run->load_triggered_task(claimed_row);
                {
                  common::ObSpinLockGuard guard(ls_executor_lock_);
                  dec_leader_executor_ref_(*leader_exec_to_run);
                }
                if (OB_SUCCESS != tmp_ret) {
                  LOG_WARN("load_triggered_task failed, finishing task", KR(tmp_ret), K(ls_id), K(row));
                  int fail_ret = finish_triggered_task_(
                      row, tmp_ret, ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE);
                  if (OB_SUCCESS != fail_ret) {
                    LOG_WARN("fail to finish triggered task after load failure", KR(fail_ret), K(row));
                  }
                } else if (row.task_type_ == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_MEM_SYNC_TASK) {
                  // MemSync tasks are added to waiting_map, not async_task_opt.
                  // Immediately mark the inner-table row as FINISH to prevent
                  // repeated loading on the next scheduler cycle.
                  int fail_ret = finish_triggered_task_(
                      row, OB_SUCCESS, ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE);
                  if (OB_SUCCESS != fail_ret) {
                    LOG_WARN("fail to finish triggered mem sync task", KR(fail_ret), K(row));
                  }
                }
              }
              if (OB_SUCCESS != tmp_ret) {
                // Claim/load failures should not abort the tenant scheduler loop;
                // this row has either been left for the next round or finished above.
                tmp_ret = OB_SUCCESS;
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    last_triggered_task_check_ts_ = now;
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::load_leader_tasks(
    const ObSEArray<std::pair<ObLSID, ObVecIdxLeaderExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &leader_executors_to_run,
    uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  bool has_ivf_index = false;
  ObSEArray<uint64_t, DEFAULT_TABLE_ARRAY_SIZE> vec_table_id_array;
  if (OB_FAIL(ObPluginVectorIndexUtils::get_tenant_vector_index_ids(
      tenant_id_, has_ivf_index, vec_table_id_array))) {
    LOG_WARN("fail to get tenant vector index ids", KR(ret), K_(tenant_id));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < leader_executors_to_run.count(); i++) {
    ObVecIdxLeaderExecutors *exec = leader_executors_to_run.at(i).second;
    const share::ObLSID &ls_id = leader_executors_to_run.at(i).first;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(load_leader_ls_tasks(ls_id, *exec, has_ivf_index, task_trace_base_num))) {
      LOG_WARN("fail to load leader ls tasks", K(tmp_ret), K(ls_id));
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::load_follower_tasks(
    const ObSEArray<std::pair<ObLSID, ObVecIdxFollowerExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &follower_executors_to_run,
    uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < follower_executors_to_run.count(); i++) {
    ObVecIdxFollowerExecutors *exec = follower_executors_to_run.at(i).second;
    const share::ObLSID &ls_id = follower_executors_to_run.at(i).first;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(load_follower_ls_tasks(ls_id, *exec, task_trace_base_num))) {
      LOG_WARN("fail to load follower ls tasks", K(tmp_ret), K(ls_id));
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_and_schedule_leader_ls_tasks(
    const ObSEArray<std::pair<ObLSID, ObVecIdxLeaderExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &leader_executors_to_run)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<uint64_t> conflict_table_id_set;
  common::hash::ObHashSet<uint64_t> conflict_index_task_set;
  bool ddl_conflict_checked = false;
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(conflict_table_id_set.create(16))) {
      LOG_INFO("fail to create conflict_table_id_set, skip DDL conflict check", K(tmp_ret));
    } else if (OB_TMP_FAIL(conflict_index_task_set.create(16))) {
      LOG_INFO("fail to create conflict_index_task_set, skip DDL conflict check", K(tmp_ret));
    } else if (OB_TMP_FAIL(ObVecITaskExecutor::check_has_hnsw_ddl(tenant_id_, conflict_table_id_set, conflict_index_task_set))) {
      LOG_INFO("fail to check hnsw ddl, skip DDL conflict check", K(tmp_ret));
    } else {
      ddl_conflict_checked = true;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < leader_executors_to_run.count(); i++) {
    ObVecIdxLeaderExecutors *exec = leader_executors_to_run.at(i).second;
    const share::ObLSID &ls_id = leader_executors_to_run.at(i).first;
    ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
    if (OB_ISNULL(exec) || OB_ISNULL(service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", KR(ret), KP(exec), KP(service_));
    } else if (OB_FAIL(service_->acquire_vector_index_mgr(ls_id, index_ls_mgr))) {
      LOG_WARN("fail to acquire vector index mgr", KR(ret), K(ls_id));
    } else if (OB_ISNULL(index_ls_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", KR(ret), KP(index_ls_mgr));
    } else {
      ObVecIndexTaskCtxArray task_ctx_array;
      ObVecIndexTaskCtxArray cancelled_ctx_array;
      ObVecIndexTaskCtxArray queued_ctx_array; // collect newly queued tasks for inner table update
      // running tasks hit by DDL conflict; cancel_task() re-acquires task_ctx->lock_,
      // so it must run after ctx_guard is released — collected here, cancelled below.
      ObVecIndexTaskCtxArray ddl_conflict_cancel_array;
      ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
      ObVecIndexPriorityQueueManager &queue_manager = service_->get_vec_index_priority_queue_manager();
      if (task_opt.is_stop()) {
        // LS is being destroyed, skip scheduling new tasks into queue.
        LOG_INFO("ls task_opt is stopped, skip scheduling", K(ls_id));
      } else {
        FOREACH_X(iter, task_opt.get_async_task_map(), OB_SUCC(ret)) {
          ObVecIndexAsyncTaskCtx *task_ctx = iter->second;
          if (OB_ISNULL(task_ctx)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", KR(ret));
          } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_result(task_ctx))) {
            LOG_WARN("fail to check task result", KR(ret), KPC(task_ctx));
          } else {
            switch (task_ctx->task_status_.status_) {
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE:
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING:
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE:
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN: {
                common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
                if (task_ctx->in_thread_pool_) {
                  if (ddl_conflict_checked) {
                    bool is_conflict = false;
                    int tmp_ret = OB_SUCCESS;
                    if (OB_TMP_FAIL(ObVecITaskExecutor::check_task_ddl_conflict(
                            task_ctx, index_ls_mgr,
                            conflict_table_id_set, conflict_index_task_set, is_conflict))) {
                      LOG_WARN("fail to check ddl conflict for running task", K(tmp_ret), KPC(task_ctx));
                    }
                    if (is_conflict) {
                      (void)task_ctx->set_err_msg(ObString(VEC_TASK_CANCEL_MSG_DDL_CONFLICT));
                      if (OB_TMP_FAIL(ddl_conflict_cancel_array.push_back(task_ctx))) {
                        LOG_WARN("fail to push back ddl_conflict_cancel_array", K(tmp_ret), KPC(task_ctx));
                      }
                    }
                  }
                  LOG_TRACE("task is in thread pool already", KPC(task_ctx));
                } else if (task_ctx->in_queue_) {
                  LOG_TRACE("task is in priority queue already", KPC(task_ctx));
                } else {
                  ObVecIndexAsyncTaskType task_type =
                      static_cast<ObVecIndexAsyncTaskType>(task_ctx->task_status_.task_type_);
                  int64_t tablet_id_val = task_ctx->task_status_.tablet_id_.id();
                  if (is_task_disabled_(task_type, tablet_id_val)) {
                    task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
                    task_ctx->task_status_.ret_code_ = OB_CANCELED;
                    task_ctx->task_status_.all_finished_ = true;
                    {
                      int tmp_ret = OB_SUCCESS;
                      if (OB_TMP_FAIL(task_ctx->set_err_msg(ObString(VEC_TASK_CANCEL_MSG_DISABLED)))) {
                        LOG_WARN("fail to set disabled cancel err msg", KR(tmp_ret), KPC(task_ctx));
                      }
                    }
                    LOG_INFO("[VEC_IDX_TASK_DISABLED] task disabled, finishing with OB_CANCELED",
                             K_(tenant_id), K(ls_id), K(task_type), K(tablet_id_val),
                             K(task_ctx->task_status_.task_id_), K(task_ctx->task_status_.trace_id_));
                    break;
                  }
                  if (ddl_conflict_checked) {
                    bool is_conflict = false;
                    int tmp_ret = OB_SUCCESS;
                    if (OB_TMP_FAIL(ObVecITaskExecutor::check_task_ddl_conflict(
                            task_ctx, index_ls_mgr,
                            conflict_table_id_set, conflict_index_task_set, is_conflict))) {
                      LOG_WARN("fail to check ddl conflict, treat as no conflict", K(tmp_ret), KPC(task_ctx));
                    }
                    if (is_conflict) {
                      task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
                      task_ctx->task_status_.ret_code_ = OB_CANCELED;
                      task_ctx->task_status_.all_finished_ = true;
                      (void)task_ctx->set_err_msg(ObString(VEC_TASK_CANCEL_MSG_DDL_CONFLICT));
                      LOG_INFO("[VEC_ASYNC_TASK] task cancelled due to DDL conflict",
                               K_(tenant_id), K(ls_id), KPC(task_ctx));
                      break;
                    }
                  }
                  int64_t trigger_type = task_ctx->task_status_.trigger_type_;
                  int push_ret = OB_SUCCESS;
                  if (trigger_type == OB_VEC_TRIGGER_MANUAL) {
                    push_ret = queue_manager.push_manual(task_ctx);
                  } else {
                    push_ret = queue_manager.push(task_ctx, task_type);
                  }
                  if (OB_SUCCESS != push_ret) {
                    if (OB_SIZE_OVERFLOW == push_ret) {
                      LOG_WARN("queue full, skip task and retry next round",
                               K_(tenant_id), K(ls_id), K(task_type), K(trigger_type), KPC(task_ctx));
                    } else {
                      ret = push_ret;
                      LOG_WARN("fail to push task to priority queue",
                               KR(ret), K_(tenant_id), K(ls_id), K(*task_ctx));
                    }
                  } else {
                    task_ctx->in_queue_ = true;
                    task_ctx->task_status_.priority_ = static_cast<int64_t>(get_priority_by_task_type(
                        static_cast<ObVecIndexAsyncTaskType>(task_ctx->task_status_.task_type_),
                        task_ctx->task_status_.trigger_type_));
                    task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_QUEUE;
                    task_opt.inc_ls_queued_task_cnt();
                    LOG_INFO("[VEC_ASYNC_TASK] leader task enqueued, PREPARE->QUEUE",
                             K_(tenant_id), K(ls_id), K(task_ctx->task_status_.trace_id_),
                             K(task_ctx->task_status_.task_id_), K(task_ctx->task_status_.tablet_id_),
                             K(task_ctx->task_status_.task_type_), K(task_ctx->task_status_.trigger_type_));
                    int tmp_ret = OB_SUCCESS;
                    if (OB_TMP_FAIL(queued_ctx_array.push_back(task_ctx))) {
                      LOG_WARN("fail to push back queued_ctx_array", KR(tmp_ret), KPC(task_ctx));
                    }
                  }
                }
                break;
              }
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_QUEUE: {
                // task is in priority queue, nothing to do
                LOG_TRACE("task is in queue status", KPC(task_ctx));
                break;
              }
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH: {
                if (task_ctx->in_thread_pool_) {
                  LOG_TRACE("finished task still in thread pool, wait for handler exit", KPC(task_ctx));
                } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
                  LOG_WARN("fail to update task status to inner table", KR(ret), K_(tenant_id), K(ls_id), K(*task_ctx));
                } else if (OB_FAIL(task_ctx_array.push_back(task_ctx))) {
                  LOG_WARN("fail to push back task_ctx_array", KR(ret), K(task_ctx));
                }
                break;
              }
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL: {
                if (OB_FAIL(handle_cancelled_task_(*task_ctx, ls_id, cancelled_ctx_array))) {
                  LOG_WARN("fail to handle cancelled task", KR(ret), K_(tenant_id), K(ls_id), KPC(task_ctx));
                }
                break;
              }
              default:
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected task status", KR(ret), K(task_ctx->task_status_));
                break;
            }
          }
        }
      } // end if (!task_opt.is_stop())
      // cancel running tasks hit by DDL conflict outside of spinlock:
      // cancel_task() acquires task_ctx->lock_ internally and is idempotent,
      // so a task that finished in the meantime degrades to a no-op.
      for (int64_t j = 0; j < ddl_conflict_cancel_array.count(); j++) {
        int tmp_ret = OB_SUCCESS;
        ObVecIndexAsyncTaskCtx *cancel_ctx = ddl_conflict_cancel_array.at(j);
        if (OB_TMP_FAIL(cancel_ctx->cancel_task())) {
          LOG_WARN("fail to cancel running task due to DDL conflict", K(tmp_ret), KPC(cancel_ctx));
        } else {
          LOG_INFO("[VEC_ASYNC_TASK] running task cancelled due to DDL conflict",
                   K_(tenant_id), K(ls_id), KPC(cancel_ctx));
        }
      }
      // update QUEUE status to inner table outside of spinlock.
      // Only overwrite rows still in PREPARE — prevents racing with handle thread's
      // QUEUE->RUNNING update when pop_tasks_to_work runs later in the same round.
      ObSEArray<int64_t, 1> expected_prepare;
      (void)expected_prepare.push_back(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE);
      for (int64_t j = 0; j < queued_ctx_array.count(); j++) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code_if_match(
                queued_ctx_array.at(j), expected_prepare))) {
          LOG_WARN("fail to update queue status to inner table", KR(tmp_ret),
                   K_(tenant_id), K(ls_id), KPC(queued_ctx_array.at(j)));
        }
      }
      for (int64_t j = 0; j < cancelled_ctx_array.count(); j++) {
        int tmp_ret = ObVecIndexAsyncTaskUtil::update_status_and_ret_code(cancelled_ctx_array.at(j));
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to update cancelled task status to inner table", K(tmp_ret),
                   K_(tenant_id), K(ls_id), KPC(cancelled_ctx_array.at(j)));
        } else if (OB_SUCCESS != (tmp_ret = task_ctx_array.push_back(cancelled_ctx_array.at(j)))) {
          LOG_WARN("fail to push back task_ctx_array for cancelled task", K(tmp_ret),
                   K_(tenant_id), K(ls_id), KPC(cancelled_ctx_array.at(j)));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::clear_task_ctxs(task_opt, task_ctx_array))) {
        LOG_WARN("fail to clean map", KR(ret), K(task_ctx_array));
      }
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::check_and_schedule_follower_ls_tasks(
    const ObSEArray<std::pair<ObLSID, ObVecIdxFollowerExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &follower_executors_to_run)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<uint64_t> conflict_table_id_set;
  common::hash::ObHashSet<uint64_t> conflict_index_task_set;
  bool ddl_conflict_checked = false;
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(conflict_table_id_set.create(16))) {
      LOG_INFO("fail to create conflict_table_id_set, skip DDL conflict check", K(tmp_ret));
    } else if (OB_TMP_FAIL(conflict_index_task_set.create(16))) {
      LOG_INFO("fail to create conflict_index_task_set, skip DDL conflict check", K(tmp_ret));
    } else if (OB_TMP_FAIL(ObVecITaskExecutor::check_has_hnsw_ddl(tenant_id_, conflict_table_id_set, conflict_index_task_set))) {
      LOG_INFO("fail to check hnsw ddl, skip DDL conflict check", K(tmp_ret));
    } else {
      ddl_conflict_checked = true;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < follower_executors_to_run.count(); i++) {
    ObVecIdxFollowerExecutors *exec = follower_executors_to_run.at(i).second;
    const share::ObLSID &ls_id = follower_executors_to_run.at(i).first;
    ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
    if (OB_ISNULL(exec) || OB_ISNULL(service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", KR(ret), KP(exec), KP(service_));
    } else if (OB_FAIL(service_->acquire_vector_index_mgr(ls_id, index_ls_mgr))) {
      LOG_WARN("fail to acquire vector index mgr", KR(ret), K(ls_id));
    } else if (OB_ISNULL(index_ls_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", KR(ret), KP(index_ls_mgr));
    } else {
      ObVecIndexTaskCtxArray task_ctx_array;
      ObVecIndexTaskCtxArray cancelled_ctx_array;
      ObVecIndexTaskCtxArray queued_ctx_array; // collect newly queued tasks for inner table update
      // running tasks hit by DDL conflict; cancel_task() re-acquires task_ctx->lock_,
      // so it must run after ctx_guard is released — collected here, cancelled below.
      ObVecIndexTaskCtxArray ddl_conflict_cancel_array;
      ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
      ObVecIndexPriorityQueueManager &queue_manager = service_->get_vec_index_priority_queue_manager();
      if (task_opt.is_stop()) {
        // LS is being destroyed, skip scheduling new tasks into queue.
        LOG_INFO("ls task_opt is stopped, skip scheduling", K(ls_id));
      } else {
        FOREACH_X(iter, task_opt.get_async_task_map(), OB_SUCC(ret)) {
          ObVecIndexAsyncTaskCtx *task_ctx = iter->second;
          if (OB_ISNULL(task_ctx)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", KR(ret));
          } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_result(task_ctx))) {
            LOG_WARN("fail to check task result", KR(ret), KPC(task_ctx));
          } else {
            switch (task_ctx->task_status_.status_) {
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE:
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING:
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE:
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN: {
                common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
                if (task_ctx->in_thread_pool_) {
                  if (ddl_conflict_checked) {
                    bool is_conflict = false;
                    int tmp_ret = OB_SUCCESS;
                    if (OB_TMP_FAIL(ObVecITaskExecutor::check_task_ddl_conflict(
                            task_ctx, index_ls_mgr,
                            conflict_table_id_set, conflict_index_task_set, is_conflict))) {
                      LOG_WARN("fail to check ddl conflict for running task", K(tmp_ret), KPC(task_ctx));
                    }
                    if (is_conflict) {
                      (void)task_ctx->set_err_msg(ObString(VEC_TASK_CANCEL_MSG_DDL_CONFLICT));
                      if (OB_TMP_FAIL(ddl_conflict_cancel_array.push_back(task_ctx))) {
                        LOG_WARN("fail to push back ddl_conflict_cancel_array", K(tmp_ret), KPC(task_ctx));
                      }
                    }
                  }
                  LOG_TRACE("task is in thread pool already", KPC(task_ctx));
                } else if (task_ctx->in_queue_) {
                  LOG_TRACE("task is in priority queue already", KPC(task_ctx));
                } else {
                  ObVecIndexAsyncTaskType task_type =
                      static_cast<ObVecIndexAsyncTaskType>(task_ctx->task_status_.task_type_);
                  int64_t tablet_id_val = task_ctx->task_status_.tablet_id_.id();
                  if (is_task_disabled_(task_type, tablet_id_val)) {
                    task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
                    task_ctx->task_status_.ret_code_ = OB_CANCELED;
                    task_ctx->task_status_.all_finished_ = true;
                    {
                      int tmp_ret = OB_SUCCESS;
                      if (OB_TMP_FAIL(task_ctx->set_err_msg(ObString(VEC_TASK_CANCEL_MSG_DISABLED)))) {
                        LOG_WARN("fail to set disabled cancel err msg", KR(tmp_ret), KPC(task_ctx));
                      }
                    }
                    LOG_INFO("[VEC_IDX_TASK_DISABLED] task disabled, finishing with OB_CANCELED",
                             K_(tenant_id), K(ls_id), K(task_type), K(tablet_id_val),
                             K(task_ctx->task_status_.task_id_), K(task_ctx->task_status_.trace_id_));
                    break;
                  }
                  if (ddl_conflict_checked) {
                    bool is_conflict = false;
                    int tmp_ret = OB_SUCCESS;
                    if (OB_TMP_FAIL(ObVecITaskExecutor::check_task_ddl_conflict(
                            task_ctx, index_ls_mgr,
                            conflict_table_id_set, conflict_index_task_set, is_conflict))) {
                      LOG_WARN("fail to check ddl conflict, treat as no conflict", K(tmp_ret), KPC(task_ctx));
                    }
                    if (is_conflict) {
                      task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
                      task_ctx->task_status_.ret_code_ = OB_CANCELED;
                      task_ctx->task_status_.all_finished_ = true;
                      (void)task_ctx->set_err_msg(ObString(VEC_TASK_CANCEL_MSG_DDL_CONFLICT));
                      LOG_INFO("[VEC_ASYNC_TASK] task cancelled due to DDL conflict",
                               K_(tenant_id), K(ls_id), KPC(task_ctx));
                      break;
                    }
                  }
                  int64_t trigger_type = task_ctx->task_status_.trigger_type_;
                  int push_ret = OB_SUCCESS;
                  if (trigger_type == OB_VEC_TRIGGER_MANUAL) {
                    push_ret = queue_manager.push_manual(task_ctx);
                  } else {
                    push_ret = queue_manager.push(task_ctx, task_type);
                  }
                  if (OB_SUCCESS != push_ret) {
                    if (OB_SIZE_OVERFLOW == push_ret) {
                      LOG_WARN("queue full, skip task and retry next round",
                               K_(tenant_id), K(ls_id), K(task_type), K(trigger_type), KPC(task_ctx));
                    } else {
                      ret = push_ret;
                      LOG_WARN("fail to push task to priority queue",
                               KR(ret), K_(tenant_id), K(ls_id), K(*task_ctx));
                    }
                  } else {
                    task_ctx->in_queue_ = true;
                    task_ctx->task_status_.priority_ = static_cast<int64_t>(get_priority_by_task_type(
                        static_cast<ObVecIndexAsyncTaskType>(task_ctx->task_status_.task_type_),
                        task_ctx->task_status_.trigger_type_));
                    task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_QUEUE;
                    task_opt.inc_ls_queued_task_cnt();
                    LOG_INFO("[VEC_ASYNC_TASK] follower task enqueued, PREPARE->QUEUE",
                             K_(tenant_id), K(ls_id), K(task_ctx->task_status_.trace_id_),
                             K(task_ctx->task_status_.task_id_), K(task_ctx->task_status_.tablet_id_),
                             K(task_ctx->task_status_.task_type_), K(task_ctx->task_status_.trigger_type_));
                    int tmp_ret = OB_SUCCESS;
                    if (OB_TMP_FAIL(queued_ctx_array.push_back(task_ctx))) {
                      LOG_WARN("fail to push back queued_ctx_array", KR(tmp_ret), KPC(task_ctx));
                    }
                  }
                }
                break;
              }
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_QUEUE: {
                // task is in priority queue, nothing to do
                LOG_TRACE("task is in queue status", KPC(task_ctx));
                break;
              }
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH: {
                if (task_ctx->in_thread_pool_) {
                  LOG_TRACE("finished task still in thread pool, wait for handler exit", KPC(task_ctx));
                } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
                  LOG_WARN("fail to update task status to inner table", KR(ret), K_(tenant_id), K(ls_id), K(*task_ctx));
                } else if (OB_FAIL(task_ctx_array.push_back(task_ctx))) {
                  LOG_WARN("fail to push back task_ctx_array", KR(ret), K(task_ctx));
                }
                break;
              }
              case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL: {
                if (OB_FAIL(handle_cancelled_task_(*task_ctx, ls_id, cancelled_ctx_array))) {
                  LOG_WARN("fail to handle cancelled task", KR(ret), K_(tenant_id), K(ls_id), KPC(task_ctx));
                }
                break;
              }
              default:
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected task status", KR(ret), K(task_ctx->task_status_));
                break;
            }
          }
        }
      } // end if (!task_opt.is_stop())
      // cancel running tasks hit by DDL conflict outside of spinlock:
      // cancel_task() acquires task_ctx->lock_ internally and is idempotent,
      // so a task that finished in the meantime degrades to a no-op.
      for (int64_t j = 0; j < ddl_conflict_cancel_array.count(); j++) {
        int tmp_ret = OB_SUCCESS;
        ObVecIndexAsyncTaskCtx *cancel_ctx = ddl_conflict_cancel_array.at(j);
        if (OB_TMP_FAIL(cancel_ctx->cancel_task())) {
          LOG_WARN("fail to cancel running task due to DDL conflict", K(tmp_ret), KPC(cancel_ctx));
        } else {
          LOG_INFO("[VEC_ASYNC_TASK] running task cancelled due to DDL conflict",
                   K_(tenant_id), K(ls_id), KPC(cancel_ctx));
        }
      }
      // update QUEUE status to inner table outside of spinlock.
      // Only overwrite rows still in PREPARE — prevents racing with handle thread's
      // QUEUE->RUNNING update when pop_tasks_to_work runs later in the same round.
      ObSEArray<int64_t, 1> expected_prepare;
      (void)expected_prepare.push_back(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE);
      for (int64_t j = 0; j < queued_ctx_array.count(); j++) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code_if_match(
                queued_ctx_array.at(j), expected_prepare))) {
          LOG_WARN("fail to update queue status to inner table", KR(tmp_ret),
                   K_(tenant_id), K(ls_id), KPC(queued_ctx_array.at(j)));
        }
      }
      for (int64_t j = 0; j < cancelled_ctx_array.count(); j++) {
        int tmp_ret = ObVecIndexAsyncTaskUtil::update_status_and_ret_code(cancelled_ctx_array.at(j));
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to update cancelled task status to inner table", K(tmp_ret),
                   K_(tenant_id), K(ls_id), KPC(cancelled_ctx_array.at(j)));
        } else if (OB_SUCCESS != (tmp_ret = task_ctx_array.push_back(cancelled_ctx_array.at(j)))) {
          LOG_WARN("fail to push back task_ctx_array for cancelled task", K(tmp_ret),
                   K_(tenant_id), K(ls_id), KPC(cancelled_ctx_array.at(j)));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::clear_task_ctxs(task_opt, task_ctx_array))) {
        LOG_WARN("fail to clean map", KR(ret), K(task_ctx_array));
      }
    }
  }
  return ret;
}

// Check if there's a running task with the same tablet in thread pool
// Query by tablet_id + all task_types
static bool has_running_task_with_same_tablet(ObVecIndexAsyncTaskOption &task_opt, ObVecIndexAsyncTaskCtx *process_task_ctx)
{
  bool has_running = false;
  common::ObTabletID tablet_id = common::ObTabletID(OB_INVALID_ID);
  if (OB_NOT_NULL(process_task_ctx)) {
    tablet_id = process_task_ctx->task_status_.tablet_id_;
  }
  for (uint32_t task_type = 0; task_type < ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_TASK_TYPE_INVALID; ++task_type) {
    ObVecIndexAsyncTaskKey key(tablet_id, task_type);
    ObVecIndexAsyncTaskCtx *running_task_ctx = nullptr;
    if (OB_SUCCESS == task_opt.get_async_task_map().get_refactored(key, running_task_ctx)) {
      if (OB_NOT_NULL(running_task_ctx) && running_task_ctx->in_thread_pool_) {
        has_running = true;
        LOG_INFO("found running task with same tablet", K(tablet_id), K(task_type), KPC(running_task_ctx), KPC(process_task_ctx));
        break;
      }
    }
  }
  return has_running;
}

int ObVecIdxAsyncTaskScheduler::pop_tasks_to_work()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObVecIndexPriorityQueueManager &queue_manager = service_->get_vec_index_priority_queue_manager();
  ObVecIndexAsyncTaskHandler &task_handle = service_->get_vec_async_task_handle();
  const int tg_id = task_handle.get_tg_id();
  if (tg_id < 0) {
    ret = OB_NOT_INIT;
    if (REACH_TIME_INTERVAL(60L * 1000000)) {
      LOG_INFO("[VEC_ASYNC_TASK] vec async task handler tg not inited, no vector index yet", KR(ret));
    }
  } else {
    int64_t thread_count = TG_GET_THREAD_CNT(tg_id);
    thread_count = (thread_count > 0) ? thread_count : 1;
    ObVecAsyncSchedDiagStat round_stat;
    int64_t final_running_count = 0;
    int64_t last_water_level = 0;
    bool pop_empty_seen = false;
    static const int64_t MAX_WATER_LEVEL_CONFIG_LEN = 4096;
    char water_level_config_buf[MAX_WATER_LEVEL_CONFIG_LEN + 1];
    water_level_config_buf[0] = '\0';
    uint64_t current_water_level_config_hash = 0;
    bool water_level_config_refreshed = true;
    ObSEArray<ObVecIndexAsyncTaskOption *, DEFAULT_LS_EXECUTOR_MAP_SIZE> all_task_opts;
    LSIndexMgrMap &ls_index_mgr_map = service_->get_ls_index_mgr_map();
    LSIndexMgrMap::iterator mgr_iter;
    for (mgr_iter = ls_index_mgr_map.begin();
         OB_SUCC(ret) && mgr_iter != ls_index_mgr_map.end();
         ++mgr_iter) {
      ObPluginVectorIndexMgr *mgr = mgr_iter->second;
      if (OB_NOT_NULL(mgr)) {
        if (OB_FAIL(all_task_opts.push_back(&mgr->get_async_task_opt()))) {
          LOG_WARN("fail to push back task_opt for mem gate", KR(ret), K_(tenant_id));
        }
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to collect task opts for mem gate", KR(ret), K_(tenant_id));
      ret = OB_SUCCESS;
    }

    // Sync per-task-type water level thresholds from tenant config before pop loop.
    // Also check whether the tenant CPU allocation has changed and resize the thread pool
    // accordingly. refresh_thread_count() is cheap (no-op when cpu is unchanged).
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      if (tenant_config.is_valid()) {
        const char *config_str = tenant_config->vector_task_thread_limit_percent.str();
        if (OB_NOT_NULL(config_str)) {
          const int64_t config_len = OB_MIN(static_cast<int64_t>(STRLEN(config_str)), MAX_WATER_LEVEL_CONFIG_LEN);
          MEMCPY(water_level_config_buf, config_str, config_len);
          water_level_config_buf[config_len] = '\0';
        }
        current_water_level_config_hash = ('\0' == water_level_config_buf[0])
            ? 0 : murmurhash(water_level_config_buf,
                             static_cast<int32_t>(STRLEN(water_level_config_buf)), 0);
        if (OB_TMP_FAIL(queue_manager.refresh_water_level_config(water_level_config_buf))) {
          water_level_config_refreshed = false;
          LOG_WARN("refresh vector async task water level config failed", K(tmp_ret),
                   K_(tenant_id),
                   "water_level_config", ObString(static_cast<int32_t>(STRLEN(water_level_config_buf)),
                                                   water_level_config_buf));
        }
      }
    }
    if (OB_TMP_FAIL(task_handle.refresh_thread_count())) {
      LOG_WARN("refresh_thread_count failed in scheduler, using current thread count", K(tmp_ret));
    }
    // Re-read thread_count after potential resize so water-level is computed on updated value.
    thread_count = TG_GET_THREAD_CNT(tg_id);
    thread_count = (thread_count > 0) ? thread_count : 1;
    // Propagate the new thread count to the queue manager so effective_threshold_ is consistent.
    queue_manager.update_thread_limit(thread_count);
    if (water_level_config_refreshed
        && last_water_level_config_hash_ != current_water_level_config_hash) {
      ObVecIndexQueuePopDiag diag;
      queue_manager.get_pop_diag(0, diag);
      LOG_INFO("[VEC_ASYNC_TASK_SCHED] water level config changed",
               K_(tenant_id), K_(last_water_level_config_hash),
               K(current_water_level_config_hash),
               "water_level_config",
               ObString(static_cast<int32_t>(STRLEN(water_level_config_buf)), water_level_config_buf),
               K(diag));
      last_water_level_config_hash_ = current_water_level_config_hash;
    }
    if (last_thread_count_ != thread_count) {
      ObVecIndexQueuePopDiag diag;
      queue_manager.get_pop_diag(0, diag);
      const int64_t running_count_for_diag = task_handle.get_async_task_ref();
      LOG_INFO("[VEC_ASYNC_TASK_SCHED] thread count changed",
               K_(tenant_id), K(tg_id), K(last_thread_count_), K(thread_count),
               K(running_count_for_diag), K(diag));
      last_thread_count_ = thread_count;
    }

    common::hash::ObHashSet<uint64_t> conflict_table_id_set;
    common::hash::ObHashSet<uint64_t> conflict_index_task_set;
    bool ddl_conflict_checked = false;
    {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(conflict_table_id_set.create(16))) {
        LOG_INFO("fail to create conflict_table_id_set, skip DDL conflict check", K(tmp_ret));
      } else if (OB_TMP_FAIL(conflict_index_task_set.create(16))) {
        LOG_INFO("fail to create conflict_index_task_set, skip DDL conflict check", K(tmp_ret));
      } else if (OB_TMP_FAIL(ObVecITaskExecutor::check_has_hnsw_ddl(tenant_id_, conflict_table_id_set, conflict_index_task_set))) {
        LOG_INFO("fail to check hnsw ddl, skip DDL conflict check", K(tmp_ret));
      } else {
        ddl_conflict_checked = true;
      }
    }

    while (OB_SUCC(ret)) {
      // recalculate water level at the beginning of each loop iteration
      // Since queue_manager is responsible for queuing, the thread pool internal
      // queue is always near-empty; async_task_ref directly equals running count.
      int64_t running_count = std::max(0L, task_handle.get_async_task_ref());
      final_running_count = running_count;

      // Use post-admission water level: (running_count + 1) * 100 / thread_count.
      // Rationale: integer division on running_count underestimates the true load
      // (e.g. thr=12, running=8 → 66%, admitting one more takes it to 9/12=75%,
      // which overshoots a 70% threshold by one slot). Comparing against the
      // water level *after* admitting this candidate makes the threshold a hard
      // cap on actual concurrency, matching the slot table in queue_manager.
      const int64_t water_level = (thread_count > 0) ? ((running_count + 1) * 100 / thread_count) : 0;
      last_water_level = water_level;

      ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
      ObLSID popped_ls_id;
      ret = queue_manager.pop(task_ctx, water_level, &popped_ls_id);
      if (OB_ENTRY_NOT_EXIST == ret) {
        pop_empty_seen = true;
        ret = OB_SUCCESS;
        break;
      }
      if (OB_FAIL(ret) || OB_ISNULL(task_ctx)) {
        LOG_WARN("fail to pop from priority queue", KR(ret), KP(task_ctx));
        break;
      }
      round_stat.pop_cnt_++;
      // Valid ctx popped from queue: decrement ls_queued_task_cnt_ (push had inc'd).
      // Invalid nodes are consumed inside pop() without returning; ObPluginVectorIndexLoadScheduler::stop()
      // already decremented the counter when marking nodes invalid.
      ObLSID ls_id = popped_ls_id;
      if (OB_NOT_NULL(task_ctx->get_ls())) {
        ls_id = task_ctx->get_ls()->get_ls_id();
      }
      ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
      if (ls_id.is_valid() && OB_NOT_NULL(service_)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = service_->get_ls_index_mgr_map().get_refactored(ls_id, index_ls_mgr))) {
          LOG_WARN("fail to get vector index ls mgr after pop", KR(tmp_ret), K(ls_id));
        } else if (OB_NOT_NULL(index_ls_mgr)) {
          index_ls_mgr->get_async_task_opt().dec_ls_queued_task_cnt();
        } else {
          LOG_WARN("null vector index ls mgr after pop", K(ls_id), KPC(task_ctx));
        }
      }
      if (OB_ISNULL(task_ctx->get_ls())) {
        LOG_WARN("task ctx ls is null, skip scheduling", KPC(task_ctx), K(ls_id));
        continue;
      }
      if (OB_NOT_NULL(index_ls_mgr) && index_ls_mgr->get_async_task_opt().is_stop()) {
        round_stat.skip_stopped_ls_cnt_++;
        LOG_INFO("ls is stopped, discard queued task ctx after valid pop", K(ls_id), KPC(task_ctx));
        continue;
      }

      // Check if there's a running task with the same tablet
      if (OB_NOT_NULL(index_ls_mgr) && has_running_task_with_same_tablet(index_ls_mgr->get_async_task_opt(), task_ctx)) {
        round_stat.skip_same_tablet_cnt_++;
        LOG_INFO("skip task because same tablet is already running, reset to PREPARE for re-enqueue", KPC(task_ctx));
        common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
        task_ctx->in_queue_ = false;
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
        continue;
      }

      // Check disable list for tasks already in queue before config was set
      {
        ObVecIndexAsyncTaskType pop_task_type =
            static_cast<ObVecIndexAsyncTaskType>(task_ctx->task_status_.task_type_);
        int64_t pop_tablet_id = task_ctx->task_status_.tablet_id_.id();
        if (is_task_disabled_(pop_task_type, pop_tablet_id)) {
          round_stat.skip_disabled_cnt_++;
          common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
          task_ctx->in_queue_ = false;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
          task_ctx->task_status_.ret_code_ = OB_CANCELED;
          task_ctx->task_status_.all_finished_ = true;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(task_ctx->set_err_msg(ObString(VEC_TASK_CANCEL_MSG_DISABLED)))) {
            LOG_WARN("fail to set disabled cancel err msg", KR(tmp_ret), KPC(task_ctx));
          }
          LOG_INFO("[VEC_IDX_TASK_DISABLED] queued task disabled, finishing with OB_CANCELED",
                   K_(tenant_id), K(ls_id), K(pop_task_type), K(pop_tablet_id), KPC(task_ctx));
          if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
            LOG_WARN("fail to update inner table for disabled queued task", KR(tmp_ret), KPC(task_ctx));
          }
          continue;
        }
      }
      if (ddl_conflict_checked && OB_NOT_NULL(index_ls_mgr)) {
        bool is_conflict = false;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObVecITaskExecutor::check_task_ddl_conflict(
                task_ctx, index_ls_mgr,
                conflict_table_id_set, conflict_index_task_set, is_conflict))) {
          LOG_WARN("fail to check ddl conflict in pop, treat as no conflict", K(tmp_ret), KPC(task_ctx));
        }
        if (is_conflict) {
          round_stat.skip_ddl_conflict_cnt_++;
          common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
          task_ctx->in_queue_ = false;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
          task_ctx->task_status_.ret_code_ = OB_CANCELED;
          task_ctx->task_status_.all_finished_ = true;
          (void)task_ctx->set_err_msg(ObString(VEC_TASK_CANCEL_MSG_DDL_CONFLICT));
          LOG_INFO("[VEC_ASYNC_TASK] queued task cancelled due to DDL conflict",
                   K_(tenant_id), K(ls_id), KPC(task_ctx));
          if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
            LOG_WARN("fail to update inner table for DDL-conflicted task", KR(tmp_ret), KPC(task_ctx));
          }
          continue;
        }
      }
      ObIAllocator *allocator = nullptr;
      if (OB_FAIL(task_handle.get_allocator_by_ls(ls_id, allocator))) {
        LOG_WARN("fail to get allocator by ls, skip this task", KR(ret), K(ls_id));
        common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
        task_ctx->in_queue_ = false;
        ret = OB_SUCCESS;  // reset ret to continue scheduling other tasks
        continue;
      }
      if (OB_SUCC(ret) && OB_ISNULL(allocator)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocator is null", KR(ret), K(ls_id));
        common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
        task_ctx->in_queue_ = false;
        continue;
      }
      int tmp_ret = OB_SUCCESS;
      bool can_start_by_mem = true;
      if (task_ctx->need_mem_limit()) {
        if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_can_start_by_memory(
                all_task_opts, task_ctx, can_start_by_mem))) {
          LOG_WARN("fail to check task memory", K(ret), KPC(task_ctx));
          ret = OB_SUCCESS;
          can_start_by_mem = true;
        } else if (!can_start_by_mem) {
          round_stat.mem_block_cnt_++;
          {
            common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
            task_ctx->in_queue_ = false;
            task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
          }
          if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
            LOG_WARN("fail to update task status to inner table", KR(tmp_ret), K_(tenant_id), K(ls_id), K(*task_ctx));
          }
          continue;
        }
      }
      bool sys_task_added_this_round = false;
      if (OB_SUCC(ret) && can_start_by_mem) {
        // Set the running marker before TG_PUSH_TASK. A fast worker can finish
        // before this scheduler thread returns from push_task().
        common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
        task_ctx->in_queue_ = false;
        task_ctx->in_thread_pool_ = true;
      }
      if (OB_SUCC(ret) && can_start_by_mem && task_ctx->sys_task_id_.is_invalid()) {
        if (OB_SUCCESS != (tmp_ret = ObVecIndexAsyncTaskUtil::add_sys_task(task_ctx))) {
          LOG_WARN("add sys task failed, rollback QUEUE->PREPARE", K(tmp_ret), KPC(task_ctx));
          {
            common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
            task_ctx->in_thread_pool_ = false;
            task_ctx->in_queue_ = false;
            task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
            task_ctx->task_status_.ret_code_ = VEC_ASYNC_TASK_DEFAULT_ERR_CODE;
            task_ctx->task_status_.all_finished_ = false;
          }
          if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
            LOG_WARN("fail to sync rollback status to inner table", KR(tmp_ret), K_(tenant_id), K(ls_id), KPC(task_ctx));
          }
          continue;
        } else {
          sys_task_added_this_round = true;
        }
      }
      if (OB_SUCC(ret) && can_start_by_mem && OB_FAIL(task_handle.push_task(tenant_id_, ls_id, task_ctx, allocator))) {
        round_stat.push_fail_cnt_++;
        LOG_WARN("fail to push task to thread pool, rollback QUEUE->PREPARE",
                 KR(ret), K_(tenant_id), K(ls_id),
                 K(task_ctx->task_status_.trace_id_),
                 K(task_ctx->task_status_.task_id_), K(task_ctx->task_status_.tablet_id_),
                 K(task_ctx->task_status_.task_type_));
        if (sys_task_added_this_round) {
          if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::remove_sys_task(task_ctx))) {
            LOG_WARN("fail to remove sys task after push failure", KR(tmp_ret), KPC(task_ctx));
          } else {
            task_ctx->sys_task_id_ = TraceId();
          }
        }
        {
          common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
          task_ctx->in_thread_pool_ = false;
          task_ctx->in_queue_ = false;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
          task_ctx->task_status_.ret_code_ = VEC_ASYNC_TASK_DEFAULT_ERR_CODE;
          task_ctx->task_status_.all_finished_ = false;
        }
        if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
          LOG_WARN("fail to sync rollback status to inner table", KR(tmp_ret), K_(tenant_id), K(ls_id), KPC(task_ctx));
        }
        break;
      } else if (OB_SUCC(ret) && can_start_by_mem) {
        round_stat.push_cnt_++;
      }
    }
    sched_diag_stat_.add(round_stat);
    final_running_count = std::max(0L, task_handle.get_async_task_ref());
    last_water_level = (thread_count > 0) ? ((final_running_count + 1) * 100 / thread_count) : 0;
    int64_t tg_queue_num = -1;
    if (OB_TMP_FAIL(TG_GET_QUEUE_NUM(tg_id, tg_queue_num))) {
      tg_queue_num = -1;
    }
    ObVecIndexQueuePopDiag final_diag;
    queue_manager.get_pop_diag(last_water_level, final_diag);
    const int64_t now = ObTimeUtility::current_time();
    if (!last_has_queued_ && final_diag.has_queued_) {
      LOG_INFO("[VEC_ASYNC_TASK_SCHED] queue became nonempty",
               K_(tenant_id), K(tg_id), K(thread_count), K(final_running_count),
               K(tg_queue_num), K(final_diag));
    } else if (last_has_queued_ && !final_diag.has_queued_) {
      LOG_INFO("[VEC_ASYNC_TASK_SCHED] queue drained",
               K_(tenant_id), K(tg_id), K(thread_count), K(final_running_count),
               K(tg_queue_num), K(last_total_queued_), K(round_stat));
    }
    if (pop_empty_seen && final_diag.has_queued_ && final_diag.blocked_by_water_level_) {
      if (!last_blocked_by_water_level_
          || now - last_blocked_diag_ts_ >= SCHED_DIAG_BLOCKED_INTERVAL_US) {
        LOG_INFO("[VEC_ASYNC_TASK_SCHED] pop blocked by water level",
                 K_(tenant_id), K(tg_id), K(thread_count), K(final_running_count),
                 K(tg_queue_num), K(last_water_level), K(final_diag));
        last_blocked_diag_ts_ = now;
      }
    } else if (last_blocked_by_water_level_ && !final_diag.blocked_by_water_level_) {
      LOG_INFO("[VEC_ASYNC_TASK_SCHED] water level block recovered",
               K_(tenant_id), K(tg_id), K(thread_count), K(final_running_count),
               K(tg_queue_num), K(last_water_level), K(final_diag));
    }
    if (pop_empty_seen && final_diag.has_queued_ && !final_diag.blocked_by_water_level_
        && now - last_unexpected_no_pop_diag_ts_ >= SCHED_DIAG_NO_POP_INTERVAL_US) {
      LOG_INFO("[VEC_ASYNC_TASK_SCHED] pop returned empty with queued tasks",
               K_(tenant_id), K(tg_id), K(thread_count), K(final_running_count),
               K(tg_queue_num), K(last_water_level), K(final_diag));
      last_unexpected_no_pop_diag_ts_ = now;
    }
    const bool active = final_diag.has_queued_ || final_running_count > 0
        || sched_diag_stat_.pop_cnt_ > 0 || sched_diag_stat_.push_cnt_ > 0
        || sched_diag_stat_.get_skip_cnt() > 0 || sched_diag_stat_.push_fail_cnt_ > 0;
    if (active && now - last_sched_snapshot_ts_ >= SCHED_DIAG_SNAPSHOT_INTERVAL_US) {
      LOG_INFO("[VEC_ASYNC_TASK_SCHED] scheduler snapshot",
               K_(tenant_id), K(tg_id), K(thread_count), K(final_running_count),
               K(tg_queue_num), K(last_water_level), K(final_diag), K(sched_diag_stat_));
      last_sched_snapshot_ts_ = now;
      sched_diag_stat_.reset();
    }
    last_has_queued_ = final_diag.has_queued_;
    last_blocked_by_water_level_ = final_diag.blocked_by_water_level_;
    last_total_queued_ = final_diag.total_queued_;
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::load_leader_ls_tasks(const ObLSID &ls_id, ObVecIdxLeaderExecutors &exec, bool has_ivf_index, uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  bool schema_changed = false;
  // Check if this LS needs task recovery from inner table (set by switch_to_leader).
  // R1 sweep: push self-owned non-FINISH inner-table rows for this LS to FINISH+CANCELED.
  if (exec.test_and_clear_need_resume()) {
    int tmp_ret = exec.async_task_exec_.resume_task();
    if (OB_SUCCESS != tmp_ret) {
      // Restore the flag so the next timer round retries the resume
      exec.set_need_resume(true);
      LOG_WARN("fail to resume tasks on leader switch", K(tmp_ret), K(ls_id), K_(tenant_id));
    } else {
      LOG_INFO("swept self residual tasks on leader switch", K(ls_id), K_(tenant_id));
    }
  }
  if (OB_FAIL(exec.async_task_exec_.check_and_set_thread_pool())) {
    LOG_WARN("fail to check and open thread pool", K(ret));
  } else {
    int first_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    if (can_schedule(ObVectorTaskScheduleType::HNSW_OPTIMIZE)
        && OB_SUCCESS != (tmp_ret = exec.async_task_exec_.load_task(task_trace_base_num))) {
      LOG_WARN("fail to load hnsw optimize task", K(tmp_ret));
      first_ret = OB_SUCC(first_ret) ? tmp_ret : first_ret;
    }
    if (OB_SUCCESS != (tmp_ret = exec.embedding_task_exec_.load_task(task_trace_base_num))) {
      LOG_WARN("fail to load embedding task", K(tmp_ret));
      first_ret = OB_SUCC(first_ret) ? tmp_ret : first_ret;
    }
    if (can_schedule(ObVectorTaskScheduleType::HNSW_FREEZE)
        && OB_SUCCESS != (tmp_ret = exec.freeze_exec_.load_task(task_trace_base_num))) {
      LOG_WARN("fail to load hnsw freeze task", K(tmp_ret));
      first_ret = OB_SUCC(first_ret) ? tmp_ret : first_ret;
    }
    if (can_schedule(ObVectorTaskScheduleType::HNSW_MERGE)
        && OB_SUCCESS != (tmp_ret = exec.merge_exec_.load_task(task_trace_base_num))) {
      LOG_WARN("fail to load hnsw merge task", K(tmp_ret));
      first_ret = OB_SUCC(first_ret) ? tmp_ret : first_ret;
    }
    if (can_schedule(ObVectorTaskScheduleType::FOLLOWER_SYNC)) {
      if (OB_SUCCESS != (tmp_ret = exec.mem_sync_exec_.check_and_set_thread_pool())) {
        LOG_WARN("fail to check and open thread pool", K(tmp_ret), K(ls_id));
        first_ret = OB_SUCC(first_ret) ? tmp_ret : first_ret;
      } else if (OB_SUCCESS != (tmp_ret = exec.mem_sync_exec_.load_task(task_trace_base_num))) {
        LOG_WARN("fail to load mem sync task", K(tmp_ret), K(ls_id));
        first_ret = OB_SUCC(first_ret) ? tmp_ret : first_ret;
      }
    }
    ret = first_ret;
  }
  // IVF_TASK
  if (can_schedule(ObVectorTaskScheduleType::IVF_TASK)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = exec.ivf_task_exec_.check_schema_version_changed(schema_changed))) {
      LOG_WARN("fail to check ivf schema version changed", K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (!schema_changed) {
      LOG_TRACE("ivf schema not changed, skip ivf task loading");
    } else if (has_ivf_index
               && OB_SUCCESS != (tmp_ret = exec.ivf_task_exec_.check_and_set_thread_pool())) {
      LOG_WARN("fail to check and open ivf thread pool", K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_SUCCESS != (tmp_ret = exec.ivf_task_exec_.load_task(task_trace_base_num))) {
      LOG_WARN("fail to load ivf task", K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::load_follower_ls_tasks(const ObLSID &ls_id, ObVecIdxFollowerExecutors &exec, uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  // R1 sweep on the follower side: when this observer just became follower for ls_id,
  // it may have left non-FINISH inner-table rows (with exec_addr=self) from its previous
  // leader life. Push them to FINISH+CANCELED. mem_sync_exec_ is reused since both follower
  // executors share the same ls_handle / tenant_id and the sweep is observer/LS-scoped.
  if (exec.test_and_clear_need_resume()) {
    int tmp_ret = exec.mem_sync_exec_.resume_task();
    if (OB_SUCCESS != tmp_ret) {
      exec.set_need_resume(true);
      LOG_WARN("fail to sweep self residual on follower switch", K(tmp_ret), K(ls_id), K_(tenant_id));
    } else {
      LOG_INFO("swept self residual tasks on follower switch", K(ls_id), K_(tenant_id));
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = exec.mem_sync_exec_.check_and_set_thread_pool())) {
    LOG_WARN("fail to check and open thread pool", K(tmp_ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  } else if (can_schedule(ObVectorTaskScheduleType::FOLLOWER_SYNC)
             && OB_SUCCESS != (tmp_ret = exec.mem_sync_exec_.load_task(task_trace_base_num))) {
    LOG_WARN("fail to load mem sync task", K(tmp_ret), K(ls_id));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  if (can_schedule(ObVectorTaskScheduleType::IVF_TASK)) {
    if (OB_SUCCESS != (tmp_ret = exec.ivf_task_exec_.load_task(task_trace_base_num))) {
      LOG_WARN("fail to load ivf task", K(tmp_ret), K(ls_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

void ObVecIdxAsyncTaskScheduler::remove_and_destroy_ls_executors(const share::ObLSID &ls_id)
{
  if (IS_NOT_INIT || !ls_id.is_valid()) {
    return;
  }
  common::ObSpinLockGuard guard(ls_executor_lock_);
  ObVecIdxLeaderExecutors *leader_exec = nullptr;
  const int lr = ls_leader_executor_map_.erase_refactored(ls_id, &leader_exec);
  if (OB_SUCCESS == lr && OB_NOT_NULL(leader_exec)) {
    retire_leader_executor_(leader_exec);
  } else if (OB_HASH_NOT_EXIST != lr) {
    int ret = lr;
    LOG_WARN("fail to erase vec idx leader executor for ls", KR(ret), K(ls_id), K(tenant_id_));
  }

  ObVecIdxFollowerExecutors *follower_exec = nullptr;
  const int fr = ls_follower_executor_map_.erase_refactored(ls_id, &follower_exec);
  if (OB_SUCCESS == fr && OB_NOT_NULL(follower_exec)) {
    retire_follower_executor_(follower_exec);
  } else if (OB_HASH_NOT_EXIST != fr) {
    int ret = fr;
    LOG_WARN("fail to erase vec idx follower executor for ls", KR(ret), K(ls_id), K(tenant_id_));
  }
}

int ObVecIdxAsyncTaskScheduler::cancel_ls_leader_tasks(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vec idx async task scheduler not inited", KR(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else {
    ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
    if (OB_FAIL(service_->acquire_vector_index_mgr(ls_id, index_ls_mgr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // LS mgr not found, nothing to cancel
      } else {
        LOG_WARN("fail to acquire vector index mgr", KR(ret), K(ls_id));
      }
    } else if (OB_ISNULL(index_ls_mgr)) {
      // no mgr, nothing to cancel
    } else {
      ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
      ObString cancel_msg(VEC_TASK_CANCEL_MSG_LEADER_SWITCH);
      // Lightweight cancel: only mark in-memory CANCEL. Scheduler tick drains
      // the deferred DB sync + kill_inner_sql so the switch path stays cheap.
      ObAsyncTaskCancelFunc cancel_func(cancel_msg, /*defer_post_work=*/true);
      if (OB_FAIL(task_opt.get_async_task_map().foreach_refactored(cancel_func))) {
        LOG_WARN("fail to cancel tasks in async task map on follower switch", KR(ret), K(ls_id));
      }
      LOG_INFO("marked leader tasks CANCEL for ls on follower switch", K(ls_id), K_(tenant_id));
    }
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::cancel_ls_follower_tasks(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vec idx async task scheduler not inited", KR(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else {
    ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
    if (OB_FAIL(service_->acquire_vector_index_mgr(ls_id, index_ls_mgr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to acquire vector index mgr", KR(ret), K(ls_id));
      }
    } else if (OB_ISNULL(index_ls_mgr)) {
      // no mgr, nothing to cancel
    } else {
      ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
      ObString cancel_msg(VEC_TASK_CANCEL_MSG_FOLLOWER_TO_LEADER);
      // Lightweight cancel: only mark in-memory CANCEL. Scheduler tick drains
      // the deferred DB sync + kill_inner_sql so the switch path stays cheap.
      ObAsyncTaskCancelFunc cancel_func(cancel_msg, /*defer_post_work=*/true);
      if (OB_FAIL(task_opt.get_async_task_map().foreach_refactored(cancel_func))) {
        LOG_WARN("fail to cancel follower tasks in async task map on leader switch",
                 KR(ret), K(ls_id));
      }
      LOG_INFO("marked follower tasks CANCEL for ls on leader switch", K(ls_id), K_(tenant_id));
    }
  }
  return ret;
}

/**
 * @brief Cancel all async task ctxs whose tablet_id matches the given one.
 *
 * Performs cancellation directly inside foreach_refactored callback where the
 * hash bucket read lock is held, preventing concurrent erase+free (UAF).
 * Aggregates the first non-SUCCESS failure into ret so callers can react;
 * per-ctx failures are logged but do not abort the sweep.
 */
int ObVecIdxAsyncTaskScheduler::cancel_tablet_async_tasks_(
    const common::ObTabletID &tablet_id,
    ObVecIndexAsyncTaskOption &task_opt)
{
  int ret = OB_SUCCESS;
  ObAsyncTaskCancelByTabletFunc cancel_func(tablet_id);
  if (OB_FAIL(task_opt.get_async_task_map().foreach_refactored(cancel_func))) {
    LOG_WARN("fail to cancel tablet tasks in async task map", KR(ret), K(tablet_id));
  } else if (OB_SUCCESS != cancel_func.get_first_fail_ret()) {
    ret = cancel_func.get_first_fail_ret();
  }
  return ret;
}

int ObVecIdxAsyncTaskScheduler::cancel_tablet_async_tasks_for_truncated_(
    const common::ObTabletID &tablet_id,
    const int64_t truncate_version,
    ObVecIndexAsyncTaskOption &task_opt)
{
  int ret = OB_SUCCESS;
  ObAsyncTaskCancelByTruncateFunc cancel_func(tablet_id, truncate_version);
  if (OB_FAIL(task_opt.get_async_task_map().foreach_refactored(cancel_func))) {
    LOG_WARN("fail to cancel tablet tasks for truncated", KR(ret), K(tablet_id), K(truncate_version));
  } else if (OB_SUCCESS != cancel_func.get_first_fail_ret()) {
    ret = cancel_func.get_first_fail_ret();
  }
  return ret;
}

/**
 * @brief Best-effort cancel every async task in every LS of this tenant.
 *
 * Called when the tenant is being deleted. set_stop is issued per task_opt to
 * stop further scheduling, then cancellation is performed directly inside
 * foreach_refactored callback where bucket read lock prevents concurrent free.
 *
 * @retval true  every task ctx was cancelled successfully (caller may latch
 *               is_tenant_cancel_done_ to avoid re-entering this loop)
 * @retval false at least one cancel_task call failed; caller should keep
 *               is_tenant_cancel_done_ false so the next tick retries.
 */
bool ObVecIdxAsyncTaskScheduler::cancel_all_tenant_tasks_()
{
  int ret = OB_SUCCESS;
  bool all_cancel_ok = true;
  LSIndexMgrMap &mgr_map = service_->get_ls_index_mgr_map();
  FOREACH(iter, mgr_map) {
    ObPluginVectorIndexMgr *mgr = iter->second;
    if (OB_ISNULL(mgr)) {
      // skip
    } else {
      ObVecIndexAsyncTaskOption &task_opt = mgr->get_async_task_opt();
      task_opt.set_stop();
      ObAsyncTaskCancelFunc cancel_func;
      if (OB_FAIL(task_opt.get_async_task_map().foreach_refactored(cancel_func))) {
        LOG_WARN("fail to cancel tasks on tenant deletion", KR(ret));
        all_cancel_ok = false;
      } else if (cancel_func.has_failure()) {
        all_cancel_ok = false;
      }
    }
  }
  LOG_INFO("cancelled all tenant tasks for tenant deletion", K_(tenant_id), K(all_cancel_ok));
  return all_cancel_ok;
}

void ObVecIdxAsyncTaskScheduler::mark_ls_need_resume(const share::ObLSID &ls_id)
{
  if (IS_NOT_INIT || !ls_id.is_valid()) {
    return;
  }
  common::ObSpinLockGuard guard(ls_executor_lock_);
  ObVecIdxLeaderExecutors *exec = nullptr;
  int ret = ls_leader_executor_map_.get_refactored(ls_id, exec);
  if (OB_SUCCESS == ret && OB_NOT_NULL(exec)) {
    exec->set_need_resume(true);
    LOG_INFO("marked ls need resume on leader switch", K(ls_id), K_(tenant_id));
  } else if (OB_HASH_NOT_EXIST == ret) {
    // executor not yet created, will be picked up by sync_ls_executors
    LOG_INFO("ls executor not found for resume mark, will be created later", K(ls_id), K_(tenant_id));
  } else {
    LOG_WARN("fail to get ls leader executor for resume mark", KR(ret), K(ls_id), K_(tenant_id));
  }
}

void ObVecIdxAsyncTaskScheduler::mark_ls_need_resume_for_follower(const share::ObLSID &ls_id)
{
  if (IS_NOT_INIT || !ls_id.is_valid()) {
    return;
  }
  common::ObSpinLockGuard guard(ls_executor_lock_);
  ObVecIdxFollowerExecutors *exec = nullptr;
  int ret = ls_follower_executor_map_.get_refactored(ls_id, exec);
  if (OB_SUCCESS == ret && OB_NOT_NULL(exec)) {
    exec->set_need_resume(true);
    LOG_INFO("marked ls need resume on follower switch", K(ls_id), K_(tenant_id));
  } else if (OB_HASH_NOT_EXIST == ret) {
    // executor not yet created, sync_ls_executors compensates by checking mgr->get_ls_leader()
    LOG_INFO("ls follower executor not found for resume mark, will be created later",
             K(ls_id), K_(tenant_id));
  } else {
    LOG_WARN("fail to get ls follower executor for resume mark", KR(ret), K(ls_id), K_(tenant_id));
  }
}

int ObVecIdxAsyncTaskScheduler::clear_ls_async_task_ctxs(
    const share::ObLSID &ls_id,
    ObVecIndexAsyncTaskOption &task_opt,
    const ObVecIndexTaskCtxArray &task_ctx_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vec idx async task scheduler not inited", KR(ret));
  } else {
    common::ObSpinLockGuard guard(ls_executor_lock_);
    ObVecIdxLeaderExecutors *leader_exec = nullptr;
    ObVecIdxFollowerExecutors *follower_exec = nullptr;
    int leader_ret = ls_leader_executor_map_.get_refactored(ls_id, leader_exec);
    int follower_ret = ls_follower_executor_map_.get_refactored(ls_id, follower_exec);
    if (OB_HASH_NOT_EXIST == leader_ret && OB_HASH_NOT_EXIST == follower_ret) {
      // neither executor found, nothing to clear
    } else if (OB_SUCCESS != leader_ret && OB_HASH_NOT_EXIST != leader_ret) {
      ret = leader_ret;
      LOG_WARN("fail to get ls leader executor", KR(ret), K(ls_id));
    } else if (OB_SUCCESS != follower_ret && OB_HASH_NOT_EXIST != follower_ret) {
      ret = follower_ret;
      LOG_WARN("fail to get ls follower executor", KR(ret), K(ls_id));
    } else {
      const bool leader_inited = (OB_NOT_NULL(leader_exec) && leader_exec->is_inited_);
      const bool follower_inited = (OB_NOT_NULL(follower_exec) && follower_exec->is_inited_);
      if (!leader_inited && !follower_inited) {
        // neither executor inited, skip
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::clear_task_ctxs(task_opt, task_ctx_array))) {
        LOG_WARN("fail to clear task ctxs", KR(ret), K(ls_id));
      }
    }
  }
  return ret;
}

void ObVecIdxAsyncTaskScheduler::refresh_disable_list_config_()
{
  int ret = OB_SUCCESS;
  // Parse into temporary variables first; only commit on full success to avoid
  // partial-update where old rules are cleared but new rules fail to load.
  bool tmp_disabled_all[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];
  MEMSET(tmp_disabled_all, 0, sizeof(tmp_disabled_all));
  DisableEntry tmp_entries[MAX_DISABLE_ENTRIES];
  int64_t tmp_count = 0;
  uint64_t new_hash = UINT64_MAX;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tenant config for disable list", KR(ret), K_(tenant_id));
  } else {
    const char *str = tenant_config->_vector_task_disable_list.str();
    new_hash = (OB_ISNULL(str) || 0 == STRLEN(str))
        ? 0 : murmurhash(str, static_cast<int32_t>(STRLEN(str)), 0);
    if (new_hash == last_disable_list_hash_) {
      return;
    }
    if (OB_ISNULL(str) || 0 == STRLEN(str)) {
      // empty config: tmp arrays stay zeroed, will clear all rules on commit
    } else {
      const int64_t MAX_LEN = 4096;
      const size_t str_len = STRLEN(str);
      if (str_len > MAX_LEN) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("_vector_task_disable_list too long, keeping old config", KR(ret), K(str_len));
      } else {
        char buf[MAX_LEN + 1];
        MEMCPY(buf, str, str_len);
        buf[str_len] = '\0';
        char *saveptr = nullptr;
        char *token = STRTOK_R(buf, ",", &saveptr);
        while (OB_SUCC(ret) && OB_NOT_NULL(token)) {
          char *colon = STRCHR(token, ':');
          if (OB_ISNULL(colon) || colon == token) {
            LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid disable list entry, skipping", "token", token);
            token = STRTOK_R(nullptr, ",", &saveptr);
            continue;
          }
          *colon = '\0';
          const char *key = token;
          const char *val_str = colon + 1;
          ObVecIndexAsyncTaskType task_type = OB_VECTOR_ASYNC_TASK_TYPE_INVALID;
          if (OB_SUCCESS != ObVecIndexAsyncTaskUtil::get_vec_task_type_by_short_name(key, task_type)) {
            LOG_WARN_RET(OB_INVALID_ARGUMENT, "unknown task type in disable list, skipping", "key", key);
            token = STRTOK_R(nullptr, ",", &saveptr);
            continue;
          }
          if ('*' == *val_str && '\0' == *(val_str + 1)) {
            if (task_type >= 0 && task_type < OB_VECTOR_ASYNC_TASK_TYPE_INVALID) {
              tmp_disabled_all[task_type] = true;
            }
          } else {
            char *endptr = nullptr;
            const int64_t tablet_id = strtoll(val_str, &endptr, 10);
            if (OB_ISNULL(endptr) || '\0' != *endptr || tablet_id <= 0) {
              LOG_WARN_RET(OB_INVALID_ARGUMENT,
                           "invalid tablet_id in disable list, skipping",
                           "key",
                           key,
                           "val",
                           val_str);
              token = STRTOK_R(nullptr, ",", &saveptr);
              continue;
            }
            if (tmp_count < MAX_DISABLE_ENTRIES) {
              tmp_entries[tmp_count].task_type_ = task_type;
              tmp_entries[tmp_count].tablet_id_ = tablet_id;
              tmp_count++;
            } else {
              ret = OB_BUF_NOT_ENOUGH;
              LOG_WARN("disable list entries exceed max, keeping old config", KR(ret), K(tmp_count));
              break;
            }
          }
          token = STRTOK_R(nullptr, ",", &saveptr);
        }
      }
    }
  }
  // Commit: only update member variables if parsing fully succeeded
  if (OB_SUCC(ret)) {
    const int64_t disable_all_cnt = static_cast<int64_t>(OB_VECTOR_ASYNC_TASK_TYPE_INVALID);
    MEMCPY(disabled_all_,
           tmp_disabled_all,
           disable_all_cnt * static_cast<int64_t>(sizeof(bool)));
    MEMCPY(disable_entries_, tmp_entries, sizeof(DisableEntry) * tmp_count);
    disable_entry_count_ = tmp_count;
    last_disable_list_hash_ = new_hash;
    // Warn if ALL task types are disabled via wildcard
    bool all_types_disabled = (disable_all_cnt > 0);
    for (int64_t i = 0; i < disable_all_cnt && all_types_disabled; ++i) {
      all_types_disabled = tmp_disabled_all[i];
    }
    if (all_types_disabled && disable_all_cnt > 0) {
      LOG_ERROR_RET(OB_SUCCESS, "[VEC_IDX_TASK_DISABLED] WARNING: all vector task types are disabled, "
                    "no vector index tasks will be scheduled", K_(tenant_id));
    }
  } else {
    LOG_WARN("refresh_disable_list_config_ failed, keeping old disable rules", KR(ret));
  }
  return;
}

bool ObVecIdxAsyncTaskScheduler::is_task_disabled_(
    ObVecIndexAsyncTaskType task_type, int64_t tablet_id) const
{
  if (OB_ISNULL(disabled_all_) || OB_ISNULL(disable_entries_)) {
    return false;
  }
  if (task_type >= 0 && task_type < OB_VECTOR_ASYNC_TASK_TYPE_INVALID
      && disabled_all_[task_type]) {
    return true;
  }
  for (int64_t i = 0; i < disable_entry_count_; ++i) {
    if (disable_entries_[i].task_type_ == task_type
        && disable_entries_[i].tablet_id_ == tablet_id) {
      return true;
    }
  }
  return false;
}

} // namespace share
} // namespace oceanbase
