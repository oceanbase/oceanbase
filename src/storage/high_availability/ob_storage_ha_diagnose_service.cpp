/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_storage_ha_diagnose_service.h"
#include "ob_storage_ha_diagnose_mgr.h"

#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "share/config/ob_server_config.h"
#include "share/ob_ddl_common.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_storage_ha_diagnose_operator.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{

ObStorageHADiagService::ObStorageHADiagService()
  : is_inited_(false),
    thread_cond_(),
    wakeup_cnt_(0),
    sql_proxy_(nullptr),
    idle_interval_ms_(DEFAULT_IDLE_INTERVAL_MS)
{
}

int ObStorageHADiagService::init(ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage ha diagnose service is already init", K(ret));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to init ha service thread cond", K(ret));
  } else {
    sql_proxy_ = sql_proxy;
    idle_interval_ms_ = DEFAULT_IDLE_INTERVAL_MS;
#ifdef ERRSIM
    const int64_t errsim_wait = GCONF.errsim_transfer_diagnose_server_wait_time / 1000;
    if (errsim_wait > 0) {
      idle_interval_ms_ = errsim_wait;
    }
#endif
    is_inited_ = true;
  }
  return ret;
}

void ObStorageHADiagService::destroy()
{
  COMMON_LOG(INFO, "ObStorageHADiagService starts to destroy");
  is_inited_ = false;
  thread_cond_.destroy();
  wakeup_cnt_ = 0;
  sql_proxy_ = nullptr;
  COMMON_LOG(INFO, "ObStorageHADiagService destroyed");
}

int ObStorageHADiagService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageHADiagService not inited", K(ret));
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    LOG_WARN("ObStorageHADiagService start thread failed", K(ret));
  } else {
    LOG_INFO("ObStorageHADiagService start");
  }
  return ret;
}

void ObStorageHADiagService::stop()
{
  if (is_inited_) {
    LOG_INFO("ObStorageHADiagService starts to stop");
    lib::ThreadPool::stop();
    wakeup();
    LOG_INFO("ObStorageHADiagService stopped");
  }
}

void ObStorageHADiagService::wait()
{
  if (is_inited_) {
    LOG_INFO("ObStorageHADiagService starts to wait");
    lib::ThreadPool::wait();
    LOG_INFO("ObStorageHADiagService finish to wait");
  }
}

void ObStorageHADiagService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

int ObStorageHADiagService::reload_config()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  const int64_t new_interval = GCONF.errsim_transfer_diagnose_server_wait_time / 1000;
  bool need_wakeup = false;
  {
    ObThreadCondGuard cond_guard(thread_cond_);
    if (new_interval > 0 && idle_interval_ms_ != new_interval) {
      LOG_INFO("reload ha diag service idle_interval_ms",
          "old", idle_interval_ms_, "new", new_interval);
      idle_interval_ms_ = new_interval;
      need_wakeup = true;
    }
  }
  if (need_wakeup) {
    wakeup();
  }
#endif
  return ret;
}

ObStorageHADiagService &ObStorageHADiagService::instance()
{
  static ObStorageHADiagService storage_ha_diag_service;
  return storage_ha_diag_service;
}

void ObStorageHADiagService::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("ObStorageHADiagService");
  ObSEArray<uint64_t, 8> tenant_ids;
  while (!has_set_stop()) {
    ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObStorageHADiagService not inited", K(ret));
    } else if (observer::ObServiceStatus::SS_SERVING != GCTX.status_) {
      // server not serving yet; try again later
    } else if (OB_ISNULL(GCTX.omt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("omt is null", K(ret));
    } else {
      if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
        LOG_WARN("failed to get mtl tenant ids", K(ret));
      } else {
        for (int64_t i = 0; i < tenant_ids.count(); ++i) {
          const uint64_t tenant_id = tenant_ids.at(i);
          if (!GCTX.omt_->is_available_tenant(tenant_id)) {
            continue;
          } else if (OB_TMP_FAIL(drain_tenant_(tenant_id))) {
            LOG_WARN("failed to drain tenant diag queue", K(tmp_ret), K(tenant_id));
          }
        }
      }
#ifdef ERRSIM
      if (should_run_gc_()) {
#else
      if (should_run_gc_() && REACH_TIME_INTERVAL(1_hour)) {
#endif
        if (OB_TMP_FAIL(do_clean_history_(ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE))) {
          LOG_WARN("failed to clean error diagnose history", K(tmp_ret));
        }
        if (OB_TMP_FAIL(do_clean_history_(ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE))) {
          LOG_WARN("failed to clean perf diagnose history", K(tmp_ret));
        }
      }
    }

    ObThreadCondGuard guard(thread_cond_);
    if (has_set_stop() || wakeup_cnt_ > 0) {
      wakeup_cnt_ = 0;
    } else {
      ObBKGDSessInActiveGuard inactive_guard;
      thread_cond_.wait(idle_interval_ms_);
    }
  }
}

#define FLUSH_DML(MODULE, DML, WRITTEN) \
  { \
    const int64_t _rc = (DML).get_row_count(); \
    if (OB_FAIL(op.batch_write_inflight_rows(*sql_proxy_, OB_SYS_TENANT_ID, \
        MODULE, DML))) { \
      LOG_WARN("[HA_DIAGNOSE] failed to flush chunk", K(ret), \
          K(report_tenant_id), K(MODULE), "row_count", _rc); \
    } else { \
      (WRITTEN) += _rc; \
    } \
  }

int ObStorageHADiagService::drain_tenant_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageHADiagMgr *mgr = nullptr;
  ObArray<ObHADiagFlushItem> batch;
  batch.set_attr(ObMemAttr(tenant_id, "HADiagnoseQueue"));
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_FAIL(guard.switch_to(tenant_id, false))) {
    if (OB_TENANT_NOT_IN_SERVER == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to switch tenant", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr = MTL(ObStorageHADiagMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObStorageHADiagMgr is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->drain_pending(batch))) {
    LOG_WARN("failed to drain pending items", K(ret), K(tenant_id));
  } else if (batch.empty()) {
    // nothing to do
  } else {
    ObStorageHADiagOperator op;
    const uint64_t report_tenant_id = tenant_id;
    share::ObDMLSqlSplicer perf_dml;
    share::ObDMLSqlSplicer error_dml;
    // Append-failure poisons the splicer (partial columns would mix into the
    // next finish_row). On first append failure, drop that table's whole batch
    // for this tick — the next drain will rebuild it.
    bool perf_poisoned = false;
    bool error_poisoned = false;
    int64_t perf_written = 0;
    int64_t error_written = 0;
    const int64_t batch_count = batch.count();
    if (OB_FAIL(op.init())) {
      LOG_WARN("failed to init ha diagnose operator", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_count; ++i) {
      const ObHADiagFlushItem &item = batch.at(i);
      const ObHAInflightDiagState &snap = item.snapshot_;
      const bool will_write_perf = snap.has_any_phase_filled();
      const bool will_write_error = (OB_SUCCESS != snap.last_err_code_);
      if (will_write_perf) {
        append_and_maybe_flush_(op, report_tenant_id, item,
            ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE, perf_dml, perf_poisoned, perf_written);
      }
      if (will_write_error) {
        append_and_maybe_flush_(op, report_tenant_id, item,
            ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE, error_dml, error_poisoned, error_written);
      }
    }
    if (OB_SUCC(ret) && !perf_poisoned && perf_dml.get_row_count() > 0) {
      FLUSH_DML(ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE, perf_dml, perf_written);
    }
    if (OB_SUCC(ret) && !error_poisoned && error_dml.get_row_count() > 0) {
      FLUSH_DML(ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE, error_dml, error_written);
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("[HA_DIAGNOSE] drain_tenant_ done",
          K(report_tenant_id), K(batch_count),
          K(perf_written), K(error_written),
          K(perf_poisoned), K(error_poisoned));
    }
  }
  return ret;
}

void ObStorageHADiagService::append_and_maybe_flush_(
    ObStorageHADiagOperator &op,
    const uint64_t report_tenant_id,
    const ObHADiagFlushItem &item,
    const ObStorageHADiagModule module,
    share::ObDMLSqlSplicer &dml,
    bool &poisoned,
    int64_t &written_rows)
{
  int ret = OB_SUCCESS;
  if (poisoned) {
    // earlier append failed for this splicer; skip the rest of this tick
  } else if (OB_FAIL(op.append_inflight_row(report_tenant_id, item.ls_id_,
          module, item.snapshot_.cur_phase_, item.snapshot_,
          item.snapshot_.last_err_code_, item.snapshot_.retry_count_, dml))) {
    LOG_WARN("[HA_DIAGNOSE] failed to append row; dropping batch",
        K(ret), K(report_tenant_id), K(module), "ls_id", item.ls_id_);
    poisoned = true;
  } else if (dml.get_row_count() >= DML_BATCH_ROWS) {
    FLUSH_DML(module, dml, written_rows);
    dml.reset();
  }
}

int ObStorageHADiagService::do_clean_history_(const ObStorageHADiagModule module)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageHADiagService not inited", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", K(ret));
  } else {
    ObStorageHADiagOperator op;
    const int64_t now = ObTimeUtility::current_time();
    int64_t delete_timestamp = now - GCONF._ha_diagnose_history_recycle_interval;
#ifdef ERRSIM
    const int64_t errsim_interval = GCONF._errsim_ha_diagnose_history_recycle_interval;
    if (0 != errsim_interval) {
      delete_timestamp = now - errsim_interval;
    }
#endif
    int64_t affected_rows = DELETE_BATCH_LIMIT;
    int64_t delete_rows = 0;
    if (OB_FAIL(op.init())) {
      LOG_WARN("failed to init ha diagnose operator", K(ret));
    } else {
      while (OB_SUCC(ret) && affected_rows >= DELETE_BATCH_LIMIT) {
        if (OB_FAIL(op.delete_expired_rows(*sql_proxy_, OB_SYS_TENANT_ID,
                module, delete_timestamp, DELETE_BATCH_LIMIT, affected_rows))) {
          LOG_WARN("failed to delete expired diag rows", K(ret),
              K(module), K(delete_timestamp));
        } else {
          delete_rows += affected_rows;
        }
      }
      if (OB_SUCC(ret) && delete_rows > 0) {
        LOG_INFO("[HA_DIAGNOSE] clean history rows success", K(module), K(delete_rows));
      }
    }
  }
  return ret;
}

bool ObStorageHADiagService::should_run_gc_() const
{
  bool bret = false;
  // History tables live on sys tenant; only the sys ls_1 leader does the
  // delete, so followers don't race on the same rows.
  if (OB_SUCCESS != ObDDLUtil::check_local_is_sys_leader()) {
    // not the sys leader
  } else {
    bret = true;
  }
  return bret;
}

#undef FLUSH_DML

}
}
