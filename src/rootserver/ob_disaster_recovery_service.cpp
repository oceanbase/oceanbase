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

#define USING_LOG_PREFIX RS

#include "ob_disaster_recovery_service.h"

#include "common/ob_role.h"                                       // ObRole
#include "logservice/palf_handle_guard.h"                         // PalfHandleGuard
#include "lib/mysqlclient/ob_mysql_transaction.h"                 // ObMySQLTransaction
#include "logservice/ob_log_service.h"                            // for ObLogService
#include "rootserver/ob_disaster_recovery_task_utils.h"           // DisasterRecoveryUtils
#include "rootserver/ob_root_utils.h"                             // ObTenantUtils
#include "rootserver/ob_disaster_recovery_task_mgr.h"             // ObDRTaskMgr
#include "rootserver/ob_disaster_recovery_worker.h"               // ObDRWorker
#include "share/ob_service_epoch_proxy.h"                         // ObServiceEpochProxy
#include "storage/tx_storage/ob_ls_handle.h"                      // ObLSHandle
#include "storage/tx_storage/ob_ls_service.h"                     // ObLSService

namespace oceanbase
{

namespace rootserver
{

int ObDRService::init()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("DRService", lib::TGDefIDs::LSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
    LOG_INFO("[DRTASK_NOTICE] ObDRService init", K_(tenant_id), KP(this));
  }
  return ret;
}

void ObDRService::destroy()
{
  ObTenantThreadHelper::destroy();
  inited_ = false;
  LOG_INFO("[DRTASK_NOTICE] ObDRService destroy", K_(tenant_id), KP(this));
}

int ObDRService::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDRService is not inited", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_) || is_user_tenant(tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_id is invalid", KR(ret), K_(tenant_id));
  }
  return ret;
}

void ObDRService::do_work()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[DRTASK_NOTICE] disaster recovery service start run");
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(is_user_tenant(tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dr service must run on sys or meta tenant", KR(ret), K_(tenant_id));
  } else if (is_meta_tenant(tenant_id_)
          && OB_FAIL(wait_tenant_data_version_ready_(OB_SYS_TENANT_ID, DATA_VERSION_4_3_5_1))) {
    // for sys tenant, no need to check data_version, sys tenant is responsible for all tenant tasks.
    // for meta tenant, need to check sys tenant data_version
    LOG_WARN("failed to wait tenant schema version ready", KR(ret), K_(tenant_id), K(DATA_CURRENT_VERSION));
  } else if (OB_FAIL(wait_tenant_schema_ready_(tenant_id_))) {
    LOG_WARN("failed to wait tenant schema ready", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ensure_service_epoch_exist_())) {
    LOG_WARN("failed to ensure service epoch", KR(ret));
  } else if (OB_FAIL(do_dr_service_work_())) {
    LOG_WARN("failed to do service work", KR(ret));
  }
  FLOG_INFO("[DRTASK_NOTICE] disaster recovery service exit");
}

int ObDRService::do_dr_service_work_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t last_check_ts = ObTimeUtility::current_time();
  uint64_t thread_idx = get_thread_idx();
  while (!has_set_stop()) {
    int64_t idle_time_us = is_dr_worker_thread_(thread_idx) ? 10L * 1000000L : 1L * 1000000L;
    int64_t service_epoch = 0;
    bool need_clean_task = false;
    ObArray<uint64_t> tenant_id_array;
    ObCurTraceId::init(GCONF.self_addr_);
    if (OB_FAIL(check_and_update_service_epoch_(service_epoch))) {
      LOG_WARN("fail to check and update service epoch", KR(ret));
    } else if (OB_FAIL(check_need_clean_task_(last_check_ts, need_clean_task))) {
      LOG_WARN("fail to check need clean task", KR(ret), K(last_check_ts));
    } else if (OB_FAIL(get_tenant_ids_(tenant_id_array))) {
      LOG_WARN("fail to get tenant id array", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_id_array.count(); ++i) {
        const uint64_t tenant_id = tenant_id_array.at(i);
        int64_t service_epoch_to_check = 0;
        if (OB_TMP_FAIL(get_service_epoch_to_check_(tenant_id, service_epoch, service_epoch_to_check))) {
          LOG_WARN("failed to get service epoch", KR(ret), KR(tmp_ret), K(tenant_id), K(service_epoch));
        } else if (is_dr_worker_thread_(thread_idx)) {
          if (OB_TMP_FAIL(try_tenant_disaster_recovery_(tenant_id, service_epoch_to_check))) {
            LOG_WARN("failed to process worker thread", KR(ret), KR(tmp_ret), K(tenant_id), K(service_epoch_to_check));
          }
        } else if (OB_TMP_FAIL(manage_dr_tasks_(tenant_id, need_clean_task, service_epoch_to_check))) {
          LOG_WARN("failed to process mgr thread", KR(ret), KR(tmp_ret), K(tenant_id), K(need_clean_task), K(service_epoch_to_check));
        }
      } // end for tenant
    }
    if (FAILEDx(adjust_idle_time_(idle_time_us))) {
       LOG_WARN("failed to adjust idle time", KR(ret), K(idle_time_us));
    }
    idle(idle_time_us);
    LOG_INFO("[DRTASK_NOTICE] disaster recovery service finish one round",
              KR(ret), K(idle_time_us), K_(tenant_id), K(thread_idx));
  }
  LOG_INFO("[DRTASK_NOTICE] disaster recovery service stop", K_(tenant_id));
  return ret;
}

int ObDRService::adjust_idle_time_(
  int64_t &idle_time_us)
{
  int ret = OB_SUCCESS;
  int64_t task_count = 0;
  uint64_t thread_idx = get_thread_idx();
  uint64_t sys_data_version = 0;
  if (is_dr_worker_thread_(thread_idx)) { // skip
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret));
  } else if (sys_data_version < DATA_VERSION_4_3_5_1) { // skip
  } else if (OB_FAIL(DisasterRecoveryUtils::get_dr_tasks_count(tenant_id_, task_count))) {
    LOG_WARN("failed to get dr_tasks count", KR(ret), K_(tenant_id));
  } else if (0 == task_count) {
    // if no task in table, idle time of mgr is extended to 30s.
    idle_time_us = 30L * 1000000L;
    LOG_TRACE("idle time of mgr is extended to 30s", K(idle_time_us), K_(tenant_id), K(thread_idx));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DISASTER_RECOVERY_WORKER_START);
int ObDRService::try_tenant_disaster_recovery_(
    const uint64_t tenant_id,
    const int64_t service_epoch_to_check)
{
  int ret = OB_SUCCESS;
  int64_t acc_dr_task = 0;
  ObDRWorker dr_worker;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(ERRSIM_DISASTER_RECOVERY_WORKER_START)) {
    // for test, return success, skip try disaster recovery
    LOG_INFO("errsim disaster recovery worker start", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (FALSE_IT(dr_worker.set_service_epoch(service_epoch_to_check))) {
  } else if (OB_FAIL(dr_worker.try_tenant_disaster_recovery(
                                  tenant_id,
                                  false/*only_for_display*/,
                                  acc_dr_task))) {
    LOG_WARN("try tenant disaster recovery failed", KR(ret), K(tenant_id));
  } else if (0 != acc_dr_task && OB_FAIL(DisasterRecoveryUtils::wakeup_local_service(tenant_id_))) {
    LOG_WARN("fail to wake up", KR(ret), K(tenant_id), K(acc_dr_task));
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("try check dr tasks over", KR(ret), K(cost), K(tenant_id), K(acc_dr_task));
  return ret;
}

int ObDRService::manage_dr_tasks_(
    const uint64_t tenant_id,
    const bool need_clean_task,
    const int64_t service_epoch_to_check)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDRTaskMgr dr_mgr;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (FALSE_IT(dr_mgr.set_service_epoch(service_epoch_to_check))) {
  } else {
    if (OB_TMP_FAIL(dr_mgr.try_pop_and_execute_task(tenant_id))) {
      LOG_WARN("failed to execute task", KR(ret), KR(tmp_ret), K(tenant_id));
    }
    if (need_clean_task && OB_TMP_FAIL(dr_mgr.try_clean_and_cancel_task(tenant_id))) {
      LOG_WARN("failed to clean task", KR(ret), KR(tmp_ret), K(tenant_id));
    }
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("try manage dr tasks over", KR(ret), K(cost), K(tenant_id), K(need_clean_task), K(service_epoch_to_check));
  return ret;
}

int ObDRService::check_need_clean_task_(
    int64_t &last_check_ts,
    bool &need_clean_task)
{
  int ret = OB_SUCCESS;
  need_clean_task = false;
  const int64_t now = ObTimeUtility::current_time();
  if (now > last_check_ts + CHECK_AND_CLEAN_TASK_INTERVAL) {
    last_check_ts = now;
    need_clean_task = true;
  } else {
    LOG_TRACE("no need to check task", K(last_check_ts), K(now));
  }
  return ret;
}

int ObDRService::get_service_epoch_to_check_(
    const uint64_t execute_task_tenant,
    const int64_t epoch_of_service_thread,
    int64_t &service_epoch_to_check)
{
  // 1. When the sys tenant or ordinary tenant's dr threads processes its own dr tasks,
  //    verify the service_epoch recorded in the memory.
  // 2. When the sys tenant's dr threads processes dr tasks of other tenants,
  //    verify whether the service_epoch value is zero.
  //    (when ordinary tenant's dr threads start to work, it will push this value to non-zero.)
  int ret = OB_SUCCESS;
  const uint64_t provide_service_tenant = tenant_id_;
  if (!is_valid_tenant_id(execute_task_tenant)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(execute_task_tenant));
  } else if (is_sys_tenant(provide_service_tenant)) {
    if (is_sys_tenant(execute_task_tenant)) {
      service_epoch_to_check = epoch_of_service_thread;
    } else {
      service_epoch_to_check = 0;
    }
  } else if (is_meta_tenant(provide_service_tenant)) {
    service_epoch_to_check = epoch_of_service_thread;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tenant id", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObDRService::get_tenant_ids_(
    ObIArray<uint64_t> &tenant_ids)
{
  // 1. Before the sys tenant data_version is pushed up to target version,
  //    sys tenant's dr thread handles the dr tasks of all tenants.
  // 2. After the sys tenant data_version is pushed up to target version,
  //    the dr thread of the ordinary tenant starts working.
  //    the sys tenant no longer processes tasks of other tenants but only processes its own tasks.
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  if (is_sys_tenant(tenant_id_)) {
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, tenant_data_version))) {
      LOG_WARN("fail to get min data version", KR(ret));
    } else if (tenant_data_version >= DATA_VERSION_4_3_5_1) {
      if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
        LOG_WARN("fail to push back tenant ids", KR(ret));
      }
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", KR(ret), KP(GCTX.schema_service_));
    } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(GCTX.schema_service_, tenant_ids))) {
      LOG_WARN("fail to get tenant id array", KR(ret));
    }
  } else if (is_meta_tenant(tenant_id_)) {
    if (OB_FAIL(tenant_ids.push_back(tenant_id_))) {
      LOG_WARN("tenant_ids push back failed", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(tenant_ids.push_back(gen_user_tenant_id(tenant_id_)))) {
      LOG_WARN("tenant_ids push back failed", KR(ret), K_(tenant_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tenant_id", KR(ret), K_(tenant_id));
  }
  LOG_TRACE("get tenant id over", KR(ret), K_(tenant_id), K(tenant_ids));
  return ret;
}

int ObDRService::ensure_service_epoch_exist_()
{
  int ret = OB_SUCCESS;
  while (!has_set_stop()) {
    ret = OB_SUCCESS;
    ObMySQLTransaction trans;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(DisasterRecoveryUtils::check_service_epoch_exist_or_insert(trans, tenant_id_))) {
      LOG_WARN("failed to check and insert service epoch", KR(ret), K_(tenant_id));
    } else if (is_meta_tenant(tenant_id_)
            && OB_FAIL(DisasterRecoveryUtils::check_service_epoch_exist_or_insert(trans, gen_user_tenant_id(tenant_id_)))) {
      LOG_WARN("failed to check and insert service epoch", KR(ret), K_(tenant_id));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      break;
    } else {
      idle(1 * 1000 * 1000);
    }
  } // end while
  if (has_set_stop()) {
    LOG_WARN("thread has been stopped", K_(tenant_id));
    ret = OB_IN_STOP_STATE;
  }
  return ret;
}

int ObDRService::check_and_update_service_epoch_(
    int64_t &service_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("sql_proxy_ is nullptr", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    MTL_SWITCH(tenant_id_) {
      int64_t proposal_id = 0;
      common::ObRole role = FOLLOWER;
      int64_t service_epoch_in_table = 0;
      ObMySQLTransaction trans;
      bool is_match = false;
      if (OB_FAIL(get_role_and_proposal_id_(role, proposal_id))) {
        LOG_WARN("fail to get role and proposal id", KR(ret), K_(tenant_id));
      } else if (common::ObRole::LEADER != role) {
        ret = OB_LS_NOT_LEADER;
        LOG_WARN("[DRTASK_NOTICE] can not do service, because not leader", K_(tenant_id), K(role));
      } else if (OB_FAIL(ObServiceEpochProxy::check_service_epoch(
                                                      *GCTX.sql_proxy_,
                                                      tenant_id_,
                                                      share::ObServiceEpochProxy::DISASTER_RECOVERY_SERVICE_EPOCH,
                                                      proposal_id,
                                                      is_match))) {
        LOG_WARN("failed to check service epoch", KR(ret), K_(tenant_id));
      } else if (is_match) {
        // skip, optimization, no need to open transactions
        LOG_TRACE("proposal_id and service epoch is match", K_(tenant_id), K(proposal_id));
      } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
        LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(share::ObServiceEpochProxy::select_service_epoch_for_update(
                                  trans,
                                  tenant_id_,
                                  share::ObServiceEpochProxy::DISASTER_RECOVERY_SERVICE_EPOCH,
                                  service_epoch_in_table))) {
        LOG_WARN("fail to get service epoch from inner table", KR(ret), K_(tenant_id));
      } else if (proposal_id < service_epoch_in_table) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("can not do service, because leader epoch is not newest", K(proposal_id), K(service_epoch_in_table));
      } else if (proposal_id == service_epoch_in_table) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("proposal_id and service epoch is equal, already checked before",
                  KR(ret), K_(tenant_id), K(proposal_id), K(service_epoch_in_table));
      } else if (OB_FAIL(update_tenant_service_epoch_(trans, proposal_id))) {
        // if proposal_id > service_epoch, update service epoch to newest one
        LOG_WARN("fail to update service epoch", KR(ret), K(proposal_id));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        service_epoch = proposal_id;
      }
    } // end MTL_SWITCH
  }
  return ret;
}

int ObDRService::get_role_and_proposal_id_(
    common::ObRole &role,
    int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  palf::PalfHandleGuard palf_handle_guard;
  logservice::ObLogService *log_service = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(is_user_tenant(tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tenant id", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MTL ObLogService is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(log_service->open_palf(SYS_LS, palf_handle_guard))) {
    LOG_WARN("open palf failed", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
    LOG_WARN("get role failed", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObDRService::update_tenant_service_epoch_(
    ObMySQLTransaction &trans,
    const int64_t proposal_id)
{
  // upgrade service epoch of user tenant and meta tenant at the same time
  // make sure they have the same value
  LOG_INFO("update tenant service_epoch ", K(proposal_id), K_(tenant_id));
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(proposal_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(proposal_id));
  } else if (OB_FAIL(share::ObServiceEpochProxy::update_service_epoch(
                      trans,
                      tenant_id_,
                      share::ObServiceEpochProxy::DISASTER_RECOVERY_SERVICE_EPOCH,
                      proposal_id,
                      affected_rows))) {
    LOG_WARN("fail to update service epoch", KR(ret), K_(tenant_id), K(proposal_id));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect update one row", KR(ret), K_(tenant_id), K(proposal_id), K(affected_rows));
  } else if (is_meta_tenant(tenant_id_)
          && OB_FAIL(share::ObServiceEpochProxy::update_service_epoch(
                      trans,
                      gen_user_tenant_id(tenant_id_),
                      share::ObServiceEpochProxy::DISASTER_RECOVERY_SERVICE_EPOCH,
                      proposal_id,
                      affected_rows))) {
    LOG_WARN("fail to update service epoch", KR(ret), K_(tenant_id), K(proposal_id));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect update one row", KR(ret), K_(tenant_id), K(proposal_id), K(affected_rows));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
