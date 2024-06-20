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

#include "ob_recover_table_job_scheduler.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/restore/ob_recover_table_initiator.h"
#include "rootserver/restore/ob_restore_service.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/ob_primary_standby_service.h"
#include "share/location_cache/ob_location_service.h"
#include "share/restore/ob_physical_restore_table_operator.h"
#include "share/restore/ob_import_util.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "observer/ob_inner_sql_connection.h"

using namespace oceanbase;
using namespace rootserver;
using namespace share;

void ObRecoverTableJobScheduler::reset()
{
  rs_rpc_proxy_ = nullptr;
  sql_proxy_ = nullptr;
  schema_service_ = nullptr;
  srv_rpc_proxy_ = nullptr;
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObRecoverTableJobScheduler::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObCommonRpcProxy &rs_rpc_proxy,
    obrpc::ObSrvRpcProxy &srv_rpc_proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = gen_user_tenant_id(MTL_ID());
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRecoverTableJobScheduler init twice", K(ret));
  } else if (OB_FAIL(helper_.init(tenant_id))) {
    LOG_WARN("failed to init table op", K(ret), K(tenant_id));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    rs_rpc_proxy_ = &rs_rpc_proxy;
    srv_rpc_proxy_ = &srv_rpc_proxy;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObRecoverTableJobScheduler::wakeup_()
{
  ObRestoreService *restore_service = nullptr;
  if (OB_ISNULL(restore_service = MTL(ObRestoreService *))) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore service must not be null");
  } else {
    restore_service->wakeup();
  }
}

void ObRecoverTableJobScheduler::do_work()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObRecoverTableJob> jobs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ObSysRecoverTableJobScheduler", K(ret));
  } else if (OB_FAIL(check_compatible_())) {
    LOG_WARN("check compatible failed", K(ret));
  } else if (OB_FAIL(helper_.get_all_recover_table_job(*sql_proxy_, jobs))) {
    LOG_WARN("failed to get recover all recover table job", K(ret));
  } else {
    ObCurTraceId::init(GCTX.self_addr());
    ARRAY_FOREACH(jobs, i) {
      ObRecoverTableJob &job = jobs.at(i);
      if (!job.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("recover table job is not valid", K(ret), K(job));
      } else if (is_sys_tenant(job.get_tenant_id())) {
        sys_process_(job);
      } else if (is_user_tenant(job.get_tenant_id())) {
        user_process_(job);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant", K(ret), K(job));
      }
    }
  }

}

int ObRecoverTableJobScheduler::check_compatible_() const
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K_(tenant_id));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("min data version is smaller than v4.2.1", K(ret), K_(tenant_id), K(data_version));
  } else if (is_sys_tenant(tenant_id_)) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id_), data_version))) {
    LOG_WARN("fail to get data version", K(ret), "tenant_id", gen_meta_tenant_id(tenant_id_));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("min data version is smaller than v4.2.1", K(ret), K_(tenant_id), K(data_version));
  }

  return ret;
}

int ObRecoverTableJobScheduler::try_advance_status_(share::ObRecoverTableJob &job, const int err_code)
{
  int ret = OB_SUCCESS;
  share::ObRecoverTableStatus next_status;
  const uint64_t tenant_id = job.get_tenant_id();
  const int64_t job_id = job.get_job_id();
  bool need_advance_status = true;
  if (err_code != OB_SUCCESS) {
    if (ObImportTableUtil::can_retrieable_err(err_code)) {
      need_advance_status = false;
    } else {
      share::ObTaskId trace_id(*ObCurTraceId::get_trace_id());
      next_status = ObRecoverTableStatus::FAILED;
      if (job.get_result().is_comment_setted()) {
      } else if (OB_FAIL(job.get_result().set_result(
          err_code, trace_id, GCONF.self_addr_))) {
        LOG_WARN("failed to set result", K(ret));
      }
      LOG_WARN("[RECOVER_TABLE]recover table job failed", K(err_code), K(job));
      ROOTSERVICE_EVENT_ADD("recover_table", "recover_table_failed", K(tenant_id), K(job_id), K(err_code), K(trace_id));
    }
  } else if (job.get_tenant_id() == OB_SYS_TENANT_ID) {
    next_status = ObRecoverTableStatus::get_sys_next_status(job.get_status());
  } else {
    next_status = ObRecoverTableStatus::get_user_next_status(job.get_status());
  }
  if (next_status.is_finish()) {
    job.set_end_ts(ObTimeUtility::current_time());
  }
  if (OB_FAIL(ret)) {
  } else if (need_advance_status && OB_FAIL(helper_.advance_status(*sql_proxy_, job, next_status))) {
    LOG_WARN("failed to advance statsu", K(ret), K(job), K(next_status));
  } else {
    wakeup_();
    ROOTSERVICE_EVENT_ADD("recover_table", "advance_status", K(tenant_id), K(job_id), K(next_status));
  }
  return ret;
}

void ObRecoverTableJobScheduler::sys_process_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  LOG_INFO("ready to schedule sys recover table job", K(job));
  switch(job.get_status()) {
    case ObRecoverTableStatus::Status::PREPARE: {
      if (OB_FAIL(sys_prepare_(job))) {
        LOG_WARN("failed to do sys prepare work", K(ret), K(job));
      }
      break;
    }
    case ObRecoverTableStatus::Status::RECOVERING: {
      if (OB_FAIL(recovering_(job))) {
        LOG_WARN("failed to do sys recovering work", K(ret), K(job));
      }
      break;
    }
    case ObRecoverTableStatus::Status::COMPLETED:
    case ObRecoverTableStatus::Status::FAILED: {
      if (OB_FAIL(sys_finish_(job))) {
        LOG_WARN("failed to do sys finish work", K(ret), K(job));
      }
      break;
    }
    default: {
      ret = OB_ERR_SYS;
      LOG_WARN("invalid sys recover job status", K(ret), K(job));
      break;
    }
  }
}

int ObRecoverTableJobScheduler::check_target_tenant_version_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  const uint64_t target_tenant_id = job.get_target_tenant_id();
  // check data version
  if (OB_FAIL(GET_MIN_DATA_VERSION(target_tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(target_tenant_id));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("min data version is smaller than v4.2.1", K(ret), K(target_tenant_id), K(data_version));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(target_tenant_id), data_version))) {
    LOG_WARN("fail to get data version", K(ret), "target_tenant_id", gen_meta_tenant_id(target_tenant_id));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("min data version is smaller than v4.2.1", K(ret), K(target_tenant_id), K(data_version));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    schema::ObSchemaGetterGuard guard;
    if (OB_TMP_FAIL(ObImportTableUtil::get_tenant_schema_guard(*schema_service_, job.get_target_tenant_id(), guard))) {
      if (OB_TENANT_NOT_EXIST == tmp_ret) {
        ret = tmp_ret;
      }
      LOG_WARN("failed to get tenant schema guard", K(tmp_ret));
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::sys_prepare_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  ObRecoverTableJob target_job;
  share::ObRecoverTablePersistHelper helper;
  DEBUG_SYNC(BEFORE_INSERT_UERR_RECOVER_TABLE_JOB);
  if (OB_FAIL(check_target_tenant_version_(job))) {
    LOG_WARN("failed to check target tenant version", K(ret));
  } else if (OB_FAIL(helper.init(job.get_target_tenant_id()))) {
    LOG_WARN("failed to init recover table persist helper", K(ret));
  }

  ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(job.get_target_tenant_id());
  if (FAILEDx(trans.start(sql_proxy_, meta_tenant_id))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (OB_FAIL(lock_recover_table_(meta_tenant_id, trans))) {
    LOG_WARN("failed to lock recover table", K(ret));
  } else if (OB_FAIL(helper.get_recover_table_job_by_initiator(trans, job, target_job))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(helper.get_recover_table_job_history_by_initiator(trans, job, target_job))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          if (OB_FAIL(insert_user_job_(job, trans, helper))) {
            LOG_WARN("failed to insert user job", K(ret), K(job));
          } else {
            ROOTSERVICE_EVENT_ADD("recover_table", "insert_user_job",
                                  "tenant_id", job.get_tenant_id(),
                                  "job_id", job.get_job_id());
          }
        } else {
          LOG_WARN("failed to get target tenant recover table job history", K(ret), K(job));
        }
      }
    } else {
      LOG_WARN("failed to get target tenant recover table job", K(ret), K(job));
    }
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", K(ret));
    }
  }

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_INSERT_USER_RECOVER_JOB_FAILED) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ROOTSERVICE_EVENT_ADD("recover_table_errsim", "insert_user_job_failed");
  }
#endif
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = try_advance_status_(job, ret))) {
    LOG_INFO("failed to advance status", K(tmp_ret), K(ret), K(job));
  }
  return ret;
}

int ObRecoverTableJobScheduler::lock_recover_table_(
    const uint64_t tenant_id, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObLockTableRequest recover_job_arg;
  recover_job_arg.table_id_ = OB_ALL_RECOVER_TABLE_JOB_TID;
  recover_job_arg.lock_mode_ = EXCLUSIVE;
  recover_job_arg.timeout_us_ = 0; // try lock
  recover_job_arg.op_type_ = IN_TRANS_COMMON_LOCK; // unlock when trans end
  observer::ObInnerSQLConnection *conn = nullptr;
  if (OB_ISNULL(conn = dynamic_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is NULL", KR(ret));
  } else if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_table(tenant_id, recover_job_arg, conn))) {
    LOG_WARN("failed to lock table", K(ret));
  }
  return ret;
}

int ObRecoverTableJobScheduler::insert_user_job_(
    const share::ObRecoverTableJob &job,
    ObMySQLTransaction &trans,
    share::ObRecoverTablePersistHelper &helper)
{
  int ret = OB_SUCCESS;
  ObRecoverTableJob target_job;

  if (OB_FAIL(target_job.assign(job))) {
    LOG_WARN("failed to assign target job", K(ret));
  } else {
    target_job.set_tenant_id(job.get_target_tenant_id());
    target_job.set_initiator_tenant_id(job.get_tenant_id());
    target_job.set_initiator_job_id(job.get_job_id());
    target_job.set_target_tenant_id(target_job.get_tenant_id());
  }
  int64_t job_id = 0;
  if (FAILEDx(ObLSBackupInfoOperator::get_next_job_id(trans, job.get_target_tenant_id(), job_id))) {
    LOG_WARN("failed to get next job id", K(ret), "tenant_id", job.get_target_tenant_id());
  } else if (OB_FALSE_IT(target_job.set_job_id(job_id))) {
  } else if (OB_FAIL(helper.insert_recover_table_job(trans, target_job))) {
    LOG_WARN("failed to insert initial recover table job", K(ret));
  }

  return ret;
}

int ObRecoverTableJobScheduler::recovering_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  share::ObRecoverTablePersistHelper helper;
  ObRecoverTableJob target_job;
  bool user_job_finish = true;
  bool user_tenant_not_exist = false;
  int tmp_ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_RECOVER_UESR_RECOVER_TABLE_JOB);
  if (OB_FAIL(helper.init(job.get_target_tenant_id()))) {
    LOG_WARN("failed to init recover table persist helper", K(ret));
  } else if (OB_FAIL(helper.get_recover_table_job_history_by_initiator(*sql_proxy_, job, target_job))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      user_job_finish = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get target tenant recover table job history", K(ret), K(job));
    }
  } else {
    ROOTSERVICE_EVENT_ADD("recover_table", "sys_wait_user_recover_finish",
                          "tenant_id", job.get_tenant_id(),
                          "job_id", job.get_job_id());
    job.set_result(target_job.get_result());
  }
  if (OB_FAIL(ret)) {
    schema::ObSchemaGetterGuard guard;
    if (OB_TMP_FAIL(ObImportTableUtil::get_tenant_schema_guard(*schema_service_, job.get_target_tenant_id(), guard))) {
      if (OB_TENANT_NOT_EXIST == tmp_ret) {
        user_tenant_not_exist = true;
      }
      LOG_WARN("failed to get tenant schema guard", K(tmp_ret));
    }
  }

  if ((OB_FAIL(ret) && user_tenant_not_exist) || (OB_SUCC(ret) && user_job_finish)) {
    if (OB_SUCC(ret) && !job.get_result().is_succeed()) {
      ret = OB_LS_RESTORE_FAILED;
    }
    job.set_end_ts(ObTimeUtility::current_time());
    if (OB_SUCCESS != (tmp_ret = try_advance_status_(job, ret))) {
      LOG_INFO("failed to advance status", K(tmp_ret), K(ret), K(job));
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::sys_finish_(const share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  bool drop_aux_tenant = GCONF._auto_drop_recovering_auxiliary_tenant;
  if (drop_aux_tenant && OB_FAIL(drop_aux_tenant_(job))) {
    LOG_WARN("failed ot drop aux tenant", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(helper_.insert_recover_table_job_history(trans, job))) {
      LOG_WARN("failed to insert recover table job history", K(ret), K(job));
    } else if (OB_FAIL(helper_.delete_recover_table_job(trans, job))) {
      LOG_WARN("failed to delete recover table job", K(ret), K(job));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit", K(ret));
      } else {
        ROOTSERVICE_EVENT_ADD("recover_table", "sys_recover_finish",
                      "tenant_id", job.get_tenant_id(),
                      "job_id", job.get_job_id());
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to roll back", K(tmp_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::drop_aux_tenant_(const share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  obrpc::ObDropTenantArg drop_tenant_arg;
  drop_tenant_arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  drop_tenant_arg.if_exist_ = false;
  drop_tenant_arg.force_drop_ = true;
  drop_tenant_arg.delay_to_drop_ = false;
  drop_tenant_arg.open_recyclebin_ = false;
  drop_tenant_arg.tenant_name_ = job.get_aux_tenant_name();
  drop_tenant_arg.drop_only_in_restore_ = false;
  common::ObAddr rs_addr;
  if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("failed to get rootservice address", K(ret));
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).drop_tenant(drop_tenant_arg))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to drop tenant", K(ret), K(drop_tenant_arg));
    }
  } else {
    LOG_INFO("[RECOVER_TABLE]drop aux tenant succeed", K(job));
    ROOTSERVICE_EVENT_ADD("recover_table", "drop_aux_tenant",
                          "tenant_id", job.get_tenant_id(),
                          "job_id", job.get_job_id(),
                          "aux_tenant_name", job.get_aux_tenant_name());
  }
  return ret;
}

void ObRecoverTableJobScheduler::user_process_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  LOG_INFO("ready to schedule user recover table job", K(job));
  switch(job.get_status()) {
    case ObRecoverTableStatus::Status::PREPARE: {
      if (OB_FAIL(user_prepare_(job))) {
        LOG_WARN("failed to do user prepare work", K(ret), K(job));
      }
      break;
    }
    case ObRecoverTableStatus::Status::RESTORE_AUX_TENANT: {
      if (OB_FAIL(restore_aux_tenant_(job))) {
        LOG_WARN("failed to do user restore aux tenant work", K(ret), K(job));
      }
      break;
    }
    case ObRecoverTableStatus::Status::ACTIVE_AUX_TENANT: {
      if (OB_FAIL(active_aux_tenant_(job))) {
        LOG_WARN("failed to do user active aux tenant work", K(ret), K(job));
      }
      break;
    }
    case ObRecoverTableStatus::Status::GEN_IMPORT_JOB: {
      if (OB_FAIL(gen_import_job_(job))) {
        LOG_WARN("failed to do user import work", K(ret), K(job));
      }
      break;
    }
    case ObRecoverTableStatus::Status::CANCELING: {
      if (OB_FAIL(canceling_(job))) {
        LOG_WARN("failed to do user canceling", K(ret), K(job));
      }
      break;
    }
    case ObRecoverTableStatus::Status::IMPORTING: {
      if (OB_FAIL(importing_(job))) {
        LOG_WARN("failed to do user importing work", K(ret), K(job));
      }
      break;
    }
    case ObRecoverTableStatus::Status::COMPLETED:
    case ObRecoverTableStatus::Status::FAILED: {
      if (OB_FAIL(user_finish_(job))) {
        LOG_WARN("failed to do user finish work", K(ret), K(job));
      }
      break;
    }
    default: {
      ret = OB_ERR_SYS;
      LOG_WARN("invalid sys recover job status", K(ret), K(job));
      break;
    }
  }
}

int ObRecoverTableJobScheduler::canceling_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  share::ObImportTableJobPersistHelper helper;
  share::ObImportTableJob import_job;
  bool cancel_import_job_finish = false;
  if (OB_FAIL(helper.init(job.get_tenant_id()))) {
    LOG_WARN("failed to init helper", K(ret));
  } else if (OB_FAIL(helper.get_import_table_job_by_initiator(
      *sql_proxy_, job.get_tenant_id(), job.get_job_id(), import_job))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      cancel_import_job_finish = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get import table job by initiator", K(ret));
    }
  }

  if (OB_SUCC(ret) && cancel_import_job_finish) {
    share::ObTaskId trace_id(*ObCurTraceId::get_trace_id());
    job.get_result().set_result(OB_CANCELED, trace_id, GCONF.self_addr_);
    if (OB_FAIL(try_advance_status_(job, OB_CANCELED))) {
      LOG_WARN("failed to advance status", K(ret));
    } else {
      LOG_INFO("[RECOVER_TABLE]cancel recover table job finish", K(job), K(import_job));
      ROOTSERVICE_EVENT_ADD("recover_table", "cancel recover job finish",
                            "tenant_id", job.get_tenant_id(),
                            "recover_job_id", job.get_job_id());
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::user_prepare_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_advance_status_(job, ret))) {
    LOG_WARN("failed to advance status", K(ret));
  }
  return ret;
}

int ObRecoverTableJobScheduler::restore_aux_tenant_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  ObRestorePersistHelper restore_helper;
  ObHisRestoreJobPersistInfo restore_history_info;
  bool aux_tenant_restore_finish = true;
  int tmp_ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_RESTORE_AUX_TENANT);
  if (OB_FAIL(restore_helper.init(OB_SYS_TENANT_ID, share::OBCG_STORAGE /*group_id*/))) {
    LOG_WARN("failed to init retore helper", K(ret));
  } else if (OB_FAIL(restore_helper.get_restore_job_history(
      *sql_proxy_, job.get_initiator_job_id(), job.get_initiator_tenant_id(), restore_history_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      aux_tenant_restore_finish = false;
      ret = OB_SUCCESS;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO("[RECOVER_TABLE]aux tenant restore not finish, wait later", K(job));
      }
    } else {
      LOG_WARN("failed to get restore job history", K(ret),
        "initiator_job_id", job.get_job_id(), "initiator_tenant_id", job.get_tenant_id());
    }
  } else {
    LOG_INFO("[RECOVER_TABLE]aux tenant restore finish", K(restore_history_info), K(job));
    ROOTSERVICE_EVENT_ADD("recover_table", "restore_aux_tenant_finish",
                          "tenant_id", job.get_tenant_id(),
                          "job_id", job.get_job_id(),
                          "aux_tenant_name", job.get_aux_tenant_name());
    const uint64_t aux_tenant_id = restore_history_info.restore_tenant_id_;
    schema::ObSchemaGetterGuard guard;
    schema::ObTenantStatus status;
    if (!restore_history_info.is_restore_success()) {
      ret = OB_LS_RESTORE_FAILED;  // TODO(zeyong) adjust error code to restore tenant failed later.
      LOG_WARN("[RECOVER_TABLE]restore aux tenant failed", K(ret), K(restore_history_info), K(job));
      job.get_result().set_result(false, restore_history_info.comment_);
    } else if (OB_FAIL(check_aux_tenant_(job, aux_tenant_id))) {
      LOG_WARN("failed to check aux tenant", K(ret), K(aux_tenant_id));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_advance_status_(job, ret))) {
      LOG_WARN("failed to advance status", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::active_aux_tenant_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObRestorePersistHelper restore_helper;
  ObHisRestoreJobPersistInfo restore_history_info;
  if (OB_FAIL(restore_helper.init(OB_SYS_TENANT_ID, share::OBCG_STORAGE /*group_id*/))) {
    LOG_WARN("failed to init retore helper", K(ret));
  } else if (OB_FAIL(restore_helper.get_restore_job_history(
      *sql_proxy_, job.get_initiator_job_id(), job.get_initiator_tenant_id(), restore_history_info))) {
      LOG_WARN("failed to get restore job history", K(ret),
        "initiator_job_id", job.get_job_id(), "initiator_tenant_id", job.get_tenant_id());
  } else if (OB_FAIL(ban_multi_version_recycling_(job, restore_history_info.restore_tenant_id_))) {
    LOG_WARN("failed to ban multi version cecycling", K(ret));
  } else if (OB_FAIL(failover_to_primary_(job, restore_history_info.restore_tenant_id_))) {
    LOG_WARN("failed to failover to primary", K(ret), K(restore_history_info));
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    schema::ObSchemaGetterGuard guard;
    if (OB_TMP_FAIL(ObImportTableUtil::get_tenant_schema_guard(*schema_service_,
                                                               restore_history_info.restore_tenant_id_,
                                                               guard))) {
      if (OB_TENANT_NOT_EXIST == tmp_ret) {
        ret = tmp_ret;
      }
      LOG_WARN("failed to get tenant schema guard", K(tmp_ret));
    }
  }

  if (OB_SUCC(ret) || OB_TENANT_NOT_EXIST == ret) {
    if (OB_SUCCESS != (tmp_ret = try_advance_status_(job, ret))) {
      LOG_WARN("failed to advance status", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::ban_multi_version_recycling_(share::ObRecoverTableJob &job, const uint64_t aux_tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = aux_tenant_id;
  const int64_t MAX_UNDO_RETENTION = 31536000; // 1 year
  int64_t affected_row = 0;
  ObSqlString sql;
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    if (OB_FAIL(sql.assign_fmt("alter system set undo_retention = %ld", MAX_UNDO_RETENTION))) {
      LOG_WARN("failed to assign fmt", K(ret));
    } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_row))) {
      LOG_WARN("failed to set undo retention", K(ret));
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::failover_to_primary_(
    share::ObRecoverTableJob &job, const uint64_t aux_tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader;
  obrpc::ObSwitchTenantArg switch_tenant_arg;
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    if (OB_FAIL(switch_tenant_arg.init(aux_tenant_id, obrpc::ObSwitchTenantArg::OpType::FAILOVER_TO_PRIMARY, "", false))) {
      LOG_WARN("failed to init switch tenant arg", K(ret), K(aux_tenant_id));
    } else if (OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.switch_tenant(switch_tenant_arg))) {
      LOG_WARN("failed to switch_tenant", KR(ret), K(switch_tenant_arg));
    } else {
      LOG_INFO("[RECOVER_TABLE]succeed to switch aux tenant role to primary", K(aux_tenant_id), K(job));
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::check_aux_tenant_(share::ObRecoverTableJob &job, const uint64_t aux_tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  schema::ObTenantStatus status;
  schema::ObSchemaGetterGuard aux_tenant_guard;
  schema::ObSchemaGetterGuard recover_tenant_guard;
  bool is_compatible = true;
  if (!schema_service_->is_tenant_refreshed(aux_tenant_id)) {
    ret = OB_EAGAIN;
    LOG_WARN("wait schema refreshed", K(ret), K(aux_tenant_id));
  } else if (!schema_service_->is_tenant_refreshed(job.get_tenant_id())) {
    ret = OB_EAGAIN;
    LOG_WARN("wait schema refreshed", K(ret), K(job));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(aux_tenant_id, aux_tenant_guard))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ObImportResult::Comment comment;
      if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(),
          "aux tenant %.*s has been dropped", job.get_aux_tenant_name().length(), job.get_aux_tenant_name().ptr()))) {
        LOG_WARN("failed to databuff printf", K(ret));
      } else {
        job.get_result().set_result(false, comment);
      }
    }
    LOG_WARN("failed to get tenant schema guard", K(ret), K(aux_tenant_id));
  } else if (OB_FAIL(aux_tenant_guard.get_tenant_status(aux_tenant_id, status))) {
    LOG_WARN("failed to get tenant status", K(ret), K(aux_tenant_id));
  } else if (schema::ObTenantStatus::TENANT_STATUS_NORMAL != status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("aux tenant status is not normal", K(ret), K(aux_tenant_id), K(status));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(job.get_tenant_id(), recover_tenant_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), "tenant_id", job.get_tenant_id());
  } else if (OB_FAIL(check_tenant_compatibility(aux_tenant_guard, recover_tenant_guard, is_compatible))) {
    LOG_WARN("failed to get check tenant compatibility", K(ret));
  } else if (!is_compatible) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("recover from different compatibility tenant is not supported", K(ret));
    if (OB_TMP_FAIL(job.get_result().set_result(false, "recover from different compatibility tenant is not supported"))) {
      LOG_WARN("failed to set result", K(ret), K(tmp_ret));
    }
  } else if (OB_FAIL(check_case_sensitive_compatibility(aux_tenant_guard, recover_tenant_guard, is_compatible))) {
    LOG_WARN("failed to check case sensitive compatibility", K(ret));
  } else if (!is_compatible) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("recover from different case sensitive compatibility tenant is not supported", K(ret));
    if (OB_TMP_FAIL(job.get_result().set_result(false, "recover from different case sensitive compatibility tenant is not supported"))) {
      LOG_WARN("failed to set result", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::check_tenant_compatibility(
    share::schema::ObSchemaGetterGuard &aux_tenant_guard,
    share::schema::ObSchemaGetterGuard &recover_tenant_guard,
    bool &is_compatible)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode aux_compat_mode;
  lib::Worker::CompatMode recover_compat_mode;
  is_compatible = false;
  const uint64_t aux_tenant_id = aux_tenant_guard.get_tenant_id();
  const uint64_t recover_tenant_id = recover_tenant_guard.get_tenant_id();
  if (OB_FAIL(aux_tenant_guard.get_tenant_compat_mode(aux_tenant_id, aux_compat_mode))) {
    LOG_WARN("failed to get tenant compat mode", K(ret), K(aux_tenant_id));
  } else if (OB_FAIL(recover_tenant_guard.get_tenant_compat_mode(recover_tenant_id, recover_compat_mode))) {
    LOG_WARN("failed to get tenant compat mode", K(ret), K(recover_tenant_id));
  } else {
    is_compatible = aux_compat_mode == recover_compat_mode;
    if (!is_compatible) {
      LOG_WARN("[RECOVER_TABLE]tenant compat mode is different", K(is_compatible),
                                                                 K(aux_tenant_id),
                                                                 K(aux_compat_mode),
                                                                 K(recover_tenant_id),
                                                                 K(recover_compat_mode));
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::check_case_sensitive_compatibility(
    share::schema::ObSchemaGetterGuard &aux_tenant_guard,
    share::schema::ObSchemaGetterGuard &recover_tenant_guard,
    bool &is_compatible)
{
  int ret = OB_SUCCESS;
  common::ObNameCaseMode aux_mode;
  common::ObNameCaseMode recover_mode;
  is_compatible = false;
  const uint64_t aux_tenant_id = aux_tenant_guard.get_tenant_id();
  const uint64_t recover_tenant_id = recover_tenant_guard.get_tenant_id();
  if (OB_FAIL(ObImportTableUtil::get_tenant_name_case_mode(aux_tenant_id, aux_mode))) {
    LOG_WARN("failed to get tenant name case mode", K(ret), K(aux_tenant_id));
  } else if (OB_FAIL(ObImportTableUtil::get_tenant_name_case_mode(recover_tenant_id, recover_mode))) {
    LOG_WARN("failed to get tenant name case mode", K(ret), K(recover_tenant_id));
  } else {
    is_compatible = aux_mode == recover_mode;
    if (!is_compatible) {
      LOG_WARN("[RECOVER_TABLE]tenant name case mode is different", K(is_compatible),
                                                                    K(aux_tenant_id),
                                                                    K(aux_mode),
                                                                    K(recover_tenant_id),
                                                                    K(recover_mode));
    }
  }
  return ret;
}

int ObRecoverTableJobScheduler::gen_import_job_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[RECOVER_TABLE]generate import table job", K(job));
  share::ObImportTableJobPersistHelper import_helper;
  share::ObImportTableJob import_job;
  import_job.set_tenant_id(job.get_tenant_id());
  import_job.set_job_id(job.get_job_id());
  import_job.set_initiator_job_id(job.get_job_id());
  import_job.set_initiator_tenant_id(job.get_tenant_id());
  import_job.set_start_ts(ObTimeUtility::current_time());
  import_job.set_status(ObImportTableJobStatus::INIT);
  int tmp_ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard guard;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObMySQLTransaction trans;
  int64_t job_id = 0;
  if (OB_FAIL(import_job.set_src_tenant_name(job.get_aux_tenant_name()))) {
    LOG_WARN("failed to set src tenant name", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(guard.get_tenant_id(job.get_aux_tenant_name(), tenant_id))) {
    if (OB_ERR_INVALID_TENANT_NAME == ret) {
      ObImportResult::Comment comment;
      if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(),
          "aux tenant %.*s has been dropped", job.get_aux_tenant_name().length(), job.get_aux_tenant_name().ptr()))) {
        LOG_WARN("failed to databuff printf", K(ret));
      } else {
        job.get_result().set_result(false, comment);
      }
    }
    LOG_WARN("failed to get tenant id", K(ret), "aux_tenant_name", job.get_aux_tenant_name());
  } else if (OB_FALSE_IT(import_job.set_src_tenant_id(tenant_id))) {
  } else if (OB_FAIL(import_job.get_import_arg().assign(job.get_import_arg()))) {
    LOG_WARN("failed to assign import arg", K(ret));
  } else if (OB_FAIL(import_helper.init(import_job.get_tenant_id()))) {
    LOG_WARN("failed to init import job", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(job.get_tenant_id())))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (OB_FAIL(ObLSBackupInfoOperator::get_next_job_id(trans, job.get_tenant_id(), job_id))) {
    LOG_WARN("failed to get next job id", K(ret));
  } else if (OB_FALSE_IT(import_job.set_job_id(job_id))) {
  } else if (OB_FAIL(import_helper.insert_import_table_job(trans, import_job))) {
    LOG_WARN("failed to insert into improt table job", K(ret));
  }

  if (trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to commit trans", K(ret), K(tmp_ret));
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("[RECOVER_TABLE]succeed generate import job", K(job), K(import_job));
      ROOTSERVICE_EVENT_ADD("recover_table", "generate_import_job",
                            "tenant_id", job.get_tenant_id(),
                            "job_id", job.get_job_id(),
                            "import_job_id", import_job.get_job_id());
    }
  }

  if (OB_TMP_FAIL(try_advance_status_(job, ret))) {
    LOG_WARN("failed to advance status", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObRecoverTableJobScheduler::importing_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  share::ObImportTableJobPersistHelper helper;
  share::ObImportTableJob import_job;
  if (OB_FAIL(helper.init(job.get_tenant_id()))) {
    LOG_WARN("failed to init helper", K(ret));
  } else if (OB_FAIL(helper.get_import_table_job_history_by_initiator(
      *sql_proxy_, job.get_tenant_id(), job.get_job_id(), import_job))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO("[RECOVER_TABLE]import table is not finish, wait later", K(job));
      }
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get recover table job histroy by initiator", K(ret));
    }
  } else if (OB_FALSE_IT(job.set_end_ts(ObTimeUtility::current_time()))) {
  } else if (OB_FALSE_IT(job.set_result(import_job.get_result()))) {
  } else if (!job.get_result().is_succeed() && OB_FALSE_IT(ret = OB_LS_RESTORE_FAILED)) {
  } else if (OB_FAIL(try_advance_status_(job, ret))) {
    LOG_WARN("failed to advance status", K(ret));
  } else {
    LOG_INFO("[RECOVER_TABLE]import table job finish", K(job), K(import_job));
    ROOTSERVICE_EVENT_ADD("recover_table", "import job finish",
                          "tenant_id", job.get_tenant_id(),
                          "job_id", job.get_job_id(),
                          "import_job_id", import_job.get_job_id());
  }
  return ret;
}

int ObRecoverTableJobScheduler::user_finish_(const share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t tenant_id = job.get_tenant_id();
  const int64_t job_id = job.get_job_id();
  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (OB_FAIL(helper_.insert_recover_table_job_history(trans, job))) {
    LOG_WARN("failed to insert recover table job history", K(ret), K(job));
  } else if (OB_FAIL(helper_.delete_recover_table_job(trans, job))) {
    LOG_WARN("failed to delete recover table job", K(ret), K(job));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("end trans failed", K(ret), K(tmp_ret));
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("[RECOVER_TABLE] recover table finish", K(job));
      ROOTSERVICE_EVENT_ADD("recover_table", "recover table job finish",
                            "tenant_id", job.get_tenant_id(),
                            "job_id", job.get_job_id());
    }
  }
  return ret;
}
