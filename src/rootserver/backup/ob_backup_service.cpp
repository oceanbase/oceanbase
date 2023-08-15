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

#include "ob_backup_service.h"
#include "ob_backup_schedule_task.h"
#include "ob_backup_task_scheduler.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase 
{
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace share;

namespace rootserver
{
/*
*----------------------------- ObBackupService -----------------------------
*/

int ObBackupService::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  common::ObMySQLProxy *sql_proxy = nullptr;
  obrpc::ObSrvRpcProxy *rpc_proxy = nullptr;
  share::schema::ObMultiVersionSchemaService *schema_service = nullptr;
  ObBackupTaskScheduler *backup_task_scheduler = nullptr;
  share::ObLocationService *location_service = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup mgr already inited", K(ret));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy should not be NULL", K(ret), KP(sql_proxy));
  } else if (OB_ISNULL(rpc_proxy = GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy should not be NULL", K(ret), KP(rpc_proxy));
  } else if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be NULL", K(ret), KP(schema_service));
  } else if (OB_ISNULL(backup_task_scheduler = MTL(ObBackupTaskScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup_task_scheduler should not be NULL", K(ret), KP(backup_task_scheduler));
  } else if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location_service should not be NULL", K(ret), KP(backup_task_scheduler));
  } else if (OB_FAIL(backup_task_scheduler->register_backup_srv(*this))) {
    LOG_WARN("failed to register backup srv", K(ret));
  } else if (OB_FAIL(sub_init(*sql_proxy, *rpc_proxy, *schema_service, *location_service, *backup_task_scheduler))) {
    LOG_WARN("failed to do sub init", K(ret));
  } else {
    tenant_id_ = tenant_id;
    task_scheduler_ = backup_task_scheduler;
    schema_service_ = schema_service;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupService::run2()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[backupService]backup service start");
  int64_t last_trigger_ts = ObTimeUtility::current_time();
  while (!has_set_stop()) {
    set_idle_time(ObBackupBaseService::OB_MIDDLE_IDLE_TIME);
    ObCurTraceId::init(GCONF.self_addr_);
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
      LOG_WARN("failed to get schema ", KR(ret), K(tenant_id_));
    } else if (OB_NOT_NULL(tenant_schema) && tenant_schema->is_normal()) {
      if (can_schedule()) {
        process(last_trigger_ts);
      } else {
        task_scheduler_->wakeup();
        wakeup();
      }
    }
    idle();
  }
  LOG_INFO("[backupService]backup service stop");
}

bool ObBackupService::can_schedule()
{
  bool can = false;
  if (is_sys_tenant(tenant_id_)) {
  // sys tenant has no task need be reload by backup task scheduler, so no need to wait reload, always return can
    can = true;
  } else {
    can = ATOMIC_LOAD(&can_schedule_);
  }
  return can;
}

void ObBackupService::enable_backup()
{
  ATOMIC_SET(&can_schedule_, true);
}

void ObBackupService::disable_backup()
{
  ATOMIC_SET(&can_schedule_, false);
}

void ObBackupService::destroy()
{
  ObBackupBaseService::destroy();
  task_scheduler_ = nullptr;
  is_inited_ = false;
}

/*
*----------------------------- ObBackupDataService -----------------------------
*/

int ObBackupDataService::sub_init(
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    schema::ObMultiVersionSchemaService &schema_service,
    share::ObLocationService &loacation_service,
    ObBackupTaskScheduler &task_scheduler)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup mgr already inited", K(ret));
  } else if (OB_FAIL(backup_data_scheduler_.init(
      tenant_id, sql_proxy, rpc_proxy, schema_service, task_scheduler, *this))) {
    LOG_WARN("fail to init backup data scheduler", K(ret));
  } else if (OB_FAIL(create("BackupDataSrv", *this, ObWaitEventIds::BACKUP_DATA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to create backup data service", K(ret));
  }
  return ret;
}

int ObBackupDataService::process(int64_t &last_schedule_ts) {
  int ret = OB_SUCCESS;
  UNUSED(last_schedule_ts);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(backup_data_scheduler_.process())) {
    LOG_WARN("failed to process backup data", K(ret));
  }
  return ret;
}

ObIBackupJobScheduler *ObBackupDataService::get_scheduler(const BackupJobType &type)
{
  ObIBackupJobScheduler *ptr = nullptr;
  if (BackupJobType::BACKUP_DATA_JOB == type) {
    ptr = &backup_data_scheduler_;
  }
  return ptr;
}

int ObBackupDataService::get_need_reload_task(
    common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(backup_data_scheduler_.get_need_reload_task(allocator, tasks))) {
    LOG_WARN("failed to get need reload task", K(ret));
  }
  return ret;
}

int ObBackupDataService::handle_backup_database(const obrpc::ObBackupDatabaseArg &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(backup_data_scheduler_.start_backup_data(arg))) {
    LOG_WARN("fail to start backup data", K(ret), K(arg));
  } else {
    FLOG_INFO("success to call start backup", K(arg));
  }
  return ret;
}

int ObBackupDataService::handle_backup_database_cancel(const uint64_t tenant_id, const ObIArray<uint64_t> &managed_tenant_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id));
  } else if (OB_FAIL(backup_data_scheduler_.cancel_backup_data(tenant_id, managed_tenant_ids))) {
    LOG_WARN("fail to cancel backup data", K(ret), K(tenant_id), K(managed_tenant_ids));
  } else {
    FLOG_INFO("success to call cancel backup", K(tenant_id), K(managed_tenant_ids));
  }
  return ret;
}

/*
*----------------------------- ObBackupCleanService -----------------------------
*/

int ObBackupCleanService::sub_init(
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    schema::ObMultiVersionSchemaService &schema_service,
    share::ObLocationService &loacation_service,
    ObBackupTaskScheduler &task_scheduler)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup mgr already inited", K(ret));
  } else if (OB_FAIL(backup_clean_scheduler_.init(
      tenant_id, sql_proxy, rpc_proxy, schema_service, task_scheduler, *this))) {
    LOG_WARN("fail to init backup clean scheduler", K(ret));
  } else if (OB_FAIL(register_job_(&backup_clean_scheduler_))) {
    LOG_WARN("fail to regist job", K(ret), "job_type", backup_clean_scheduler_.get_job_type());
  } else if (OB_FAIL(backup_auto_obsolete_delete_trigger_.init(tenant_id,
      sql_proxy, rpc_proxy, schema_service, task_scheduler, *this))) {
    LOG_WARN("fail to init backup data scheduler", K(ret));
  } else if (OB_FAIL(register_trigger_(&backup_auto_obsolete_delete_trigger_))) {
    LOG_WARN("fail to regist job", K(ret), "job_type", backup_auto_obsolete_delete_trigger_.get_trigger_type());
  } else if (OB_FAIL(create("BackupCleanSrv", *this, ObWaitEventIds::BACKUP_CLEAN_SERVICE_COND_WAIT))) {
    LOG_WARN("create BackupService thread failed", K(ret));
  }
  return ret;
}

int ObBackupCleanService::process(int64_t &last_schedule_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    process_trigger_(last_schedule_ts);
    process_scheduler_();
  }
  return ret;
}

ObIBackupJobScheduler *ObBackupCleanService::get_scheduler(const BackupJobType &type)
{
  ObIBackupJobScheduler *ptr = nullptr;
  int ret = OB_SUCCESS;
  bool find_job = false;
  ARRAY_FOREACH(jobs_, i) {
    ObIBackupJobScheduler *job = jobs_.at(i);
    if (nullptr == job) {
    } else if (type == job->get_job_type()) {
      ptr = job;
    }
  }
  return ptr;
}

int ObBackupCleanService::get_need_reload_task(
    common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks)
{
  int ret = OB_SUCCESS;
  ObSArray<ObBackupScheduleTask *> need_reload_tasks;
  for (int i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObIBackupJobScheduler *job = jobs_.at(i);
    need_reload_tasks.reset();
    if (nullptr == job) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("nullptr backup job", K(ret));
    } else if (OB_FAIL(job->get_need_reload_task(allocator, need_reload_tasks))) {
      LOG_WARN("failed to get need reload task", K(ret), K(*job));
    } else if (need_reload_tasks.empty()) {
    } else if (OB_FAIL(append(tasks, need_reload_tasks))) {
      LOG_WARN("failed to append tasks", K(ret), K(need_reload_tasks));
    }
  }
  return ret;
}

int ObBackupCleanService::handle_backup_delete(const obrpc::ObBackupCleanArg &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    switch (arg.type_) {
    case ObNewBackupCleanType::CANCEL_DELETE: {
      if (OB_FAIL(backup_clean_scheduler_.cancel_backup_clean_job(arg))) {
        LOG_WARN("failed to start schedule backup data clean", K(ret), K(arg));
      }
      break;
    };
    case ObNewBackupCleanType::DELETE_BACKUP_SET: 
    case ObNewBackupCleanType::DELETE_BACKUP_PIECE: {
    // TODO(wenjinyu.wjy) 4.3 support delete backup set/piece
      ret = OB_NOT_SUPPORTED;
      break;
    };
    case ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP:
    case ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP_BACKUP: {
      if (OB_FAIL(handle_backup_delete_obsolete(arg))) {
        LOG_WARN("failed to handle delete backup obsolete data", K(ret), K(arg));
      }
      break;
    };
    case ObNewBackupCleanType::DELETE_BACKUP_ALL: {
    // TODO(wenjinyu.wjy) 4.3 support delete backup all function
      ret = OB_NOT_SUPPORTED;
      break;
    };
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid backup clean arg", K(ret), K(arg));
      break;
    }
    }
  }
  FLOG_INFO("finish handle backup delete", K(ret), K(arg)); 
  return ret;
}


int ObBackupCleanService::handle_delete_policy(const obrpc::ObDeletePolicyArg &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    switch (arg.type_) {
    case ObPolicyOperatorType::ADD_POLICY: {
      if (OB_FAIL(backup_clean_scheduler_.add_delete_policy(arg))) {
        LOG_WARN("failed to add delete policy", K(ret), K(arg));
      }
      break;
    };
    case ObPolicyOperatorType::DROP_POLICY: {
      if (OB_FAIL(backup_clean_scheduler_.drop_delete_policy(arg))) {
        LOG_WARN("failed to drop delete policy", K(ret), K(arg));
      }
      break;
    };
    case ObPolicyOperatorType::CHANGE_POLICY: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("currently does not support changing policy", K(ret), K(arg));
      break;
    };
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid delete policy arg", K(ret), K(arg));
      break;
    }
    }
  }
  FLOG_INFO("finish handle delete policy", K(ret), K(arg)); 
  return ret;
}

int ObBackupCleanService::handle_backup_delete_obsolete(const obrpc::ObBackupCleanArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup delete arg type is not obsolete backup arg", K(ret), K(arg));
  } else if (OB_FAIL(backup_clean_scheduler_.start_schedule_backup_clean(arg))) {
    LOG_WARN("failed to start schedule backup clean", K(ret), K(arg));
  }
  return ret;
}


int ObBackupCleanService::register_job_(ObIBackupJobScheduler *new_job)
{
  int ret = OB_SUCCESS;
  if (nullptr == new_job) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new job is nullptr", K(ret));
  } else if (OB_FAIL(jobs_.push_back(new_job))) {
    LOG_WARN("regist job error", K(ret));
  } else {
    BackupJobType job_type = new_job->get_job_type();
    LOG_INFO("backup mgr register job", K(job_type));
  }
  return ret;
}

int ObBackupCleanService::register_trigger_(ObIBackupTrigger *new_trigger)
{
  int ret = OB_SUCCESS;
  if (nullptr == new_trigger) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new trigger is nullptr", K(ret));
  } else if (OB_FAIL(triggers_.push_back(new_trigger))) {
    LOG_WARN("regist trigger error", K(ret));
  } else {
    BackupTriggerType trigger_type = new_trigger->get_trigger_type();
    LOG_INFO("backup mgr register trigger", K(trigger_type));
  }
  return ret;
}

void ObBackupCleanService::process_trigger_(int64_t &last_trigger_ts)
{
  int tmp_ret = OB_SUCCESS;
  const int64_t now_ts = ObTimeUtility::current_time();
#ifdef ERRSIM
  const int64_t MAX_TRIGGET_TIME_INTERVAL = GCONF.trigger_auto_backup_delete_interval;
#else
  const int64_t MAX_TRIGGET_TIME_INTERVAL = 60 * 60 * 1000 * 1000L;// 1h
#endif
  if (now_ts > (last_trigger_ts + MAX_TRIGGET_TIME_INTERVAL)) {
    for (int64_t i = 0; i < triggers_.count(); ++i) {
      ObIBackupTrigger *trigger = triggers_.at(i);
      if (OB_SUCCESS != (tmp_ret = trigger->process())) {  // status move forward, generate task and add task
        LOG_WARN_RET(tmp_ret, "job status move forward failed", K(*trigger));
      }
    }
    last_trigger_ts = now_ts;
  }
}

void ObBackupCleanService::process_scheduler_()
{
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; i < jobs_.count(); ++i) {
    ObIBackupJobScheduler *job = jobs_.at(i);
    if (OB_SUCCESS != (tmp_ret = job->process())) {  // status move forward, generate task and add task
      LOG_WARN_RET(tmp_ret, "job status move forward failed", K(*job));
    }
  }
}

}  // namespace rootserver
}  // namespace oceanbase
