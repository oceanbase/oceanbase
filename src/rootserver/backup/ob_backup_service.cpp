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
#include "rootserver/ob_server_manager.h"

namespace oceanbase 
{
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace share;

namespace rootserver
{

int64_t ObBackupMgrIdling::get_idle_interval_us()
{
  const int64_t backup_check_interval = GCONF._backup_idle_time;
  return backup_check_interval;
}

ObBackupService::ObBackupService()
    : is_inited_(false),
      mgr_mtx_(),
      can_schedule_(false),
      idling_(stop_),
      backup_data_scheduler_(),
      backup_clean_scheduler_(),
      backup_auto_obsolete_delete_trigger_(),
      jobs_(),
      triggers_(),
      task_scheduler_(nullptr),
      lease_service_(nullptr)
{
}

int ObBackupService::init(
    ObServerManager &server_mgr, 
    common::ObMySQLProxy &sql_proxy, 
    obrpc::ObSrvRpcProxy &rpc_proxy,
    schema::ObMultiVersionSchemaService &schema_service, 
    ObBackupLeaseService &lease_service,
    ObBackupTaskScheduler &task_scheduler)
{
  int ret = OB_SUCCESS;
  const int64_t backup_mgr_thread_cnt = 1;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup mgr already inited", K(ret));
  } else if (OB_FAIL(backup_data_scheduler_.init(
      sql_proxy, rpc_proxy, schema_service, lease_service, task_scheduler, *this))) {
    LOG_WARN("fail to init backup data scheduler", K(ret));
  } else if (OB_FAIL(register_job_(&backup_data_scheduler_))) {
    LOG_WARN("fail to regist job", K(ret), "job_type", backup_data_scheduler_.get_job_type());
  } else if (OB_FAIL(backup_clean_scheduler_.init(
      server_mgr, sql_proxy, rpc_proxy, schema_service, lease_service, task_scheduler, *this))) {
    LOG_WARN("fail to init backup clean scheduler", K(ret));
  } else if (OB_FAIL(register_job_(&backup_clean_scheduler_))) {
    LOG_WARN("fail to regist job", K(ret), "job_type", backup_clean_scheduler_.get_job_type());
  } else if (OB_FAIL(backup_auto_obsolete_delete_trigger_.init(
      server_mgr, sql_proxy, rpc_proxy, schema_service, lease_service, task_scheduler, *this))) {
    LOG_WARN("fail to init backup auto obsolete delete trigger", K(ret));
  } else if (OB_FAIL(register_trigger_(&backup_auto_obsolete_delete_trigger_))) {
    LOG_WARN("fail to regist job", K(ret), "job_type", backup_auto_obsolete_delete_trigger_.get_trigger_type());
  } else if (OB_FAIL(create(backup_mgr_thread_cnt, "BackupMgr"))) {
    LOG_WARN("create thread failed", K(ret), K(backup_mgr_thread_cnt));
  } else {
    task_scheduler_ = &task_scheduler;
    lease_service_ = &lease_service;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupService::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
  LOG_INFO("Backup mgr stop", K(ret));
}

int ObBackupService::register_job_(ObIBackupJobScheduler *new_job)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mgr_mtx_);
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

int ObBackupService::register_trigger_(ObIBackupTrigger *new_trigger)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mgr_mtx_);
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

int ObBackupService::idle() const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", K(ret));
  } 
  return ret;
}

void ObBackupService::wakeup()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    idling_.wakeup();
  }
}

int ObBackupService::get_job(const BackupJobType &type, ObIBackupJobScheduler *&new_job) 
{
  int ret = OB_SUCCESS;
  new_job = nullptr;
  ObMutexGuard guard(mgr_mtx_);
  for (int i = 0; i < jobs_.count(); ++i) {
    ObIBackupJobScheduler *tmp_job = jobs_.at(i);
    if (type == tmp_job->get_job_type()) {
      new_job = jobs_.at(i);
      break;
    }
  }
  if (nullptr == new_job) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job not exist", K(ret), K(type));
  }
  return ret;
}

void ObBackupService::start_trigger_(int64_t &last_trigger_ts)
{
  int ret = OB_SUCCESS;
  const int64_t now_ts = ObTimeUtility::current_time();
#ifdef ERRSIM
  const int64_t MAX_TRIGGET_TIME_INTERVAL = GCONF.trigger_auto_backup_delete_interval;
#else
  const int64_t MAX_TRIGGET_TIME_INTERVAL = 60 * 60 * 1000 * 1000L;// 1h
#endif
  if (now_ts > (last_trigger_ts + MAX_TRIGGET_TIME_INTERVAL)) {
    for (int64_t i = 0; i < triggers_.count(); ++i) {
      ObIBackupTrigger *trigger = triggers_.at(i);
      if (OB_FAIL(trigger->process())) {  // status move forward, generate task and add task
        LOG_WARN("job status move forward failed", K(ret), K(*trigger));
      }
    }
    last_trigger_ts = now_ts;
  }
}

void ObBackupService::start_scheduler_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < jobs_.count(); ++i) {
    ObIBackupJobScheduler *job = jobs_.at(i);
    if (OB_FAIL(job->process())) {  // status move forward, generate task and add task
      LOG_WARN("job status move forward failed", K(ret), K(*job));
    }
  }
} 
void ObBackupService::run3()
{
  int ret = OB_SUCCESS;
  LOG_INFO("backup mgr start");
  int64_t last_trigger_ts = ObTimeUtility::current_time();
  while (!stop_) {
    if (can_schedule()) {
      if (OB_FAIL(lease_service_->check_lease())) {
        LOG_WARN("fail to check lease", K(ret));
      } else {
        LOG_INFO("start scheduler");
        ObCurTraceId::init(GCONF.self_addr_);
        start_trigger_(last_trigger_ts);
        start_scheduler_();
      }
    } else {
      task_scheduler_->wakeup();
      wakeup();
    }
    
    if (OB_FAIL(idle())) {
      LOG_WARN("idle failed", K(ret));
    } else {
      continue;
    }
  }
  LOG_INFO("backup mgr stop");
}

int ObBackupService::handle_backup_database(const obrpc::ObBackupDatabaseArg &arg)
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

int ObBackupService::handle_backup_database_cancel(const uint64_t tenant_id, const ObIArray<uint64_t> &managed_tenant_ids)
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

int ObBackupService::handle_backup_delete(const obrpc::ObBackupCleanArg &arg)
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
    // TODO 4.1 support delete backup set/piece 
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
    // TODO 4.1 support delete backup all function 
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


int ObBackupService::handle_delete_policy(const obrpc::ObDeletePolicyArg &arg)
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

int ObBackupService::handle_backup_delete_obsolete(const obrpc::ObBackupCleanArg &arg)
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

int ObBackupService::get_need_reload_task(common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks) 
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mgr_mtx_);
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

bool ObBackupService::can_schedule() 
{
  bool can = ATOMIC_LOAD(&can_schedule_);
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

}  // namespace rootserver
}  // namespace oceanbase