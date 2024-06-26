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
#include "ob_backup_clean_scheduler.h"
#include "ob_backup_clean_ls_task_mgr.h"
#include "ob_backup_clean_task_mgr.h"
#include "ob_backup_schedule_task.h"
#include "ob_backup_task_scheduler.h"
#include "share/backup/ob_backup_clean_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "rootserver/ob_root_utils.h"
#include "share/backup/ob_backup_helper.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_store.h"

namespace oceanbase
{
using namespace omt;
using namespace common::hash;
using namespace share;
namespace rootserver
{
ObBackupCleanScheduler::ObBackupCleanScheduler()
 : ObIBackupJobScheduler(BackupJobType::BACKUP_CLEAN_JOB),
   is_inited_(false),
   tenant_id_(OB_INVALID_TENANT_ID),
   sql_proxy_(nullptr),
   rpc_proxy_(nullptr),
   schema_service_(nullptr),
   task_scheduler_(nullptr),
   backup_service_(nullptr)
{
}

int ObBackupCleanScheduler::init(
    const uint64_t tenant_id,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObBackupTaskScheduler &task_scheduler,
    ObBackupCleanService &backup_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    schema_service_ = &schema_service;
    task_scheduler_ = &task_scheduler;
    backup_service_ = &backup_service;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupCleanScheduler::force_cancel(const uint64_t &tenant_id)
{
  // Backup clean tasks do not need to update external file, this interface is not implemented
  return OB_SUCCESS;
}

int ObBackupCleanScheduler::fill_template_delete_policy_(const obrpc::ObDeletePolicyArg &in_arg, ObDeletePolicyAttr &policy_attr)
{
  int ret= OB_SUCCESS;
  if (OB_FAIL(databuff_printf(policy_attr.policy_name_, sizeof(policy_attr.policy_name_), "%s", in_arg.policy_name_))) {
    LOG_WARN("fail to assign policy_name_", K(ret));
  } else if (OB_FAIL(databuff_printf(policy_attr.recovery_window_, sizeof(policy_attr.recovery_window_), "%s", in_arg.recovery_window_))) {
    LOG_WARN("fail to assign recovery_window_", K(ret));
  } else {
    policy_attr.tenant_id_ = in_arg.initiator_tenant_id_;
    policy_attr.redundancy_ = in_arg.redundancy_;
    policy_attr.backup_copies_ = in_arg.backup_copies_;
  }
  return ret;
}

int ObBackupCleanScheduler::add_delete_policy(const obrpc::ObDeletePolicyArg &in_arg)
{
  int ret= OB_SUCCESS;
  ObDeletePolicyAttr policy_attr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean scheduler is not init", K(ret));
  } else if (!in_arg.is_valid()) { 
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_arg));
  } else if (OB_FAIL(fill_template_delete_policy_(in_arg, policy_attr))) {
    LOG_WARN("failed to fill backup clean arg", K(ret), K(in_arg));
  } else if (is_sys_tenant(in_arg.initiator_tenant_id_)) {
    if (1 != in_arg.clean_tenant_ids_.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("must special one tenant", K(ret), K(in_arg));
    } else if (FALSE_IT(policy_attr.tenant_id_ = in_arg.clean_tenant_ids_.at(0))) {
    } else if (OB_FAIL(ObDeletePolicyOperator::insert_delete_policy(*sql_proxy_, policy_attr))) {
      LOG_WARN("failed to start cluster backup clean", K(ret), K(in_arg));
    }
  } else {
    if (!in_arg.clean_tenant_ids_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant backup clean should not initiate another tenant", K(ret), K(in_arg));
    } else if (OB_FAIL(ObDeletePolicyOperator::insert_delete_policy(*sql_proxy_, policy_attr))) {
      LOG_WARN("failed to start tenant backup clean", K(ret), K(in_arg));
    }
  }
  FLOG_INFO("[BACKUP_CLEAN] add delete policy", K(ret), K(in_arg)); 
  return ret;
}

int ObBackupCleanScheduler::drop_delete_policy(const obrpc::ObDeletePolicyArg &in_arg)
{
  int ret= OB_SUCCESS;
  ObDeletePolicyAttr policy_attr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean scheduler is not init", K(ret));
  } else if (!in_arg.is_valid()) { 
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_arg));
  } else if (OB_FAIL(fill_template_delete_policy_(in_arg, policy_attr))) {
    LOG_WARN("failed to fill delete policy arg", K(ret), K(in_arg));
  } else if (is_sys_tenant(in_arg.initiator_tenant_id_)) {
    if (1 != in_arg.clean_tenant_ids_.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("it must special one user tenant", K(ret), K(in_arg));
    } else if (FALSE_IT(policy_attr.tenant_id_ = in_arg.clean_tenant_ids_.at(0))) {
    } else if (OB_FAIL(ObDeletePolicyOperator::drop_delete_policy(*sql_proxy_, policy_attr))) {
      LOG_WARN("failed to start cluster backup clean", K(ret), K(in_arg));
    }
  } else {
    if (!in_arg.clean_tenant_ids_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant backup clean should not initiate another tenant", K(ret), K(in_arg));
    } else if (OB_FAIL(ObDeletePolicyOperator::drop_delete_policy(*sql_proxy_, policy_attr))) {
      LOG_WARN("failed to start tenant backup clean", K(ret), K(in_arg));
    }
  }
  FLOG_INFO("[BACKUP_CLEAN]drop_delete_policy", K(ret), K(in_arg)); 
  return ret;
}

int ObBackupCleanScheduler::get_job_need_reload_task(
    const ObBackupCleanJobAttr &job, 
    common::ObIAllocator &allocator, 
    common::ObIArray<ObBackupScheduleTask *> &tasks)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  ObArray<ObBackupCleanTaskAttr> task_attrs;
  if (OB_FAIL(ObBackupCleanTaskOperator::get_backup_clean_tasks(*sql_proxy_, job.job_id_, job.tenant_id_, for_update, task_attrs))) {
    LOG_WARN("failed to get set tasks", K(ret), K(job));
  } else if (task_attrs.empty()) {
    LOG_INFO("[BACKUP_CLEAN]no set tasks, no need to reload");
  } else {
    for (int j = 0; OB_SUCC(ret) && j < task_attrs.count(); ++j) {
      const ObBackupCleanTaskAttr &task_attr = task_attrs.at(j);
      ObArray<ObBackupCleanLSTaskAttr> ls_tasks;
      if (OB_FAIL(ObBackupCleanLSTaskOperator::get_ls_tasks_from_task_id(*sql_proxy_, task_attr.task_id_, task_attr.tenant_id_, for_update, ls_tasks))) {
        LOG_WARN("failed to get ls tasks", K(ret), K(task_attr));
      } else if (ls_tasks.empty()) {
        LOG_INFO("[BACKUP_CLEAN]no ls tasks, no need to reload", K(task_attr));
      } else if (OB_FAIL(do_get_need_reload_task_(task_attr, ls_tasks, allocator, tasks))) {
        LOG_WARN("failed to get need reload task", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupCleanScheduler::do_get_need_reload_task_(
    const ObBackupCleanTaskAttr &task_attr, 
    const ObArray<ObBackupCleanLSTaskAttr> &ls_tasks,
    common::ObIAllocator &allocator,
    ObIArray<ObBackupScheduleTask *> &tasks)
{
  int ret = OB_SUCCESS;
  if (!task_attr.is_valid() || ls_tasks.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_attr), K(ls_tasks));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ls_tasks.count(); ++i) {
      const ObBackupCleanLSTaskAttr &ls_task = ls_tasks.at(i);
      ObBackupScheduleTask *task = nullptr;
      if (OB_FAIL(build_task_(task_attr, ls_task, allocator, task))) {
        LOG_WARN("failed to build task", K(ret), K(task_attr), K(ls_task));
      } else if (ObBackupTaskStatus::Status::PENDING == ls_task.status_.status_
          || ObBackupTaskStatus::Status::DOING == ls_task.status_.status_) {
        if (OB_FAIL(tasks.push_back(task))) {
          LOG_WARN("failed to push back task", K(ret), K(*task));
        }
      }
    }
  }
  return ret;
}

int ObBackupCleanScheduler::get_need_reload_task(
    common::ObIAllocator &allocator, 
    common::ObIArray<ObBackupScheduleTask *> &tasks)
{
  int ret = OB_SUCCESS;
  bool for_update = false;
  ObArray<ObBackupCleanJobAttr> jobs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObBackupCleanJobOperator::get_jobs(*sql_proxy_, tenant_id_, for_update, jobs))) {
    LOG_WARN("failed to get backup clean jobs", K(ret));
  } else if (jobs.empty()) {
    LOG_INFO("[BACKUP_CLEAN]no job need to reload");
  } else {
    bool is_valid = false;
    for (int i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
      const ObBackupCleanJobAttr &job = jobs.at(i);
      if (is_sys_tenant(job.tenant_id_) || ObBackupCleanStatus::Status::DOING != job.status_.status_) {
        // do nothing
      } else if (OB_FAIL(ObBackupCleanCommon::check_tenant_status(*schema_service_, job.tenant_id_, is_valid))) {
        LOG_WARN("failed to check tenant status", K(ret));
      } else if (!is_valid) {
        LOG_INFO("[BACKUP_CLEAN]tenant status is not valid, no need to reload task");
      } else if (OB_FAIL(get_job_need_reload_task(job, allocator, tasks))){
        LOG_WARN("failed to get job need reload task", K(ret));
      }
    }
  }
  LOG_INFO("[BACKUP_CLEAN]get_need_reload_task", K(ret));
  return ret;
}

int ObBackupCleanScheduler::build_task_(
    const ObBackupCleanTaskAttr &task_attr, 
    const ObBackupCleanLSTaskAttr &ls_task, 
    common::ObIAllocator &allocator, 
    ObBackupScheduleTask *&task)
{
  int ret = OB_SUCCESS;
  int64_t task_deep_copy_size = 0;
  void *raw_ptr = nullptr;
  ObBackupCleanLSTask tmp_task;
  if (OB_FAIL(tmp_task.build(task_attr, ls_task))) {
    LOG_WARN("failed to build task", K(ret), K(task_attr), K(ls_task));
  } else if (FALSE_IT(task_deep_copy_size = tmp_task.get_deep_copy_size())) {
  } else if (nullptr == (raw_ptr = allocator.alloc(task_deep_copy_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate task", K(ret));
  } else if (OB_FAIL(tmp_task.clone(raw_ptr, task))) {
    LOG_WARN("fail to clone input task", K(ret));
  } else if (nullptr == task) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input task ptr is null", K(ret));
  } 
  return ret;
}

int ObBackupCleanScheduler::get_need_cancel_tenants_(
    const uint64_t tenant_id, 
    const common::ObIArray<uint64_t> &backup_tenant_ids,
    common::ObIArray<uint64_t> &need_cancel_backup_tenants)
{
  int ret = OB_SUCCESS;
  need_cancel_backup_tenants.reset();
  if (is_sys_tenant(tenant_id) && backup_tenant_ids.empty()) {
    if (OB_FAIL(schema_service_->get_tenant_ids(need_cancel_backup_tenants))) {
      LOG_WARN("fail to get all tenants", K(ret));
    }
  } else if (is_sys_tenant(tenant_id) && !backup_tenant_ids.empty()) {
    if (OB_FAIL(need_cancel_backup_tenants.assign(backup_tenant_ids))) {
      LOG_WARN("fail to assign backup delete tenant ids", K(backup_tenant_ids));
    }
  } else if (is_user_tenant(tenant_id) && backup_tenant_ids.empty()) {
    if (OB_FAIL(need_cancel_backup_tenants.push_back(tenant_id))) {
      LOG_WARN("fail to push backup delete tenant", K(ret), K(tenant_id));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or backup delete tenant ids", K(ret), K(tenant_id), K(backup_tenant_ids));
  }
  return ret;
}

int ObBackupCleanScheduler::cancel_backup_clean_job(const obrpc::ObBackupCleanArg &in_arg)
{
  //step1: get jobs, if geting null job, means the job may not exist or it has finished, do nothing
  //  if job status is INIT, DOING , advancing job status to CANCELING
  int ret = OB_SUCCESS;
  ObSArray<uint64_t> need_cancel_backup_tenants;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean scheduler not init", K(ret));
  } else if (OB_FAIL(get_need_cancel_tenants_(in_arg.initiator_tenant_id_, in_arg.clean_tenant_ids_, need_cancel_backup_tenants))) {
    LOG_WARN("failed to get need cancel backup delete tenants", K(ret), K(in_arg));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < need_cancel_backup_tenants.count(); ++i) {
      const uint64_t need_cancel_backup_tenant = need_cancel_backup_tenants.at(i);
      bool is_valid = false;
      if (!is_valid_tenant_id(need_cancel_backup_tenant)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tenant", K(ret), K(need_cancel_backup_tenant));
      } else if (OB_FAIL(ObBackupCleanCommon::check_tenant_status(*schema_service_, need_cancel_backup_tenant, is_valid))) {
      } else if (!is_valid) { 
        LOG_INFO("tenant status not normal, no need to schedule backup", K(need_cancel_backup_tenant));
      } else if (OB_FAIL(cancel_tenant_jobs_(need_cancel_backup_tenant))) {
        LOG_WARN("failed to cancel tenant jobs", K(ret), K(need_cancel_backup_tenant));
      }
    }
  }

  return ret;
}

int ObBackupCleanScheduler::cancel_tenant_jobs_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObArray<ObBackupCleanJobAttr> job_attrs;
  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (OB_FAIL(ObBackupCleanJobOperator::get_jobs(trans, tenant_id, true, job_attrs))) {
    LOG_WARN("failed to get job_attrs", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < job_attrs.count(); ++i) {
      ObBackupCleanJobAttr &job_attr = job_attrs.at(i);
      if (!job_attr.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid job", K(ret), K(job_attr));
      } else if (ObBackupCleanStatus::Status::COMPLETED == job_attr.status_.status_
          || ObBackupCleanStatus::Status::CANCELED == job_attr.status_.status_
          || ObBackupCleanStatus::Status::CANCELING == job_attr.status_.status_
          || ObBackupCleanStatus::Status::FAILED == job_attr.status_.status_) {
        // do nothing 
      } else if (ObBackupCleanStatus::Status::INIT == job_attr.status_.status_
          || ObBackupCleanStatus::Status::DOING == job_attr.status_.status_) {
        ObBackupCleanStatus next_status;
        next_status.status_ = ObBackupCleanStatus::Status::CANCELING;
        int result = OB_CANCELED;
        if (OB_FAIL(backup_service_->check_leader())) {
          LOG_WARN("failed to check leader", K(ret));
        } else if (OB_FAIL(ObBackupCleanJobOperator::advance_job_status(trans, job_attr, next_status, result, job_attr.end_ts_))) {
          LOG_WARN("failed to update backup delete job status to CANCELING", K(ret), K(job_attr));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid job status", K(ret), K(job_attr));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      } else {
        backup_service_->wakeup();
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback trans", K(ret), K(tmp_ret));
      }
    } 
  }

  return ret;
}

int ObBackupCleanScheduler::start_schedule_backup_clean(const obrpc::ObBackupCleanArg &in_arg)
{
  int ret = OB_SUCCESS;
  ObBackupCleanJobAttr template_job_attr;
  FLOG_INFO("[BACKUP_CLEAN]start_schedule_backup_clean", K(in_arg)); 
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean scheduler is not init", K(ret));
  } else if (!in_arg.is_valid()) { 
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_arg));
  } else if (OB_FAIL(fill_template_job_(in_arg, template_job_attr))) {
    LOG_WARN("failed to fill backup clean arg", K(ret), K(in_arg));
  } else if (is_sys_tenant(in_arg.initiator_tenant_id_)) {
    // backup clean initiate by sys tenant
    // if clean_tenant_ids is empty, clean all tenants, else backup clean tenant which is only in clean_tenant_ids.
    if (OB_FAIL(start_sys_backup_clean_(template_job_attr))) {
      LOG_WARN("failed to start cluster backup clean", K(ret), K(in_arg));
    }
  } else {
    // backup clean initiate by user tenant
    if (!in_arg.clean_tenant_ids_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant backup clean should not initiate another tenant", K(ret), K(in_arg));
    } else if (OB_FAIL(start_tenant_backup_clean_(template_job_attr))) {
      LOG_WARN("failed to start tenant backup clean", K(ret), K(in_arg));
    }
  }
  if (OB_SUCC(ret)) {
    backup_service_->wakeup();
    FLOG_INFO("[BACKUP_CLEAN]insert backup clean job succeed");
  }
  return ret;
}

int ObBackupCleanScheduler::fill_template_job_(const obrpc::ObBackupCleanArg &in_arg, ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  if (!in_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_arg));
  } else if (OB_FAIL(job_attr.description_.assign(in_arg.description_))) {
    LOG_WARN("failed to assign description", K(in_arg.description_));
  } else {
    const int64_t current_time = ObTimeUtility::current_time();
    job_attr.tenant_id_ = in_arg.initiator_tenant_id_;
    job_attr.incarnation_id_ = 1;
    job_attr.initiator_tenant_id_ = in_arg.initiator_tenant_id_;
    job_attr.clean_type_ = in_arg.type_; 
    job_attr.dest_id_ = in_arg.dest_id_;
    job_attr.status_.status_ = ObBackupCleanStatus::Status::INIT;
    job_attr.start_ts_ = current_time;
    job_attr.end_ts_ = 0;
    job_attr.result_ = OB_SUCCESS;
    if (is_sys_tenant(in_arg.initiator_tenant_id_)) {
      job_attr.job_level_.level_ = in_arg.clean_tenant_ids_.empty() ? ObBackupLevel::Level::CLUSTER : ObBackupLevel::Level::SYS_TENANT;
    } else {
      job_attr.job_level_.level_ = ObBackupLevel::Level::USER_TENANT;
    }
    if (OB_FAIL(job_attr.set_clean_parameter(in_arg.value_))) {
      LOG_WARN("failed to set clean parameter", K(in_arg));
    }
  }
  return ret;
}

int ObBackupCleanScheduler::get_need_clean_tenant_ids_(ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  if (job_attr.is_delete_backup_piece() || job_attr.is_delete_backup_set()) {
    if (1 != job_attr.executor_tenant_id_.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("sys tenant clean set/piece need set tenant", K(ret), K(job_attr));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sys tenant clean set/piece need set tenant");
    } else if (is_sys_tenant(job_attr.executor_tenant_id_.at(0))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("sys tenant clean set/piece can not specify sys tenant", K(ret), K(job_attr));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sys tenant clean set/piece can not specify sys tenant");
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!job_attr.executor_tenant_id_.empty()) {
    for (int i = 0; OB_SUCC(ret) && i < job_attr.executor_tenant_id_.count(); ++i) {
      const uint64_t tenant_id = job_attr.executor_tenant_id_.at(i);
      bool is_valid = false;
      if (OB_FAIL(ObBackupCleanCommon::check_tenant_status(*schema_service_, tenant_id, is_valid))) {
        LOG_WARN("failed to check tenant status", K(tenant_id));
      } else if (!is_valid) {
        ret = OB_BACKUP_CAN_NOT_START;
        LOG_WARN("tenant status are not valid, can't start backup clean", K(ret), K(tenant_id));
      }
    }
  } else if (OB_FAIL(ObBackupCleanCommon::get_all_tenants(*schema_service_, job_attr.executor_tenant_id_))) {
    LOG_WARN("failed to get all tenants", K(ret));
  } else if (job_attr.executor_tenant_id_.empty()) {
    ret = OB_BACKUP_CAN_NOT_START;
    LOG_WARN("can't start cluster backup clean with no tenant", K(ret));
    LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, "no tenant need backup clean");
  }
  return ret;
}

int ObBackupCleanScheduler::persist_job_task_(ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(job_attr.tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret), K(job_attr));
  } else {
    if (OB_FAIL(get_next_job_id_(trans, job_attr.tenant_id_, job_attr.job_id_))) {
      LOG_WARN("failed to get next job id", K(ret));
    } else if (OB_FALSE_IT(job_attr.initiator_job_id_ = job_attr.job_id_)) {
    } else if (OB_FAIL(backup_service_->check_leader())) {
      LOG_WARN("failed to check leader", K(ret));
    } else if (OB_FAIL(ObBackupCleanJobOperator::insert_job(trans, job_attr))) {
      LOG_WARN("failed to insert user tenant backup clean job", K(ret), K(job_attr));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", KR(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupCleanScheduler::start_sys_backup_clean_(const ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObBackupCleanJobAttr new_job_attr;
  if (!job_attr.is_tmplate_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(job_attr));
  } else if (OB_FAIL(new_job_attr.assign(job_attr))) {
    LOG_WARN("failed to assign clean job attr", K(ret), K(job_attr));
  } else if (OB_FAIL(get_need_clean_tenant_ids_(new_job_attr))) {
    LOG_WARN("failed to get can clean tenant ids", K(ret), K(new_job_attr));
  } else if (OB_FAIL(persist_job_task_(new_job_attr))) {
    LOG_WARN("failed to insert job", K(ret), K(new_job_attr));
  }
  FLOG_INFO("[BACKUP_CLEAN] finish start sys tenant backup clean", K(ret), K(job_attr));
  return ret;
}

int ObBackupCleanScheduler::start_tenant_backup_clean_(const ObBackupCleanJobAttr &job_attr) 
{
  int ret = OB_SUCCESS;
  ObBackupCleanJobAttr new_job_attr;
  bool is_valid = false;
  ObMySQLTransaction trans;
  if (!job_attr.is_tmplate_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tmplate job", K(ret), K(job_attr));
  } else if (OB_FAIL(new_job_attr.assign(job_attr))) {
    LOG_WARN("failed to assign job_attr", K(ret), K(job_attr));
  } else if (OB_FAIL(ObBackupCleanCommon::check_tenant_status(*schema_service_, new_job_attr.tenant_id_, is_valid))) {
    LOG_WARN("failed to check tenant can do backup", K(ret), K(new_job_attr));
  } else if (!is_valid) {
    ret = OB_NOT_SUPPORTED; 
    LOG_WARN("tenant status is not valid, can't start backup clean", K(ret), K(new_job_attr));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(new_job_attr.tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret), K(new_job_attr));
  } else {
    if (OB_FAIL(get_next_job_id_(trans, new_job_attr.tenant_id_, new_job_attr.job_id_))) {
      LOG_WARN("failed to get next job id", K(ret));
    } else if (OB_FAIL(new_job_attr.executor_tenant_id_.push_back(new_job_attr.tenant_id_))) {
      LOG_WARN("failed to push back tenant id", K(ret));
    } else if (OB_FALSE_IT(new_job_attr.initiator_job_id_ = new_job_attr.job_id_)) { 
    } else if (OB_FAIL(backup_service_->check_leader())) {
      LOG_WARN("failed to check leader", K(ret));
    } else if (OB_FAIL(ObBackupCleanJobOperator::insert_job(trans, new_job_attr))) {
      LOG_WARN("failed to insert backup clean job", K(ret), K(new_job_attr));
    } 
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", KR(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupCleanScheduler::get_next_job_id_(common::ObISQLClient &trans, const uint64_t tenant_id, int64_t &job_id)
{
  int ret = OB_SUCCESS;
  job_id = -1;
  if (OB_FAIL(ObLSBackupInfoOperator::get_next_job_id(trans, tenant_id, job_id))) {
    LOG_WARN("failed to get next job id", K(ret));
  } else if (-1 == job_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid job id", K(ret));
  }
  return ret;
}

int ObBackupCleanScheduler::handle_execute_over(
    const ObBackupScheduleTask *task, 
    const share::ObHAResultInfo &result_info,
    bool &can_remove)
{
  //cases of call handle_execute_over
  //1. observer return a rpc to tell task scheduler task finish (success or fail)
  //2. task scheduler find a task not on the dest observer
  //3. task scheduler do execute failed 
  int ret = OB_SUCCESS;
  can_remove = false;
  bool is_valid = false;
  ObBackupCleanLSTaskAttr ls_attr;
  ObMySQLTransaction trans;
  ObLSID ls_id(task->get_ls_id());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr == task) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ObBackupCleanCommon::check_tenant_status(*schema_service_, task->get_tenant_id(), is_valid))) {
    LOG_WARN("failed to check tenant status", K(ret), K(*task));
  } else if (!is_valid) {
    can_remove = true;
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(task->get_tenant_id())))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(ObBackupCleanLSTaskOperator::get_ls_task(trans, true/*for update*/, task->get_task_id(), task->get_tenant_id(), ls_id, ls_attr))) {
      LOG_WARN("failed to get log stream task", K(ret), K(*task));
    } else if (ObBackupTaskStatus::Status::DOING == ls_attr.status_.status_) {
      ObBackupTaskStatus next_status;
      next_status.status_ = ObBackupTaskStatus::Status::FINISH;
      ls_attr.end_ts_ = ObTimeUtility::current_time(); 
      if (OB_FAIL(ObBackupCleanLSTaskMgr::advance_ls_task_status(*backup_service_, trans, ls_attr, next_status, result_info.result_, ls_attr.end_ts_))) {
        LOG_WARN("failed to advance status", K(ret), K(ls_attr), K(next_status), K(result_info));
      } 
    } else {
      LOG_WARN("concurrent scenario! this task will need reload to redo.", K(ls_attr));
    }
    
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      } else {
        can_remove = true;
        backup_service_->wakeup();
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback trans", K(ret), K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObBackupCleanScheduler::handle_failed_job_(const uint64_t tenant_id, const int64_t result, ObIBackupDeleteMgr &job_mgr, ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sys tenant does not handle failed", K(ret), K(tenant_id));
  } else if (!job_mgr.is_can_retry(result) || job_attr.retry_count_ >= OB_MAX_RETRY_TIMES) {
    if (OB_FAIL(job_mgr.deal_non_reentrant_job(result))) {
      LOG_WARN("failed to deal failed job", K(ret), K(job_attr));
    }
  } else {
    job_attr.retry_count_++;
    if (OB_FAIL(ObBackupCleanJobOperator::update_retry_count(*sql_proxy_, job_attr))) {
      LOG_WARN("failed to persist retry times", K(ret), K(job_attr));
    } else {
      backup_service_->wakeup();
    }
  }
  return ret;
} 

int ObBackupCleanScheduler::process_tenant_delete_jobs_(const uint64_t tenant_id, ObArray<ObBackupCleanJobAttr> &clean_jobs)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIBackupDeleteMgr *job_mgr = nullptr;
  if (OB_FAIL(ObBackupDeleteMgrAlloctor::alloc(tenant_id, job_mgr))) {
    LOG_WARN("fail to alloc job mgr", K(ret), K(tenant_id));
  } else if (OB_ISNULL(job_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup delete job mgr can't be nullptr", K(ret), K(tenant_id));
  }

  for (int j = 0; OB_SUCC(ret) && j < clean_jobs.count(); ++j) {
    ObBackupCleanJobAttr &job_attr = clean_jobs.at(j);
    job_mgr->reset();
    if (!job_attr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup clean job is not valid", K(ret), K(job_attr));
    } else if (OB_FAIL(job_mgr->init(tenant_id, job_attr, *sql_proxy_, *rpc_proxy_, *task_scheduler_, *schema_service_, *backup_service_))) {
      LOG_WARN("failed to init tenant backup clean job mgr", K(ret), K(job_attr));
    } else if (OB_SUCCESS != (tmp_ret = job_mgr->process())) { // tenant level backups are isolated
      LOG_WARN("failed to schedule tenant backup clean job", K(tmp_ret), K(job_attr));
      if (!is_sys_tenant(tenant_id) && OB_SUCCESS != (tmp_ret = handle_failed_job_(tenant_id, tmp_ret, *job_mgr, job_attr))) {
        LOG_WARN("failed to handle user tenant failed job", K(tmp_ret), K(job_attr)); 
      } else {
        backup_service_->wakeup();
      }
    }
  }
  if (OB_NOT_NULL(job_mgr)) {
    ObBackupDeleteMgrAlloctor::free(job_mgr);
    job_mgr = nullptr;
  }
  return ret;
}

int ObBackupCleanScheduler::process()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupCleanJobAttr> clean_jobs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean scheduler not init", K(ret));
  } else if (OB_FAIL(ObBackupCleanJobOperator::get_jobs(*sql_proxy_, tenant_id_, false/*not update*/, clean_jobs))) {
    LOG_WARN("failed to get backup clean jobs", K(ret));
  } else if (clean_jobs.empty()) {
    // do nothing
  } else if (OB_FAIL(process_tenant_delete_jobs_(tenant_id_, clean_jobs))) {
    LOG_WARN("failed to process tenant delete jobs", K(ret), K(tenant_id_), K(clean_jobs));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]finish process backup clean jobs", K(tenant_id_), K(clean_jobs));
  }
  return ret;
}


/*
 *-----------------------------ObIBackupDeleteMgr-------------------------
 */

ObIBackupDeleteMgr::ObIBackupDeleteMgr()
 : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    job_attr_(nullptr),
    sql_proxy_(nullptr),
    rpc_proxy_(nullptr),
    task_scheduler_(nullptr),
    schema_service_(nullptr),
    backup_service_(nullptr)
{
}

int ObIBackupDeleteMgr::init(
    const uint64_t tenant_id, 
    ObBackupCleanJobAttr &job_attr,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    ObBackupTaskScheduler &task_scheduler,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObBackupCleanService &backup_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(job_attr));
  } else {
    tenant_id_ = tenant_id;
    job_attr_ = &job_attr;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    task_scheduler_ = &task_scheduler;
    schema_service_ = &schema_service;
    backup_service_ = &backup_service;
    is_inited_ = true;
  }
  return ret;
}

void ObIBackupDeleteMgr::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  job_attr_ = nullptr;
  sql_proxy_ = nullptr;
  rpc_proxy_ = nullptr;
  task_scheduler_ = nullptr;
  schema_service_ = nullptr;
  backup_service_ = nullptr;
  is_inited_ = false;
}

bool ObIBackupDeleteMgr::is_can_retry(const int err) const 
{
  return job_attr_->can_retry_ && ObBackupUtils::is_need_retry_error(err);
}

//*******************************ObUserTenantBackupDeleteMgr**********************

int ObUserTenantBackupDeleteMgr::deal_non_reentrant_job(const int error) 
{
  int ret = OB_SUCCESS;
  LOG_INFO("[BACKUP_CLEAN] start deal non reentrant error", K(error)); 
  ObBackupCleanStatus next_status;
  next_status.status_ = ObBackupCleanStatus::Status::FAILED;
  job_attr_->result_ = error;
  job_attr_->end_ts_ = ObTimeUtility::current_time();
  ObArray<ObBackupCleanTaskAttr> task_attrs;
  if (IS_NOT_INIT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_scheduler_->cancel_tasks(BackupJobType::BACKUP_CLEAN_JOB, job_attr_->job_id_, job_attr_->tenant_id_))) {
    LOG_WARN("failed to cancel backup tasks", K(ret), K(*job_attr_));
  } else if (ObBackupCleanStatus::Status::INIT != job_attr_->status_.status_
      && ObBackupCleanStatus::Status::DOING != job_attr_->status_.status_) {
    // do nothing
  } else if (OB_FAIL(ObBackupCleanTaskOperator::get_backup_clean_tasks(*sql_proxy_, job_attr_->job_id_,
      job_attr_->tenant_id_, false/*need_lock*/, task_attrs))) {
    LOG_WARN("failed to get backup clean tasks", K(ret));
  } else {
    int64_t success_task_count = 0;
    for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
      SMART_VAR(ObBackupCleanTaskMgr, task_mgr) {
        const ObBackupCleanTaskAttr &task_attr = task_attrs.at(i);
        if (ObBackupCleanStatus::Status::INIT == task_attr.status_.status_ 
           || ObBackupCleanStatus::Status::DOING == task_attr.status_.status_
           || ObBackupCleanStatus::Status::CANCELING == task_attr.status_.status_) {
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_DELETE_EXCEPTION_HANDLING) OB_SUCCESS;
#endif
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(task_mgr.init(job_attr_->tenant_id_, task_attr.task_id_, *job_attr_, *sql_proxy_,
              *rpc_proxy_, *task_scheduler_, *backup_service_))) {
            LOG_WARN("failed to init set task mgr", K(ret));
          } else if (OB_FAIL(task_mgr.deal_failed_task(task_attr.result_))) {
            LOG_WARN("failed to deal failed set task", K(ret), K(task_attr));
          } else {
            LOG_WARN("deal failed set task", K(ret), K(task_attr));
          }
        } else if (ObBackupCleanStatus::Status::COMPLETED == task_attr.status_.status_) {
          success_task_count++;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(advance_job_status_(*sql_proxy_, next_status, job_attr_->result_, job_attr_->end_ts_))) {
      LOG_WARN("failed to move job status to FAILED", K(ret), K(*job_attr_), K(next_status));
    } else if (FALSE_IT(job_attr_->success_task_count_ = success_task_count)) {
    } else if (OB_FAIL(ObBackupCleanJobOperator::update_task_count(*sql_proxy_, *job_attr_, false/*is_total*/))) {
      LOG_WARN("failed to update job success task count", K(ret)); 
    } else {
      LOG_INFO("succeed deal with failed job, advance job status to FAILED", K(*job_attr_));
    }
    backup_service_->wakeup(); 
  }

  return ret;
}

int ObUserTenantBackupDeleteMgr::check_data_backup_dest_validity_()
{
  int ret = OB_SUCCESS;
  ObBackupPathString backup_dest_str;
  ObBackupDestMgr dest_mgr;
  share::ObBackupHelper backup_helper;
  if (OB_FAIL(backup_helper.init(job_attr_->tenant_id_, *sql_proxy_))) {
    LOG_WARN("failed to init backup helper", K(ret));
  } else if (OB_FAIL(backup_helper.get_backup_dest(backup_dest_str))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("[BACKUP_CLEAN]data backup dest not exist", K(ret), K(job_attr_->job_id_), K(job_attr_->tenant_id_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get backup dest", K(ret));
    }
  } else if (backup_dest_str.is_empty()) {
    // do nothing
  } else if (OB_FAIL(dest_mgr.init(job_attr_->tenant_id_, ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA, backup_dest_str,  *sql_proxy_))) {
    LOG_WARN("failed to init dest manager", K(ret), K(job_attr_), K(backup_dest_str));
  } else if (OB_FAIL(dest_mgr.check_dest_validity(*rpc_proxy_, true/*need_format_file*/))) {
    LOG_WARN("failed to check backup dest validity", K(ret), K(job_attr_), K(backup_dest_str));
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::check_log_archive_dest_validity_()
{
  int ret = OB_SUCCESS;
  ObBackupPathString archive_dest_str;
  bool need_lock = false;
  share::ObArchivePersistHelper helper;
  common::ObSArray<std::pair<int64_t, int64_t>> dest_array;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(helper.init(job_attr_->tenant_id_))) {
    LOG_WARN("failed to init archive helper", K(ret), K(job_attr_));
  } else if (OB_FAIL(helper.get_valid_dest_pairs(*sql_proxy_, dest_array))) {
    LOG_WARN("failed to get archive dest", K(ret), K(job_attr_));
  } else if (0 == dest_array.count()) {
    LOG_INFO("[BACKUP_CLEAN]log archive dest not exit", K(ret), K(job_attr_->job_id_), K(job_attr_->tenant_id_));
  } else {
    ObBackupDestMgr dest_mgr;
    for (int i = 0; OB_SUCC(ret) && i < dest_array.count(); i++) {
      std::pair<int64_t, int64_t> archive_dest = dest_array.at(i);
      dest_mgr.reset();
      if (OB_FAIL(helper.get_archive_dest(*sql_proxy_, need_lock, archive_dest.first, archive_dest_str))) {
        LOG_WARN("failed to get archive path", K(ret));
      } else if (OB_FAIL(dest_mgr.init(job_attr_->tenant_id_, ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG, archive_dest_str,  *sql_proxy_))) {
        LOG_WARN("failed to init dest manager", K(ret), K(job_attr_), K(archive_dest_str));
      } else if (OB_FAIL(dest_mgr.check_dest_validity(*rpc_proxy_, true/*need_format_file*/))) {
        LOG_WARN("failed to check archive dest validity", K(ret), K(job_attr_), K(archive_dest_str));
      }
    }
  }

  return ret;
}

int ObUserTenantBackupDeleteMgr::check_dest_validity_()
{ 
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_data_backup_dest_validity_())) {
    LOG_WARN("failed to check data backup dest validity", K(ret), K(job_attr_));
  } else if (OB_FAIL(check_log_archive_dest_validity_())) {
    LOG_WARN("failed to check log archive dest validity", K(ret), K(job_attr_));
  }

  return ret;
}

int ObUserTenantBackupDeleteMgr::process()
{
  //execute different operations according to the status of the job_attr
  //status including : INIT DOING COMPLETED FAILED CANCELING CANCELED
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP_CLEAN]schedule user tenant backup delete job", K(*job_attr_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clean scheduler not init", K(ret));
  } else {
    ObBackupCleanStatus::Status status = job_attr_->status_.status_;
    switch (status) {
      case ObBackupCleanStatus::Status::INIT: {
        if (OB_FAIL(check_dest_validity_())) {
          LOG_WARN("failed to check clean dest validity", K(ret), K(*job_attr_));
        } else if (OB_FAIL(persist_backup_clean_task_())) {
          LOG_WARN("failed to persist log stream task", K(ret), K(*job_attr_));
        }
        break;
      } 
      case ObBackupCleanStatus::Status::DOING: {
        DEBUG_SYNC(BACKUP_DELETE_STATUS_DOING);
        if (OB_FAIL(do_backup_clean_task_())) {
          LOG_WARN("failed to do backup clean", K(ret), K(*job_attr_));
        }
        break;
      }
      case ObBackupCleanStatus::Status::COMPLETED: 
      case ObBackupCleanStatus::Status::FAILED: 
      case ObBackupCleanStatus::Status::CANCELED: {
        DEBUG_SYNC(BACKUP_DELETE_STATUS_COMPLETED);
        if (OB_FAIL(move_to_history_())) {
          LOG_WARN("failed to move job and set to histroy", K(ret));
        }
        break;
      }

      case ObBackupCleanStatus::Status::CANCELING: {
        if (OB_FAIL(cancel_())) {
          LOG_WARN("failed to cancel backup clean", K(ret), K(*job_attr_));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown backup clean status", K(ret), K(*job_attr_));
      }
    }
    FLOG_INFO("[BACKUP_CLEAN]finish schedule backup clean job", K(*job_attr_), K(ret), K(status));
  }

  return ret;
}

int ObUserTenantBackupDeleteMgr::move_to_history_()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP_CLEAN]start move_task_to_history"); 
  ObArray<ObBackupCleanTaskAttr> task_attrs;
  ObMySQLTransaction trans;
  if (is_sys_tenant(job_attr_->initiator_tenant_id_) && OB_FAIL(report_failed_to_initiator_())) {
    LOG_WARN("fail to report job finish to initiator tenant id", K(ret), K(*job_attr_));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(ObBackupCleanTaskOperator::get_backup_clean_tasks(trans, job_attr_->job_id_,
        job_attr_->tenant_id_, false/*need_lock*/, task_attrs))) {
      LOG_WARN("failed to get backup clean tasks", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
        SMART_VAR(ObBackupCleanTaskMgr, task_mgr) {
          const ObBackupCleanTaskAttr &task_attr = task_attrs.at(i);
          if (OB_FAIL(task_mgr.init(tenant_id_, task_attr.task_id_, *job_attr_, trans,
            *rpc_proxy_, *task_scheduler_, *backup_service_))) {
            LOG_WARN("failed to init set task mgr", K(ret));
          } else if (OB_FAIL(task_mgr.process())) {
            LOG_WARN("failed to task move history", K(ret), K(task_attr));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ObBackupCleanJobOperator::move_job_to_his(trans, tenant_id_, job_attr_->job_id_))) {
        LOG_WARN("failed to move job to history", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      } else {
        LOG_INFO("succeed to move job to history. backup delete job finish", "tenant_id", job_attr_->tenant_id_, "job_id", job_attr_->job_id_);
        backup_service_->wakeup();
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to roll back status", K(ret), K(tmp_ret));
      }
    }
  }
  
  return ret;
}

int ObUserTenantBackupDeleteMgr::report_failed_to_initiator_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupCleanJobOperator::report_failed_to_sys_tenant(*sql_proxy_, *job_attr_))) {
    LOG_WARN("failed to report failed to sys tenant");
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::update_task_to_canceling_(ObArray<ObBackupCleanTaskAttr> &task_attrs)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
    ObBackupCleanTaskAttr &task_attr = task_attrs.at(i);
    if (!task_attr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set task attr is valid", K(ret));
    } else if (ObBackupCleanStatus::Status::INIT == task_attr.status_.status_ || ObBackupCleanStatus::Status::DOING == task_attr.status_.status_) {
      task_attr.status_.status_ = ObBackupCleanStatus::Status::CANCELING;
      if (OB_FAIL(ObBackupCleanTaskOperator::advance_task_status(*sql_proxy_, task_attr,  task_attr.status_))) {
        LOG_WARN("failed to advance task status", K(ret));
      }
    }
  }

  return ret;
}

int ObUserTenantBackupDeleteMgr::do_cancel_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupCleanTaskAttr> task_attrs;
  if (OB_FAIL(ObBackupCleanTaskOperator::get_backup_clean_tasks(*sql_proxy_, job_attr_->job_id_,
      job_attr_->tenant_id_, false/*need_lock*/, task_attrs))) {
    LOG_WARN("failed to get backup clean tasks", K(ret));
  } else if (OB_FAIL(update_task_to_canceling_(task_attrs))) {
    LOG_WARN("failed to update set task canceling", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
      SMART_VAR(ObBackupCleanTaskMgr, task_mgr) {
        const ObBackupCleanTaskAttr &task_attr = task_attrs.at(i);
        if (ObBackupCleanStatus::Status::CANCELING == task_attr.status_.status_) { 
          if (OB_FAIL(task_mgr.init(tenant_id_, task_attr.task_id_, *job_attr_, *sql_proxy_, *rpc_proxy_,
              *task_scheduler_, *backup_service_))) {
            LOG_WARN("failed to init set task mgr", K(ret));
          } else if (OB_FAIL(task_mgr.process())) {
            LOG_WARN("failed to cancel task", K(ret), K(task_attr));
          }
        }
      }
    }
  }
  return ret; 
}

int ObUserTenantBackupDeleteMgr::advance_status_canceled()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupCleanTaskAttr> task_attrs;
  bool has_doing_task = false;
  bool has_failed_task = false;
  int64_t success_task_count = 0;
  if (OB_FAIL(ObBackupCleanTaskOperator::get_backup_clean_tasks(*sql_proxy_, job_attr_->job_id_,
      job_attr_->tenant_id_, false/*need_lock*/, task_attrs))) {
    LOG_WARN("failed to get backup clean tasks", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
      const ObBackupCleanTaskAttr &task_attr = task_attrs.at(i);
      if (!task_attr.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set task attr is valid", K(ret));
      } else if (ObBackupCleanStatus::Status::INIT == task_attr.status_.status_
          || ObBackupCleanStatus::Status::DOING == task_attr.status_.status_
          || ObBackupCleanStatus::Status::CANCELING == task_attr.status_.status_) {
        has_doing_task = true;
      } else if (ObBackupCleanStatus::Status::FAILED == task_attr.status_.status_ && !has_failed_task) {
        has_failed_task = true;
        job_attr_->result_ = task_attr.result_; 
      } else if (ObBackupCleanStatus::Status::COMPLETED == task_attr.status_.status_) {
        success_task_count++;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!has_doing_task) {
    ObBackupCleanStatus next_status;
    job_attr_->success_task_count_ = success_task_count;
    next_status.status_ = ObBackupCleanStatus::Status::CANCELED;
    job_attr_->end_ts_ = ObTimeUtility::current_time();
    job_attr_->result_ = (true == has_failed_task) ? job_attr_->result_ : OB_CANCELED; 
    if (OB_FAIL(advance_job_status_(*sql_proxy_, next_status, job_attr_->result_, job_attr_->end_ts_))) {
      LOG_WARN("failed to advance_job_status", K(ret));
    } else if (OB_FAIL(ObBackupCleanJobOperator::update_task_count(*sql_proxy_, *job_attr_, false/*is_total*/))) {
      LOG_WARN("failed to update job task count", K(ret));
    }
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::cancel_()
{
  int ret = OB_SUCCESS;

 if (OB_FAIL(do_cancel_())) {
    LOG_WARN("failed to cancel set tasks", K(ret));
  } else if (OB_FAIL(advance_status_canceled())) {
    LOG_WARN("failed to advance status canceled", K(ret)); 
  } else {
    backup_service_->wakeup();
  } 

  return ret;
}

int ObUserTenantBackupDeleteMgr::do_backup_clean_tasks_(const ObArray<ObBackupCleanTaskAttr> &task_attrs)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
    SMART_VAR(ObBackupCleanTaskMgr, task_mgr) {
      const ObBackupCleanTaskAttr &task_attr = task_attrs.at(i);
      if (ObBackupCleanStatus::Status::FAILED == task_attr.status_.status_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set task status can not be failed", K(ret), K(task_attr));  
      } else if (ObBackupCleanStatus::Status::COMPLETED == task_attr.status_.status_
          || ObBackupCleanStatus::Status::CANCELED == task_attr.status_.status_) {
        // do nothing
      } else if (OB_FAIL(task_mgr.init(tenant_id_, task_attr.task_id_, *job_attr_, *sql_proxy_,
          *rpc_proxy_, *task_scheduler_, *backup_service_))) {
        LOG_WARN("failed to init set task mgr", K(ret));
      } else if (OB_FAIL(task_mgr.process())) {
        LOG_WARN("failed to process task", K(ret), K(task_attr));
      }
    }
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::handle_backup_clean_task(
    const ObArray<ObBackupCleanTaskAttr> &task_attrs,
    ObBackupCleanStatus &next_status,
    int &result)
{
  int ret = OB_SUCCESS;
  result = OB_SUCCESS;
  next_status.status_ = ObBackupCleanStatus::Status::MAX_STATUS;
  bool has_failed = false;
  bool has_task = false;
  int64_t success_task_count = 0;
  for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
    const ObBackupCleanTaskAttr &task_attr = task_attrs.at(i);
    if (!task_attr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is invalid", K(ret), K(task_attr)); 
    } else if (ObBackupCleanStatus::Status::INIT == task_attr.status_.status_
        || ObBackupCleanStatus::Status::DOING == task_attr.status_.status_
        || ObBackupCleanStatus::Status::CANCELING == task_attr.status_.status_) {
      has_task = true;
    } else if (ObBackupCleanStatus::Status::FAILED == task_attr.status_.status_ && !has_failed) { // return first failed
      has_failed = true;
      result = task_attr.result_;
    } else if (ObBackupCleanStatus::Status::COMPLETED == task_attr.status_.status_) {
      success_task_count++; 
    }
  }
  job_attr_->success_task_count_ = success_task_count;
  if (OB_FAIL(ret)) {
  } else if (!has_failed && has_task) {
    if (OB_FAIL(do_backup_clean_tasks_(task_attrs))) {
      LOG_WARN("failed to do set tasks", K(ret)); 
    } 
  } else if (has_failed && has_task) {
    if (OB_FAIL(do_cancel_())) {
      LOG_WARN("failed to cancel task", K(ret)); 
    }
  } else if (has_failed && !has_task) {
    next_status.status_ = ObBackupCleanStatus::Status::FAILED; 
  } else {
    next_status.status_ = ObBackupCleanStatus::Status::COMPLETED;
  }
  FLOG_INFO("[BACKUP_CLEAN]handle backup clean task", K(ret), K(*job_attr_), K(next_status.status_), K(has_failed), K(has_task)); 
  return ret;
}

int ObUserTenantBackupDeleteMgr::do_backup_clean_task_()
{
  int ret = OB_SUCCESS;
  ObBackupCleanStatus next_status;
  ObArray<ObBackupCleanTaskAttr> task_attrs;
  int result = OB_SUCCESS;

  if (OB_FAIL(ObBackupCleanTaskOperator::get_backup_clean_tasks(*sql_proxy_, job_attr_->job_id_,
      job_attr_->tenant_id_, false/*need_lock*/, task_attrs))) {
    LOG_WARN("failed to get backup clean tasks", K(ret));
  } else if (OB_FAIL(handle_backup_clean_task(task_attrs, next_status, result))) {
    LOG_WARN("failed to handle set tasks", K(ret));
  } else if (ObBackupCleanStatus::Status::COMPLETED == next_status.status_
      || ObBackupCleanStatus::Status::FAILED == next_status.status_) {
    int64_t end_ts = ObTimeUtility::current_time(); 
    if (OB_FAIL(advance_job_status_(*sql_proxy_, next_status, result, end_ts))) {
      LOG_WARN("failed to advance status", K(ret));
    } else if (OB_FAIL(ObBackupCleanJobOperator::update_task_count(*sql_proxy_, *job_attr_, false/*is_total*/))) {
      LOG_WARN("failed to update job task count", K(ret)); 
    } else {
      backup_service_->wakeup();
    }
  }

  return ret;
}

int ObUserTenantBackupDeleteMgr::get_delete_backup_set_infos_(ObArray<ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  ObBackupSetFileDesc backup_set_info;
  int64_t copies_num = 0;
  if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(*sql_proxy_, false/*need_lock*/, job_attr_->backup_set_id_,
      job_attr_->incarnation_id_, job_attr_->tenant_id_, job_attr_->dest_id_, backup_set_info))) {
    LOG_WARN("failed to get backup set file", K(ret)); 
  } else if (!backup_set_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED; 
    LOG_WARN("backup set info is invalid", K(ret), K(backup_set_info));
  } else if (ObBackupSetFileDesc::BackupSetStatus::DOING == backup_set_info.status_) {
    ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
    LOG_WARN("backup set do not allow clean, because status of backup set is not finish", K(ret), K(backup_set_info));
  } else if (ObBackupSetFileDesc::BackupSetStatus::FAILED == backup_set_info.status_) {
    // do nothing
  } else if (ObBackupFileStatus::BACKUP_FILE_DELETED == backup_set_info.file_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup set has been deleted", K(ret), K(backup_set_info));
  } else if (ObBackupFileStatus::BACKUP_FILE_DELETING == backup_set_info.file_status_) {
    // do nothing
  }
  
  if (OB_SUCC(ret) && OB_FAIL(set_list.push_back(backup_set_info))) {
    LOG_WARN("failed to push backup set list", K(ret));
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_delete_backup_piece_infos_(ObArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  // TODO(wenjinyu.wjy) 4.3 support
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_delete_obsolete_backup_set_infos_(
    ObBackupSetFileDesc &clog_data_clean_point,
    ObArray<ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS; 
  ObArray<ObBackupSetFileDesc> backup_set_infos;
  CompareBackupSetInfo backup_set_info_cmp;
  ObBackupPathString backup_dest_str;
  ObBackupPathString backup_path_str;
  share::ObBackupHelper backup_helper;
  ObBackupDest backup_dest;
  if (OB_FAIL(backup_helper.init(job_attr_->tenant_id_, *sql_proxy_))) {
    LOG_WARN("fail to init backup help", K(ret));
  } else if (OB_FAIL(backup_helper.get_backup_dest(backup_dest_str))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get backup dest", K(ret));
    }
  } else if (backup_dest_str.is_empty()) {
    // do nothing
  } else if (OB_FAIL(backup_dest.set(backup_dest_str))) {
    LOG_WARN("fail to set backup dest", K(ret), K(backup_dest_str)); 
  } else if (OB_FAIL(backup_dest.get_backup_path_str(backup_path_str.ptr(), backup_path_str.capacity()))) {
    LOG_WARN("fail to get backup path str", K(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_candidate_obsolete_backup_sets(*sql_proxy_, job_attr_->tenant_id_,
      job_attr_->expired_time_, backup_path_str.ptr(), backup_set_infos))) {
    LOG_WARN("failed to get candidate obsolete backup sets", K(ret)); 
  } else if (FALSE_IT(lib::ob_sort(backup_set_infos.begin(), backup_set_infos.end(), backup_set_info_cmp))) {
  } else {
    for (int64_t i = backup_set_infos.count() - 1 ; OB_SUCC(ret) && i >= 0; i--) {
      bool need_deleted = false;
      const ObBackupSetFileDesc &backup_set_info = backup_set_infos.at(i);
      if (!backup_set_info.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("backup set info is invalid", K(ret)); 
      } else if (ObBackupSetFileDesc::BackupSetStatus::DOING == backup_set_info.status_) {
        need_deleted = false;
      } else if (ObBackupSetFileDesc::BackupSetStatus::FAILED == backup_set_info.status_
          || ObBackupFileStatus::BACKUP_FILE_DELETING == backup_set_info.file_status_) {
        need_deleted = true;
        LOG_INFO("[BACKUP_CLEAN] allow delete", K(backup_set_info));  
      } else if (backup_set_info.backup_type_.is_full_backup() && !clog_data_clean_point.is_valid()) {
        if (OB_FAIL(clog_data_clean_point.assign(backup_set_info))) {
          LOG_WARN("backup set info is invalid", K(ret)); 
        } else {
          need_deleted = false;
        }
      } else if (!backup_set_info.backup_type_.is_full_backup()
          && backup_set_info.backup_set_id_ > clog_data_clean_point.backup_set_id_) {
        need_deleted = false;
      } else {
        need_deleted = true;      
      }

      if (OB_FAIL(ret)) {
      } else if (need_deleted && OB_FAIL(set_list.push_back(backup_set_info))) {
        LOG_WARN("failed to push back set list", K(ret));
      }
    }
    LOG_INFO("[BACKUP_CLEAN]finish get delete obsolete backup set infos ", K(ret), K(clog_data_clean_point), K(set_list)); 
  }
  return ret;
}

bool ObUserTenantBackupDeleteMgr::can_backup_pieces_be_deleted(const ObArchivePieceStatus &status)
{
  return ObArchivePieceStatus::Status::INACTIVE == status.status_
      || ObArchivePieceStatus::Status::FROZEN == status.status_;
}

int ObUserTenantBackupDeleteMgr::get_backup_dest_str(const bool is_backup_backup, char *backup_dest_str, int64_t str_len)
{
  int ret = OB_SUCCESS; 
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_all_dest_backup_piece_infos_(
    const ObBackupSetFileDesc &clog_data_clean_point,
    ObArray<ObTenantArchivePieceAttr> &backup_piece_infos)
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper archive_table_op;
  common::ObArray<std::pair<int64_t, int64_t>> dest_array;
  if (!clog_data_clean_point.is_valid()) {
    LOG_INFO("[BACKUP_CLEAN]point is invalid", K(clog_data_clean_point));
  } else if (OB_FAIL(archive_table_op.init(job_attr_->tenant_id_))) {
    LOG_WARN("failed to init archive helper", K(ret)); 
  } else if (OB_FAIL(archive_table_op.get_valid_dest_pairs(*sql_proxy_, dest_array))) {
    LOG_WARN("failed to init archive helper", K(ret)); 
  } else if (0 == dest_array.count()) {
    // do nothing
  } else {
    ObBackupDest backup_dest;
    ObBackupPathString backup_dest_str;
    ObBackupPathString backup_path_str;
    bool need_lock = false;
    for (int i = 0; OB_SUCC(ret) && i < dest_array.count(); i++) {
      std::pair<int64_t, int64_t> archive_dest = dest_array.at(i);
      if (OB_FAIL(archive_table_op.get_archive_dest(*sql_proxy_, need_lock, archive_dest.first, backup_dest_str))) {
        LOG_WARN("failed to get archive path", K(ret));
      } else if (OB_FAIL(backup_dest.set(backup_dest_str))) {
        LOG_WARN("fail to set backup dest", K(ret), K(backup_dest_str)); 
      } else if (OB_FAIL(backup_dest.get_backup_path_str(backup_path_str.ptr(), backup_path_str.capacity()))) {
        LOG_WARN("fail to get backup path str", K(ret), K(backup_dest));
      } else if (OB_FAIL(archive_table_op.get_candidate_obsolete_backup_pieces(*sql_proxy_, clog_data_clean_point.start_replay_scn_, backup_path_str.ptr(), backup_piece_infos))) {
        LOG_WARN("failed to get candidate obsolete backup sets", K(ret)); 
      }
    }
  }

  return ret;
}

int ObUserTenantBackupDeleteMgr::get_delete_obsolete_backup_piece_infos_(const ObBackupSetFileDesc &clog_data_clean_point, ObArray<ObTenantArchivePieceAttr> &piece_list)
{ 
  int ret = OB_SUCCESS;
  CompareBackupPieceInfo backup_piece_info_cmp;
  ObArray<ObTenantArchivePieceAttr> backup_piece_infos;
  if (OB_FAIL(get_all_dest_backup_piece_infos_(clog_data_clean_point, backup_piece_infos))) {
    LOG_WARN("failed to get all dest backup piece infos", K(ret), K(clog_data_clean_point)); 
  } else if (FALSE_IT(lib::ob_sort(backup_piece_infos.begin(), backup_piece_infos.end(), backup_piece_info_cmp))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_infos.count(); i++) {
      const ObTenantArchivePieceAttr &backup_piece_info = backup_piece_infos.at(i);
      if (!backup_piece_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup piece info is invalid", K(ret));
      } else if (!can_backup_pieces_be_deleted(backup_piece_info.status_)) {
        ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
        LOG_WARN("piece can not be deleted", K(ret), K(backup_piece_info));
      } else if (OB_FAIL(piece_list.push_back(backup_piece_info))) {
        LOG_WARN("failed to push back piece list", K(ret));
      }
    }
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_delete_obsolete_backup_infos_(ObArray<ObBackupSetFileDesc> &set_list, ObArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  ObBackupSetFileDesc clog_data_clean_point;
  if (OB_FAIL(get_delete_obsolete_backup_set_infos_(clog_data_clean_point, set_list))) {
    LOG_WARN("failed to get delete obsolete backup set infos", K(ret));
  } else if (OB_FAIL(get_delete_obsolete_backup_piece_infos_(clog_data_clean_point, piece_list))) {
    LOG_WARN("failed to get delete obsolete backup piece infos", K(ret));
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_need_cleaned_backup_infos_(ObArray<ObBackupSetFileDesc> &set_list, ObArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS; 
  if (job_attr_->is_delete_backup_set()) {
    if (OB_FAIL(get_delete_backup_set_infos_(set_list))) {
      LOG_WARN("failed to get delete backup set infos", K(ret));
    }
  } else if (job_attr_->is_delete_backup_piece()) {
    if (OB_FAIL(get_delete_backup_piece_infos_(piece_list))) {
      LOG_WARN("failed to get delete backup piece infos", K(ret));
    }
  } else if (job_attr_->is_delete_obsolete_backup()) {
    if (OB_FAIL(get_delete_obsolete_backup_infos_(set_list, piece_list))) {
      LOG_WARN("failed to get delete obsolete backup infos", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup clean type is invalid", K(ret), K_(*job_attr));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("[BACKUP_CLEAN]success get need clean backup list", K_(*job_attr), K(set_list), K(piece_list)); 
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::persist_backup_clean_tasks_(
    common::ObISQLClient &trans,
    const ObArray<ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < set_list.count(); i++) {
    const ObBackupSetFileDesc &backup_set_info = set_list.at(i);
    ObBackupCleanTaskAttr task_attr;
    if (OB_FAIL(get_backup_clean_task_(backup_set_info, task_attr))) {
      LOG_WARN("failed to get backup set task", K(ret));
    } else if (!task_attr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup clean set task is valid", K(ret), K(task_attr), K(backup_set_info));
    } else if (OB_FAIL(backup_service_->check_leader())) {
      LOG_WARN("failed to check leader", K(ret));
    } else if (OB_FAIL(ObBackupCleanTaskOperator::insert_backup_clean_task(trans, task_attr))) {
      LOG_WARN("failed to insert backup task", K(ret), K(task_attr));
    } else {
      LOG_INFO("[BACKUP_CLEAN]success insert backup clean task", K(ret), K(task_attr), K(backup_set_info)); 
    }
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::persist_backup_piece_task_(
    common::ObISQLClient &trans,
    const ObArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < piece_list.count(); i++) {
    const ObTenantArchivePieceAttr &backup_piece_info = piece_list.at(i);
    ObBackupCleanTaskAttr task_attr;
    if (OB_FAIL(get_backup_piece_task_(backup_piece_info, task_attr))) {
      LOG_WARN("failed to insert backup piece task", K(ret));
    } else if (!task_attr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup clean set task is valid", K(ret));
    } else if (OB_FAIL(backup_service_->check_leader())) {
      LOG_WARN("failed to check leader", K(ret));
    } else if (OB_FAIL(ObBackupCleanTaskOperator::insert_backup_clean_task(trans, task_attr))) {
      LOG_WARN("failed to insert backup task", K(ret), K(task_attr));
    }
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::check_current_task_exist_(bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupCleanTaskOperator::check_current_task_exist(*sql_proxy_, job_attr_->tenant_id_, is_exist))) {
    LOG_WARN("failed to check current set task exist", K(ret), K(job_attr_->tenant_id_));
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::persist_backup_clean_task_()
{
  int ret = OB_SUCCESS;
  ObBackupCleanStatus next_status;
  ObMySQLTransaction trans;
  ObArray<ObBackupSetFileDesc> set_list;
  ObArray<ObTenantArchivePieceAttr> piece_list;
  bool is_exist = true;
  if (OB_FAIL(check_current_task_exist_(is_exist))) {
    LOG_WARN("failed to get tenant backup infos", K(ret));
  } else if (is_exist) {
    // do nithing
    FLOG_INFO("[BACKUP_CLEAN]task alrealdy exist", K_(job_attr));
  } else if (OB_FAIL(get_need_cleaned_backup_infos_(set_list, piece_list))) {
    LOG_WARN("failed to get tenant backup infos", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(persist_backup_clean_tasks_(trans, set_list))) {
      LOG_WARN("failed to persist backup set tasks", K(ret));
    } else if (OB_FAIL(persist_backup_piece_task_(trans, piece_list))) {
      LOG_WARN("failed to persist backup piece tasks", K(ret));
    } else if (FALSE_IT(next_status.status_ = ObBackupCleanStatus::Status::DOING)) {
    } else if (OB_FAIL(advance_job_status_(trans, next_status, job_attr_->result_, job_attr_->end_ts_))) {
      LOG_WARN("failed to advance status", K(ret));
    } else if (FALSE_IT(job_attr_->task_count_ = set_list.count() + piece_list.count())) {
    } else if (OB_FAIL(ObBackupCleanJobOperator::update_task_count(trans, *job_attr_, true/*is_total*/))) {
      LOG_WARN("failed to update job task count", K(ret)); 
    }
    DEBUG_SYNC(BACKUP_DELETE_STATUS_INIT); 
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      } else {
        backup_service_->wakeup();
      }   
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to roll back status", K(ret), K(tmp_ret));
      } 
    }
  }

  return ret;
}

int ObUserTenantBackupDeleteMgr::get_backup_clean_task_(const ObBackupSetFileDesc &backup_set_info, ObBackupCleanTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_attr.backup_path_.assign(backup_set_info.backup_path_))) {
    LOG_WARN("failed to assign backup dest", K(ret), K(*job_attr_));
  } else if (OB_FAIL(get_next_task_id_(task_attr.task_id_))) {
    LOG_WARN("failed to get next task id");
  } else {
    task_attr.tenant_id_ = backup_set_info.tenant_id_;
    task_attr.job_id_ = job_attr_-> job_id_;
    task_attr.incarnation_id_ = backup_set_info.incarnation_;
    task_attr.task_type_ = ObBackupCleanTaskType::TYPE::BACKUP_SET;
    task_attr.backup_set_id_ = backup_set_info.backup_set_id_;
    task_attr.dest_id_ = backup_set_info.dest_id_;
    task_attr.status_.status_ = ObBackupCleanStatus::Status::INIT;
    task_attr.result_ = OB_SUCCESS;
    const int64_t current_time = ObTimeUtility::current_time();
    task_attr.start_ts_ = current_time;
    task_attr.end_ts_ = 0;
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_backup_piece_task_(const ObTenantArchivePieceAttr &backup_piece_info, ObBackupCleanTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_attr.backup_path_.assign(backup_piece_info.path_))) {
    LOG_WARN("failed to assign backup dest", K(ret), K(*job_attr_));
  } else if (OB_FAIL(get_next_task_id_(task_attr.task_id_))) {
    LOG_WARN("failed to get next task id", K(backup_piece_info), K(task_attr));
  } else {
    task_attr.tenant_id_ = backup_piece_info.key_.tenant_id_;
    task_attr.job_id_ = job_attr_->job_id_;
    task_attr.incarnation_id_ = backup_piece_info.incarnation_;
    task_attr.task_type_ = ObBackupCleanTaskType::TYPE::BACKUP_PIECE;
    task_attr.backup_piece_id_ = backup_piece_info.key_.piece_id_;
    task_attr.round_id_ = backup_piece_info.key_.round_id_;
    task_attr.dest_id_ = backup_piece_info.key_.dest_id_;
    task_attr.status_.status_ = ObBackupCleanStatus::Status::INIT;
    task_attr.result_ = OB_SUCCESS;
    const int64_t current_time = ObTimeUtility::current_time();
    task_attr.start_ts_ = current_time;
    task_attr.end_ts_ = 0;
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_next_task_id_(int64_t &task_id)
{
  int ret = OB_SUCCESS;
  task_id = -1;
  if (OB_FAIL(ObLSBackupInfoOperator::get_next_task_id(*sql_proxy_, tenant_id_, task_id))) {
    LOG_WARN("failed to get next task id", K(ret));
  } else if (-1 == task_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task id", K(ret), K(task_id));
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::advance_job_status_(
    common::ObISQLClient &trans,
    const ObBackupCleanStatus &next_status, 
    const int result, 
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupCleanJobOperator::advance_job_status(trans, *job_attr_, next_status, result, end_ts))) {
    LOG_WARN("failed to advance job status", K(ret), K(*job_attr_), K(next_status), K(result), K(end_ts));
  }
  return ret;
}

//******************** ObSysTenantBackupDeleteMgr *******************
int ObSysTenantBackupDeleteMgr::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tenant backup delete scheduler not init", K(ret));
  } else {
    ObBackupCleanStatus::Status status = job_attr_->status_.status_;
    switch (status) {
      case ObBackupCleanStatus::Status::INIT: {
        if (OB_FAIL(handle_user_tenant_backup_delete_())) {
          LOG_WARN("fail to inser user tenant job");
        }
        break;
      } 
      case ObBackupCleanStatus::Status::DOING: {
        if (OB_FAIL(statistic_user_tenant_job_())) {
          LOG_WARN("failed to backup data", K(ret), K(*job_attr_));
        }
        break;
      }
      case ObBackupCleanStatus::Status::COMPLETED: 
      case ObBackupCleanStatus::Status::FAILED: 
      case ObBackupCleanStatus::Status::CANCELED: {
        if (OB_FAIL(move_to_history_())) {
          LOG_WARN("failed to move job to histroy", K(ret), K(*job_attr_));
        }
        break;
      }
      case ObBackupCleanStatus::Status::CANCELING: {
        if (OB_FAIL(cancel_user_tenant_job_())) {
          LOG_WARN("failed to cancel backup delete job", K(ret), K(*job_attr_));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown backup delete status", K(ret), K(*job_attr_));
      }
    }
  }
  return ret;
}

int ObSysTenantBackupDeleteMgr::handle_user_tenant_backup_delete_()
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  int64_t cnt = 0;
  ObBackupCleanJobAttr new_job_attr;
  for (int64_t i = 0; OB_SUCC(ret) && i < job_attr_->executor_tenant_id_.count(); ++i) {
    is_valid = false;
    cnt = 0;
    const uint64_t user_tenant_id = job_attr_->executor_tenant_id_.at(i);
    if (OB_FAIL(ObBackupCleanCommon::check_tenant_status(*schema_service_, user_tenant_id, is_valid))) {
      LOG_WARN("fail to check tenant status", K(ret), K(user_tenant_id));
    } else if (!is_valid) {
      LOG_WARN("tenant status not valid, this tenant shouldn't be backup", K(user_tenant_id), K(is_valid));
    } else if (OB_FAIL(ObBackupCleanJobOperator::cnt_jobs(*sql_proxy_, user_tenant_id, job_attr_->tenant_id_, job_attr_->job_id_, cnt))) {
      LOG_WARN("fail to cnt user backup delete job initiated by cur sys tenant job", K(ret), K(user_tenant_id));
    } else if (cnt != 0) {
      LOG_INFO("user tenant job has been inserted, just pass", K(user_tenant_id));
    } else if (OB_FAIL(do_handle_user_tenant_backup_delete_(user_tenant_id))) {
      if (OB_BACKUP_CAN_NOT_START == ret) { 
        LOG_WARN("tenant can't start backup now just pass", K(ret)); 
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to do insert user tenant job", K(ret), K(user_tenant_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObBackupCleanStatus next_status;
    next_status.status_ = ObBackupCleanStatus::Status::DOING;
    if (OB_FAIL(advance_status_(*sql_proxy_, next_status))) {
      LOG_WARN("fail to advance sys job status", K(ret), K(*job_attr_), K(next_status));
    } else {
      backup_service_->wakeup();
      LOG_INFO("succeed handle user tenant backup delete, advance sys job to DOING", K(*job_attr_), K(next_status));
    }
  }
  return ret;
}


int ObSysTenantBackupDeleteMgr::do_handle_user_tenant_backup_delete_(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObAddr rs_addr;
  obrpc::ObBackupCleanArg backup_delete_arg;
  ObAddr leader;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    backup_delete_arg.tenant_id_ = tenant_id;
    backup_delete_arg.initiator_tenant_id_ = job_attr_->tenant_id_;
    backup_delete_arg.initiator_job_id_ = job_attr_->job_id_;
    if (OB_FAIL(backup_delete_arg.description_.assign(job_attr_->description_))) {
      LOG_WARN("fail to assign backup description", K(ret));
    } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) { 
      ret = OB_ERR_SYS;
      LOG_WARN("rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("failed to get rootservice address", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).backup_delete(backup_delete_arg))) {
      LOG_WARN("backup clean rpc failed", K(ret), K(backup_delete_arg));
    } else {
      LOG_INFO("succeed handle user backup delete tenant", K(backup_delete_arg));
    }
  }
  return ret;
}


int ObSysTenantBackupDeleteMgr::statistic_user_tenant_job_()
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  int64_t cnt = 0;
  int64_t finish_user_backup_job  = 0;
  LOG_INFO("sys tenant start to statistic user tenant job", K(*job_attr_));
  for (int64_t i = 0; OB_SUCC(ret) && i < job_attr_->executor_tenant_id_.count(); ++i) {
    const uint64_t user_tenant_id = job_attr_->executor_tenant_id_.at(i);
    is_valid = false;
    cnt = 0;
    if (OB_FAIL(ObBackupCleanCommon::check_tenant_status(*schema_service_, user_tenant_id, is_valid))) {
      LOG_WARN("fail to check tenant status", K(ret), K(user_tenant_id));
    } else if (!is_valid) {
      finish_user_backup_job++;
      LOG_WARN("tenant status not valid, just pass the tenant", K(user_tenant_id), K(is_valid));
    } else if (OB_FAIL(ObBackupCleanJobOperator::cnt_jobs(*sql_proxy_, user_tenant_id, job_attr_->tenant_id_, job_attr_->job_id_, cnt))) {
      LOG_WARN("fail to cnt user backup delete job initiated by cur sys tenant job", K(ret), K(user_tenant_id));
    } else if (0 == cnt) {
      finish_user_backup_job++;
      LOG_INFO("user tenant backup delete job not finish, wait later", K(user_tenant_id));
    }
  }
  if (OB_SUCC(ret) && finish_user_backup_job == job_attr_->executor_tenant_id_.count()) {
    ObBackupCleanJobAttr tmp_job_attr;
    ObBackupCleanStatus next_status;
    // for double check. 
    if (OB_FAIL(ObBackupCleanJobOperator::get_job(*sql_proxy_, false/*no update*/, 
        job_attr_->tenant_id_, job_attr_->job_id_, false/*not initiator*/, tmp_job_attr))) {
      LOG_WARN("fail to get sys backup delete job", K(ret), "job_id", job_attr_->job_id_);
    } else if (tmp_job_attr.result_ != OB_SUCCESS) {
      next_status.status_ = ObBackupCleanStatus::Status::FAILED;
    } else {
      next_status.status_ = ObBackupCleanStatus::Status::COMPLETED;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(advance_status_(*sql_proxy_, next_status, tmp_job_attr.result_, ObTimeUtility::current_time()))) {
        LOG_WARN("fail to advance sys job status", K(ret), K(*job_attr_), K(next_status));
      } else {
        LOG_INFO("[BACKUP_CLEAN]user job finished, sys job move to next status", K(next_status), K(*job_attr_));
      }
    }
  }
  return ret;
}


int ObSysTenantBackupDeleteMgr::move_to_history_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("fail to check leader", K(ret));
  } else if (OB_FAIL(ObBackupCleanJobOperator::move_job_to_his(*sql_proxy_, job_attr_->tenant_id_, job_attr_->job_id_))) {
    LOG_WARN("failed to move job to history table", K(ret), K(*job_attr_));
  } else {
    LOG_INFO("sys tenant backup delete job succeed move to history table", K(*job_attr_));
  }
  return ret;
}

int ObSysTenantBackupDeleteMgr::cancel_user_tenant_job_()
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  int64_t cnt = 0;
  int64_t finish_canceled_job = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < job_attr_->executor_tenant_id_.count(); ++i) {
    const uint64_t user_tenant_id = job_attr_->executor_tenant_id_.at(i);
    is_valid = false;
    cnt = 0;
    ObBackupCleanJobAttr tmp_job_attr;
    if (OB_FAIL(ObBackupCleanCommon::check_tenant_status(*schema_service_, user_tenant_id, is_valid))) {
      LOG_WARN("fail to check tenant status", K(ret), K(user_tenant_id));
    } else if (!is_valid) {
      finish_canceled_job++;
      LOG_WARN("tenant status not valid, just pass the tenant", K(user_tenant_id), K(is_valid));
    } else if (OB_FAIL(ObBackupJobOperator::cnt_jobs(*sql_proxy_, user_tenant_id, job_attr_->tenant_id_, cnt))) {
      LOG_WARN("fail to cnt user backup delete job initiated by cur sys tenant job", K(ret), K(user_tenant_id));
    } else if (0 == cnt) {
      finish_canceled_job ++;
      LOG_INFO("user tenant backup delete job not finish, wait later", K(user_tenant_id));
    } else if (OB_FAIL(ObBackupCleanJobOperator::get_job(*sql_proxy_, false/*no update*/, 
        user_tenant_id, job_attr_->job_id_, true/**/, tmp_job_attr))) {
      LOG_WARN("fail to get backup delete job", K(ret), K(user_tenant_id), "initiator job id", job_attr_->job_id_);
    } else if (ObBackupCleanStatus::Status::INIT == tmp_job_attr.status_.status_
        || ObBackupCleanStatus::Status::DOING == tmp_job_attr.status_.status_) {
      ObBackupCleanStatus next_status;
      next_status.status_ = ObBackupCleanStatus::Status::CANCELING;
      if (OB_FAIL(backup_service_->check_leader())) {
        LOG_WARN("fail to check leader", K(ret));
      } else if (OB_FAIL(ObBackupCleanJobOperator::advance_job_status(*sql_proxy_, tmp_job_attr, next_status))) {
        LOG_WARN("fail to advance user job to CANCELING", K(ret), K(tmp_job_attr), K(next_status));
      } else {
        FLOG_INFO("succeed advance user tenant job to CANCELING", K(tmp_job_attr));
      }
    } 
  }

  if (OB_SUCC(ret) && finish_canceled_job == job_attr_->executor_tenant_id_.count()) {
    ObBackupCleanStatus next_status;
    next_status.status_ = ObBackupCleanStatus::Status::CANCELED;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(advance_status_(*sql_proxy_, next_status, job_attr_->result_, ObTimeUtility::current_time()))) {
        LOG_WARN("fail to advance sys job status", K(ret), K(*job_attr_), K(next_status));
      } else {
        FLOG_INFO("[BACKUP_CLEAN]succeed schedule sys backup delete job", K(*job_attr_));
      }
    }
  }
  return ret;
}

int ObSysTenantBackupDeleteMgr::advance_status_(
    common::ObISQLClient &sql_proxy, 
    const ObBackupCleanStatus &next_status, 
    const int result, 
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("fail to check leader", K(ret));
  } else if (OB_FAIL(ObBackupCleanJobOperator::advance_job_status(sql_proxy, *job_attr_, next_status, result, end_ts))) {
    LOG_WARN("failed to advance job status", K(ret), K(*job_attr_), K(next_status), K(result), K(end_ts));
  }
  return ret;
}

//******************ObBackupAutoObsoleteDeleteTrigger*********************
ObBackupAutoObsoleteDeleteTrigger::ObBackupAutoObsoleteDeleteTrigger()
 : ObIBackupTrigger(BackupTriggerType::BACKUP_AUTO_DELETE_TRIGGER),
   is_inited_(false),
   tenant_id_(OB_INVALID_TENANT_ID),
   sql_proxy_(nullptr),
   rpc_proxy_(nullptr),
   schema_service_(nullptr),
   task_scheduler_(nullptr),
   backup_service_(nullptr)
{
}

int ObBackupAutoObsoleteDeleteTrigger::init(
    const uint64_t tenant_id,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObBackupTaskScheduler &task_scheduler,
    ObBackupCleanService &backup_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    tenant_id_ = gen_user_tenant_id(tenant_id);
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    schema_service_ = &schema_service;
    task_scheduler_ = &task_scheduler;
    backup_service_ = &backup_service;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupAutoObsoleteDeleteTrigger::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean scheduler not init", K(ret));
  } else if (OB_FAIL(start_auto_delete_obsolete_data_())) {
    LOG_WARN("failed to start auto obsolete delete", K(ret));
  }

  return ret; 
}

int ObBackupAutoObsoleteDeleteTrigger::parse_time_interval_(const char *str, int64_t &val)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  val = 0;

  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(str));
  } else {
    val = ObConfigTimeParser::get(str, is_valid);
    if (!is_valid) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid time interval str", K(ret), K(str));
    }
  }
  return ret;
}

int ObBackupAutoObsoleteDeleteTrigger::get_delete_policy_parameter_(const ObDeletePolicyAttr &delete_policy, int64_t &recovery_window)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_time_interval_(delete_policy.recovery_window_, recovery_window))) {
    LOG_WARN("failed to parse recover window", K(ret), K(delete_policy));
  }
  return ret;
} 

int ObBackupAutoObsoleteDeleteTrigger::start_auto_delete_obsolete_data_()
{
  int ret = OB_SUCCESS;
  ObBackupCleanScheduler backup_clean_scheduler;
  common::ObSArray<uint64_t> tenant_ids;
  const int64_t now_ts = ObTimeUtil::current_time();
  ObDeletePolicyAttr default_delete_policy;
  int64_t recovery_window = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup auto delete obsolete bakcup do not init", K(ret));
  } else if (is_user_tenant(tenant_id_)) {
    recovery_window = 0;
    default_delete_policy.reset();
    obrpc::ObBackupCleanArg arg;
    arg.initiator_tenant_id_ = tenant_id_; // cluster-level automatic backup clean
    arg.tenant_id_ = tenant_id_;
    arg.type_ = ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP;
    if (OB_FAIL(ObDeletePolicyOperator::get_default_delete_policy(*sql_proxy_, arg.tenant_id_, default_delete_policy))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get all tenants", K(ret), K(arg));
      }
    } else if (OB_FAIL(get_delete_policy_parameter_(default_delete_policy, recovery_window))) {
    } else if (recovery_window <= 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("recovery window is unepxected", K(ret), K(arg), K(recovery_window));
    } else if (FALSE_IT(arg.value_ = now_ts - recovery_window)) {
    } else if (OB_FAIL(backup_service_->handle_backup_delete(arg))) {
      LOG_WARN("failed to schedule backup clean", K(ret), K(arg));
    }
    FLOG_INFO("[BACKUP_CLEAN] finish schedule auto delete", K(tenant_ids));
  }
  return ret;
}

//******************** ObBackupCleanCommon ********************
int ObBackupCleanCommon::check_tenant_status(
    share::schema::ObMultiVersionSchemaService &schema_service,
    const uint64_t tenant_id,
    bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_dropped = false;
  is_valid = false;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  if (OB_FAIL(schema_service.check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
    LOG_WARN("failed to check if tenant has been dropped", K(ret), K(tenant_id));
  } else if (is_dropped) {
    is_valid = false;
    LOG_WARN("tenant is dropped, can't not backup now", K(tenant_id));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    is_valid = false;
    LOG_WARN("tenant schema is null, tenant may has been dropped", K(ret), K(tenant_id));
  } else if (tenant_schema->is_normal()) {
    is_valid = true;
  } else if (tenant_schema->is_creating()) {
    is_valid = false;
    LOG_WARN("tenant is creating, can't not backup now", K(tenant_id));
  } else if (tenant_schema->is_restore()) {
    is_valid = false;
    LOG_WARN("tenant is doing restore, can't not backup now", K(tenant_id));
  } else if (tenant_schema->is_dropping()) {
    is_valid = false;
    LOG_WARN("tenant is dropping, can't not backup now", K(tenant_id));
  } else if (tenant_schema->is_in_recyclebin()) {
    is_valid = false;
    LOG_WARN("tenant is in recyclebin, can't not backup now", K(tenant_id));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown tenant status", K(tenant_id), K(tenant_schema));
  }
  return ret;
}

int ObBackupCleanCommon::get_all_tenants(
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObIArray<uint64_t> &tenants) 
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tmp_tenants;
  bool is_valid = false;
  if (OB_FAIL(ObTenantUtils::get_tenant_ids(&schema_service, tmp_tenants))) {
    LOG_WARN("failed to get all tenants id", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < tmp_tenants.count(); ++i ) {
      const uint64_t tenant_id = tmp_tenants.at(i);
      if (!is_user_tenant(tenant_id) || is_sys_tenant(tenant_id)) {
      } else if (OB_FAIL(check_tenant_status(schema_service, tenant_id, is_valid))) {
        LOG_WARN("failed to check can do backup clean", K(ret), K(tenant_id));
      } else if (!is_valid) {
        LOG_INFO("tenant status not valid, no need to backup clean", K(tenant_id));
      } else if (OB_FAIL(tenants.push_back(tenant_id))) {
        LOG_WARN("failed to push back tenant", K(ret));
      }
    }
  }
  return ret;
}

/*
 *----------------------ObBackupDeleteMgrAlloctor-------------------------
 */
int ObBackupDeleteMgrAlloctor::alloc(const uint64_t tenant_id, ObIBackupDeleteMgr *&job_mgr)
{
  int ret = OB_SUCCESS;
  if (is_meta_tenant(tenant_id)) {
    job_mgr = OB_NEW(ObUserTenantBackupDeleteMgr, "UserDeleteMgr");
  } else if (is_sys_tenant(tenant_id)) {
    job_mgr = OB_NEW(ObSysTenantBackupDeleteMgr, "SysDeleteMgr");
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id, only meta tenant and sys tenant can alloc backup delete job mgr", K(tenant_id));
  }
  return ret;
}

void ObBackupDeleteMgrAlloctor::free(ObIBackupDeleteMgr *job_mgr)
{
  ob_delete(job_mgr); 
}

} // namespace oceanbase
} // namespace rootserver
