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
#include "ob_backup_clean_selector.h"
#include "ob_backup_clean_ls_task_mgr.h"
#include "ob_backup_clean_task_mgr.h"
#include "ob_backup_task_scheduler.h"
#include "share/backup/ob_backup_clean_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_helper.h"
#include "share/backup/ob_backup_clean_util.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_connectivity.h"
#include "storage/tx/ob_ts_mgr.h"
#include "rootserver/ob_root_utils.h"

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
    schema::ObMultiVersionSchemaService &schema_service,
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
  } else if (OB_FAIL(databuff_printf(policy_attr.recovery_window_,
                      sizeof(policy_attr.recovery_window_), "%s", in_arg.recovery_window_))) {
    LOG_WARN("fail to assign recovery_window_", K(ret));
  } else {
    policy_attr.tenant_id_ = in_arg.initiator_tenant_id_;
    policy_attr.redundancy_ = in_arg.redundancy_;
    policy_attr.backup_copies_ = in_arg.backup_copies_;
  }
  return ret;
}

int ObBackupCleanScheduler::add_delete_policy_(const ObDeletePolicyAttr &policy_attr)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  bool policy_exists = false;
  bool backup_set_dest_exists = false;
  if (!policy_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(policy_attr));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(policy_attr.tenant_id_)))) {
    LOG_WARN("failed to start transaction", K(ret), "tenant_id", policy_attr.tenant_id_);
  } else if (OB_FAIL(ObBackupCleanUtil::lock_policy_table_then_check(
                        trans, policy_attr.tenant_id_, policy_exists, false/*log_only*/))) {
    LOG_WARN("failed to lock and check policy exists", K(ret));
  } else if (policy_exists) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("policy already exists, cannot insert duplicate", K(ret), K(policy_attr));
    LOG_USER_ERROR(OB_ERR_POLICY_EXIST, "only one backup clean policy can be set for each tenant");
  } else {
    if (0 == strcmp(policy_attr.policy_name_, OB_STR_BACKUP_CLEAN_POLICY_NAME_DEFAULT)) {
      if (OB_FAIL(ObDeletePolicyOperator::insert_delete_policy(trans, policy_attr))) {
        LOG_WARN("failed to insert delete policy", K(ret), K(policy_attr));
      }
    } else if (0 == strcmp(policy_attr.policy_name_, OB_STR_BACKUP_CLEAN_POLICY_NAME_LOG_ONLY)) {
      // if policy name is "log_only", we expect backup dest table is empty
      if (OB_FAIL(backup::ObBackupUtils::check_tenant_backup_dest_exists(
                      policy_attr.tenant_id_, backup_set_dest_exists, *sql_proxy_))) {
        LOG_WARN("failed to check backup set dest exists", K(ret));
      } else if (backup_set_dest_exists) {
        ret = OB_LOG_ONLY_POLICY_NOT_ALLOWED_TO_SET;
        LOG_WARN("backup dest already exists, cannot set log_only policy", K(ret), K(policy_attr));
      } else {
        if (OB_FAIL(ObDeletePolicyOperator::insert_delete_policy(trans, policy_attr))) {
          LOG_WARN("failed to insert delete policy", K(ret), K(policy_attr));
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid policy name", K(ret), K(policy_attr));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true))) {
      LOG_WARN("failed to commit transaction", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("failed to rollback transaction", K(tmp_ret));
    }
  }
  return ret;
}

int ObBackupCleanScheduler::drop_delete_policy_(const ObDeletePolicyAttr &policy_attr)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;

  if (!policy_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(policy_attr));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(policy_attr.tenant_id_)))) {
    LOG_WARN("failed to start transaction", K(ret), "tenant_id", policy_attr.tenant_id_);
  } else {
    bool exists = false;
    // lock policy table and check if policy exists
    if (OB_FAIL(ObBackupCleanUtil::lock_policy_table_then_check(trans, policy_attr.tenant_id_, exists, false/*log_only*/))) {
      LOG_WARN("failed to lock and check policy exists", K(ret));
    } else if (exists) {
      if (OB_FAIL(ObDeletePolicyOperator::drop_delete_policy(trans, policy_attr))) {
        LOG_WARN("failed to drop delete policy", K(ret), K(policy_attr));
      }
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("policy does not exist, cannot drop", K(ret), K(policy_attr));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupCleanScheduler::add_delete_policy(const obrpc::ObDeletePolicyArg &in_arg)
{
  int ret = OB_SUCCESS;
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
    } else if (OB_FAIL(add_delete_policy_(policy_attr))) {
      LOG_WARN("failed to add delete policy with transaction", K(ret), K(policy_attr));
    }
  } else {
    if (!in_arg.clean_tenant_ids_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant backup clean should not initiate another tenant", K(ret), K(in_arg));
    } else if (OB_FAIL(add_delete_policy_(policy_attr))) {
      LOG_WARN("failed to add delete policy with transaction", K(ret), K(policy_attr));
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
    } else if (OB_FAIL(drop_delete_policy_(policy_attr))) {
      LOG_WARN("failed to drop delete policy with transaction", K(ret), K(policy_attr));
    }
  } else {
    if (!in_arg.clean_tenant_ids_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant backup clean should not initiate another tenant", K(ret), K(in_arg));
    } else if (OB_FAIL(drop_delete_policy_(policy_attr))) {
      LOG_WARN("failed to drop delete policy with transaction", K(ret), K(policy_attr));
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
  ObBackupCleanLSTask tmp_task;
  if (OB_FAIL(tmp_task.build(task_attr, ls_task))) {
    LOG_WARN("failed to build task", K(ret), K(task_attr), K(ls_task));
  } else if (OB_FAIL(tmp_task.clone(allocator, task))) {
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
  } else if (is_sys_tenant(in_arg.tenant_id_)) {
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

// turn the rpc::ObBackupCleanArg to ObBackupCleanJobAttr
int ObBackupCleanScheduler::fill_template_job_(const obrpc::ObBackupCleanArg &in_arg, ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  if (!in_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_arg));
  } else if (OB_FAIL(job_attr.description_.assign(in_arg.description_))) {
    LOG_WARN("failed to assign description", K(in_arg.description_));
  } else if (OB_FAIL(job_attr.executor_tenant_id_.assign(in_arg.clean_tenant_ids_))) {
    LOG_WARN("failed to assgin backup tenant id", K(ret));
  } else {
    const int64_t current_time = ObTimeUtility::current_time();
    job_attr.tenant_id_ = in_arg.tenant_id_;
    job_attr.incarnation_id_ = 1;
    job_attr.initiator_tenant_id_ = in_arg.initiator_tenant_id_;
    job_attr.initiator_job_id_ = in_arg.initiator_job_id_;
    job_attr.clean_type_ = in_arg.type_; 
    job_attr.dest_id_ = in_arg.dest_id_;
    job_attr.status_.status_ = ObBackupCleanStatus::Status::INIT;
    job_attr.start_ts_ = current_time;
    job_attr.end_ts_ = 0;
    job_attr.result_ = OB_SUCCESS;
    job_attr.backup_path_type_ = in_arg.dest_type_;
    if (is_sys_tenant(in_arg.initiator_tenant_id_)) {
      job_attr.job_level_.level_ = in_arg.clean_tenant_ids_.empty() ? ObBackupLevel::Level::CLUSTER : ObBackupLevel::Level::SYS_TENANT;
    } else {
      job_attr.job_level_.level_ = ObBackupLevel::Level::USER_TENANT;
    }
    if (OB_FAIL(job_attr.backup_path_.assign(in_arg.dest_path_))) {
      LOG_WARN("failed to assign backup path", K(ret), "dest_path", in_arg.dest_path_);
    } else {
      ObArray<int64_t> value_array;
      if (OB_FAIL(in_arg.get_value_array(value_array))) {
        LOG_WARN("failed to get value array", K(ret), K(in_arg));
      } else if (OB_FAIL(job_attr.set_clean_parameter(value_array))) {
        LOG_WARN("failed to set clean parameter", K(ret), K(value_array));
      }
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
        ret = OB_BACKUP_CLEAN_CAN_NOT_START;
        LOG_WARN("tenant status are not valid, can't start backup clean", K(ret), K(tenant_id));
      }
    }
  } else if (OB_FAIL(ObBackupCleanCommon::get_all_tenants(*schema_service_, job_attr.executor_tenant_id_))) {
    LOG_WARN("failed to get all tenants", K(ret));
  } else if (job_attr.executor_tenant_id_.empty()) {
    ret = OB_BACKUP_CLEAN_CAN_NOT_START;
    LOG_WARN("can't start cluster backup clean with no tenant", K(ret));
    LOG_USER_ERROR(OB_BACKUP_CLEAN_CAN_NOT_START, "no tenant need backup clean");
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

// do some basic check before generate backup clean job
int ObBackupCleanScheduler::backup_clean_pre_checker_(ObBackupCleanJobAttr &job_attr, const bool is_sys_tenant)
{
  int ret = OB_SUCCESS;
  if (ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP == job_attr.clean_type_) {
    if (is_sys_tenant) {
      for (int64_t i = 0; OB_SUCC(ret) && i < job_attr.executor_tenant_id_.count(); ++i) {
        const uint64_t tenant_id = job_attr.executor_tenant_id_.at(i);
        ObDeletePolicyAttr delete_policy;
        if (OB_FAIL(ObDeletePolicyOperator::get_delete_policy(*sql_proxy_, tenant_id, delete_policy))) {
          LOG_WARN("failed to get delete policy for user tenant", K(ret), K(tenant_id));
          LOG_USER_ERROR(OB_BACKUP_CLEAN_CAN_NOT_START, "no delete policy for some user tenant");
        }
      }
      SCN gts;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(OB_TS_MGR.get_gts(OB_SYS_TENANT_ID, nullptr, gts))) {
        LOG_WARN("failed to get gts for sys tenant", K(ret));
      } else if (!gts.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("gts is invalid", K(ret), K(gts));
      } else {
        job_attr.expired_time_ = gts.convert_to_ts();
        LOG_INFO("[BACKUP_CLEAN] sys tenant set gts time for all tenants", "expired_time", job_attr.expired_time_);
      }
    }
  } else if (ObNewBackupCleanType::DELETE_BACKUP_ALL == job_attr.clean_type_) {
    // check if target user tenant has ever use this dest
    bool is_exist = false;
    ObBackupDestType::TYPE dest_type = ObBackupDestType::TYPE::DEST_TYPE_MAX;
    LOG_INFO("system tenant check if target user tenant has ever use this dest", K(job_attr));
    ObBackupDest backup_dest;
    if (OB_FAIL(backup_dest.set(job_attr.backup_path_))) {
      LOG_WARN("failed to set backup dest", K(ret), K(job_attr));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_type(*sql_proxy_,
          is_sys_tenant ? job_attr.executor_tenant_id_.at(0) : job_attr.tenant_id_, backup_dest, dest_type))) {
      LOG_WARN("failed to get dest type", K(ret), K(job_attr));
    } else if (job_attr.backup_path_type_ != dest_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("delete all backup can not use data backup dest", K(ret), K(job_attr));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "mismatched dest type");
    }
    LOG_INFO("[BACKUP_CLEAN] backup clean pre checker end", K(ret), K(job_attr));
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
  } else if (OB_FAIL(backup_clean_pre_checker_(new_job_attr, true/*is_sys_tenant*/))) {
    LOG_WARN("failed to check backup clean pre checker", K(ret), K(new_job_attr));
  } else if (OB_FAIL(persist_job_task_(new_job_attr))) {
    LOG_WARN("failed to insert job", K(ret), K(new_job_attr));
  }
  FLOG_INFO("[BACKUP_CLEAN] finish start sys tenant backup clean", K(ret), K(job_attr));
  return ret;
}

int ObBackupCleanScheduler::set_tenant_obsolete_parameter_(ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObDeletePolicyAttr delete_policy;
  int64_t recovery_window = 0;
  int64_t current_gts_time = 0;
  SCN gts;

  if (job_attr.expired_time_ > 0) {  // the job is initiated by sys tenant, current gts time is already seted
    current_gts_time = job_attr.expired_time_;
    LOG_INFO("[BACKUP_CLEAN] use system tenant gts time", K(current_gts_time), "tenant_id", job_attr.tenant_id_);
  } else {
    if (OB_FAIL(OB_TS_MGR.get_gts(job_attr.tenant_id_, nullptr, gts))) {
      LOG_WARN("failed to get gts", K(ret));
    } else if (!gts.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gts is invalid", K(ret), K(gts));
    } else {
      current_gts_time = gts.convert_to_ts();
      LOG_INFO("[BACKUP_CLEAN] use tenant gts time", K(current_gts_time), "tenant_id", job_attr.tenant_id_);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDeletePolicyOperator::get_delete_policy(*sql_proxy_, job_attr.tenant_id_, delete_policy))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get delete policy", K(ret), K(job_attr));
    } else {
      LOG_WARN("no delete policy", K(ret), K(job_attr));
    }
  } else if (OB_FAIL(get_delete_policy_parameter_(delete_policy, recovery_window))) {
    LOG_WARN("failed to get delete policy parameter", K(ret), K(delete_policy));
  } else if (recovery_window <= 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("recovery window is unexpected", K(ret), K(job_attr), K(recovery_window));
  } else if (OB_FALSE_IT(job_attr.expired_time_ = current_gts_time - recovery_window)) {
  } else if (0 == strcmp(delete_policy.policy_name_, OB_STR_BACKUP_CLEAN_POLICY_NAME_LOG_ONLY)) {
    job_attr.backup_path_type_ = ObBackupDestType::DEST_TYPE_ARCHIVE_LOG;
  } else if (0 == strcmp(delete_policy.policy_name_, OB_STR_BACKUP_CLEAN_POLICY_NAME_DEFAULT)) {
    // for default policy, we delete data backup dest and log archive dest, but just use type DATA_BACKUP_DEST
    job_attr.backup_path_type_ = ObBackupDestType::DEST_TYPE_BACKUP_DATA;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported delete policy type", K(ret), K(delete_policy));
  }
  LOG_INFO("[BACKUP_CLEAN] finish set tenant obsolete parameter", K(ret), K(job_attr));
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
  } else if (OB_FAIL(backup_clean_pre_checker_(new_job_attr, false/*is_sys_tenant*/))) {
    LOG_WARN("failed to check backup clean pre checker", K(ret), K(new_job_attr));
  } else if (ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP == job_attr.clean_type_) {
    // read policy, then set the delete obsolete backup type (default or log_only), and set expired time
    if (OB_FAIL(set_tenant_obsolete_parameter_(new_job_attr))) {
      LOG_WARN("failed to set tenant obsolete parameter", K(ret), K(job_attr));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(new_job_attr.tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret), K(new_job_attr));
  } else {
    if (OB_FAIL(get_next_job_id_(trans, new_job_attr.tenant_id_, new_job_attr.job_id_))) {
      LOG_WARN("failed to get next job id", K(ret));
    } else if (OB_FAIL(new_job_attr.executor_tenant_id_.push_back(new_job_attr.tenant_id_))) {
      LOG_WARN("failed to push back tenant id", K(ret));
    } else if (OB_FALSE_IT(new_job_attr.initiator_job_id_ = new_job_attr.tenant_id_ == new_job_attr.initiator_tenant_id_ ?
          0/*no parent job*/ : new_job_attr.initiator_job_id_)) {
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
  LOG_INFO("[BACKUP_CLEAN] finish start tenant backup clean", K(ret), K(new_job_attr));
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
    const ObHAResultInfo &result_info,
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
    ObLSID ls_id(task->get_ls_id());
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

int ObBackupCleanScheduler::get_delete_policy_parameter_(const ObDeletePolicyAttr &delete_policy, int64_t &recovery_window)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupCleanUtil::parse_time_interval(delete_policy.recovery_window_, recovery_window))) {
    LOG_WARN("failed to parse recover window", K(ret), K(delete_policy));
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
      if (OB_SUCCESS != (tmp_ret = handle_failed_job_(tenant_id, tmp_ret, *job_mgr, job_attr))) {
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
    schema::ObMultiVersionSchemaService &schema_service,
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
    } else if (OB_FAIL(ObBackupCleanJobOperator::update_comment(*sql_proxy_, *job_attr_))) {
      LOG_WARN("failed to update comment", K(ret));
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
  ObBackupHelper backup_helper;
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
  } else if (OB_FAIL(dest_mgr.init(job_attr_->tenant_id_, ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA, backup_dest_str, *sql_proxy_))) {
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
  ObArchivePersistHelper helper;
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
        if (OB_FAIL(persist_backup_clean_task_())) {
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
        if (OB_OBJECT_STORAGE_OBJECT_LOCKED_BY_WORM == task_attr.result_) {
          // do nothing
          // The clean task failed due to the worm.
          // The failure of a single task does not affect the execution of other tasks.
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set task status can not be failed", K(ret), K(task_attr));
        }
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
  bool has_failed_by_worm = false;
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
      if (OB_OBJECT_STORAGE_OBJECT_LOCKED_BY_WORM == task_attr.result_) {
        // Unlike other failures, a failure due to the worm does not affect the execution of other tasks.
        // However, when all tasks are completed or failed due to worm,
        // the job should be marked as failed, with the error code recorded as OB_OBJECT_STORAGE_OBJECT_LOCKED_BY_WORM.
        has_failed_by_worm = true;
      } else {
        result = task_attr.result_;
        has_failed = true;
      }
    } else if (ObBackupCleanStatus::Status::COMPLETED == task_attr.status_.status_) {
      success_task_count++; 
    }
  }
  job_attr_->success_task_count_ = success_task_count;
  if (OB_FAIL(ret)) {
  } else if (has_task) {
    if (!has_failed) {
      if (OB_FAIL(do_backup_clean_tasks_(task_attrs))) {
        LOG_WARN("failed to do set tasks", K(ret));
      }
    } else {
      if (OB_FAIL(do_cancel_())) {
        LOG_WARN("failed to cancel task", K(ret));
      }
    }
  } else {
    if (has_failed) {
      next_status.status_ = ObBackupCleanStatus::Status::FAILED;
    } else if (has_failed_by_worm) {
      result = OB_OBJECT_STORAGE_OBJECT_LOCKED_BY_WORM;
      next_status.status_ = ObBackupCleanStatus::Status::FAILED;
    } else {
      next_status.status_ = ObBackupCleanStatus::Status::COMPLETED;
    }
  }

  FLOG_INFO("[BACKUP_CLEAN]handle backup clean task", K(ret),
      K(*job_attr_), K(next_status.status_), K(has_failed), K(has_task));
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
    // If the backup all's all tasks are completed, delete the files of the upper level.
    if (job_attr_->is_delete_backup_all() && ObBackupCleanStatus::Status::COMPLETED == next_status.status_) {
      if (OB_FAIL(delete_backup_all_meta_info_files_())) {
        LOG_WARN("failed to delete backup all meta info files", K(ret));
      }
    }
    int64_t end_ts = ObTimeUtility::current_time(); 
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(advance_job_status_(*sql_proxy_, next_status, result, end_ts))) {
      LOG_WARN("failed to advance status", K(ret));
    } else if (OB_FAIL(ObBackupCleanJobOperator::update_task_count(*sql_proxy_, *job_attr_, false/*is_total*/))) {
      LOG_WARN("failed to update job task count", K(ret)); 
    } else {
      backup_service_->wakeup();
    }
  }

  return ret;
}

int ObUserTenantBackupDeleteMgr::get_delete_backup_set_infos_(ObIArray<ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  ObBackupDeleteSelector selector;
  if (OB_FAIL(selector.init(*sql_proxy_, *schema_service_, *job_attr_, *rpc_proxy_, *this))) {
    LOG_WARN("failed to init selector", K(ret));
  } else if (OB_FAIL(selector.get_delete_backup_set_infos(set_list))) {
    LOG_WARN("failed to get delete backup set infos", K(ret));
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_delete_backup_piece_infos_(ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  ObBackupDeleteSelector selector;
  if (OB_FAIL(selector.init(*sql_proxy_, *schema_service_, *job_attr_, *rpc_proxy_, *this))) {
    LOG_WARN("failed to init selector", K(ret));
  } else if (OB_FAIL(selector.get_delete_backup_piece_infos(piece_list))) {
    LOG_WARN("failed to get delete archivelog piece infos", K(ret));
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_delete_backup_all_infos_(ObIArray<ObBackupSetFileDesc> &set_list,
                                                  ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  ObBackupDeleteSelector selector;
  if (OB_FAIL(selector.init(*sql_proxy_, *schema_service_, *job_attr_, *rpc_proxy_, *this))) {
    LOG_WARN("failed to init selector", K(ret));
  } else if (OB_FAIL(selector.get_delete_backup_all_infos(set_list, piece_list))) {
    LOG_WARN("failed to get delete backup all infos", K(ret));
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::get_delete_obsolete_backup_infos_(ObArray<ObBackupSetFileDesc> &set_list, ObArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  ObBackupDeleteSelector selector;
  if (OB_FAIL(selector.init(*sql_proxy_, *schema_service_, *job_attr_, *rpc_proxy_, *this))) {
    LOG_WARN("failed to init selector", K(ret));
  } else if (OB_FAIL(selector.get_delete_obsolete_infos(set_list, piece_list))) {
    LOG_WARN("failed to get delete obsolete backup infos", K(ret));
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
  } else if (job_attr_->is_delete_backup_all()) {
    if (OB_FAIL(get_delete_backup_all_infos_(set_list, piece_list))) {
      LOG_WARN("failed to get delete backup all infos", K(ret));
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
    LOG_WARN("failed to get tenant backup infos", K(ret));   // Check here to prevent concurrent execution
  } else if (is_exist) {
    // do nothing
    FLOG_INFO("[BACKUP_CLEAN]task already exist", K_(job_attr));
  } else if (OB_FAIL(get_need_cleaned_backup_infos_(set_list, piece_list))) {
    LOG_WARN("failed to get tenant backup infos", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(persist_backup_clean_tasks_(trans, set_list))) {
      LOG_WARN("failed to persist backup set tasks", K(ret));
    } else if (OB_FAIL(persist_backup_piece_task_(trans, piece_list))) {
      LOG_WARN("failed to persist archivelog piece tasks", K(ret));
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

int ObUserTenantBackupDeleteMgr::delete_backup_all_meta_info_files_()
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  if (OB_FAIL(backup_dest.set(job_attr_->backup_path_))) {
    LOG_WARN("failed to init backup dest", K(ret), "path", job_attr_->backup_path_);
  } else {
    if (ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA == job_attr_->backup_path_type_) {
      if (OB_FAIL(delete_backup_dest_meta_info_files_(backup_dest))) {
        LOG_WARN("failed to delete backup dest meta info files", K(ret), "dest", backup_dest);
      }
    } else if (ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG == job_attr_->backup_path_type_) {
      if (OB_FAIL(delete_archive_dest_meta_info_files_(backup_dest))) {
        LOG_WARN("failed to delete archive dest meta info files", K(ret), "dest", backup_dest);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported dest_type for meta info cleanup", K(ret), "dest_type", job_attr_->backup_path_type_);
    }
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::delete_backup_dest_meta_info_files_(ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  const ObBackupStorageInfo *storage_info = backup_dest.get_storage_info();
  ObBackupPath format_path;
  ObBackupStore backup_store;
  ObBackupPathString format_path_str;
  ObBackupPath backup_sets_path;
  ObBackupPath check_file_path;
  ObBackupPath backup_set_path;
  ObBackupCheckFile check_file;

  if (OB_FAIL(backup_store.init(backup_dest))) {
    LOG_WARN("failed to init backup store", K(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_sets_dir_path(backup_dest, backup_sets_path))) {
    LOG_WARN("failed to get backup sets dir path", K(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(backup_sets_path, storage_info))) {
    LOG_WARN("failed to delete backup sets dir files", K(ret), K(backup_sets_path));
  } else if (OB_FAIL(check_file.init(job_attr_->tenant_id_, *sql_proxy_))) {
    LOG_WARN("failed to init check file", K(ret), K(job_attr_->tenant_id_));
  } else if (OB_FAIL(check_file.get_check_file_path(backup_dest, check_file_path))) {
    LOG_WARN("failed to get check file dir path", K(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(check_file_path, storage_info))) {
    LOG_WARN("failed to delete check file dir files", K(ret), K(check_file_path));
  } else if (OB_FAIL(backup_store.get_format_file_path(format_path_str))) {
    LOG_WARN("failed to get format file path", K(ret), K(backup_dest));
  } else if (OB_FAIL(format_path.init(format_path_str.ptr()))) {
    LOG_WARN("failed to init format path", K(ret), K(format_path_str));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_file(format_path, storage_info))) {
    LOG_WARN("failed to delete format file", K(ret), K(format_path));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_set_dir_path(backup_dest, backup_set_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(backup_set_path, storage_info))) {
    LOG_WARN("failed to delete backup set dir files", K(ret), K(backup_set_path));
  } else {
    LOG_INFO("delete all backup dest files finished", K(ret), K(backup_set_path), "tenant_id", job_attr_->tenant_id_);
  }
  return ret;
}

int ObUserTenantBackupDeleteMgr::delete_archive_dest_meta_info_files_(ObBackupDest &log_archive_dest)
{
  int ret = OB_SUCCESS;
  const ObBackupStorageInfo *storage_info = log_archive_dest.get_storage_info();
  ObBackupPath format_path;
  ObBackupStore backup_store;
  ObBackupPathString format_path_str;
  ObBackupPath rounds_path;
  ObBackupPath pieces_path;
  ObBackupPath check_file_path;
  ObBackupPath log_archive_dest_path;
  ObBackupCheckFile check_file;

  if (OB_FAIL(backup_store.init(log_archive_dest))) {
    LOG_WARN("failed to init backup store", K(ret), K(log_archive_dest));
  } else if (OB_FAIL(ObArchivePathUtil::get_rounds_dir_path(log_archive_dest, rounds_path))) {
    LOG_WARN("failed to get rounds dir path", K(ret), K(log_archive_dest));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(rounds_path, storage_info))) {
    LOG_WARN("failed to delete rounds dir files", K(ret), K(rounds_path));
  } else if (OB_FAIL(ObArchivePathUtil::get_pieces_dir_path(log_archive_dest, pieces_path))) {
    LOG_WARN("failed to get pieces dir path", K(ret), K(log_archive_dest));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(pieces_path, storage_info))) {
    LOG_WARN("failed to delete pieces dir files", K(ret), K(pieces_path));
  } else if (OB_FAIL(check_file.init(job_attr_->tenant_id_, *sql_proxy_))) {
    LOG_WARN("failed to init check file", K(ret), K(job_attr_->tenant_id_));
  } else if (OB_FAIL(check_file.get_check_file_path(log_archive_dest, check_file_path))) {
    LOG_WARN("failed to get check file dir path", K(ret), K(log_archive_dest));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(check_file_path, storage_info))) {
    LOG_WARN("failed to delete check file dir files", K(ret), K(check_file_path));
  } else if (OB_FAIL(backup_store.get_format_file_path(format_path_str))) {
    LOG_WARN("failed to get format file path", K(ret), K(log_archive_dest));
  } else if (OB_FAIL(format_path.init(format_path_str.ptr()))) {
    LOG_INFO("failed to init format path", K(ret), K(format_path_str));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_file(format_path, storage_info))) {
    LOG_WARN("failed to delete format file", K(ret), K(format_path));
  } else if (OB_FAIL(log_archive_dest_path.init(log_archive_dest.get_root_path()))) {
    LOG_WARN("failed to init log archive dest path", K(ret), K(log_archive_dest));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(log_archive_dest_path, storage_info))) {
    LOG_WARN("failed to delete log archive dest dir files", K(ret), K(log_archive_dest_path));
  } else {
    LOG_INFO("delete all archive dest files finished", K(ret), K(log_archive_dest_path), "tenant_id", job_attr_->tenant_id_);
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
  bool can_start = true;
  for (int64_t i = 0; OB_SUCC(ret) && can_start && i < job_attr_->executor_tenant_id_.count(); ++i) {
    const uint64_t user_tenant_id = job_attr_->executor_tenant_id_.at(i);
    bool is_valid = false;
    if (OB_FAIL(ObBackupCleanCommon::check_tenant_status(*schema_service_, user_tenant_id, is_valid))) {
      LOG_WARN("fail to check tenant status", K(ret), K(user_tenant_id));
    } else if (!is_valid) {
      LOG_INFO("tenant has been dropped or invalid, skip this tenant", K(user_tenant_id));
    } else if (OB_FAIL(ObBackupCleanJobOperator::cnt_jobs_of_user_tenant(*sql_proxy_, user_tenant_id, cnt))) {
      // TODO(yuhan): bad case, if tenant is always has clean job, sys tenant job will never be started
      LOG_WARN("fail to cnt user tenant backup delete job", K(ret), K(user_tenant_id));
    } else if (cnt != 0) {
      can_start = false;
      LOG_INFO("user tenant has clean job in progress, can't clean now", K(user_tenant_id));
    }
  }
  if (OB_SUCC(ret) && can_start) {
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
    backup_delete_arg.type_ = job_attr_->clean_type_;
    backup_delete_arg.dest_type_ = job_attr_->backup_path_type_;
    ObSArray<int64_t> value_array;
    if (ObNewBackupCleanType::DELETE_BACKUP_SET == job_attr_->clean_type_) {
      if (OB_FAIL(value_array.assign(job_attr_->backup_set_ids_))) {
        LOG_WARN("fail to assign backup set ids", K(ret));
      }
    } else if (ObNewBackupCleanType::DELETE_BACKUP_PIECE == job_attr_->clean_type_)  {
      if (OB_FAIL(value_array.assign(job_attr_->backup_piece_ids_))) {
        LOG_WARN("fail to assign backup piece ids", K(ret));
      }
    } else if (ObNewBackupCleanType::DELETE_BACKUP_ALL == job_attr_->clean_type_) {
      if (OB_FAIL(value_array.push_back(job_attr_->dest_id_))) {
        LOG_WARN("fail to push dest id", K(ret));
      }
    } else if (ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP == job_attr_->clean_type_) {
      // set the expired time into value_array
      if (OB_FAIL(value_array.push_back(job_attr_->expired_time_))) {
        LOG_WARN("fail to push expired time", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid clean type", K(ret), "clean_type", job_attr_->clean_type_);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("fail to set value array", K(ret));
    } else if (OB_FAIL(backup_delete_arg.set_value_array(value_array))) {
      LOG_WARN("fail to assign value array", K(ret));
    } else if (OB_FAIL(backup_delete_arg.description_.assign(job_attr_->description_))) {
      LOG_WARN("fail to assign backup description", K(ret));
    } else if (OB_FAIL(backup_delete_arg.dest_path_.assign(job_attr_->backup_path_))) {
      LOG_WARN("fail to assign dest path", K(ret));
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
    } else if (OB_FAIL(ObBackupCleanJobOperator::get_job(*sql_proxy_, false/*no update*/, 
        user_tenant_id, job_attr_->job_id_, true/**/, tmp_job_attr))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        finish_canceled_job++;
        ret = OB_SUCCESS;
        LOG_INFO("tenant backup job has finished", K(user_tenant_id), KPC(job_attr_));
      } else {
        LOG_WARN("fail to get backup delete job", K(ret), K(user_tenant_id), "initiator job id", job_attr_->job_id_);
      }
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
    schema::ObMultiVersionSchemaService &schema_service,
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

int ObBackupAutoObsoleteDeleteTrigger::start_auto_delete_obsolete_data_()
{
  int ret = OB_SUCCESS;
  ObBackupCleanScheduler backup_clean_scheduler;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup auto delete obsolete bakcup do not init", K(ret));
  } else if (is_user_tenant(tenant_id_)) {
    obrpc::ObBackupCleanArg arg;
    arg.initiator_tenant_id_ = tenant_id_; // cluster-level automatic backup clean
    arg.tenant_id_ = tenant_id_;
    arg.type_ = ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP;
    if (OB_FAIL(backup_service_->handle_backup_delete(arg))) {
      LOG_WARN("failed to schedule backup clean", K(ret), K(arg));
    }
    FLOG_INFO("[BACKUP_CLEAN] finish schedule auto delete", K(tenant_id_));
  }
  return ret;
}


//******************** ObBackupCleanCommon ********************
int ObBackupCleanCommon::check_tenant_status(
    schema::ObMultiVersionSchemaService &schema_service,
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
    schema::ObMultiVersionSchemaService &schema_service,
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
