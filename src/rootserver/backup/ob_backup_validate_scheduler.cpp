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
#include "ob_backup_validate_scheduler.h"
#include "ob_backup_validate_task_mgr.h"
#include "ob_backup_task_scheduler.h"
#include "share/backup/ob_backup_validate_table_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_helper.h"
#include "share/backup/ob_backup_store.h"
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_archive_path.h"
#include "storage/backup/ob_backup_data_store.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
using namespace omt;
using namespace common::hash;
using namespace share;
using namespace storage;
namespace rootserver
{
ObBackupValidateScheduler::ObBackupValidateScheduler()
  : ObIBackupJobScheduler(BackupJobType::VALIDATE_JOB),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    sql_proxy_(nullptr),
    rpc_proxy_(nullptr),
    schema_service_(nullptr),
    task_scheduler_(nullptr),
    validate_service_(nullptr)
{
}

int ObBackupValidateScheduler::init(
    const uint64_t tenant_id,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObBackupTaskScheduler &task_scheduler,
    ObBackupService &validate_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_VALIDATE]init twice", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    schema_service_ = &schema_service;
    task_scheduler_ = &task_scheduler;
    validate_service_ = &validate_service;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupValidateScheduler::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObBackupValidateJobAttr> validate_jobs;
  bool can_add_task = false;
  bool has_canceling_job = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]backup validate schduler not init", KR(ret));
  } else if (OB_FAIL(ObBackupValidateJobOperator::get_jobs(*sql_proxy_, tenant_id_, validate_jobs))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get jobs", KR(ret), K_(tenant_id));
  } else if (validate_jobs.empty()) {
  } else if (OB_FAIL(task_scheduler_->check_can_add_task(can_add_task))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check can add task", K(ret));
  } else if (OB_FALSE_IT(has_canceling_job = check_has_canceling_job_(validate_jobs))) {
  } else if (!can_add_task && !has_canceling_job) {
    // if queue is full and no canceling job, skip
    LOG_INFO("[BACKUP_VALIDATE]queue is full and no canceling job, skip process this round",
             K(has_canceling_job), K(can_add_task));
  } else {
    ObIBackupValidateMgr *job_mgr = nullptr;
    if (OB_FAIL(ObBackupValidateJobMgrAllocator::new_job_mgr(tenant_id_, job_mgr))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to alloc job mgr", KR(ret), K_(tenant_id));
    } else if (OB_ISNULL(job_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]job mgr can not be nullptr", KR(ret), K_(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < validate_jobs.count(); ++i) {
      ObBackupValidateJobAttr &job_attr = validate_jobs.at(i);
      job_mgr->reset();
      if (!job_attr.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]job attr is invalid", KR(ret), K(job_attr));
      } else if (OB_FAIL(job_mgr->init(tenant_id_, job_attr, *sql_proxy_, *rpc_proxy_, *task_scheduler_,
                                          *schema_service_, *validate_service_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to init job mgr", KR(ret), K(job_attr));
      } else if (OB_TMP_FAIL(job_mgr->process())) {
        LOG_WARN("[BACKUP_VALIDATE]failed to process job", KR(tmp_ret), K_(tenant_id), K(job_attr));
        if (job_attr.status_.is_validate_finish()) { // completed, failed or canceled job no need to deal error code
        } else if (!is_sys_tenant(tenant_id_) && OB_TMP_FAIL(handle_failed_job_(tenant_id_, tmp_ret, *job_mgr, job_attr))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to handle failed job", KR(tmp_ret), K_(tenant_id), K(job_attr));
        } else {
          validate_service_->wakeup();
        }
      } else if (!job_attr.status_.is_validate_finish()) {
        LOG_INFO("[BACKUP_VALIDATE]job is not finished, break", K_(tenant_id), K(job_attr));
        break;
      }
    }
    if (OB_NOT_NULL(job_mgr)) {
      ObBackupValidateJobMgrAllocator::delete_job_mgr(tenant_id_, job_mgr);
    }
  }
  return ret;
}

bool ObBackupValidateScheduler::check_has_canceling_job_(
    const ObIArray<ObBackupValidateJobAttr> &validate_jobs)
{
  bool has_canceling_job = false;
  for (int64_t i = 0; i < validate_jobs.count(); ++i) {
    if (ObBackupValidateStatus::Status::CANCELING == validate_jobs.at(i).status_.status_) {
      has_canceling_job = true;
      break;
    }
  }
  return has_canceling_job;
}

int ObBackupValidateScheduler::handle_failed_job_(
    const uint64_t tenant_id,
    const int result,
    ObIBackupValidateMgr &job_mgr,
    share::ObBackupValidateJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]sys tenant does not handle failed", K(ret), K(tenant_id));
  } else if (!job_mgr.is_can_retry(result)) {
    if (OB_FAIL(job_mgr.deal_non_reentrant_job(result))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to deal failed job", K(ret), K(job_attr));
    }
  } else {
    job_attr.retry_count_++;
    if (OB_FAIL(ObBackupValidateJobOperator::update_retry_count(*sql_proxy_, job_attr))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to persist retry times", K(ret), K(job_attr));
    } else {
      validate_service_->wakeup();
    }
  }
  return ret;
}

int ObBackupValidateScheduler::reload_task(
    common::ObIAllocator &allocator,
    ObBackupTaskSchedulerQueue &queue)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupValidateJobAttr> jobs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]not init", K(ret));
  } else if (OB_FAIL(ObBackupValidateJobOperator::get_jobs(*sql_proxy_, tenant_id_, jobs))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup validate jobs", K(ret));
  } else if (jobs.empty()) {
    LOG_INFO("[BACKUP_VALIDATE]no job need to reload");
  } else {
    bool is_valid = false;
    for (int i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
      const ObBackupValidateJobAttr &job = jobs.at(i);
      if (is_sys_tenant(job.tenant_id_) || ObBackupValidateStatus::Status::DOING != job.status_.status_) {
        // do nothing
      } else if (OB_FAIL(check_tenant_status(*schema_service_, job.tenant_id_, is_valid))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check tenant status", K(ret));
      } else if (!is_valid) {
        LOG_INFO("[BACKUP_VALIDATE]tenant status is not valid, no need to reload task");
      } else if (OB_FAIL(get_job_need_reload_task(job, allocator, queue))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to get job need reload task", K(ret));
      }
    }
  }
  LOG_INFO("[BACKUP_VALIDATE]reload_task", K(ret));
  return ret;
}

int ObBackupValidateScheduler::get_job_need_reload_task(
    const share::ObBackupValidateJobAttr &job,
    common::ObIAllocator &allocator,
    ObBackupTaskSchedulerQueue &queue)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupValidateTaskAttr> task_attrs;
  if (OB_FAIL(ObBackupValidateTaskOperator::get_tasks(*sql_proxy_, job.tenant_id_, job.job_id_, task_attrs))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup validate tasks", K(ret), K(job));
  } else if (task_attrs.empty()) {
    LOG_INFO("[BACKUP_VALIDATE]no task need to reload", K(job));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); ++i) {
      const ObBackupValidateTaskAttr &task_attr = task_attrs.at(i);
      if (OB_FAIL(do_reload_for_task_(task_attr, allocator, queue))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to reload ls tasks", K(ret), K(task_attr));
      }
    }
  }
  return ret;
}

int ObBackupValidateScheduler::do_reload_for_task_(
    const share::ObBackupValidateTaskAttr &task_attr,
    common::ObIAllocator &allocator,
    ObBackupTaskSchedulerQueue &queue)
{
  int ret = OB_SUCCESS;
  if (!task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_attr));
  } else {
    ObBackupValidateLSTaskOperator::LSTaskIterator iter;
    if (OB_FAIL(iter.init(*sql_proxy_, task_attr.tenant_id_, task_attr.task_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to init iterator", K(ret));
    } else {
      ObBackupValidateLSTaskAttr ls_task;
      while (OB_SUCC(ret)) {
        ls_task.reset();
        if (OB_FAIL(iter.next(ls_task))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("[BACKUP_VALIDATE]failed to get next ls task", K(ret));
          }
        } else if (OB_FAIL(do_reload_single_validate_ls_task_(task_attr, ls_task, allocator, queue))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to reload single ls task", K(ret), K(ls_task));
        }
      }
    }
  }
  return ret;
}

int ObBackupValidateScheduler::do_reload_single_validate_ls_task_(
    const share::ObBackupValidateTaskAttr &task_attr,
    const share::ObBackupValidateLSTaskAttr &ls_task,
    common::ObIAllocator &allocator,
    ObBackupTaskSchedulerQueue &queue)
{
  int ret = OB_SUCCESS;
  ObBackupScheduleTask *task = nullptr;
  if (!task_attr.is_valid() || !ls_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(ret), K(task_attr), K(ls_task));
  } else {
    bool queue_is_full = false;
    if (OB_FAIL(build_task_(task_attr, ls_task, allocator, task))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to build task", K(ret), K(task_attr), K(ls_task));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]task is nullptr", K(ret));
    } else if (OB_FAIL(queue.check_queue_is_full(queue_is_full))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check queue is full", K(ret));
    } else if (queue_is_full) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("[BACKUP_VALIDATE]queue is full, stop reload ls tasks", KPC(task));
    } else if (OB_FAIL(queue.push_task(*task))) {
      if (OB_ENTRY_EXIST == ret) {
        LOG_DEBUG("[BACKUP_VALIDATE]task already exist in queue, skip", KPC(task));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[BACKUP_VALIDATE]failed to push task to queue", K(ret), KPC(task));
      }
    }
    // free task memory after pushing to queue (or on failure)
    if (nullptr != task) {
      task->~ObBackupScheduleTask();
      allocator.free(task);
      task = nullptr;
    }
  }
  return ret;
}

int ObBackupValidateScheduler::build_task_(
    const share::ObBackupValidateTaskAttr &task_attr,
    const share::ObBackupValidateLSTaskAttr &ls_task_attr,
    common::ObIAllocator &allocator,
    ObBackupScheduleTask *&task)
{
  int ret = OB_SUCCESS;
  ObBackupValidateLSTask tmp_ls_task;
  if (OB_FAIL(tmp_ls_task.build(task_attr, ls_task_attr))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to build ls task", KR(ret), K(task_attr), K(ls_task_attr));
  } else if (OB_FAIL(tmp_ls_task.clone(allocator, task))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to clone input task", KR(ret), K(tmp_ls_task));
  } else if (nullptr == task) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]task is nullptr", KR(ret), K(tmp_ls_task));
  }
  return ret;
}

int ObBackupValidateScheduler::handle_execute_over(
    const ObBackupScheduleTask *task,
    const share::ObHAResultInfo &result_info,
    bool &can_remove)
{
  int ret = OB_SUCCESS;
  can_remove = false;
  bool is_valid = true;
  ObMySQLTransaction trans;
  ObLSID ls_id(task->get_ls_id());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]not init", K(ret));
  } else if (nullptr == task) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(ret));
  } else if (OB_FAIL(check_tenant_status(*schema_service_, task->get_tenant_id(), is_valid))) {
    LOG_WARN("[BACKUP_VALIDATE]fail to check tenant status", K(ret));
  } else if (!is_valid) {
    can_remove = true;
    LOG_INFO("[BACKUP_VALIDATE]tenant status is not valid, can remove task", K(task->get_tenant_id()));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(task->get_tenant_id())))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret));
  } else {
    ObBackupValidateLSTaskAttr ls_attr;
    if (OB_FAIL(ObBackupValidateLSTaskOperator::get_ls_task(trans, true/*for_update*/, task->get_task_id(),
                                                                  task->get_tenant_id(), ls_id, ls_attr))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get ls tasks", KR(ret), K(*task));
    } else if (OB_FAIL(result_info.get_comment_str(ls_attr.comment_))) {
      LOG_WARN("failed to get comment str", K(ret), K(result_info));
    } else if (ObBackupTaskStatus::Status::DOING == ls_attr.status_.status_) {
      ObBackupTaskStatus next_status;
      next_status.status_ = ObBackupTaskStatus::Status::FINISH;
      ls_attr.end_ts_ = ObTimeUtility::current_time();
      if (OB_FAIL(ObBackupValidateLSTaskOperator::advance_ls_task_status(trans, ls_attr, next_status,
                                                                          result_info.result_, ls_attr.end_ts_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to advance ls task status", KR(ret), K(ls_attr), K(result_info));
      } else if (OB_SUCCESS == result_info.result_) {
        const uint64_t tenant_id = task->get_tenant_id();
        const int64_t task_id = static_cast<int64_t>(task->get_task_id());
        if (OB_FAIL(ObBackupValidateTaskOperator::add_finish_ls_count(trans, tenant_id, task_id))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to add finish ls count", KR(ret), K(tenant_id), K(task_id));
        }
      }
    } else {
      LOG_WARN("concurrent scenario! this task will need reload to redo.", K(ls_attr));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to end trans", KR(ret));
      } else {
        can_remove = true;
        validate_service_->wakeup();
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(false/*rollback*/))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to end trans", KR(ret), KR(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateScheduler::force_cancel(const uint64_t tenant_id)
{
  // Backup validate tasks do not need to update external file, this interface is not implemented
  return OB_NOT_SUPPORTED;
}

int ObBackupValidateScheduler::cancel_backup_validate_job(
    const uint64_t tenant_id,
    const ObIArray<uint64_t> &execute_tenant_ids)
{
  // Three cancellation strategies:
  // 1. if tenant_id is sys tenant and execute tenant ids is empty, cancel all the validate jobs of all the tenants
  // 2. if tenant_id is sys tenant and execute tenant ids isn't empty, cancel all the validate jobs of tenants in execute tenant ids
  // 3. if tenant_id is user tenant, cancel all validate jobs of tenant id.
  int ret = OB_SUCCESS;
  ObSArray<uint64_t> need_cancel_validate_tenants;
  bool is_valid = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]backup validate scheduler not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_need_cancel_tenants_(tenant_id, execute_tenant_ids, need_cancel_validate_tenants))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get need cancel validate tenants", K(ret), K(tenant_id), K(execute_tenant_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < need_cancel_validate_tenants.count(); ++i) {
      const uint64_t need_cancel_validate_tenant = need_cancel_validate_tenants.at(i);
      if (!is_valid_tenant_id(need_cancel_validate_tenant)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[BACKUP_VALIDATE]invalid tenant", K(ret), K(need_cancel_validate_tenant));
      } else if (OB_FAIL(check_tenant_status(*schema_service_, need_cancel_validate_tenant, is_valid))) {
      } else if (!is_valid) {
        LOG_INFO("tenant status not normal, no need to schedule validate", K(need_cancel_validate_tenant));
      } else if (OB_FAIL(ObBackupValidateJobOperator::cancel_jobs(*sql_proxy_, need_cancel_validate_tenant))) {
        LOG_WARN("fail to cancel validate jobs", K(ret), K(need_cancel_validate_tenant));
      } else {
        // wakeup backup validate user tenant service to check job status
        validate_service_->wakeup_tenant_service(need_cancel_validate_tenant);
      }
    }
  }
  return ret;
}

int ObBackupValidateScheduler::get_need_cancel_tenants_(
    const uint64_t tenant_id,
    const ObIArray<uint64_t> &execute_tenant_ids,
    ObIArray<uint64_t> &need_cancel_validate_tenants)
{
  int ret = OB_SUCCESS;
  need_cancel_validate_tenants.reset();
  if (is_sys_tenant(tenant_id) && execute_tenant_ids.empty()) {
    if (OB_FAIL(schema_service_->get_tenant_ids(need_cancel_validate_tenants))) {
      LOG_WARN("[BACKUP_VALIDATE]fail to get all tenants", K(ret));
    }
  } else if (is_sys_tenant(tenant_id) && !execute_tenant_ids.empty()) {
    if (OB_FAIL(need_cancel_validate_tenants.assign(execute_tenant_ids))) {
      LOG_WARN("[BACKUP_VALIDATE]fail to assign execute tenant ids", K(execute_tenant_ids));
    }
  } else if (is_user_tenant(tenant_id) && execute_tenant_ids.empty()) {
    if (OB_FAIL(need_cancel_validate_tenants.push_back(tenant_id))) {
      LOG_WARN("[BACKUP_VALIDATE]fail to push validate tenant", K(ret), K(tenant_id));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid tenant_id or execute tenant ids", K(ret), K(tenant_id), K(execute_tenant_ids));
  }
  return ret;
}

int ObBackupValidateScheduler::start_schedule_backup_validate(const obrpc::ObBackupValidateArg &arg)
{
  int ret = OB_SUCCESS;
  ObBackupValidateJobAttr template_job_attr;
  ObBackupDest validate_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]backup validate schduler not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(fill_template_job_(arg, template_job_attr, validate_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to fill template job", KR(ret), K(arg));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID == arg.tenant_id_) {
    //validate task initiate by sys tenant
    if (OB_FAIL(start_sys_backup_validate_(template_job_attr, validate_dest))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to start sys backup data validate",
                  KR(ret), K(template_job_attr), K(validate_dest));
    }
  } else {
    //validate task initiate by user tenant
    if (!arg.execute_tenant_ids_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[BACKUP_VALIDATE]tenant validation task should not initiate another tenant", KR(ret), K(arg));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "validate: tenant validation task should not initiate another user tenant");
    } else if (OB_FAIL(start_tenant_backup_validate_(template_job_attr, validate_dest))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to start tenant backup data validate",
                  KR(ret), K(template_job_attr), K(validate_dest));
    } else {
      // user tenant validate - wakeup self
      validate_service_->wakeup_tenant_service(arg.tenant_id_);
    }
  }

  if (OB_SUCC(ret)) {
    validate_service_->wakeup();
    FLOG_INFO("[BACKUP_VALIDATE]wakeup backup validate service", K(arg));
  }
  ROOTSERVICE_EVENT_ADD("backup_validate", "start_backup_validate", "tenant_id", arg.tenant_id_,  "result", ret);
  return ret;
}

int ObBackupValidateScheduler::start_sys_backup_validate_(
    const share::ObBackupValidateJobAttr &job_attr,
    const ObBackupDest &validate_dest)
{
  int ret = OB_SUCCESS;
  ObBackupValidateJobAttr new_job_attr;
  common::ObMySQLTransaction trans;
  if (!job_attr.is_tmplate_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(job_attr));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(job_attr.tenant_id_)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret), K(job_attr));
  } else if (OB_FAIL(new_job_attr.assign(job_attr))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign job attr", KR(ret), K(job_attr));
  } else if (!job_attr.executor_tenant_ids_.empty()) {
    const int64_t exec_tenant_count = job_attr.executor_tenant_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < exec_tenant_count; ++i) {
      const uint64_t &executor_tenant_id = job_attr.executor_tenant_ids_.at(i);
      bool is_valid = true;
      bool is_doing = true;
      bool need_validate = false;
      if (OB_FAIL(check_tenant_status(*schema_service_, executor_tenant_id, is_valid))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check tenant status", KR(ret), K(executor_tenant_id));
      } else if (!is_valid) {
        ret = OB_TENANT_NOT_IN_SERVER;
        LOG_WARN("[BACKUP_VALIDATE]tenant not in server", KR(ret), K(executor_tenant_id));
      } else if (!job_attr.validate_path_.is_empty()) {
        if (1 != exec_tenant_count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(job_attr));
        } else if (OB_FAIL(insert_backup_storage_info_for_validate_(trans, validate_dest, job_attr.tenant_id_))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to insert backup storage info for validate",
              KR(ret), K(validate_dest), K(job_attr), K(executor_tenant_id));
        }
      } else if (OB_FAIL(check_backup_dest_and_archive_dest_(executor_tenant_id, job_attr, need_validate))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check dest", KR(ret), K(executor_tenant_id), K(job_attr));
      } else if (!need_validate) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[BACKUP_VALIDATE]tenant has not set backup dest or archive dest",
                    KR(ret), K(executor_tenant_id), K(job_attr));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "validate: tenant has not set backup dest or archive dest");
      }
    }
  } else if (OB_FAIL(get_need_validate_tenants_(new_job_attr, new_job_attr.executor_tenant_ids_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get all tenants", KR(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(persist_job_task_(trans, new_job_attr))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to persist job task", KR(ret), K(new_job_attr));
  }
  if (trans.is_started()) {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to commit trans", KR(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(false))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateScheduler::start_tenant_backup_validate_(
    const share::ObBackupValidateJobAttr &job_attr,
    const ObBackupDest &validate_dest)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObBackupValidateJobAttr new_job_attr;
  ObBackupPathString validate_path;
  bool is_doing = true;
  common::ObMySQLTransaction trans;
  bool need_validate = false;
  if (!job_attr.is_tmplate_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(job_attr));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(job_attr.tenant_id_)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret), K(job_attr));
  } else if (!job_attr.validate_path_.is_empty()) {
    bool need_insert = false;
    if (OB_FAIL(validate_dest.get_backup_dest_str(validate_path.ptr(), validate_path.capacity()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest str", KR(ret), K(validate_dest));
    } else if (OB_FAIL(check_dest_connectivity_(trans, job_attr.tenant_id_, validate_path))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check dest connectivity", KR(ret), K(job_attr), K(validate_path));
    } else if (OB_FAIL(insert_backup_storage_info_for_validate_(trans, validate_dest, job_attr.tenant_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to insert backup storage info for validate",
                    KR(ret), K(validate_dest), K(job_attr));
    }
  } else if (OB_FAIL(check_backup_dest_and_archive_dest_(job_attr.tenant_id_, job_attr, need_validate))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check backup dest and archive dest", KR(ret), K(job_attr));
  } else if (!need_validate) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]tenant has not set backup dest or archive dest", KR(ret), K(job_attr));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "validate: tenant has not set backup dest or archive dest");
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_job_attr.assign(job_attr))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign job attr", KR(ret), K(job_attr));
  } else if (OB_FAIL(check_tenant_status(*schema_service_, new_job_attr.tenant_id_, is_valid))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check tenant status", KR(ret), K(job_attr));
  } else if (!is_valid) {
    ret = OB_TENANT_NOT_IN_SERVER;
    LOG_WARN("[BACKUP_VALIDATE]tenant not in server", KR(ret), K(job_attr));
  } else if (OB_FALSE_IT(new_job_attr.initiator_job_id_ = new_job_attr.tenant_id_ == new_job_attr.initiator_tenant_id_ ?
                            0/*no parent job*/ : new_job_attr.initiator_job_id_)) {
  } else if (OB_FAIL(persist_job_task_(trans, new_job_attr))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to persist job task", KR(ret), K(new_job_attr));
  }
  if (trans.is_started()) {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to commit trans", KR(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(false))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateScheduler::persist_job_task_(
    common::ObISQLClient &trans,
    share::ObBackupValidateJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  const uint64_t &job_tenant_id = job_attr.tenant_id_;
  ObArray<ObBackupValidateJobAttr> existing_jobs;

  if (0 == job_attr.initiator_job_id_ && OB_FAIL(ObBackupValidateJobOperator::get_jobs(trans, job_tenant_id, existing_jobs))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get existing jobs", KR(ret), K(job_tenant_id));
  } else if (!existing_jobs.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[BACKUP_VALIDATE]cannot create new validate job when other validate jobs exist",
             KR(ret), K(job_tenant_id), K(existing_jobs));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "creating new backup validate job when other validate jobs exist");
  } else if (OB_FAIL(get_next_job_id_(trans, job_tenant_id, job_attr.job_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get next job id", KR(ret), K(job_attr));
  } else if (OB_FAIL(validate_service_->check_leader())) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check leader", KR(ret));
  } else if (OB_FAIL(ObBackupValidateJobOperator::insert_job(trans, job_attr))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to insert job", KR(ret), K(job_attr));
  }
  return ret;
}

int ObBackupValidateScheduler::check_dest_connectivity_(
    common::ObISQLClient &trans,
    const uint64_t tenant_id,
    const ObBackupPathString &validate_path)
{
  int ret = OB_SUCCESS;
  ObBackupDestMgr dest_mgr;
  if (OB_FAIL(dest_mgr.init(tenant_id, ObBackupDestType::TYPE::DEST_TYPE_BACKUP_VALIDATE, validate_path, trans))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init dest mgr", KR(ret), K(tenant_id), K(validate_path));
  } else if (OB_FAIL(dest_mgr.check_dest_connectivity(*rpc_proxy_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check dest validity", KR(ret), K(tenant_id), K(validate_path));
  }
  return ret;
}

int ObBackupValidateScheduler::get_need_validate_tenants_(
    const share::ObBackupValidateJobAttr &job_attr,
    ObIArray<uint64_t> &tenants) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tmp_tenants;
  if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tmp_tenants))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get tenant ids", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenants.count(); ++i) {
      const uint64_t &tenant_id = tmp_tenants.at(i);
      bool is_valid = true;
      bool need_validate = false;
      tmp_ret = OB_SUCCESS;
      if (!is_user_tenant(tenant_id) || OB_SYS_TENANT_ID == tenant_id) {
      } else if (OB_TMP_FAIL(check_tenant_status(*schema_service_, tenant_id, is_valid))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check tenant status", KR(ret), K(tenant_id));
      } else if (!is_valid) {
        LOG_INFO("[BACKUP_VALIDATE]tenant not in server", K(tenant_id));
      } else if (OB_FAIL(check_backup_dest_and_archive_dest_(tenant_id, job_attr, need_validate))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check backup dest and archive dest", KR(ret), K(tenant_id), K(job_attr));
      } else if (!need_validate) {
        // skip
      } else if (OB_FAIL(tenants.push_back(tenant_id))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to push back tenant id", KR(ret), K(tenant_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (tenants.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]all tenant can not do validation task", K(ret));
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "all tenant can not do validation task");
    }
  }

  return ret;
}

int ObBackupValidateScheduler::check_tenant_set_backup_dest_(const uint64_t tenant_id, bool &is_setted) const
{
  int ret = OB_SUCCESS;
  share::ObBackupHelper backup_helper;
  ObBackupPathString backup_dest_str;
  is_setted = false;
  if (OB_FAIL(backup_helper.init(tenant_id, *sql_proxy_))) {
    LOG_WARN("failed to init backup helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(backup_helper.get_backup_dest(backup_dest_str))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get backup dest", K(ret), K(tenant_id));
    }
  } else if (!backup_dest_str.is_empty()) {
    is_setted = true;
  }
  return ret;
}

int ObBackupValidateScheduler::check_backup_dest_and_archive_dest_(
  const uint64_t tenant_id,
  const share::ObBackupValidateJobAttr &job_attr,
  bool &need_validate) const
{
  int ret = OB_SUCCESS;
  bool has_set_archive_path = false;
  bool has_set_backup_path = false;
  need_validate = false;
  if (job_attr.need_set_backup_dest()) {
    if (OB_FAIL(check_tenant_set_backup_dest_(tenant_id, has_set_backup_path))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check tenant set backup dest", K(ret), K(tenant_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (job_attr.need_set_archive_dest()) {
    if (OB_FAIL(check_tenant_set_archive_dest_(tenant_id, has_set_archive_path))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check tenant set archive dest", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret) && (has_set_archive_path || has_set_backup_path)) {
    need_validate = true;
  }
  return ret;
}

int ObBackupValidateScheduler::check_tenant_set_archive_dest_(const uint64_t tenant_id, bool &is_setted) const
{
  int ret = OB_SUCCESS;
  share::ObArchivePersistHelper helper;
  ObBackupPathString archive_dest_str;
  is_setted = false;
  const int64_t dest_no = 0;
  if (OB_FAIL(helper.init(tenant_id))) {
    LOG_WARN("failed to init backup helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_archive_dest(*sql_proxy_, false/*need lock*/, dest_no, archive_dest_str))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get archive dest", K(ret), K(tenant_id));
    }
  } else if (!archive_dest_str.is_empty()) {
    is_setted = true;
  }
  return ret;
}

int ObBackupValidateScheduler::check_tenant_status(
  share::schema::ObMultiVersionSchemaService &schema_service,
  uint64_t tenant_id, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_dropped = false;
  is_valid = true;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  if (OB_FAIL(schema_service.check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
    LOG_WARN("BACKUP_VALIDATE]failed to check if tenant has been dropped", K(ret), K(tenant_id));
  } else if (is_dropped) {
    is_valid = false;
    LOG_WARN("BACKUP_VALIDATE]tenant is dropped, can't not do validate now", K(tenant_id));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("BACKUP_VALIDATE]failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("BACKUP_VALIDATE]failed to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    is_valid = false;
    LOG_WARN("tenant schema is null, tenant may has been dropped", K(ret), K(tenant_id));
  } else if (tenant_schema->is_creating()) {
    is_valid = false;
    LOG_WARN("BACKUP_VALIDATE]tenant is creating, can't not do validate now", K(tenant_id));
  } else if (tenant_schema->is_restore()) {
    is_valid = false;
    LOG_WARN("BACKUP_VALIDATE]tenant is doing restore, can't not do validate now", K(tenant_id));
  } else if (tenant_schema->is_dropping()) {
    is_valid = false;
    LOG_WARN("BACKUP_VALIDATE]tenant is dropping, can't not do validate now", K(tenant_id));
  } else if (tenant_schema->is_in_recyclebin()) {
    is_valid = false;
    LOG_WARN("BACKUP_VALIDATE]tenant is in recyclebin, can't not do validate now", K(tenant_id));
  } else if (tenant_schema->is_normal()) {
    is_valid = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("BACKUP_VALIDATE]unknown tenant status", K(tenant_id), K(tenant_schema));
  }

  return ret;
}

int ObBackupValidateScheduler::fill_template_job_(
    const obrpc::ObBackupValidateArg &in_arg,
    share::ObBackupValidateJobAttr &job_attr,
    ObBackupDest &validate_dest)
{
  int ret = OB_SUCCESS;
  int64_t incarnation_id = 1;
  if (!in_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(in_arg));
  } else if (OB_FAIL(job_attr.description_.assign(in_arg.description_))) {
    LOG_WARN("BACKUP_VALIDATE]failed to assgin description", KR(ret));
  } else if (OB_FAIL(job_attr.executor_tenant_ids_.assign(in_arg.execute_tenant_ids_))) {
    LOG_WARN("BACKUP_VALIDATE]failed to assgin backup tenant id", KR(ret));
  } else {
    job_attr.tenant_id_ = in_arg.tenant_id_;
    job_attr.initiator_job_id_ = in_arg.initiator_job_id_;
    job_attr.initiator_tenant_id_ = in_arg.initiator_tenant_id_;
    job_attr.type_ = in_arg.validate_type_;
    job_attr.level_ = in_arg.validate_level_;
    job_attr.start_ts_ = ObTimeUtility::current_time();
    job_attr.incarnation_id_ = incarnation_id;
    job_attr.status_ = ObBackupValidateStatus::INIT;
    job_attr.end_ts_ = 0;
    job_attr.result_ = OB_SUCCESS;
    if (ObBackupValidateType::ValidateType::BACKUPSET == job_attr.type_.type_
            && OB_FAIL(job_attr.backup_set_ids_.assign(in_arg.set_or_piece_ids_))) {
      LOG_WARN("failed to assign backup set ids", KR(ret), K(in_arg.set_or_piece_ids_));
    } else if (ObBackupValidateType::ValidateType::ARCHIVELOG_PIECE == job_attr.type_.type_
            && OB_FAIL(job_attr.logarchive_piece_ids_.assign(in_arg.set_or_piece_ids_))) {
      LOG_WARN("failed to assign log archive piece ids", KR(ret), K(in_arg.set_or_piece_ids_));
    } else if (!in_arg.validate_dest_.is_empty()
                  && OB_FAIL(get_path_and_set_path_type_(in_arg.validate_dest_, job_attr, validate_dest))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get path and check path type", KR(ret), K(in_arg));
    }
  }

  return ret;
}

int ObBackupValidateScheduler::get_path_and_set_path_type_(
    const ObBackupPathString &backup_dest,
    share::ObBackupValidateJobAttr &job_attr,
    ObBackupDest &dest)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  bool is_empty = false;
  ObBackupValidatePathType path_type;
  share::ObBackupStore store;
  path_type = ObBackupValidatePathType::ValidatePathType::MAX_PATH_TYPE;
  bool is_inner_backup_job = false;

  if (backup_dest.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup dest is empty", KR(ret), K(backup_dest));
  } else if (OB_FAIL(dest.set(backup_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to set backup dest", KR(ret), K(backup_dest));
  } else if (OB_FAIL(dest.get_backup_path_str(job_attr.validate_path_.ptr(), job_attr.validate_path_.capacity()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup path str", KR(ret), K(dest));
  } else if (OB_FAIL(store.init(dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init store", KR(ret), K(dest));
  } else if (OB_FAIL(store.dest_is_empty_directory(is_empty))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check dest is empty directory", KR(ret), K(dest));
  } else if (is_empty) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "validate: empty directory");
  } else if (OB_FAIL(store.is_format_file_exist(is_exist))) { // first judge format file exist
    LOG_WARN("[BACKUP_VALIDATE]failed to check format file exist", KR(ret), K(dest));
  } else if (is_exist) {
    ObBackupFormatDesc format_desc;
    if (OB_FAIL(store.read_format_file(format_desc))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to read format file", KR(ret), K(dest));
    } else if (ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA == format_desc.dest_type_ ) {
      path_type = ObBackupValidatePathType::ValidatePathType::BACKUP_DEST;
    } else if (ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG == format_desc.dest_type_ ) {
      path_type = ObBackupValidatePathType::ValidatePathType::ARCHIVELOG_DEST;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("BACKUP_VALIDATE]invalid validate dest type", K(ret), K(format_desc));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT,
          "validate: Only data backup path or log archive path supports backup verification.");
    }
  } else if (OB_FAIL(store.is_single_backup_set_info_exist(is_exist))) {   //Determine whether it is BACKUP_SET_DEST
    LOG_WARN("BACKUP_VALIDATE]failed to check single backup set info exist", KR(ret), K(dest));
  } else if (is_exist) {
    path_type = ObBackupValidatePathType::ValidatePathType::BACKUP_SET_DEST;
  }

  //Determine whether it is ARCHIVELOG_PIECE_DEST
  ObArchiveStore archive_store;
  if (OB_FAIL(ret) || is_exist) {
  } else if (OB_FAIL(archive_store.init(dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init archive store", KR(ret), K(dest));
  } else if (OB_FAIL(archive_store.is_tenant_archive_piece_infos_file_exist(is_exist))) {
    LOG_WARN("BACKUP_VALIDATE]fail to check if tenant arhive piece info is exist", KR(ret), K(dest));
  } else if (is_exist) {
    path_type = ObBackupValidatePathType::ValidatePathType::ARCHIVELOG_PIECE_DEST;
  }

  //check path_and_validate_type_consistent
  if (OB_FAIL(ret)) {
  } else if (!is_exist) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("BACKUP_VALIDATE]invalid argument", KR(ret), K(dest));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "validate: missing format file and metadata file to identify the path type.");
  } else if (OB_FAIL(job_attr.set_path_type(path_type))) {
    LOG_WARN("BACKUP_VALIDATE]failed to set path type", KR(ret), K(path_type));
  }

  return ret;
}

int ObBackupValidateScheduler::get_next_job_id_(common::ObISQLClient &trans, const uint64_t tenant_id, int64_t &job_id)
{
  int ret = OB_SUCCESS;
  job_id = OB_INVALID_JOB_ID;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObLSBackupInfoOperator::get_next_job_id(trans, tenant_id, job_id))) {
    LOG_WARN("BACKUP_VALIDATE]failed to get next job id", KR(ret), K(tenant_id));
  } else if (OB_INVALID_JOB_ID == job_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("BACKUP_VALIDATE]invalid job id", KR(ret), K(job_id));
  }
  return ret;
}

/*
*----------------------ObIBackupValidateMgr-------------------------
*/
ObIBackupValidateMgr::ObIBackupValidateMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    job_attr_(nullptr),
    sql_proxy_(nullptr),
    rpc_proxy_(nullptr),
    task_scheduler_(nullptr),
    schema_service_(nullptr),
    backup_validate_service_(nullptr)
{
}

int ObIBackupValidateMgr::init(
    const uint64_t tenant_id,
    share::ObBackupValidateJobAttr &job_attr,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    ObBackupTaskScheduler &task_scheduler,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObBackupService &backup_validate_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_VALIDATE]backup validate mgr init twice", KR(ret));
  } else if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(job_attr));
  } else {
    tenant_id_ = tenant_id;
    job_attr_ = &job_attr;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    task_scheduler_ = &task_scheduler;
    schema_service_ = &schema_service;
    backup_validate_service_ = &backup_validate_service;
    is_inited_ = true;
  }
  return ret;
}

void ObIBackupValidateMgr::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  job_attr_ = nullptr;
  sql_proxy_ = nullptr;
  rpc_proxy_ = nullptr;
  task_scheduler_ = nullptr;
  schema_service_ = nullptr;
  backup_validate_service_ = nullptr;
  is_inited_ = false;
}

bool ObIBackupValidateMgr::is_can_retry(const int err) const
{
  return job_attr_->can_retry_ && job_attr_->retry_count_ < OB_MAX_RETRY_TIMES && ObBackupUtils::is_need_retry_error(err);
}

int ObIBackupValidateMgr::advance_status(
    common::ObISQLClient &sql_proxy,
    const share::ObBackupValidateStatus &next_status,
    const int result,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_validate_service_->check_leader())) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check leader", KR(ret));
  } else if (OB_FAIL(ObBackupValidateJobOperator::advance_job_status(sql_proxy, *job_attr_,
                                                                        next_status, result, end_ts))) {
     LOG_WARN("[BACKUP_VALIDATE]failed to advance job status", KR(ret));
  } else {
    job_attr_->status_ = next_status;
  }
  return ret;
}

/*
*----------------------ObUserTenantBackupValidateJobMgr-------------------------
*/

int ObUserTenantBackupValidateJobMgr::deal_non_reentrant_job(const int err)
{
  int ret = OB_SUCCESS;
  ObBackupValidateStatus next_status;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate job mgr is not init", KR(ret));
  } else {
    LOG_INFO("[BACKUP_VALIDATE]start to deal non reentrant job", KPC(job_attr_), KP(err));
    next_status.status_ = ObBackupValidateStatus::Status::FAILED;
    job_attr_->result_ = err;
    job_attr_->end_ts_ = ObTimeUtility::current_time();
    ObMySQLTransaction trans;
    bool is_exist_set_task = true;
    bool is_start = false;
    ObArray<share::ObBackupValidateTaskAttr> task_attrs;
    if (OB_FAIL(task_scheduler_->cancel_tasks(BackupJobType::VALIDATE_JOB,
                                                  job_attr_->job_id_, job_attr_->tenant_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to cancel tasks", KR(ret), KPC(job_attr_));
    } else if (job_attr_->comment_.is_empty()) {
      ObHAResultInfo result_info(ObHAResultInfo::ROOT_SERVICE,
                                GCONF.self_addr_,
                                share::ObTaskId(*ObCurTraceId::get_trace_id()),
                                err);
      if (OB_FAIL(result_info.get_comment_str(job_attr_->comment_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to get comment str", KR(ret), K(result_info));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(job_attr_->tenant_id_)))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret), KPC_(job_attr));
    } else if (OB_FAIL(ObBackupValidateJobOperator::append_comment(trans, job_attr_->tenant_id_,
                                                                    job_attr_->job_id_, job_attr_->comment_.ptr()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update comment", KR(ret), KPC(job_attr_));
    } else if (ObBackupValidateStatus::Status::INIT == job_attr_->status_.status_) {
    } else if (OB_FAIL(ObBackupValidateTaskOperator::get_tasks(*sql_proxy_, job_attr_->tenant_id_,
                                                            job_attr_->job_id_, task_attrs))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get tasks", KR(ret), KPC(job_attr_));
    } else if (ObBackupValidateStatus::Status::DOING == job_attr_->status_.status_) {
      ObBackupValidateTaskMgr set_task_mgr;
      for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
        const ObBackupValidateTaskAttr &task_attr = task_attrs.at(i);
        if (ObBackupValidateStatus::Status::INIT == task_attr.status_.status_
            || ObBackupValidateStatus::Status::DOING == task_attr.status_.status_
            || ObBackupValidateStatus::Status::CANCELING == task_attr.status_.status_) {
          if (OB_FAIL(set_task_mgr.init(tenant_id_, task_attr.task_id_, *job_attr_, *sql_proxy_,
            *rpc_proxy_, *task_scheduler_, *backup_validate_service_))) {
            LOG_WARN("[BACKUP_VALIDATE]failed to init set task mgr", KR(ret), KPC(job_attr_));
          } else if (OB_FAIL(set_task_mgr.deal_failed_task(OB_CANCELED))) {
            LOG_WARN("[BACKUP_VALIDATE]failed to deal failed set task", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(advance_status(trans, next_status, err, job_attr_->end_ts_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to move job status to FAILED", KR(ret), KPC(job_attr_));
      }
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to end trans", KR(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      share::ObTaskId trace_id(*ObCurTraceId::get_trace_id());
      LOG_INFO("[BACKUP_VALIDATE]succeed deal with failed job, advance job status to FAILED", KPC(job_attr_));
      ROOTSERVICE_EVENT_ADD("backup_validate", "deal with failed job", "tenant_id",
        job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "result", err, "trace_id", trace_id);
      backup_validate_service_->wakeup();
    }
  }

  return ret;
}

ObUserTenantBackupValidateJobMgr::ObUserTenantBackupValidateJobMgr()
  : ObIBackupValidateMgr(),
    backup_dest_id_(OB_INVALID_DEST_ID),
    archive_dest_id_(OB_INVALID_DEST_ID)
{
}

ObUserTenantBackupValidateJobMgr::~ObUserTenantBackupValidateJobMgr()
{
  backup_dest_id_ = OB_INVALID_DEST_ID;
  archive_dest_id_ = OB_INVALID_DEST_ID;
}

void ObUserTenantBackupValidateJobMgr::reset()
{
  backup_dest_id_ = OB_INVALID_DEST_ID;
  archive_dest_id_ = OB_INVALID_DEST_ID;
  info_collector_.reset();
  ObIBackupValidateMgr::reset();
}

int ObUserTenantBackupValidateJobMgr::init(const uint64_t tenant_id, share::ObBackupValidateJobAttr &job_attr,
    common::ObMySQLProxy &sql_proxy, obrpc::ObSrvRpcProxy &rpc_proxy,
    ObBackupTaskScheduler &task_scheduler, share::schema::ObMultiVersionSchemaService &schema_service,
    ObBackupService &backup_validate_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIBackupValidateMgr::init(tenant_id, job_attr, sql_proxy, rpc_proxy,
                                            task_scheduler, schema_service, backup_validate_service))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init backup validate job mgr", KR(ret), K(tenant_id), K(job_attr));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]backup validate job mgr do not init", KR(ret));
  } else {
    ObBackupValidateStatus::Status status = job_attr_->status_.status_;
    switch (status) {
      case ObBackupValidateStatus::Status::INIT: {
        if (OB_FAIL(persist_set_task_())) {
          LOG_WARN("BACKUP_VALIDATE]failed to persist log stream task", K(ret), KPC(job_attr_));
        }
        break;
      }
      case ObBackupValidateStatus::Status::DOING: {
        if (OB_FAIL(do_validate_task_())) {
          LOG_WARN("BACKUP_VALIDATE]failed to do validate task", K(ret), KPC(job_attr_));
        }
        break;
      }
      case ObBackupValidateStatus::Status::COMPLETED:
      case ObBackupValidateStatus::Status::FAILED:
      case ObBackupValidateStatus::Status::CANCELED: {
        if (OB_FAIL(move_to_history_())) {
          LOG_WARN("BACKUP_VALIDATE]failed to move job and set to histroy", K(ret), KPC(job_attr_));
        }
        break;
      }
      case ObBackupValidateStatus::Status::CANCELING: {
        if (OB_FAIL(cancel_())) {
          LOG_WARN("BACKUP_VALIDATE]failed to cancel validate", K(ret), KPC(job_attr_));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("BACKUP_VALIDATE]unknown backup status", K(ret), KPC(job_attr_));
      }
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::do_validate_task_()
{
  int ret = OB_SUCCESS;
  ObBackupValidateStatus next_status;
  ObArray<share::ObBackupValidateTaskAttr> task_attrs;
  int result = OB_SUCCESS;
  if (OB_FAIL(ObBackupValidateTaskOperator::get_tasks(*sql_proxy_, job_attr_->tenant_id_,
                                                          job_attr_->job_id_, task_attrs))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get tasks", KR(ret), KPC(job_attr_));
  } else if (OB_FAIL(handle_backup_validate_task_(task_attrs, next_status, result))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to handle backup validate task", KR(ret), KPC(job_attr_), K(task_attrs));
  } else if (ObBackupValidateStatus::Status::COMPLETED == next_status.status_
                || ObBackupValidateStatus::Status::FAILED == next_status.status_) {
    int64_t end_ts = ObTimeUtility::current_time();
    if (OB_FAIL(advance_status(*sql_proxy_, next_status, result, end_ts))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to move job status to FAILED", KR(ret),
                  KPC(job_attr_), K(next_status), K(result));
    } else if (OB_FAIL(ObBackupValidateJobOperator::update_task_count(*sql_proxy_, *job_attr_, false/*is_total*/))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update task count", KR(ret), KPC(job_attr_));
    } else {
      backup_validate_service_->wakeup();
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::handle_backup_validate_task_(
    const ObArray<share::ObBackupValidateTaskAttr> &task_attrs,
    share::ObBackupValidateStatus &next_status,
    int &result)
{
  int ret = OB_SUCCESS;
  result = OB_SUCCESS;
  next_status.status_ = ObBackupValidateStatus::Status::MAX_STATUS;
  bool has_failed = false;
  bool has_task = false;
  int64_t success_task_count = 0;
  for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); ++i) {
    const ObBackupValidateTaskAttr &task_attr = task_attrs.at(i);
    if (!task_attr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]task attr is invalid", KR(ret), K(task_attr));
    } else if (ObBackupValidateStatus::Status::INIT == task_attr.status_.status_
        || ObBackupValidateStatus::Status::DOING == task_attr.status_.status_
        || ObBackupValidateStatus::Status::CANCELING == task_attr.status_.status_) {
      has_task = true;
    } else if (ObBackupValidateStatus::Status::FAILED == task_attr.status_.status_ && !has_failed) {
      bool need_advance = false;
      if (OB_FAIL(check_need_advance_job_failed_(task_attr, need_advance))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check need advance job failed", KR(ret), K(task_attr));
      } else if (need_advance) {
        result = task_attr.result_;
        has_failed = true;
      }
    } else if (ObBackupValidateStatus::Status::COMPLETED == task_attr.status_.status_) {
      success_task_count++;
    }
  }
  job_attr_->success_task_count_ = success_task_count;
  if (OB_FAIL(ret)) {
  } else if (has_task) {
    if (!has_failed) {
      if (OB_FAIL(do_validate_tasks_(task_attrs))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to do validate tasks", KR(ret));
      }
    } else {
      if (OB_FAIL(do_cancel_validate_())) {
        LOG_WARN("[BACKUP_VALIDATE]failed to cancel validate task", KR(ret));
      }
    }
  } else {
    if (has_failed) {
      next_status.status_ = ObBackupValidateStatus::Status::FAILED;
    } else {
      next_status.status_ = ObBackupValidateStatus::Status::COMPLETED;
    }
  }

  FLOG_INFO("[BACKUP_VALIDATE]handle backup validate task", KR(ret),
      KPC(job_attr_), K(next_status.status_), K(has_failed), K(has_task));
  return ret;
}

int ObUserTenantBackupValidateJobMgr::check_need_advance_job_failed_(
    const ObBackupValidateTaskAttr &task_attr,
    bool &need_advance) const
{
  int ret = OB_SUCCESS;
  if (!task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]validate task attr is invalid", KR(ret), K(task_attr));
  } else {
    need_advance = true;
    const int64_t dest_id = task_attr.dest_id_;
    const uint64_t tenant_id = task_attr.tenant_id_;
    const bool need_lock = false;
    if (!job_attr_->validate_path_.is_empty()) {
      //skip
      // if specified validate path, can't get set status
      // so don't need to check set status
    } else if (task_attr.type_.is_backupset()) {
      const int64_t set_id = task_attr.validate_id_;
      bool is_available = false;
      if (OB_FAIL(check_backup_set_is_available_(tenant_id, set_id, is_available))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check backup set is available", KR(ret), K(tenant_id), K(set_id));
      } else {
        need_advance = is_available;
      }
    } else if (task_attr.type_.is_archivelog()) {
      if (task_attr.plus_archivelog_) {
        int64_t backup_set_id = 0;
        if (OB_FAIL(get_initiator_task_backup_set_id_(task_attr, backup_set_id))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to get initiator backup set id for complement piece", KR(ret), K(task_attr));
        } else {
          bool is_available = false;
          if (OB_FAIL(check_backup_set_is_available_(tenant_id, backup_set_id, is_available))) {
            LOG_WARN("[BACKUP_VALIDATE]failed to check backup set is available for complement piece", KR(ret),
                        K(tenant_id), K(backup_set_id), K(task_attr.initiator_task_id_));
          } else {
            need_advance = is_available;
          }
        }
      } else {
        share::ObArchivePersistHelper archive_persist_helper;
        ObTenantArchivePieceAttr piece_attr;
        const int64_t piece_id = task_attr.validate_id_;
        if (OB_FAIL(archive_persist_helper.init(tenant_id))) {
          LOG_WARN("[BACKUP_VALIDATE]fail to init archive persist helper", KR(ret));
        } else if (OB_FAIL(archive_persist_helper.get_piece(*sql_proxy_, dest_id, piece_id, need_lock, piece_attr))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to get archive piece", KR(ret), K(dest_id), K(piece_id));
        } else if (ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE == piece_attr.file_status_) {
          need_advance = true;
        } else {
          need_advance = false;
        }
      }
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::check_backup_set_is_available_(
    const uint64_t tenant_id,
    const int64_t backup_set_id,
    bool &is_available) const
{
  int ret = OB_SUCCESS;
  is_available = false;
  ObBackupSetFileDesc backup_set_desc;
  const bool need_lock = false;
  const int64_t incarnation = OB_START_INCARNATION;
  if (OB_INVALID_ID == tenant_id || backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_one_backup_set_file(*sql_proxy_, need_lock, backup_set_id,
                                                                          incarnation, tenant_id, backup_set_desc))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup set file", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE == backup_set_desc.file_status_) {
    is_available = true;
  } else {
    is_available = false;
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::get_initiator_task_backup_set_id_(
    const ObBackupValidateTaskAttr &task_attr,
    int64_t &backup_set_id) const
{
  int ret = OB_SUCCESS;
  backup_set_id = 0;
  if (!task_attr.is_valid() || !task_attr.type_.is_archivelog() || task_attr.initiator_task_id_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_attr));
  } else {
    ObBackupValidateTaskAttr initiator_task_attr;
    if (OB_FAIL(ObBackupValidateTaskOperator::get_backup_validate_task(
            *sql_proxy_, task_attr.initiator_task_id_, task_attr.tenant_id_, initiator_task_attr))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get initiator backup validate task", KR(ret),
          K(task_attr.tenant_id_), K(task_attr.initiator_task_id_));
    } else if (!initiator_task_attr.type_.is_backupset()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]initiator task is not backup set task", KR(ret), K(initiator_task_attr), K(task_attr));
    } else {
      backup_set_id = initiator_task_attr.validate_id_;
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::do_validate_tasks_(const ObArray<share::ObBackupValidateTaskAttr> &task_attrs)
{
  int ret = OB_SUCCESS;
  bool is_task_running = false;
  for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
    is_task_running = false;
    SMART_VAR(ObBackupValidateTaskMgr, task_mgr) {
      const ObBackupValidateTaskAttr &task_attr = task_attrs.at(i);
      if (ObBackupValidateStatus::Status::COMPLETED == task_attr.status_.status_
          || ObBackupValidateStatus::Status::CANCELED == task_attr.status_.status_
          || ObBackupValidateStatus::Status::FAILED == task_attr.status_.status_) {
        // do nothing
      } else if (OB_FAIL(task_mgr.init(tenant_id_, task_attr.task_id_, *job_attr_, *sql_proxy_,
          *rpc_proxy_, *task_scheduler_, *backup_validate_service_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to init validate task mgr", KR(ret));
      } else if (OB_FAIL(task_mgr.process())) {
        LOG_WARN("[BACKUP_VALIDATE]failed to process validate task", KR(ret), K(task_attr));
      } else if (OB_FAIL(task_mgr.check_task_running(is_task_running))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check task is running", KR(ret), K(task_attr));
      } else if (is_task_running) {
        LOG_INFO("[BACKUP_VALIDATE]validate task is running", K(task_attr));
        break;
      }
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::do_cancel_validate_()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupValidateTaskAttr> task_attrs;
  if (OB_FAIL(ObBackupValidateTaskOperator::get_tasks(*sql_proxy_, job_attr_->tenant_id_,
      job_attr_->job_id_, task_attrs))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup validate tasks", KR(ret));
  } else if (OB_FAIL(update_task_to_canceling_(task_attrs))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to update validate task canceling", KR(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
      SMART_VAR(ObBackupValidateTaskMgr, task_mgr) {
        const share::ObBackupValidateTaskAttr &task_attr = task_attrs.at(i);
        if (ObBackupValidateStatus::Status::CANCELING == task_attr.status_.status_) {
          if (OB_FAIL(task_mgr.init(tenant_id_, task_attr.task_id_, *job_attr_, *sql_proxy_,
              *rpc_proxy_, *task_scheduler_, *backup_validate_service_))) {
            LOG_WARN("[BACKUP_VALIDATE]failed to init validate task mgr", KR(ret));
          } else if (OB_FAIL(task_mgr.process())) {
            LOG_WARN("[BACKUP_VALIDATE]failed to cancel validate task", KR(ret), K(task_attr));
          }
        }
      }
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::update_task_to_canceling_(ObArray<share::ObBackupValidateTaskAttr> &task_attrs)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
    share::ObBackupValidateTaskAttr &task_attr = task_attrs.at(i);
    if (!task_attr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]validate task attr is invalid", KR(ret));
    } else if (ObBackupValidateStatus::Status::INIT == task_attr.status_.status_
        || ObBackupValidateStatus::Status::DOING == task_attr.status_.status_) {
      ObBackupValidateStatus next_status(ObBackupValidateStatus::Status::CANCELING);
      common::ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(task_attr.tenant_id_)))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret));
      } else if (OB_FAIL(ObBackupValidateTaskOperator::advance_task_status(trans, task_attr,
                                                  next_status, OB_SUCCESS, 0/*end_ts*/))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to advance task status", KR(ret));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to end trans", KR(ret), K(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::persist_set_task_()
{
  int ret = OB_SUCCESS;
  ObBackupValidateStatus next_status;
  ObMySQLTransaction trans;
  ObArray<share::ObBackupSetFileDesc> set_list;
  ObArray<ObPieceKey> piece_list;
  ObHashMap<int64_t/*backup_set_id*/, ObArray<ObPieceKey>> complement_piece_map;
  if (OB_FAIL(complement_piece_map.create(16, ObMemAttr(MTL_ID(), "BackupValidate")))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to create complement piece map", KR(ret));
  } else if (OB_FAIL(check_dest_validity_())) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest id", KR(ret));
  } else if (OB_FAIL(get_need_validate_infos_(set_list, piece_list, complement_piece_map))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get need validate backup infos", KR(ret), KPC(job_attr_));
  } else if (OB_FAIL(inner_persist_set_task_(set_list, piece_list, complement_piece_map))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to generate set task", KR(ret), KPC(job_attr_));
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::get_need_validate_infos_(
    ObArray<share::ObBackupSetFileDesc> &set_list,
    ObArray<ObPieceKey> &piece_list,
    ObHashMap<int64_t, ObArray<ObPieceKey>> &complement_piece_map)
{
  int ret = OB_SUCCESS;
  const ObBackupValidateType &validate_type = job_attr_->type_;
  const ObBackupPathString &validate_path = job_attr_->validate_path_;

  if (OB_FAIL(info_collector_.init(*sql_proxy_, *job_attr_, backup_dest_id_, archive_dest_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init info collector", KR(ret));
  } else {
    if (validate_type.need_validate_backup_set()) {
      if (OB_FAIL(info_collector_.collect_backup_set_info(set_list))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to collect backup set info", KR(ret), KPC(job_attr_));
      }
    }

    if (OB_SUCC(ret) && validate_type.need_validate_archive_piece()) {
      if (OB_FAIL(info_collector_.collect_archive_piece_info(piece_list))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to collect archive piece info", KR(ret), KPC(job_attr_));
      }
    }
  }

  if (OB_SUCC(ret) && !set_list.empty()) {
    if (OB_FAIL(info_collector_.collect_complement_log_piece_info(set_list, complement_piece_map))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to collect complement log piece info", KR(ret));
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::check_dest_validity_()
{
  int ret = OB_SUCCESS;
  bool is_backup_dest_exist = false;
  bool is_archive_dest_exist = false;
  if (!job_attr_->type_.is_archivelog() && OB_FAIL(check_data_backup_dest_validity_(is_backup_dest_exist))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check data backup dest validity", KR(ret), KPC(job_attr_));
  } else if (!job_attr_->type_.is_backupset() && OB_FAIL(check_log_archive_dest_validity_(is_archive_dest_exist))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check data archive dest validity", KR(ret), KPC(job_attr_));
  }
  if (OB_SUCC(ret) && !is_backup_dest_exist && !is_archive_dest_exist) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("[BACKUP_VALIDATE]validation dest not exist", KR(ret), KPC(job_attr_));
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::check_data_backup_dest_validity_(bool &is_backup_dest_exist)
{
  int ret = OB_SUCCESS;
  ObBackupPathString backup_dest_str;
  ObBackupDestMgr dest_mgr;
  share::ObBackupHelper backup_helper;
  ObBackupDest backup_dest;
  const uint64_t &tenant_id = job_attr_->tenant_id_;
  const int64_t &job_id = job_attr_->job_id_;
  const ObBackupPathString &validate_path = job_attr_->validate_path_;
  is_backup_dest_exist = true;
  // if validate_path_ not empty, the validate_path_ will be validated when parsing validation arguments.
  if (validate_path.is_empty()) {
    if (OB_FAIL(backup_helper.init(tenant_id, *sql_proxy_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to init backup helper", KR(ret), K(tenant_id));
    } else if (OB_FAIL(backup_helper.get_backup_dest(backup_dest_str))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("[BACKUP_VALIDATE]data backup dest not exist", K(ret), K(job_id), K(tenant_id));
        is_backup_dest_exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest", K(ret));
      }
    } else if (OB_FAIL(backup_dest.set(backup_dest_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set backup dest", KR(ret), K(backup_dest_str));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, tenant_id,
                                                                    backup_dest, backup_dest_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest id", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dest_mgr.init(tenant_id,
                                       ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA, backup_dest_str,  *sql_proxy_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to init dest mgr", KR(ret), K(backup_dest_str));
    } else if (OB_FAIL(dest_mgr.check_dest_validity(*rpc_proxy_, true/*need_format_file*/, false/*need_check_permission*/))) {
      LOG_WARN("[BACKUP_VALIDATE]fail to check dest validity",
                    KR(ret), K(job_id), K(tenant_id));
    }
  } else {
    // validate_path_ is not empty, get dest_id from validate_path_
    if (OB_FAIL(backup_dest.set_storage_path(validate_path.str()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set backup dest", KR(ret), K(validate_path));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, tenant_id,
                                                                    backup_dest, backup_dest_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest id", KR(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::check_log_archive_dest_validity_(bool &is_archive_dest_exist)
{
  int ret = OB_SUCCESS;
  ObBackupPathString archive_dest_str;
  share::ObArchivePersistHelper helper;
  ObBackupDestMgr dest_mgr;
  ObBackupDest archive_dest;
  const int64_t dest_no = 0;
  const uint64_t &tenant_id = job_attr_->tenant_id_;
  const int64_t &job_id = job_attr_->job_id_;
  const ObBackupPathString &validate_path = job_attr_->validate_path_;
  is_archive_dest_exist = true;
  // if validate_path_ not empty, the validate_path_ will be validated when parsing validation arguments.
  if (validate_path.is_empty()) {
    if (OB_FAIL(helper.init(tenant_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to init archive helper", KR(ret), K(tenant_id));
    } else if (OB_FAIL(helper.get_archive_dest(*sql_proxy_, false/*need_lock*/, dest_no, archive_dest_str))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("[BACKUP_VALIDATE]archive dest not exist", K(ret), K(job_id), K(tenant_id));
        is_archive_dest_exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[BACKUP_VALIDATE]failed to get archive dest", K(ret));
      }
    } else if (OB_FAIL(archive_dest.set(archive_dest_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set archive dest", KR(ret), K(archive_dest_str));
    } else if (OB_FAIL(helper.get_dest_id(*sql_proxy_, false/*need_lock*/, dest_no, archive_dest_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get archive dest id", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dest_mgr.init(tenant_id, ObBackupDestType::DEST_TYPE_ARCHIVE_LOG,
                                archive_dest_str, *sql_proxy_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to init dest mgr", K(ret), K(job_id), K(tenant_id));
    } else if (OB_FAIL(dest_mgr.check_dest_validity(*rpc_proxy_, true/*need_format_file*/, false/*need_check_permission*/))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check dest validity", K(ret), K(job_id), K(tenant_id));
    }
  } else {
    // validate_path_ is not empty, get dest_id from validate_path_
    if (OB_FAIL(archive_dest.set_storage_path(validate_path.str()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set archive dest", KR(ret), K(validate_path));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, tenant_id,
                                                                    archive_dest, archive_dest_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get archive dest id", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::get_backup_set_info_by_desc_(
    const share::ObBackupSetDesc &desc,
    storage::ObExternBackupSetInfoDesc &backup_set_info)
{
  int ret = OB_SUCCESS;
  ObBackupPathString &path = job_attr_->validate_path_;
  ObBackupDataStore store;
  ObBackupDest validate_dest;

  if (path.is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]backup validate path is empty", KR(ret), K(path));
  } else if (OB_FAIL(validate_dest.set(path))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to set validate dest", KR(ret), KPC(job_attr_));
  } else if (OB_FAIL(store.init(validate_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init backup data store", KR(ret), K(validate_dest));
  } else if (OB_FAIL(store.read_backup_set_info_by_desc(desc, backup_set_info))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to read backup set info by desc", KR(ret), K(desc));
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::inner_persist_set_task_(
    const ObArray<share::ObBackupSetFileDesc> &set_list,
    const ObArray<share::ObPieceKey> &piece_list,
    const common::hash::ObHashMap<int64_t, ObArray<share::ObPieceKey>> &complement_piece_map)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;

  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret));
  } else if (!set_list.empty() && OB_FAIL(persist_backup_set_tasks_(trans, set_list, complement_piece_map))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to persist backup set tasks", KR(ret));
  } else if (!piece_list.empty() && OB_FAIL(persist_archive_piece_tasks_(trans, piece_list))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to persist archive piece tasks", KR(ret));
  } else {
    ObBackupValidateStatus next_status;
    next_status.status_ = ObBackupValidateStatus::Status::DOING;
    job_attr_->task_count_ = set_list.count() + piece_list.count();
    for (ObHashMap<int64_t, ObArray<share::ObPieceKey>>::const_iterator iter = complement_piece_map.begin();
            iter != complement_piece_map.end(); ++iter) {
      job_attr_->task_count_ += iter->second.count();
    }
    if (OB_FAIL(advance_status(trans, next_status, job_attr_->result_, job_attr_->end_ts_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to advance job status", KR(ret));
    } else if (OB_FAIL(ObBackupValidateJobOperator::update_task_count(trans, *job_attr_, true/*is_total*/))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update job task count", KR(ret), KPC(job_attr_));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to commit trans", KR(ret));
      } else {
        backup_validate_service_->wakeup();
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(false))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to roll back trans", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::persist_backup_set_tasks_(
    common::ObISQLClient &trans,
    const ObArray<share::ObBackupSetFileDesc> &set_list,
    const ObHashMap<int64_t, ObArray<share::ObPieceKey>> &complement_piece_map)
{
  int ret = OB_SUCCESS;
  if (set_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]set list is empty", KR(ret));
  } else {
    ObBackupValidateTaskAttr task_attr;
    for (int64_t i = 0; OB_SUCC(ret) && i < set_list.count(); i++) {
      const share::ObBackupSetFileDesc &backup_set_info = set_list.at(i);
      task_attr.reset();
      if (OB_FAIL(construct_backup_set_task_(backup_set_info, task_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to construct backup set task", KR(ret), K(backup_set_info));
      } else if (OB_FAIL(backup_validate_service_->check_leader())) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check leader", KR(ret));
      } else if (OB_FAIL(ObBackupValidateTaskOperator::insert_task(trans, task_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to insert backup set task", KR(ret), K(task_attr));
      } else {
        LOG_INFO("[BACKUP_VALIDATE]successfully inserted backup set task", K(task_attr), K(backup_set_info));
        ObArray<share::ObPieceKey> piece_keys;
        if (OB_FAIL(complement_piece_map.get_refactored(backup_set_info.backup_set_id_, piece_keys))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("[BACKUP_VALIDATE]failed to get complement piece keys", KR(ret), K(backup_set_info.backup_set_id_));
          }
        } else if (OB_FAIL(persist_complement_piece_tasks_(trans, task_attr, piece_keys))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to persist complement piece tasks", KR(ret), K(piece_keys), K(task_attr));
        }
      }
    }
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::persist_complement_piece_tasks_(
    common::ObISQLClient &trans,
    const ObBackupValidateTaskAttr &task_attr,
    const ObArray<share::ObPieceKey> &piece_keys)
{
  int ret = OB_SUCCESS;
  if (piece_keys.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]piece keys is empty", KR(ret));
  } else {
    ObBackupValidateTaskAttr complement_task_attr;
    ObBackupDest complement_dest;
    ObBackupDest piece_dest;
    ObBackupPath piece_path;
    const ObBackupPathString &set_path = task_attr.validate_path_;
    const uint64_t &tenant_id = task_attr.tenant_id_;
    const int64_t &dest_id = task_attr.dest_id_;
    if (OB_FAIL(get_complement_dest_(tenant_id, dest_id, set_path, complement_dest))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get complement dest", KR(ret), K(tenant_id), K(dest_id), K(set_path));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < piece_keys.count(); j++) {
      complement_task_attr.reset();
      piece_dest.reset();
      piece_path.reset();
      const share::ObPieceKey &piece_key = piece_keys.at(j);
      if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(complement_dest, piece_key.dest_id_, piece_key.round_id_,
                                                            piece_key.piece_id_, piece_path))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to get piece dir path", KR(ret), K(piece_key), K(complement_dest));
      } else if (OB_FAIL(piece_dest.set(piece_path.get_ptr(), complement_dest.get_storage_info()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to set backup dest", KR(ret), K(piece_path));
      } else if (OB_FAIL(construct_piece_task_common_(piece_dest, piece_key, true/*is_complement*/,
                                                      task_attr.task_id_, complement_task_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to construct complement piece task", KR(ret), K(piece_key), K(task_attr.task_id_));
      } else if (FALSE_IT(complement_task_attr.dest_id_ = dest_id)) {
      } else if (OB_FAIL(ObBackupValidateTaskOperator::insert_task(trans, complement_task_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to insert complement piece task", KR(ret), K(complement_task_attr));
      } else {
        LOG_INFO("[BACKUP_VALIDATE]successfully inserted complement piece task",
                K(complement_task_attr), K(piece_key), K(task_attr.task_id_));
      }
    }
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::get_complement_dest_(
    const uint64_t tenant_id,
    const int64_t dest_id,
    const ObBackupPathString &set_path,
    ObBackupDest &complement_dest)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_tenant_dest;
  ObBackupDest set_dest;
  char raw_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == dest_id || set_path.is_empty() || OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(dest_id), K(set_path), KP(sql_proxy_));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*sql_proxy_, tenant_id, dest_id,
                                                                            backup_tenant_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest by dest id", KR(ret), K(dest_id), K(tenant_id));
  } else if (OB_FAIL(ObBackupUtils::get_raw_path(set_path.ptr(), raw_path, OB_MAX_BACKUP_PATH_LENGTH))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get raw path", KR(ret), K(set_path));
  } else if (OB_FAIL(set_dest.set(raw_path, backup_tenant_dest.get_storage_info()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to set set dest", KR(ret), K(set_path), K(backup_tenant_dest));
  } else if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(set_dest, complement_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get complement dest", KR(ret), K(set_dest));
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::persist_archive_piece_tasks_(
    common::ObISQLClient &trans,
    const ObArray<share::ObPieceKey> &piece_list)
{
  int ret = OB_SUCCESS;
  if (piece_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]piece list is empty", KR(ret));
  } else {
    ObBackupValidateTaskAttr task_attr;
    ObBackupDest piece_dest;
    for (int64_t i = 0; OB_SUCC(ret) && i < piece_list.count(); i++) {
      const share::ObPieceKey &piece_key = piece_list.at(i);
      task_attr.reset();
      piece_dest.reset();
      if (OB_FAIL(get_effective_piece_dir_path(job_attr_, piece_key, piece_dest))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to get effective piece dir path", KR(ret), K(piece_key));
      } else if (OB_FAIL(construct_piece_task_common_(piece_dest, piece_key,
                                                          false/*is_complement*/, 0/*initiator_task_id*/, task_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to construct archive piece task", KR(ret), K(piece_key));
      } else if (OB_FAIL(backup_validate_service_->check_leader())) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check leader", KR(ret));
      } else if (OB_FAIL(ObBackupValidateTaskOperator::insert_task(trans, task_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to insert archive piece task", KR(ret), K(task_attr));
      } else {
        LOG_INFO("[BACKUP_VALIDATE]successfully inserted archive piece task", K(task_attr), K(piece_key));
      }
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::construct_backup_set_task_(
    const share::ObBackupSetFileDesc &backup_set_info,
    ObBackupValidateTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest set_dest;
  int batch_size = 1;
  if (OB_FAIL(ObLSBackupInfoOperator::get_next_task_id(*sql_proxy_, tenant_id_, batch_size, task_attr.task_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get next task id", KR(ret));
  } else if (-1 == task_attr.task_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]invalid task id", KR(ret), K(task_attr.task_id_));
  } else if (OB_FAIL(get_effective_set_dir_path(job_attr_, backup_set_info, set_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get effective set dir path", KR(ret), K(backup_set_info));
  } else if (OB_FAIL(set_dest.get_backup_path_str(task_attr.validate_path_.ptr(), task_attr.validate_path_.capacity()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup path", KR(ret), K(set_dest));
  } else {
    task_attr.tenant_id_ = job_attr_->tenant_id_;
    task_attr.incarnation_id_ = job_attr_->incarnation_id_;
    task_attr.job_id_ = job_attr_->job_id_;
    task_attr.type_ = ObBackupValidateType::ValidateType::BACKUPSET;
    task_attr.path_type_ = ObBackupValidatePathType::ValidatePathType::BACKUP_SET_DEST;
    task_attr.dest_id_ = backup_dest_id_;
    task_attr.plus_archivelog_ = backup_set_info.plus_archivelog_;
    task_attr.initiator_task_id_ = 0;
    task_attr.validate_id_ = backup_set_info.backup_set_id_;
    task_attr.validate_level_ = job_attr_->level_.level_;
    task_attr.round_id_ = 0;
    task_attr.start_ts_ = ObTimeUtility::current_time();
    task_attr.end_ts_ = 0;
    task_attr.status_ = ObBackupValidateStatus::Status::INIT;
    task_attr.result_ = OB_SUCCESS;
    task_attr.can_retry_ = true;
    task_attr.retry_count_ = 0;
    task_attr.total_ls_count_ = 0;
    task_attr.finish_ls_count_ = 0;
    task_attr.total_bytes_ = 0;
    task_attr.validate_bytes_ = 0;
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::construct_piece_task_common_(
    const share::ObBackupDest &piece_dest,
    const share::ObPieceKey &piece_key,
    const bool is_complement,
    const int64_t initiator_task_id,
    ObBackupValidateTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  int batch_size = 1;
  if (OB_FAIL(ObLSBackupInfoOperator::get_next_task_id(*sql_proxy_, tenant_id_, batch_size, task_attr.task_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get next task id", KR(ret));
  } else if (-1 == task_attr.task_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]invalid task id", KR(ret), K(task_attr.task_id_));
  }  else if (OB_FAIL(piece_dest.get_backup_path_str(task_attr.validate_path_.ptr(), task_attr.validate_path_.capacity()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup path", KR(ret), K(piece_dest));
  } else {
    task_attr.tenant_id_ = job_attr_->tenant_id_;
    task_attr.incarnation_id_ = job_attr_->incarnation_id_;
    task_attr.job_id_ = job_attr_->job_id_;
    task_attr.type_ = ObBackupValidateType::ValidateType::ARCHIVELOG_PIECE;
    task_attr.path_type_ = ObBackupValidatePathType::ValidatePathType::ARCHIVELOG_PIECE_DEST;
    task_attr.dest_id_ = archive_dest_id_;
    task_attr.plus_archivelog_ = is_complement;
    task_attr.initiator_task_id_ = initiator_task_id;
    task_attr.validate_id_ = piece_key.piece_id_;
    task_attr.validate_level_ = job_attr_->level_;
    task_attr.round_id_ = piece_key.round_id_;
    task_attr.start_ts_ = ObTimeUtility::current_time();
    task_attr.end_ts_ = 0;
    task_attr.status_ = ObBackupValidateStatus::Status::INIT;
    task_attr.result_ = OB_SUCCESS;
    task_attr.can_retry_ = true;
    task_attr.retry_count_ = 0;
    task_attr.total_ls_count_ = 0;
    task_attr.finish_ls_count_ = 0;
    task_attr.total_bytes_ = 0;
    task_attr.validate_bytes_ = 0;
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::get_effective_set_dir_path(
    const share::ObBackupValidateJobAttr *job_attr,
    const share::ObBackupSetFileDesc &backup_set_info,
    share::ObBackupDest &set_dest)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;
  share::ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info.backup_set_id_;
  desc.backup_type_ = backup_set_info.backup_type_;
  share::ObBackupPath set_path;
  const uint64_t &tenant_id = backup_set_info.tenant_id_;
  const int64_t &dest_id = backup_dest_id_;

  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*sql_proxy_, tenant_id, dest_id, backup_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest by dest id", KR(ret), K(tenant_id), K(dest_id));
  } else if (job_attr->validate_path_.is_empty() || job_attr->path_type_.is_dest_level_path()) {
    if (OB_FAIL(ObBackupPathUtil::get_backup_set_dir_path(backup_dest, desc, set_path))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get backup set dir path", KR(ret), K(backup_set_info));
    }
  } else if (OB_FAIL(set_path.init(ObString(backup_dest.get_root_path())))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign validate path", KR(ret), K(job_attr->validate_path_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_dest.set(set_path.get_ptr(), backup_dest.get_storage_info()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to set backup dest", KR(ret), K(set_path));
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::get_effective_piece_dir_path(
    const share::ObBackupValidateJobAttr *job_attr,
    const share::ObPieceKey &piece_key,
    share::ObBackupDest &piece_dest)
{
  int ret = OB_SUCCESS;
  const uint64_t &tenant_id = job_attr->tenant_id_;
  const int64_t &dest_id = archive_dest_id_;

  if (job_attr->validate_path_.is_empty() || job_attr->path_type_.is_dest_level_path()) {
    share::ObBackupDest archive_dest;
    share::ObBackupPath piece_path;
    if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*sql_proxy_,
                                                                            tenant_id, dest_id, archive_dest))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get archive dest by dest id", KR(ret), K(tenant_id), K(dest_id));
    } else if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(archive_dest, piece_key.dest_id_, piece_key.round_id_,
                                                      piece_key.piece_id_, piece_path))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get archive piece dir path", KR(ret), K(piece_key));
    } else if (OB_FAIL(piece_dest.set(piece_path.get_ptr(), archive_dest.get_storage_info()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set backup dest", KR(ret), K(piece_path));
    }
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*sql_proxy_, tenant_id,
                                                                                dest_id, piece_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get archive dest by dest id", KR(ret), K(tenant_id), K(dest_id));
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::move_to_history_()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP_VALIDATE]start move_task_to_history");
  ObArray<share::ObBackupValidateTaskAttr> task_attrs;
  if (is_sys_tenant(job_attr_->initiator_tenant_id_) && OB_FAIL(report_failure_to_initiator_())) {
    LOG_WARN("[BACKUP_VALIDATE]fail to report job finish to initiator tenant id", KR(ret), KPC(job_attr_));
  } else if (OB_FAIL(ObBackupValidateTaskOperator::get_tasks(*sql_proxy_, job_attr_->tenant_id_,
      job_attr_->job_id_, task_attrs))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup validate tasks", KR(ret));
  }  else {
    // (1) The number of set tasks may be large.
    // (2) Before executing move_to_history, the job and tasks status have been pushed to COMPLETED/FAILED/CANCELED,
    // even if some set tasks are moved into the history table and an error occurs,
    // the job move_to_history process is still re-enterable.
    // (3) Based on the above analysis, there is no need to use trans at the job level.
    for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
      SMART_VAR(ObBackupValidateTaskMgr, task_mgr) {
        const share::ObBackupValidateTaskAttr &task_attr = task_attrs.at(i);
        if (OB_FAIL(task_mgr.init(tenant_id_, task_attr.task_id_, *job_attr_, *sql_proxy_,
          *rpc_proxy_, *task_scheduler_, *backup_validate_service_))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to init validate task mgr", KR(ret));
        } else if (OB_FAIL(task_mgr.do_clean_up())) {
          LOG_WARN("[BACKUP_VALIDATE]failed to task move history", KR(ret), K(task_attr));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(inner_move_to_history_())) {
      LOG_WARN("[BACKUP_VALIDATE]failed to move job to history", KR(ret), KPC(job_attr_));
    }
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::inner_move_to_history_()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;

  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret));
  } else {
    if (!job_attr_->validate_path_.is_empty()) {
      // If validate_path is not empty, remove backup storage info with DEST_TYPE_BACKUP_VALIDATE
      if (OB_FAIL(ObBackupStorageInfoOperator::remove_backup_storage_info(trans, job_attr_->tenant_id_,
                      ObBackupDestType::TYPE::DEST_TYPE_BACKUP_VALIDATE, job_attr_->validate_path_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to remove backup storage info for validate", KR(ret), KPC(job_attr_));
      }
    }
    if (FAILEDx(ObBackupValidateJobOperator::move_to_history(trans, job_attr_->tenant_id_, job_attr_->job_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to move job to history", KR(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to commit trans", KR(ret));
      } else {
        LOG_INFO("[BACKUP_VALIDATE]succeed to move job to history. backup validate job finish",
            "tenant_id", job_attr_->tenant_id_, "job_id", job_attr_->job_id_);
        backup_validate_service_->wakeup();
        // wakeup sys tenant backup validate service to advance sys job status
        // if the job is initiated by sys tenant
        if (is_sys_tenant(job_attr_->initiator_tenant_id_)) {
          backup_validate_service_->wakeup_tenant_service(job_attr_->initiator_tenant_id_);
        }
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(false))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to roll back status", KR(ret), K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObUserTenantBackupValidateJobMgr::report_failure_to_initiator_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupValidateJobOperator::report_failed_to_sys_tenant(*sql_proxy_, *job_attr_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to report failed to sys tenant", KR(ret));
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::advance_status_canceled()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupValidateTaskAttr> task_attrs;
  bool has_doing_task = false;
  bool has_failed_task = false;
  int64_t success_task_count = 0;
  if (OB_FAIL(ObBackupValidateTaskOperator::get_tasks(*sql_proxy_, job_attr_->tenant_id_,
      job_attr_->job_id_, task_attrs))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup validate tasks", KR(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < task_attrs.count(); i++) {
      const share::ObBackupValidateTaskAttr &task_attr = task_attrs.at(i);
      if (!task_attr.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]validate task attr is invalid", KR(ret));
      } else if (ObBackupValidateStatus::Status::INIT == task_attr.status_.status_
          || ObBackupValidateStatus::Status::DOING == task_attr.status_.status_
          || ObBackupValidateStatus::Status::CANCELING == task_attr.status_.status_) {
        has_doing_task = true;
      } else if (ObBackupValidateStatus::Status::FAILED == task_attr.status_.status_ && !has_failed_task) {
        has_failed_task = true;
        job_attr_->result_ = task_attr.result_;
      } else if (ObBackupValidateStatus::Status::COMPLETED == task_attr.status_.status_) {
        success_task_count++;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!has_doing_task) {
    share::ObBackupValidateStatus next_status;
    job_attr_->success_task_count_ = success_task_count;
    next_status.status_ = ObBackupValidateStatus::Status::CANCELED;
    job_attr_->end_ts_ = ObTimeUtility::current_time();
    job_attr_->result_ = (true == has_failed_task) ? job_attr_->result_ : OB_CANCELED;
    if (OB_FAIL(advance_status(*sql_proxy_, next_status, job_attr_->result_, job_attr_->end_ts_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to advance_job_status", KR(ret));
    } else if (OB_FAIL(ObBackupValidateJobOperator::update_task_count(*sql_proxy_, *job_attr_, false/*is_total*/))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update job task count", KR(ret));
    }
  }
  return ret;
}

int ObUserTenantBackupValidateJobMgr::cancel_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(do_cancel_validate_())) {
    LOG_WARN("[BACKUP_VALIDATE]failed to cancel validate tasks", KR(ret));
  } else if (OB_FAIL(advance_status_canceled())) {
    LOG_WARN("[BACKUP_VALIDATE]failed to advance status canceled", KR(ret));
  } else {
    backup_validate_service_->wakeup();
  }

  return ret;
}

int ObBackupValidateScheduler::insert_backup_storage_info_for_validate_(
    common::ObMySQLTransaction &trans,
    const ObBackupDest &validate_dest,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t dest_id = 0;
  const ObBackupDestType::TYPE backup_dest_type = ObBackupDestType::TYPE::DEST_TYPE_BACKUP_VALIDATE;
  if (OB_FAIL(ObLSBackupInfoOperator::get_next_dest_id(trans, tenant_id, dest_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get next dest id", KR(ret), K(tenant_id));
  } else {
    ObBackupIOInfo io_info;
    io_info.max_iops_ = validate_dest.get_storage_info()->max_iops_;
    io_info.max_bandwidth_ = validate_dest.get_storage_info()->max_bandwidth_;
    if (OB_FAIL(ObBackupStorageInfoOperator::insert_backup_storage_info(trans, tenant_id, validate_dest,
                                                                        backup_dest_type, dest_id, io_info,
                                                                        false/*can_update*/))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        //The user specified path validation
        //The path may already exist in the __all_backup_storage_info table,
        // such as the validation path is the current backup path or a previously used backup path
        //So the validation path encounters a pk duplicate, which is expected
        LOG_INFO("[BACKUP_VALIDATE]backup storage info already exists", KR(ret), K(tenant_id));
        ret = OB_SUCCESS;
        trans.reset_last_error();
      } else {
        LOG_WARN("[BACKUP_VALIDATE]failed to insert backup storage info", KR(ret), K(tenant_id));
      }
    } else {
      LOG_INFO("[BACKUP_VALIDATE]successfully inserted backup storage info for validate",
                K(tenant_id), K(dest_id));
    }
  }
  return ret;
}

/*
*----------------------ObSysTenantBackupValidateJobMgr-------------------------
*/
int ObSysTenantBackupValidateJobMgr::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]backup validate mgr not init", KR(ret));
  } else {
    ObBackupValidateStatus::Status status = job_attr_->status_.status_;
    switch (status) {
      case ObBackupValidateStatus::Status::INIT: {
        if (OB_FAIL(handle_user_tenant_backup_validate_())) {
          LOG_WARN("[BACKUP_VALIDATE]failed to handle user tenant job", KR(ret));
        }
        break;
      }
      case ObBackupValidateStatus::Status::DOING: {
        if (OB_FAIL(statistic_user_tenant_job_())) {
          LOG_WARN("[BACKUP_VALIDATE]failed to statistic user tenant job", KR(ret), KPC(job_attr_));
        }
        break;
      }
      case ObBackupValidateStatus::Status::COMPLETED:
      case ObBackupValidateStatus::Status::FAILED:
      case ObBackupValidateStatus::Status::CANCELED: {
        if (OB_FAIL(move_to_history_())) {
          LOG_WARN("[BACKUP_VALIDATE]failed to move to history", KR(ret), KPC(job_attr_));
        }
        break;
      }
      case ObBackupValidateStatus::Status::CANCELING: {
        if (OB_FAIL(cancel_user_tenant_job_())) {
          LOG_WARN("[BACKUP_VALIDATE]failed to cancel backup delete job", KR(ret), KPC(job_attr_));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]unknown backup validate status", KR(ret), KPC(job_attr_));
        break;
      }
    }
  }
  return ret;
}

int ObSysTenantBackupValidateJobMgr::handle_user_tenant_backup_validate_()
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  int64_t cnt = 0;
  const int64_t executor_tenant_ids_count = job_attr_->executor_tenant_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < executor_tenant_ids_count; ++i) {
    is_valid = false;
    cnt = 0;
    const uint64_t tenant_id = job_attr_->executor_tenant_ids_.at(i);
    if (OB_FAIL(ObBackupValidateScheduler::check_tenant_status(*schema_service_, tenant_id, is_valid))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check tenant status", KR(ret), K(tenant_id));
    } else if (!is_valid) {
      cnt++;
    } else if (OB_FAIL(ObBackupValidateJobOperator::cnt_jobs(*sql_proxy_,
                                              tenant_id, job_attr_->tenant_id_, job_attr_->job_id_, cnt))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to cnt jobs", KR(ret), K(tenant_id));
    } else if (cnt != 0) {
      LOG_WARN("[BACKUP_VALIDATE]user tenant job has been inserted, just pass", K(tenant_id));
    } else if (OB_FAIL(do_handle_user_tenant_backup_validate_(tenant_id))) {
      if (!(ObBackupUtils::is_need_retry_error(ret) && 1 != executor_tenant_ids_count)) {
        LOG_WARN("[BACKUP_VALIDATE]tenant can't do validation now, just pass", KR(ret));
        share::ObTaskId trace_id(*ObCurTraceId::get_trace_id());
        ROOTSERVICE_EVENT_ADD("backup_validate", "handle_user_tenant_backup_validate_",
                                  K(tenant_id), K(ret), K(trace_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[BACKUP_VALIDATE]failed to handle user tenant backup validate", KR(ret), K(tenant_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObBackupValidateStatus next_status(ObBackupValidateStatus::Status::DOING);
    if (OB_FAIL(advance_status(*sql_proxy_, next_status, OB_SUCCESS/*job result*/, job_attr_->end_ts_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to advance status",
                        KR(ret), KPC(job_attr_), K(next_status));
    } else {
      backup_validate_service_->wakeup();
      LOG_INFO("[BACKUP_VALIDATE]handle user tenant backup validate success", KPC(job_attr_), K(next_status));
    }
  }
  return ret;
}

int ObSysTenantBackupValidateJobMgr::do_handle_user_tenant_backup_validate_(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObAddr rs_addr;
  obrpc::ObBackupValidateArg arg;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id));
  } else {
    char validate_dest_buf[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
    arg.tenant_id_ = tenant_id;
    arg.initiator_tenant_id_ = job_attr_->tenant_id_;
    arg.initiator_job_id_ = job_attr_->job_id_;
    arg.validate_type_.type_ = job_attr_->type_.type_;
    arg.validate_level_.level_ = job_attr_->level_.level_;
    if (!job_attr_->validate_path_.is_empty()) {
      ObBackupDest validate_dest;
      const uint64_t storage_tenant_id = job_attr_->tenant_id_;
      if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, storage_tenant_id,
              job_attr_->validate_path_, validate_dest))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to get validate dest", KR(ret), K(storage_tenant_id), KPC(job_attr_));
      } else if (OB_FAIL(validate_dest.get_backup_dest_str(validate_dest_buf, sizeof(validate_dest_buf)))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to get validate dest str", KR(ret), K(validate_dest));
      }
    }
    if (FAILEDx(arg.validate_dest_.assign(validate_dest_buf))) {
      LOG_WARN("[BACKUP_VALIDATE]fail to assign validate path", KR(ret), K(validate_dest_buf));
    } else if (!job_attr_->logarchive_piece_ids_.empty()
                  && OB_FAIL(arg.set_or_piece_ids_.assign(job_attr_->logarchive_piece_ids_))) {
      LOG_WARN("[BACKUP_VALIDATE]fail to assign logarchive piece ids", KR(ret), K(job_attr_->logarchive_piece_ids_));
    } else if (!job_attr_->backup_set_ids_.empty()
                  && OB_FAIL(arg.set_or_piece_ids_.assign(job_attr_->backup_set_ids_))) {
      LOG_WARN("[BACKUP_VALIDATE]fail to assign backup set ids", KR(ret), K(job_attr_->backup_set_ids_));
    } else if (OB_FAIL(arg.description_.assign(job_attr_->description_))) {
      LOG_WARN("[BACKUP_VALIDATE]fail to assign description", KR(ret), K(job_attr_->description_));
    } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("failed to get rootservice address", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).backup_validate(arg))) {
      LOG_WARN("failed to post backup ls data res", K(ret), K(arg));
    } else {
      LOG_INFO("succeed handle user backup tenant", K(arg));
    }
  }
  return ret;
}

int ObSysTenantBackupValidateJobMgr::statistic_user_tenant_job_()
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  int64_t cnt = 0;
  int64_t finish_user_validate_job = 0;
  ObBackupValidateJobAttr tmp_job_attr;
  ObBackupValidateStatus next_status;
  const uint64_t tenant_id = job_attr_->tenant_id_;
  const int64_t job_id = job_attr_->job_id_;
  const ObSArray<uint64_t> &executor_tenant_ids = job_attr_->executor_tenant_ids_;

  LOG_INFO("[BACKUP_VALIDATE]sys tenant start to statistic user tenant job", KPC(job_attr_));
  for (int64_t i = 0; OB_SUCC(ret) && i < executor_tenant_ids.count(); ++i) {
    const uint64_t user_tenant_id = executor_tenant_ids.at(i);
    is_valid = false;
    cnt = 0;
    if (OB_FAIL(ObBackupValidateScheduler::check_tenant_status(*schema_service_, user_tenant_id, is_valid))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check tenant status", KR(ret), K(user_tenant_id));
    } else if (!is_valid) {
      finish_user_validate_job++;
    } else if (OB_FAIL(ObBackupValidateJobOperator::cnt_jobs(*sql_proxy_, user_tenant_id,
                                                                tenant_id, job_id, cnt))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to cnt jobs initiated by cur sys tenant job", KR(ret), K(user_tenant_id));
    } else if (0 == cnt) {
      finish_user_validate_job++;
      LOG_INFO("[BACKUP_VALIDATE]user tenant job has been finished", K(user_tenant_id));
    }
  }
  if (OB_SUCC(ret) && finish_user_validate_job == executor_tenant_ids.count()) {
    if (OB_FAIL(ObBackupValidateJobOperator::get_job(*sql_proxy_, false /*need lock*/, tenant_id_, job_id,
                                                        false /*is_initiator*/, tmp_job_attr))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get job", KR(ret), K(tenant_id_), "job_id", job_id);
    } else if (tmp_job_attr.result_ != OB_SUCCESS) {
      next_status.status_ = ObBackupValidateStatus::Status::FAILED;
    } else {
      next_status.status_ = ObBackupValidateStatus::Status::COMPLETED;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObBackupValidateJobOperator::advance_job_status(*sql_proxy_, *job_attr_, next_status,
                                                            tmp_job_attr.result_, ObTimeUtility::current_time()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to advance job status",
                        KR(ret), KPC(job_attr_), K(next_status), "job_id", job_id);
      } else {
        backup_validate_service_->wakeup();
        LOG_INFO("[BACKUP_VALIDATE]user job finished, sys job move to next status",
                    KR(ret), K(next_status), KPC(job_attr_));
      }
    }
  }
  return ret;
}

int ObSysTenantBackupValidateJobMgr::move_to_history_()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret));
  } else {
    if (OB_FAIL(backup_validate_service_->check_leader())) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check leader", KR(ret));
    } else if (OB_FAIL(ObBackupValidateJobOperator::move_to_history(trans, job_attr_->tenant_id_, job_attr_->job_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to move to history", KR(ret), KPC(job_attr_));
    } else if (!job_attr_->validate_path_.is_empty()) {
      const uint64_t tenant_id = job_attr_->tenant_id_;
      if (OB_FAIL(ObBackupStorageInfoOperator::remove_backup_storage_info(trans, tenant_id,
              ObBackupDestType::TYPE::DEST_TYPE_BACKUP_VALIDATE, job_attr_->validate_path_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to remove backup storage info for sys tenant", KR(ret),
            K(tenant_id), KPC(job_attr_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to commit trans", KR(ret));
      } else {
        LOG_INFO("[BACKUP_VALIDATE]succeed to move to history", KPC(job_attr_));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(false))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to roll back status", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObSysTenantBackupValidateJobMgr::cancel_user_tenant_job_()
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  int64_t finish_canceled_job = 0;
  const int64_t end_ts = 0;
  const int64_t job_id = job_attr_->job_id_;
  const ObSArray<uint64_t> &executor_tenant_ids = job_attr_->executor_tenant_ids_;

  for (int64_t i = 0; OB_SUCC(ret) && i < executor_tenant_ids.count(); ++i) {
    const uint64_t user_tenant_id = executor_tenant_ids.at(i);
    is_valid = false;
    ObBackupValidateJobAttr tmp_job_attr;
    if (OB_FAIL(ObBackupValidateScheduler::check_tenant_status(*schema_service_, user_tenant_id, is_valid))) {
      LOG_WARN("[BACKUP_VALIDATE]fail to check tenant status", K(ret), K(user_tenant_id));
    } else if (!is_valid) {
      finish_canceled_job++;
      LOG_WARN("[BACKUP_VALIDATE]tenant status not valid, just pass the tenant", K(user_tenant_id), K(is_valid));
    } else if (OB_FAIL(ObBackupValidateJobOperator::get_job(*sql_proxy_, false/*need_lock*/,
        user_tenant_id, job_id, true/*is_initiator*/, tmp_job_attr))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        finish_canceled_job++;
        ret = OB_SUCCESS;
        LOG_INFO("[BACKUP_VALIDATE]tenant backup validate job has finished", K(user_tenant_id), KPC(job_attr_));
      } else {
        LOG_WARN("[BACKUP_VALIDATE]fail to get validate job",
                    K(ret), K(user_tenant_id), "initiator job id", job_id);
      }
    } else if (tmp_job_attr.status_.status_ == ObBackupValidateStatus::Status::INIT
        || tmp_job_attr.status_.status_ == ObBackupValidateStatus::Status::DOING) {
          ObBackupValidateStatus next_status(ObBackupValidateStatus::Status::CANCELING);
      if (OB_FAIL(backup_validate_service_->check_leader())) {
        LOG_WARN("[BACKUP_VALIDATE]fail to check leader", K(ret));
      } else if (OB_FAIL(ObBackupValidateJobOperator::advance_job_status(*sql_proxy_, tmp_job_attr,
                                                                              next_status, OB_SUCCESS, end_ts))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to advance user job to CANCELING", K(ret), K(tmp_job_attr), K(next_status));
      } else {
        FLOG_INFO("[BACKUP_VALIDATE]succeed advance user tenant job to CANCELING", K(tmp_job_attr));
        // wakeup user tenant backup validate service to handle cancel
        backup_validate_service_->wakeup_tenant_service(user_tenant_id);
      }
    }
  }

  if (OB_SUCC(ret) && finish_canceled_job == executor_tenant_ids.count()) {
    ObBackupValidateStatus next_status(ObBackupValidateStatus::Status::CANCELED);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(advance_status(*sql_proxy_, next_status, OB_CANCELED, ObTimeUtility::current_time()))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to advance sys job status", K(ret), KPC(job_attr_), K(next_status));
      } else {
        backup_validate_service_->wakeup();
        FLOG_INFO("[BACKUP_VALIDATE]succeed schedule sys backup job", KPC(job_attr_));
      }
    }
  }
  return ret;
}

/*
*----------------------ObBackupValidateJobMgrAllocator-------------------------
*/
int ObBackupValidateJobMgrAllocator::new_job_mgr(const uint64_t tenant_id, ObIBackupValidateMgr *&job_mgr)
{
  int ret = OB_SUCCESS;
  if (is_meta_tenant(tenant_id)) {
    job_mgr = OB_NEW(ObUserTenantBackupValidateJobMgr, "UserValiJobMgr");
  } else if (is_sys_tenant(tenant_id)) {
    job_mgr = OB_NEW(ObSysTenantBackupValidateJobMgr, "SysValiJobMgr");
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid tenant_id, only meta tenant and sys tenant can alloc backup job mgr",
            K(ret), K(tenant_id));
  }
  return ret;
}

void ObBackupValidateJobMgrAllocator::delete_job_mgr(const uint64_t tenant_id, ObIBackupValidateMgr *job_mgr)
{
  if (OB_ISNULL(job_mgr)) {
  } else if (is_sys_tenant(tenant_id)) {
    OB_DELETE(ObIBackupValidateMgr, "SysValidateJobMgr", job_mgr);
  } else if (is_meta_tenant(tenant_id)) {
    OB_DELETE(ObIBackupValidateMgr, "UserValidateJobMgr", job_mgr);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "not free backup job mgr, mem leak", K(tenant_id));
  }
  job_mgr = nullptr;
}

}//namespace rootserver
}//namespace oceanbase