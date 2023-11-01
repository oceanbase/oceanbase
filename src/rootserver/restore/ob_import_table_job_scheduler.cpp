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

#include "ob_import_table_job_scheduler.h"
#include "ob_recover_table_initiator.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "storage/ddl/ob_ddl_server_client.h"
#include "share/backup/ob_backup_struct.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/restore/ob_import_util.h"
#include "rootserver/restore/ob_restore_service.h"

using namespace oceanbase;
using namespace rootserver;
using namespace common;
using namespace share;

ObImportTableJobScheduler::ObImportTableJobScheduler()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    schema_service_(nullptr),
    sql_proxy_(nullptr),
    job_helper_(),
    task_helper_()
{}

int ObImportTableJobScheduler::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = gen_user_tenant_id(MTL_ID());
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObImportTableJobScheduler init twice", K(ret));
  } else if (OB_FAIL(job_helper_.init(tenant_id))) {
    LOG_WARN("failed to init table op", K(ret), K(tenant_id));
  } else if (OB_FAIL(task_helper_.init(tenant_id))) {
    LOG_WARN("failed to init table op", K(ret), K(tenant_id));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObImportTableJobScheduler::wakeup_()
{
  ObRestoreService *restore_service = nullptr;
  if (OB_ISNULL(restore_service = MTL(ObRestoreService *))) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore service must not be null");
  } else {
    restore_service->wakeup();
  }
}

void ObImportTableJobScheduler::do_work()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  ObArray<share::ObImportTableJob> jobs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ObImportTableJobScheduler", K(ret));
  } else if (is_sys_tenant(tenant_id_)) {
    // no import table job in sys tenant
  } else if (OB_FAIL(check_compatible_())) {
    LOG_WARN("check compatible failed", K(ret));
  } else if (OB_FAIL(job_helper_.get_all_import_table_jobs(*sql_proxy_, jobs))) {
    LOG_WARN("failed to get recover all recover table job", K(ret));
  } else {
    ObCurTraceId::init(GCTX.self_addr());
    ARRAY_FOREACH(jobs, i) {
      ObImportTableJob &job = jobs.at(i);
      if (!job.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("recover table job is not valid", K(ret), K(job));
      } else if (is_user_tenant(job.get_tenant_id())) {
        process_(job);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant", K(ret), K(job));
      }
    }
  }
}

int ObImportTableJobScheduler::check_compatible_() const
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K_(tenant_id));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("min data version is smaller than v4.2.1", K(ret), K_(tenant_id), K(data_version));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id_), data_version))) {
    LOG_WARN("fail to get data version", K(ret), "tenant_id", gen_meta_tenant_id(tenant_id_));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("min data version is smaller than v4.2.1", K(ret), K_(tenant_id), K(data_version));
  }
  return ret;
}

int ObImportTableJobScheduler::process_(share::ObImportTableJob &job)
{
  int ret = OB_SUCCESS;
  switch(job.get_status()) {
    case ObImportTableJobStatus::INIT: {
      if (OB_FAIL(gen_import_table_task_(job))) {
        LOG_WARN("failed to gen import table task", K(ret), K(job));
      }
      break;
    }
    case ObImportTableJobStatus::IMPORT_TABLE: {
      if (OB_FAIL(deal_with_import_table_task_(job))) {
        LOG_WARN("failed to deal with import table task", K(ret), K(job));
      }
      break;
    }
    case ObImportTableJobStatus::RECONSTRUCT_REF_CONSTRAINT: {
      if (OB_FAIL(reconstruct_ref_constraint_(job))) {
        LOG_WARN("failed to deal with reconstrcut ref constraint", K(ret));
      }
      break;
    }
    case ObImportTableJobStatus::CANCELING: {
      if (OB_FAIL(canceling_(job))) {
        LOG_WARN("failed to cancel", K(ret), K(job));
      }
      break;
    }
    case ObImportTableJobStatus::IMPORT_FINISH: {
      if (OB_FAIL(finish_(job))) {
        LOG_WARN("failed to cancel", K(ret), K(job));
      }
      break;
    }
    default: {
      ret = OB_ERR_SYS;
      LOG_WARN("invalid import job status", K(ret));
      break;
    }
  }
  return ret;
}

int ObImportTableJobScheduler::reconstruct_ref_constraint_(share::ObImportTableJob &job)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObImportTableTask> import_tasks;
  ObImportTableJobStatus next_status = ObImportTableJobStatus::get_next_status(job.get_status());
  LOG_INFO("[IMPORT_TABLE]start reconstruct ref constraint", K(job));
  if (OB_FAIL(get_import_table_tasks_(job, import_tasks))) {
      LOG_WARN("failed to get import table task", K(ret));
  } else if (OB_FALSE_IT(job.set_end_ts(ObTimeUtility::current_time()))) {
  } else if (OB_FAIL(advance_status_(*sql_proxy_, job, next_status))) {
    LOG_WARN("failed to advance status", K(ret), K(job), K(next_status));
  } else {
    LOG_INFO("[IMPORT_TABLE]finish reconstruct ref constraint", K(job));
    ROOTSERVICE_EVENT_ADD("import_table", "reconstruct_ref_constraint",
                          "tenant_id", job.get_tenant_id(),
                          "job_id", job.get_job_id());
  }
  return ret;
}

int ObImportTableJobScheduler::finish_(const share::ObImportTableJob &job)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = job.get_tenant_id();
  const int64_t job_id = job.get_job_id();
  if (OB_FAIL(task_helper_.move_import_task_to_history(*sql_proxy_, tenant_id, job_id))) {
    LOG_WARN("failed to move import task to history", K(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(job_helper_.move_import_job_to_history(*sql_proxy_, tenant_id, job_id))) {
    LOG_WARN("failed to move import job to history", K(ret), K(tenant_id), K(job_id));
  } else {
    LOG_INFO("[IMPORT_TABLE]import table job finish", K(job));
    ROOTSERVICE_EVENT_ADD("import_table", "import table finish",
                          "tenant_id", job.get_tenant_id(),
                          "job_id", job.get_job_id());
  }
  return ret;
}

int ObImportTableJobScheduler::gen_import_table_task_(share::ObImportTableJob &job)
{
  int ret = OB_SUCCESS;
  ObImportTableTaskGenerator generator;
  ObArray<oceanbase::share::ObImportTableTask> import_tasks;
  ObMySQLTransaction trans;
  uint64_t meta_tenant_id = gen_meta_tenant_id(job.get_tenant_id());
  DEBUG_SYNC(BEFORE_GENERATE_IMPORT_TABLE_TASK);
  if (OB_FAIL(generator.init(*schema_service_, *sql_proxy_))) {
    LOG_WARN("failed to init import task generator", K(ret));
  } else if (OB_FAIL(generator.gen_import_task(job, import_tasks))) {
    LOG_WARN("failed to gen import table task", K(ret), K(job));
    if (!ObImportTableUtil::can_retrieable_err(ret)) {
      int tmp_ret = OB_SUCCESS;
      ObImportTableJobStatus next_status(ObImportTableJobStatus::IMPORT_FINISH);
      job.set_end_ts(ObTimeUtility::current_time());

      if (!job.get_result().is_comment_setted()) {
        share::ObTaskId trace_id(*ObCurTraceId::get_trace_id());
        ObImportResult result;
        if (OB_TMP_FAIL(result.set_result(ret, trace_id, GCONF.self_addr_))) {
          LOG_WARN("failed to set result", K(ret));
        } else {
          job.set_result(result);
        }
      }

      if (OB_TMP_FAIL(advance_status_(*sql_proxy_, job, next_status))) {
        LOG_WARN("failed to advance status", K(ret));
      }
    }
  } else if (OB_FAIL(trans.start(sql_proxy_, meta_tenant_id))) {
    LOG_WARN("failed to start trans", K(ret), K(meta_tenant_id));
  } else {
    ARRAY_FOREACH(import_tasks, i) {
      const ObImportTableTask &task = import_tasks.at(i);
      if (OB_FAIL(persist_import_table_task_(trans, task))) {
        LOG_WARN("failed to persist import table task", K(ret), K(task));
      } else {
        job.set_total_bytes(job.get_total_bytes() + task.get_total_bytes());
        job.set_total_table_count(job.get_total_table_count() + 1);
      }
    }

    ObImportTableJobStatus next_status = ObImportTableJobStatus::get_next_status(job.get_status());
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(job_helper_.report_import_job_statistics(*sql_proxy_, job))) {
      LOG_WARN("failed to report import job statistics", K(ret));
    } else if (!next_status.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error import table job status is unexpected", K(ret), K(next_status));
    } else if (OB_FAIL(advance_status_(trans, job, next_status))) {
      LOG_WARN("failed to advance to next status", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit", K(ret));
      } else {
        LOG_INFO("[IMPORT_TABLE] succeed generate import table task", K(import_tasks), K(next_status));
        ROOTSERVICE_EVENT_ADD("import_table", "generate import table task",
                              "tenant_id", job.get_tenant_id(),
                              "job_id", job.get_job_id(),
                              "task_count", import_tasks.count());
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to roll back", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObImportTableJobScheduler::deal_with_import_table_task_(share::ObImportTableJob &job)
{
  int ret = OB_SUCCESS;
  ObImportTableTask task;
  bool all_finish = false;
  if (OB_FAIL(task_helper_.get_one_not_finish_task_by_initiator(*sql_proxy_, job, all_finish, task))) {
    LOG_WARN("failed to get import table task", K(ret), K(job));
  } else if (!all_finish) {
    if (OB_FAIL(process_import_table_task_(task))) {
      LOG_WARN("failed to process import table task", K(ret), K(task));
    }
  } else if (OB_FAIL(do_after_import_all_table_(job))) {
    LOG_WARN("failed to do after import all table", K(ret), K(job));
  }
  return ret;
}

int ObImportTableJobScheduler::process_import_table_task_(share::ObImportTableTask &task)
{
  int ret = OB_SUCCESS;
  ObImportTableTaskScheduler task_mgr;
  if (OB_FAIL(task_mgr.init(*schema_service_, *sql_proxy_, task))) {
    LOG_WARN("failed to init task mgr", K(ret));
  } else {
    task_mgr.process();
  }
  return ret;
}

int ObImportTableJobScheduler::do_after_import_all_table_(share::ObImportTableJob &job)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObImportTableTask> import_tasks;
  ObImportTableJobStatus next_status = ObImportTableJobStatus::get_next_status(job.get_status());
  if (OB_FAIL(get_import_table_tasks_(job, import_tasks))) {
    LOG_WARN("failed to get import table tasks", K(ret), K(job));
  } else if (!next_status.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid import job status", K(ret), K(next_status));
  } else if (OB_FAIL(update_statistic_(import_tasks, job))) {
    LOG_WARN("failed to update statistic", K(ret));
  } else if (OB_FAIL(advance_status_(*sql_proxy_, job, next_status))) {
    LOG_WARN("failed to advance to next status", K(ret));
  } else {
    LOG_INFO("[IMPORT_TABLE]importing table finished", K(import_tasks), K(next_status));
    ROOTSERVICE_EVENT_ADD("import_table", "import table task finish",
                          "tenant_id", job.get_tenant_id(),
                          "job_id", job.get_job_id(),
                          "succeed_import_table_count", job.get_finished_table_count(),
                          "failed_import_table_count", job.get_failed_table_count());
  }
  return ret;
}

int ObImportTableJobScheduler::update_statistic_(
    common::ObIArray<share::ObImportTableTask> &import_tasks, share::ObImportTableJob &job)
{
  int ret = OB_SUCCESS;
  int64_t succeed_task_cnt = 0;
  int64_t failed_task_cnt = 0;
  ObImportResult::Comment comment;
  int64_t pos = 0;
  ARRAY_FOREACH(import_tasks, i) {
    const ObImportTableTask &task = import_tasks.at(i);
    if (task.get_result().is_succeed()) {
      succeed_task_cnt++;
    } else {
      failed_task_cnt++;
    }
  }
  ObImportResult result;

  if (OB_FAIL(databuff_printf(comment.ptr(), comment.capacity(), pos,
    "import succeed table count: %ld, failed table count: %ld", succeed_task_cnt, failed_task_cnt))) {
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to databuff_printf", K(ret));
    }
  }

  result.set_result(true, comment);
  job.set_result(result);
  job.set_finished_table_count(succeed_task_cnt);
  job.set_failed_table_count(failed_task_cnt);

  if (FAILEDx(job_helper_.report_statistics(*sql_proxy_, job))) {
    LOG_WARN("failed to report statistics", K(ret));
  }
  return ret;
}

int ObImportTableJobScheduler::canceling_(share::ObImportTableJob &job)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[IMPORT_TABLE]cancel import table job", K(job));
  ObArray<share::ObImportTableTask> import_tasks;
  if (OB_FAIL(get_import_table_tasks_(job, import_tasks))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get import table task", K(ret));
    }
  } else {
    ObImportTableTaskStatus next_status(ObImportTableTaskStatus::FINISH);
    ARRAY_FOREACH(import_tasks, i) {
      ObImportTableTask &task = import_tasks.at(i);
      obrpc::ObAbortRedefTableArg arg;
      arg.task_id_ = task.get_task_id();
      arg.tenant_id_ = task.get_tenant_id();
      bool is_exist = false;
      if (task.get_status().is_finish()) {
      } else if (OB_FAIL(check_import_ddl_task_exist_(task, is_exist))) {
        LOG_WARN("failed to check import ddl task", K(ret));
      } else if (is_exist && OB_FAIL(ObDDLServerClient::abort_redef_table(arg))) {
        LOG_WARN("failed to abort redef table", K(ret), K(arg));
      } else {
        LOG_INFO("[IMPORT_TABLE]cancel import table task", K(arg));
        share::ObTaskId trace_id(*ObCurTraceId::get_trace_id());
        ObImportResult result;
        if (OB_FAIL(result.set_result(OB_CANCELED, trace_id, GCONF.self_addr_))) {
          LOG_WARN("failed to set result", K(ret));
        } else if (OB_FALSE_IT(task.set_result(result))) {
        } else if (OB_FAIL(task_helper_.advance_status(*sql_proxy_, task, next_status))) {
          LOG_WARN("failed to cancel import task", K(ret), K(task));
        } else {
          LOG_INFO("[IMPORT_TABLE]succeed cancel import table task", K(arg));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    share::ObTaskId trace_id(*ObCurTraceId::get_trace_id());
    ObImportResult result;
    ObImportTableJobStatus next_status(ObImportTableJobStatus::IMPORT_FINISH);
    job.set_end_ts(ObTimeUtility::current_time());
    if (OB_FAIL(result.set_result(OB_CANCELED, trace_id, GCONF.self_addr_))) {
      LOG_WARN("failed to set result", K(ret));
    } else if (OB_FALSE_IT(job.set_result(result))) {
    } else if (OB_FAIL(advance_status_(*sql_proxy_, job, next_status))) {
      LOG_WARN("failed to advance status", K(ret));
    } else {
      LOG_INFO("[IMPORT_TABLE]succeed to cancel import table job", K(job));
      ROOTSERVICE_EVENT_ADD("import_table", "cancel import table task",
                      "tenant_id", job.get_tenant_id(),
                      "job_id", job.get_job_id());
    }
  }
  return ret;
}

int ObImportTableJobScheduler::check_import_ddl_task_exist_(const share::ObImportTableTask &task, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  const uint64_t tenant_id = task.get_tenant_id();
  const int64_t task_id = task.get_task_id();
  int64_t unused_user_msg_len = 0;
  ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
  if (OB_FAIL(ObDDLTaskRecordOperator::check_task_id_exist(*sql_proxy_, tenant_id, task_id, is_exist))) {
    LOG_WARN("failed to check task id exist", K(ret), K(tenant_id), K(task_id));
  } else if (is_exist) {
  } else if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(tenant_id,
                                                                      task_id,
                                                                      -1 /* target_object_id */,
                                                                      ObAddr()/*unused addr*/,
                                                                      false /* is_ddl_retry_task */,
                                                                      *sql_proxy_,
                                                                      error_message,
                                                                      unused_user_msg_len))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to load ddl user error", K(ret), K(tenant_id), K(task_id));
    }
  } else {
    is_exist = true;
  }
  return ret;
}

int ObImportTableJobScheduler::persist_import_table_task_(
    common::ObMySQLTransaction &trans, const share::ObImportTableTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_helper_.insert_import_table_task(trans, task))) {
    LOG_WARN("failed to get import table job", K(ret), K(task));
  } else {
    LOG_INFO("succeed to persist import table task", K(task));
  }
  return ret;
}

int ObImportTableJobScheduler::get_import_table_tasks_(
    const share::ObImportTableJob &job, common::ObIArray<share::ObImportTableTask> &import_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_helper_.get_all_import_table_tasks_by_initiator(*sql_proxy_, job, import_tasks))) {
    LOG_WARN("failed to get import table task", K(ret), K(job));
  }
  return ret;
}

int ObImportTableJobScheduler::advance_status_(
    common::ObISQLClient &sql_proxy, const share::ObImportTableJob &job, const share::ObImportTableJobStatus &next_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(job_helper_.advance_status(sql_proxy, job, next_status))) {
    LOG_WARN("failed to advance status", K(ret), K(job), K(next_status));
  } else {
    wakeup_();
  }
  return ret;
}

int ObImportTableTaskScheduler::init(share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy, share::ObImportTableTask &task)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObImportTableTaskScheduler init twice", K(ret));
  } else if (OB_FAIL(helper_.init(task.get_tenant_id()))) {
    LOG_WARN("failed to init recover table persist helper", K(ret));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    import_task_ = &task;
    is_inited_ = true;
  }
  return ret;
}

void ObImportTableTaskScheduler::wakeup_() {
  ObRestoreService *restore_service = nullptr;
  if (OB_ISNULL(restore_service = MTL(ObRestoreService *))) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore service must not be null");
  } else {
    restore_service->wakeup();
  }
}

void ObImportTableTaskScheduler::reset()
{
  is_inited_ = false;
  schema_service_ = nullptr;
  sql_proxy_ = nullptr;
  import_task_ = nullptr;
}

int ObImportTableTaskScheduler::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ready process import table task", KPC_(import_task));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIImportTableTaskMgr not inited", K(ret));
  } else {
    const ObImportTableTaskStatus &status = import_task_->get_status();
    switch(status) {
      case ObImportTableTaskStatus::INIT: {
        if (OB_FAIL(init_())) {
          LOG_WARN("failed to do init work", K(ret), KPC_(import_task));
        }
        break;
      }
      case ObImportTableTaskStatus::DOING: {
        if (OB_FAIL(doing_())) {
          LOG_WARN("failed to do doing work", K(ret), KPC_(import_task));
        }
        break;
      }
      case ObImportTableTaskStatus::FINISH: {
        break; // do nothing
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_WARN("invalid recover task status", K(ret));
      }
    }
  }
  return ret;
}

int ObImportTableTaskScheduler::init_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gen_import_ddl_task_())) {
    LOG_WARN("failed to generate import ddl task", K(ret), KPC_(import_task));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(try_advance_status_(ret))) {
    LOG_WARN("failed to advance status", K(tmp_ret), K(ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  return ret;
}

int ObImportTableTaskScheduler::doing_()
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  if (OB_FAIL(wait_import_ddl_task_finish_(is_finish))) {
    LOG_WARN("failed to do doing work", K(ret), KPC_(import_task));
  } else if (!is_finish) {
  } else if (OB_FAIL(try_advance_status_(ret))) {
    LOG_WARN("failed to advance status", K(ret));
  }
  return ret;
}

int ObImportTableTaskScheduler::try_advance_status_(const int err_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(err_code) && ObImportTableUtil::can_retrieable_err(err_code)) { // do nothing
  } else {

    share::ObImportTableTaskStatus next_status = import_task_->get_status().get_next_status(err_code);
    if (import_task_->get_result().is_succeed()) { // avoid to cover comment
      share::ObTaskId trace_id(*ObCurTraceId::get_trace_id());
      ObImportResult result;
      if (OB_FAIL(result.set_result(err_code, trace_id, GCONF.self_addr_))) {
        LOG_WARN("failed to set result", K(ret));
      } else if (OB_FALSE_IT(import_task_->set_result(result))) {
      }
    }
    if (FAILEDx(helper_.advance_status(*sql_proxy_, *import_task_, next_status))) {
      LOG_WARN("failed to advance status", K(ret), KPC_(import_task), K(next_status));
    } else {
      wakeup_();
    }
  }
  return ret;
}

int ObImportTableTaskScheduler::gen_import_ddl_task_()
{
  int ret = OB_SUCCESS;
  obrpc::ObRecoverRestoreTableDDLArg arg;
  bool is_exist = false;
  LOG_INFO("[IMPORT_TABLE]start to create import table", KPC_(import_task));
  if (OB_FAIL(check_import_ddl_task_exist_(is_exist))) {
    LOG_WARN("failed to check import ddl task", K(ret));
  } else if (is_exist) {
    LOG_INFO("[IMPORT_TABLE]import ddl task exist, skip it", KPC_(import_task), K(arg));
  } else if (OB_FAIL(construct_import_table_arg_(arg))) {
    LOG_WARN("failed to construct import table arg", K(ret));
  } else if (OB_FAIL(ObDDLServerClient::execute_recover_restore_table(arg))) {
    LOG_WARN("fail to start import table", K(ret), K(arg));
  } else {
    LOG_INFO("[IMPORT_TABLE]succeed execute_recover_restore_table", KPC_(import_task), K(arg));
  }
  return ret;
}

int ObImportTableTaskScheduler::check_import_ddl_task_exist_(bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  const uint64_t tenant_id = import_task_->get_tenant_id();
  const int64_t task_id = import_task_->get_task_id();
  int64_t unused_user_msg_len = 0;
  ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
  if (OB_FAIL(ObDDLTaskRecordOperator::check_task_id_exist(*sql_proxy_, tenant_id, task_id, is_exist))) {
    LOG_WARN("failed to check task id exist", K(ret), K(tenant_id), K(task_id));
  } else if (is_exist) {
  } else if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(tenant_id,
                                                                      task_id,
                                                                      -1 /* target_object_id */,
                                                                      ObAddr()/*unused addr*/,
                                                                      false /* is_ddl_retry_task */,
                                                                      *sql_proxy_,
                                                                      error_message,
                                                                      unused_user_msg_len))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to load ddl user error", K(ret), K(tenant_id), K(task_id));
    }
  } else {
    is_exist = true;
  }
  return ret;
}

int ObImportTableTaskScheduler::construct_import_table_arg_(obrpc::ObRecoverRestoreTableDDLArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard src_tenant_guard;
  const ObTableSchema *src_table_schema = nullptr;
  ObFixedLengthString<common::OB_MAX_TIMESTAMP_TZ_LENGTH> time_zone;
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(import_task_->get_src_tenant_id(), src_tenant_guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret), KPC_(import_task));
    } else if (OB_FAIL(src_tenant_guard.get_table_schema(import_task_->get_src_tenant_id(),
                                                        import_task_->get_src_database(),
                                                        import_task_->get_src_table(),
                                                        false,
                                                        src_table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), KPC_(import_task));
    }
  }
  if (FAILEDx(construct_import_table_schema_(*src_table_schema, arg.target_schema_))) {
    LOG_WARN("failed to construct import table schema", K(ret));
  } else {
    arg.src_tenant_id_ = src_table_schema->get_tenant_id();
    arg.src_table_id_ = src_table_schema->get_table_id();
    arg.ddl_task_id_ = import_task_->get_task_id();
    arg.exec_tenant_id_ = import_task_->get_tenant_id();
    const ObSysVarSchema *data_format_schema = nullptr;
    const ObSysVarSchema *nls_timestamp_format = nullptr;
    const ObSysVarSchema *nls_timestamp_tz_format = nullptr;
    if (OB_FAIL(share::ObBackupUtils::get_tenant_sys_time_zone_wrap(import_task_->get_tenant_id(),
                                                                    time_zone,
                                                                    arg.tz_info_wrap_))) {
      LOG_WARN("failed to get tenant sys timezoen wrap", K(ret), KPC_(import_task));
    } else if (OB_FAIL(src_tenant_guard.get_tenant_system_variable(import_task_->get_src_tenant_id(),
                                                                    share::SYS_VAR_NLS_DATE_FORMAT,
                                                                    data_format_schema))) {
      LOG_WARN("fail to get tenant system variable", K(ret));
    } else if (OB_FAIL(src_tenant_guard.get_tenant_system_variable(import_task_->get_src_tenant_id(),
                                                                    share::SYS_VAR_NLS_TIMESTAMP_FORMAT,
                                                                    nls_timestamp_format))) {
      LOG_WARN("fail to get tenant system variable", K(ret));
    } else if (OB_FAIL(src_tenant_guard.get_tenant_system_variable(import_task_->get_src_tenant_id(),
                                                                    share::SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT,
                                                                    nls_timestamp_tz_format))) {
      LOG_WARN("fail to get tenant system variable", K(ret));
    } else if (OB_ISNULL(data_format_schema) || OB_ISNULL(nls_timestamp_format) || OB_ISNULL(nls_timestamp_tz_format)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("var schema must not be null", K(ret));
    } else {
      arg.tz_info_ =  arg.tz_info_wrap_.get_tz_info_offset();
      arg.nls_formats_[ObNLSFormatEnum::NLS_DATE] = data_format_schema->get_value();
      arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] = nls_timestamp_format->get_value();
      arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = nls_timestamp_tz_format->get_value();
    }
  }
  return ret;
}

int ObImportTableTaskScheduler::construct_import_table_schema_(
    const share::schema::ObTableSchema &src_table_schema, share::schema::ObTableSchema &target_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard target_tenant_guard;
  if (OB_FAIL(schema_service_->get_tenant_schema_guard(import_task_->get_tenant_id(), target_tenant_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), KPC_(import_task));
  } else if (OB_FAIL(target_table_schema.assign(src_table_schema))) {
    LOG_WARN("failed to assign target table schema", K(ret));
  } else {
    target_table_schema.set_tenant_id(import_task_->get_tenant_id());
    target_table_schema.set_table_name(import_task_->get_target_table());
    target_table_schema.set_table_id(OB_INVALID_ID);

    target_table_schema.set_data_table_id(0);
    target_table_schema.clear_constraint();
    target_table_schema.clear_foreign_key_infos();
    target_table_schema.set_table_state_flag(ObTableStateFlag::TABLE_STATE_NORMAL);

    uint64_t database_id = OB_INVALID_ID;
    if (OB_FAIL(target_tenant_guard.get_database_id(import_task_->get_tenant_id(),
                                                    import_task_->get_target_database(),
                                                    database_id))) {
      LOG_WARN("failed to get database id", K(ret), KPC_(import_task));
    } else if (OB_INVALID_ID == database_id) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("invalid target database name", K(ret), K(database_id), KPC_(import_task));
    } else {
      target_table_schema.set_database_id(database_id);
    }

    uint64_t table_group_id = OB_INVALID_ID;
    if (import_task_->get_target_tablegroup().empty()) {
    } else if (FAILEDx(target_tenant_guard.get_tablegroup_id(import_task_->get_tenant_id(),
                                                             import_task_->get_target_tablegroup(),
                                                             table_group_id))) {
      LOG_WARN("failed to get table group id", K(ret), KPC_(import_task));
    } else if (OB_INVALID_ID == table_group_id) {
      ret = OB_TABLEGROUP_NOT_EXIST;
      LOG_WARN("invalid target tablegroup id", K(ret), K(table_group_id), KPC_(import_task));
    } else {
      target_table_schema.set_tablegroup_id(table_group_id);
    }

    const schema::ObTablespaceSchema *schema = nullptr;
    if (import_task_->get_target_tablespace().empty()) {
    } else if (FAILEDx(target_tenant_guard.get_tablespace_schema_with_name(import_task_->get_tenant_id(),
                                                                           import_task_->get_target_tablespace(),
                                                                           schema))) {
      LOG_WARN("failed to get tablespace schema", K(ret), KPC_(import_task));
    } else if (OB_ISNULL(schema)) {
      ret = OB_TABLESPACE_NOT_EXIST;
      LOG_WARN("tablespace must not be null", K(ret), KPC_(import_task));
    } else if (OB_FAIL(target_table_schema.set_encryption_str(schema->get_encryption_name()))) {
      LOG_WARN("failed to set encryption str", K(ret));
    } else if (OB_FAIL(target_table_schema.set_encrypt_key(schema->get_encrypt_key()))) {
      LOG_WARN("failed to set encrypt key", K(ret));
    } else {
      target_table_schema.set_master_key_id(schema->get_master_key_id());
      target_table_schema.set_tablespace_id(schema->get_tablespace_id());
    }
  }
  return ret;
}

int ObImportTableTaskScheduler::wait_import_ddl_task_finish_(bool &is_finish)
{
  int ret = OB_SUCCESS;
  int64_t unused_user_msg_len = 0;
  ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
  uint64_t tenant_id = import_task_->get_tenant_id();
  int64_t task_id = import_task_->get_task_id();
  is_finish = false;
  if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(tenant_id,
                                                                    task_id,
                                                                    -1 /* target_object_id */,
                                                                    ObAddr()/*unused addr*/,
                                                                    false /* is_ddl_retry_task */,
                                                                    *sql_proxy_,
                                                                    error_message,
                                                                    unused_user_msg_len))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if(REACH_TIME_INTERVAL(120 * 1000 * 1000)) {
        LOG_WARN("[IMPORT_TABLE]import ddl task does not finish, retry again", K(tenant_id), K(task_id));
      }
    } else {
      LOG_WARN("failed to load ddl user error", K(ret), K(tenant_id), K(task_id));
    }
  } else if (OB_SUCCESS != error_message.ret_code_) {
    ObImportResult result;
    if (OB_FAIL(result.set_result(false, error_message.user_message_))) {
      LOG_WARN("failed to set result", K(ret), K(error_message));
    } else {
      import_task_->set_result(result);
      is_finish = true;
      LOG_INFO("[IMPORT_TABLE]import table failed", KPC_(import_task), K(error_message));
    }
  } else if (OB_FAIL(statistics_import_results_())) {
    LOG_WARN("failed to statistics import result", K(ret));
  } else if (OB_FAIL(helper_.report_import_task_statistics(*sql_proxy_, *import_task_))) {
    LOG_WARN("failed to report import task statistics", K(ret), KPC_(import_task));
  } else {
    is_finish = true;
    LOG_INFO("[IMPORT_TABLE]import table succeed", KPC_(import_task), K(error_message));
  }
  return ret;
}

int ObImportTableTaskScheduler::statistics_import_results_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObTableSchema * table_schema = nullptr;
  const int64_t tenant_id = import_task_->get_tenant_id();
  const ObString &db_name = import_task_->get_target_database();
  const ObString &table_name = import_task_->get_target_table();
  if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("failed get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_table_schema(tenant_id, db_name, table_name, false/*no index*/, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(db_name), K(table_name));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table is not exist", K(tenant_id), K(db_name), K(table_name));
    ObImportResult::Comment comment;
    if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(),
      "table %.*s has been deleted by user", table_name.length(), table_name.ptr()))) {
      LOG_WARN("failed to databuff printf", K(ret), K(tmp_ret));
    } else {
      import_task_->get_result().set_result(true, comment);
    }
  } else {
    import_task_->set_completion_ts(ObTimeUtility::current_time());
    import_task_->set_imported_index_count(table_schema->get_simple_index_infos().count());
    import_task_->set_failed_index_count(import_task_->get_total_index_count() - import_task_->get_imported_index_count());
    import_task_->set_imported_constraint_count(table_schema->get_constraint_count());
    import_task_->set_failed_constraint_count(import_task_->get_total_constraint_count() - import_task_->get_imported_constraint_count());
    import_task_->set_imported_ref_constraint_count(table_schema->get_foreign_key_infos().count());
    import_task_->set_failed_ref_constraint_count(import_task_->get_total_ref_constraint_count() - import_task_->get_imported_ref_constraint_count());
    import_task_->set_imported_trigger_count(table_schema->get_trigger_list().count());
    import_task_->set_failed_trigger_count(import_task_->get_total_trigger_count() - import_task_->get_imported_trigger_count());
  }
  return ret;
}
