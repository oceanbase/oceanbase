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

#include "ob_backup_data_ls_task_mgr.h"
#include "ob_backup_schedule_task.h"
#include "ob_backup_task_scheduler.h"
#include "share/backup/ob_backup_struct.h"
#include "rootserver/backup/ob_backup_service.h"
#include "share/ls/ob_ls_status_operator.h"
#include "storage/backup/ob_backup_operator.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "storage/backup/ob_backup_data_store.h"
#include "share/backup/ob_backup_connectivity.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_operator.h"


namespace oceanbase
{
using namespace share;

namespace rootserver
{

ObBackupDataLSTaskMgr::ObBackupDataLSTaskMgr()
 : is_inited_(false),
   job_attr_(nullptr),
   ls_attr_(nullptr),
   set_task_attr_(nullptr),
   task_scheduler_(nullptr),
   sql_proxy_(nullptr),
   backup_service_(nullptr)
{
}

ObBackupDataLSTaskMgr::~ObBackupDataLSTaskMgr()
{
}

int ObBackupDataLSTaskMgr::init(
    ObBackupJobAttr &job_attr,
    ObBackupSetTaskAttr &set_task,
    ObBackupLSTaskAttr &ls_attr,
    ObBackupTaskScheduler &task_scheduler,
    common::ObISQLClient &sql_proxy,
    ObBackupDataService &backup_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[DATA_BACKUP]init twice", K(ret));
  } else if (!ls_attr.is_valid() || !job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_attr), K(job_attr));
  } else {
    job_attr_ = &job_attr;
    set_task_attr_ = &set_task;
    ls_attr_ = &ls_attr;
    task_scheduler_ = &task_scheduler;
    sql_proxy_ = &sql_proxy;
    backup_service_ = &backup_service;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupDataLSTaskMgr::process(int64_t &finish_cnt)
{
  //execute different operations according to the status of the task
  //status including : INIT PENDING DOING FINISH
  //INIT: add log stream task into task scheduler and advance status to pending
  //PENDING: do nothing, task scheduler will advance status to doing
  //DOING: do nothing, observer will advance status to finish
  //FINISH: determine whether to retry or finish task according to different results
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DATA_BACKUP]not init", K(ret));
  } else {
    FLOG_INFO("[DATA_BACKUP]schedule backup ls task", KPC(ls_attr_));
    switch (ls_attr_->status_.status_) {
      case ObBackupTaskStatus::Status::INIT: {
        if (OB_FAIL(gen_and_add_task_())) {
          LOG_WARN("[DATA_BACKUP]failed to gen and add task into task schedulers", K(ret), KPC(ls_attr_));
        }
        break;
      }
      case ObBackupTaskStatus::Status::PENDING:
      case ObBackupTaskStatus::Status::DOING:
        break;
      case ObBackupTaskStatus::Status::FINISH: {
        if (OB_FAIL(finish_(finish_cnt))) {
          LOG_WARN("[DATA_BACKUP]failed to finish task", K(ret), KPC(ls_attr_));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("[DATA_BACKUP]unknown backup task status", K(ret), KPC(ls_attr_));
      }
    }
  }
  return ret;
}

int ObBackupDataLSTaskMgr::gen_and_add_task_()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_ADD_BACKUP_TASK_INTO_SCHEDULER);
  switch (ls_attr_->task_type_.type_) {
    case ObBackupDataTaskType::Type::BACKUP_DATA_MINOR:
    case ObBackupDataTaskType::Type::BACKUP_DATA_MAJOR: {
      if (ObBackupDataTaskType::Type::BACKUP_DATA_MAJOR == ls_attr_->task_type_.type_) {
#ifdef ERRSIM
        ROOTSERVICE_EVENT_ADD("backup", "before_backup_major_sstable",
                              "tenant_id", ls_attr_->tenant_id_,
                              "ls_id", ls_attr_->ls_id_.id());
        DEBUG_SYNC(BEFORE_BACKUP_MAJOR_SSTABLE);
#endif
      }
      if (OB_FAIL(gen_and_add_backup_data_task_())) {
        LOG_WARN("[DATA_BACKUP]failed to gen and add backup data task", K(ret), KPC(ls_attr_));
      }
      break;
    }
    case ObBackupDataTaskType::Type::BACKUP_PLUS_ARCHIVE_LOG: {
      if (OB_FAIL(gen_and_add_backup_compl_log_())) {
        LOG_WARN("[DATA_BACKUP]failed to gen and add backup complement task", K(ret), KPC(ls_attr_));
      }
      break;
    }
    case ObBackupDataTaskType::Type::BACKUP_BUILD_INDEX: {
      if (OB_FAIL(gen_and_add_build_index_task_())) {
        LOG_WARN("[DATA_BACKUP]failed to gen and add build index task", K(ret), KPC(ls_attr_));
      }
      break;
    }
    case ObBackupDataTaskType::Type::BACKUP_META: {
      if (OB_FAIL(gen_and_add_backup_meta_task_())) {
        LOG_WARN("[DATA_BACKUP]failed to gen and add build index task", K(ret), KPC(ls_attr_));
      }
      break;
    }
    default: {
      ret = OB_ERR_SYS;
      LOG_ERROR("[DATA_BACKUP]unknown backup ls task type", K(ret), KPC(ls_attr_));
    }
  }
  return ret;
}

int ObBackupDataLSTaskMgr::check_ls_is_dropped(
    const share::ObBackupLSTaskAttr &ls_attr, common::ObISQLClient &sql_proxy, bool &is_dropped)
{
  int ret = OB_SUCCESS;
  ObLSStatusOperator op;
  share::ObLSStatusInfo status_info;
  is_dropped = false;
  if (share::ObBackupDataTaskType::Type::BACKUP_BUILD_INDEX == ls_attr.task_type_.type_) {
  } else if (OB_FAIL(op.get_ls_status_info(ls_attr.tenant_id_, ls_attr.ls_id_, status_info, sql_proxy))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_dropped = true;
      ret = OB_SUCCESS;
      LOG_INFO("ls has been dropped", K(ret), K(ls_attr));
    } else {
      LOG_WARN("fail to get ls status", K(ret), K(ls_attr));
    }
  } 
  return ret;
}

int ObBackupDataLSTaskMgr::gen_and_add_backup_meta_task_()
{
  //advance log stream task status to PENDING add log stream task into task scheduler 
  int ret = OB_SUCCESS;
  ObBackupDataLSMetaTask task;
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::PENDING;
  bool is_dropped = false;

  if (OB_FAIL(check_ls_is_dropped(*ls_attr_, *sql_proxy_, is_dropped))) {
    LOG_WARN("fail to check ls is dropped", K(ret));
  } else if (is_dropped) {
    share::ObBackupTaskStatus next_status(ObBackupTaskStatus::Status::FINISH);
    ls_attr_->result_ = OB_LS_NOT_EXIST;
    ls_attr_->end_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(advance_status_(next_status))) {
      LOG_WARN("fail to advance ls task status to finish", K(ret));
    }
  } else if (OB_FAIL(task.build(*job_attr_, *set_task_attr_, *ls_attr_))) {
    LOG_WARN("[DATA_BACKUP]failed to build task", K(ret), K(*job_attr_), K(*set_task_attr_), K(*ls_attr_));
  } else if (OB_FAIL(advance_status_(next_status))) {
    LOG_WARN("[DATA_BACKUP]failed to advance task status", K(ret), K(*ls_attr_), K(next_status));
  } else if (OB_FAIL(task_scheduler_->add_task(task))) {
    LOG_WARN("[DATA_BACKUP]failed to add task", K(ret), KPC(job_attr_), K(task));
  } 
  return ret;
}

int ObBackupDataLSTaskMgr::gen_and_add_backup_data_task_()
{
  //advance log stream task status to PENDING add log stream task into task scheduler
  int ret = OB_SUCCESS;
  ObBackupDataLSTask task;
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::PENDING;
  bool is_dropped = false;

  if (OB_FAIL(check_ls_is_dropped(*ls_attr_, *sql_proxy_, is_dropped))) {
    LOG_WARN("fail to check ls is dropped", K(ret));
  } else if (is_dropped) {
    share::ObBackupTaskStatus next_status(ObBackupTaskStatus::Status::FINISH);
    ls_attr_->result_ = OB_LS_NOT_EXIST;
    ls_attr_->end_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(advance_status_(next_status))) {
      LOG_WARN("fail to advance ls task status to finish", K(ret));
    }
  } else if (OB_FAIL(task.build(*job_attr_, *set_task_attr_, *ls_attr_))) {
    LOG_WARN("[DATA_BACKUP]failed to build task", K(ret), K(*job_attr_), K(*set_task_attr_), K(*ls_attr_));
  } else if (OB_FAIL(advance_status_(next_status))) {
    LOG_WARN("[DATA_BACKUP]failed to advance task status", K(ret), K(*ls_attr_), K(next_status));
  } else if (OB_FAIL(task_scheduler_->add_task(task))) {
    LOG_WARN("[DATA_BACKUP]failed to add task", K(ret), KPC(job_attr_), K(task));
  }
  return ret;
}

int ObBackupDataLSTaskMgr::gen_and_add_backup_compl_log_()
{
  // add complement log task into task scheduler
  // then complement log task status to pending
  int ret = OB_SUCCESS;
  ObBackupComplLogTask task;
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::PENDING;
  if (OB_FAIL(task.build(*job_attr_, *set_task_attr_, *ls_attr_))) {
    LOG_WARN("[DATA_BACKUP]failed to build task", K(ret), K(*job_attr_), K(*set_task_attr_), K(*ls_attr_));
  } else if (OB_FAIL(advance_status_(next_status))) {
    LOG_WARN("[DATA_BACKUP]failed to advance task status", K(ret), K(*ls_attr_), K(next_status));
  } else if (OB_FAIL(task_scheduler_->add_task(task))) {
    LOG_WARN("[DATA_BACKUP]failed to add task", K(ret), KPC(ls_attr_));
  }
  return ret;
}

int ObBackupDataLSTaskMgr::gen_and_add_build_index_task_()
{
  // add build index task into task scheduler
  // then build index task status to pending
  int ret = OB_SUCCESS;
  ObBackupBuildIndexTask task;
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::PENDING;
  if (OB_FAIL(task.build(*job_attr_, *set_task_attr_, *ls_attr_))) {
    LOG_WARN("[DATA_BACKUP]failed to build task", K(ret), K(*job_attr_), K(*set_task_attr_), K(*ls_attr_));
  } else if (OB_FAIL(advance_status_(next_status))) {
    LOG_WARN("[DATA_BACKUP]failed to advance task status", K(ret), K(*ls_attr_), K(next_status));
  } else if (OB_FAIL(task_scheduler_->add_task(task))) {
    LOG_WARN("[DATA_BACKUP]failed to add task", K(ret), KPC(ls_attr_));
  }
  return ret;
}

int ObBackupDataLSTaskMgr::advance_status_(const share::ObBackupTaskStatus &next_status)
{
  int ret  = OB_SUCCESS;
  ObSqlString extra_condition;
  ls_attr_->status_ = next_status;
  if (OB_FAIL(extra_condition.assign_fmt(
      "%s = %ld and %s = %ld", OB_STR_TURN_ID, ls_attr_->turn_id_, OB_STR_RETRY_ID, ls_attr_->retry_id_))) {
    LOG_WARN("failed to assign extra condition", K(ret));
  } else if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupLSTaskOperator::report_ls_task(*sql_proxy_, *ls_attr_, extra_condition))) {
    LOG_WARN("failed to report ls task", K(ret), KPC(ls_attr_));
  } else {
    LOG_INFO("advance ls task status", KPC(ls_attr_));
  }
  return ret;
}

int ObBackupDataLSTaskMgr::handle_execute_over(
    ObBackupDataService &backup_service, common::ObISQLClient &sql_proxy, const share::ObBackupLSTaskAttr &ls_attr,
    const share::ObHAResultInfo &result_info)
{
  int ret = OB_SUCCESS;
  ObBackupLSTaskAttr new_ls_attr;
  ObSqlString extra_condition;
  share::ObBackupDataType backup_data_type;
  int64_t full_replica_num = 0;
  if (!ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_attr));
  } else if (OB_FAIL(new_ls_attr.assign(ls_attr))) {
    LOG_WARN("failed to assign ls attr", K(ret), K(ls_attr));
  } else if (OB_FAIL(extra_condition.assign_fmt(
      "%s = %ld and %s = %ld and %s='%s'", OB_STR_TURN_ID, ls_attr.turn_id_, OB_STR_RETRY_ID, ls_attr.retry_id_,
      OB_STR_STATUS, ls_attr.status_.get_str()))) {
    LOG_WARN("failed to assign extra condition", K(ret));
  } else if (OB_FALSE_IT(new_ls_attr.end_ts_ = ObTimeUtility::current_time())) {
  } else if (OB_FAIL(ObBackupUtils::get_full_replica_num(ls_attr.tenant_id_, full_replica_num))) {
    LOG_WARN("failed to get full replica num", K(ret));
  } else if (new_ls_attr.black_servers_.count() + 1 >= full_replica_num && OB_FALSE_IT(new_ls_attr.black_servers_.reset())) {
  } else if (OB_SUCCESS != result_info.result_ && OB_FAIL(new_ls_attr.black_servers_.push_back(result_info.addr_))) {
    LOG_WARN("failed to push back black server", K(ret), K(result_info));
  } else if (OB_FALSE_IT(new_ls_attr.result_ = result_info.result_)) {
  } else if (OB_FALSE_IT(new_ls_attr.status_ = ObBackupTaskStatus::FINISH)) {
  } else if (OB_FAIL(result_info.get_comment_str(new_ls_attr.comment_))) {
    LOG_WARN("failed to get comment str", K(ret));
  } else if (OB_FAIL(ObBackupLSTaskOperator::report_ls_task(sql_proxy, new_ls_attr, extra_condition))) {
    LOG_WARN("failed to report ls task", K(ret), K(new_ls_attr), K(extra_condition));
  } else if (!ls_attr.task_type_.is_backup_data()) {
    // do nothing
  } else if (OB_FAIL(ls_attr.task_type_.get_backup_data_type(backup_data_type))) {
    LOG_WARN("failed to get backup data type", K(ret), K(ls_attr));
  } else if (OB_FAIL(backup::ObLSBackupOperator::mark_ls_task_info_final(ls_attr.task_id_, ls_attr.tenant_id_,
      ls_attr.ls_id_, ls_attr.turn_id_, ls_attr.retry_id_, backup_data_type, sql_proxy))) {
    LOG_WARN("[DATA_BACKUP]failed to update ls task info final to True", K(ret), K(ls_attr));
  }
  return ret;
}

int ObBackupDataLSTaskMgr::redo_ls_task(
    ObBackupDataService &backup_service,
    common::ObISQLClient &sql_proxy,
    const ObBackupLSTaskAttr &ls_attr,
    const int64_t start_turn_id,
    const int64_t turn_id,
    const int64_t retry_id)
{
  int ret  = OB_SUCCESS;
  share::ObBackupDataType backup_data_type;
  ObBackupLSTaskAttr new_ls_attr;
  ObSqlString extra_condition;
  if (!ls_attr.is_valid() || start_turn_id <= 0 || turn_id <= 0 || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_attr), K(start_turn_id), K(turn_id), K(retry_id));
  } else if (OB_FAIL(new_ls_attr.assign(ls_attr))) {
    LOG_WARN("failed to assign new ls attr", K(ret), K(ls_attr));
  } else if (OB_FAIL(extra_condition.assign_fmt(
    "%s = %ld and %s = %ld and %s='%s'", OB_STR_TURN_ID, ls_attr.turn_id_, OB_STR_RETRY_ID, ls_attr.retry_id_,
      OB_STR_STATUS, ls_attr.status_.get_str()))) {
    LOG_WARN("failed to assign extra condition", K(ret));
  } else {
    new_ls_attr.start_turn_id_ = start_turn_id;
    new_ls_attr.turn_id_ = turn_id;
    new_ls_attr.retry_id_ = retry_id;
    new_ls_attr.status_ = ObBackupTaskStatus::INIT;
    new_ls_attr.result_ = OB_SUCCESS;
    new_ls_attr.dst_.reset();
    new_ls_attr.task_trace_id_.reset();
    if (OB_FAIL(backup_service.check_leader())) {
      LOG_WARN("[DATA_BACKUP]failed to check leader", K(ret));
    } else if (OB_FAIL(ObBackupLSTaskOperator::report_ls_task(sql_proxy, new_ls_attr, extra_condition))) {
      LOG_WARN("[DATA_BACKUP]failed to redo ls task", K(ret), K(new_ls_attr), K(extra_condition));
    } else if (!ls_attr.task_type_.is_backup_data()) {
      // do nothing
    } else if (OB_FAIL(ls_attr.task_type_.get_backup_data_type(backup_data_type))) {
      LOG_WARN("failed to get backup data type", K(ret));
    } else if (OB_FAIL(backup::ObLSBackupOperator::insert_ls_backup_task_info(ls_attr.tenant_id_, ls_attr.task_id_,
        turn_id, retry_id, ls_attr.ls_id_, ls_attr.backup_set_id_, backup_data_type, sql_proxy))) {
      LOG_WARN("failed to insert ls backup task info", K(ret), K(ls_attr), K(backup_data_type), K(turn_id), K(retry_id));
    } else {
      LOG_INFO("redo ls task", K(turn_id), K(retry_id), K(ls_attr));
    }
  }
  return ret;
}

// TODO(yangyi.yyy): record tablet size when backup meta
int ObBackupDataLSTaskMgr::finish_(int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  bool is_ls_dropped = false;
  if (OB_ISNULL(job_attr_) || OB_ISNULL(ls_attr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[DATA_BACKUP]attr should not be null", K(ret), KP_(job_attr), KP_(ls_attr));
  } else if (OB_SUCCESS == ls_attr_->result_ || OB_NO_TABLET_NEED_BACKUP == ls_attr_->result_) {
    finish_cnt++;
  } else if (OB_LS_NOT_EXIST == ls_attr_->result_ && OB_FAIL(check_ls_is_dropped(*ls_attr_, *sql_proxy_, is_ls_dropped))) {
    LOG_WARN("failed to check ls is dropped", K(ret), KPC(ls_attr_));
  } else if (is_ls_dropped) {
    finish_cnt++;
    LOG_INFO("[BACKUP_DATA]ls has been dropped", KPC(ls_attr_));
  } else {
    bool ls_can_retry = true;
    int64_t next_retry_id = ls_attr_->retry_id_ + 1;
    switch(ls_attr_->task_type_.type_) {
      case ObBackupDataTaskType::BACKUP_META: {
        int64_t max_retry_times = OB_MAX_RETRY_TIMES;
#ifdef ERRSIM
        if (0 != GCONF.errsim_max_backup_retry_count) {
          max_retry_times = GCONF.errsim_max_backup_retry_count;
        }
#endif
        // when backup meta failed, ls task max retry times is three.
        if (!ObBackupUtils::is_need_retry_error(ls_attr_->result_) || ls_attr_->retry_id_ >= max_retry_times) {
          job_attr_->can_retry_ = false;
        } 
        break;
      }
      case ObBackupDataTaskType::BACKUP_DATA_MINOR:
      case ObBackupDataTaskType::BACKUP_DATA_MAJOR:
      case ObBackupDataTaskType::BACKUP_BUILD_INDEX: {
        int64_t cur_ts = ObTimeUtility::current_time();
        if (!ObBackupUtils::is_need_retry_error(ls_attr_->result_) 
            || set_task_attr_->retry_cnt_ + next_retry_id > OB_BACKUP_MAX_RETRY_TIMES) {
          job_attr_->can_retry_ = false;
        } 
        break;
      }
      case ObBackupDataTaskType::BACKUP_PLUS_ARCHIVE_LOG: {
        // this task only retry when send task failed
        if (OB_REBALANCE_TASK_CANT_EXEC != ls_attr_->result_
            && common::OB_TIMEOUT != ls_attr_->result_) {
          job_attr_->can_retry_ = false;
        } 
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid backup task type", K(ret), KPC(ls_attr_));
        break;
      }
    }

    ObMySQLTransaction trans;
    if (OB_FAIL(ret)) {
    } else if (job_attr_->can_retry_) {
      if (ObTimeUtility::current_time() < ls_attr_->end_ts_ + OB_BACKUP_RETRY_TIME_INTERVAL) {
      } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(ls_attr_->tenant_id_)))) {
        LOG_WARN("fail to start trans", K(ret));
      } else {
        if (OB_FAIL(redo_ls_task(*backup_service_, trans, *ls_attr_, ls_attr_->start_turn_id_,
          ls_attr_->turn_id_, next_retry_id))) {
            LOG_WARN("[DATA_BACKUP]failed to redo ls task", K(ret), KPC(ls_attr_));
        } 
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            LOG_WARN("fail to commit", K(ret));
          } else {
            set_task_attr_->retry_cnt_ += next_retry_id;
            backup_service_->wakeup();
            LOG_INFO("redo backup ls task", KPC(ls_attr_));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
            LOG_WARN("fail to roll back", K(tmp_ret));
          }
        }
      }
    } else {
      ret = ls_attr_->result_;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(job_attr_->comment_.assign(ls_attr_->comment_))) {
        LOG_WARN("failed to assign comment", K(ret), K(tmp_ret));
      }
      LOG_WARN("ls task failed, backup can't not continue", K(ret), KPC(ls_attr_));
    }
  }
  return ret;
}

int ObBackupDataLSTaskMgr::cancel(int64_t &finish_cnt)
{
  // INIT: just update status to FINISH and result to OB_CANCELED
  // PENDING: call cancel_task interface of task_scheduler, and update status to FINISH and result to OB_CANCELED;
  // DOING: call cancel_task interface of task_scheduler and update status to FINISH and result to OB_CANCELED;
  // FINISH: just update status to FINISH and result to OB_CANCELED
  int ret = OB_SUCCESS;
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::FINISH;
  if (ObBackupTaskStatus::Status::PENDING == ls_attr_->status_.status_
      || ObBackupTaskStatus::Status::DOING == ls_attr_->status_.status_) {
    if (OB_FAIL(task_scheduler_->cancel_tasks(
        BackupJobType::BACKUP_DATA_JOB, ls_attr_->job_id_, ls_attr_->tenant_id_))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[DATA_BACKUP]failed to cancel task", K(ret), KPC(ls_attr_));
      }
    }
  }
  ls_attr_->result_ = OB_CANCELED;
  ls_attr_->end_ts_ = ObTimeUtility::current_time();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(advance_status_(next_status))) {
    LOG_WARN("[DATA_BACKUP]failed to advance status", K(ret), K(*ls_attr_), K(next_status));
  } else {
    ++finish_cnt;
  }
  return ret;
}

}
}
