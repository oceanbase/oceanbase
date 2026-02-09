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
#include "ob_backup_validate_task_mgr.h"
#include "ob_backup_validate_ls_task_mgr.h"
#include "share/backup/ob_backup_validate_table_operator.h"
#include "share/ob_define.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_util.h"
#include "share/backup/ob_archive_store.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"

namespace oceanbase
{
namespace rootserver
{

ObBackupValidateTaskMgr::ObBackupValidateTaskMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    task_attr_(),
    job_attr_(nullptr),
    sql_proxy_(nullptr),
    rpc_proxy_(nullptr),
    task_scheduler_(nullptr),
    validate_service_(nullptr)
{
}

int ObBackupValidateTaskMgr::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    share::ObBackupValidateJobAttr &job_attr,
    common::ObISQLClient &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    ObBackupTaskScheduler &task_scheduler,
    ObBackupService &validate_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_VALIDATE]init twice", K(ret));
  } else if (OB_FAIL(ObBackupValidateTaskOperator::get_backup_validate_task(sql_proxy, task_id,
                                                                              job_attr.tenant_id_, task_attr_))) {
    LOG_WARN("failed to get backup validate task",K(ret), "job_id", job_attr.job_id_, "tenant_id", job_attr.tenant_id_);
  } else if (!task_attr_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task attr", K(ret), K(task_attr_));
  } else {
    tenant_id_ = tenant_id;
    job_attr_ = &job_attr;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    task_scheduler_ = &task_scheduler;
    validate_service_ = &validate_service;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupValidateTaskMgr::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP_VALIDATE]schedule backup validate task", K(task_attr_));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]not init", K(ret));
  } else {
    ObBackupValidateStatus::Status status = task_attr_.status_.status_;
    switch (status) {
      case ObBackupValidateStatus::Status::INIT: {
        if (OB_FAIL(prepare_backup_validate_task_())) {
          LOG_WARN("[BACKUP_VALIDATE]failed to prepare backup validate task", K(ret), K(task_attr_));
        }
        break;
      }
      case ObBackupValidateStatus::Status::DOING: {
        if (OB_FAIL(backup_validate_ls_tasks_())) {
          LOG_WARN("[BACKUP_VALIDATE]failed to backup validate ls task", K(ret), K(task_attr_));
        }
        break;
      }
      case ObBackupValidateStatus::Status::COMPLETED:
      case ObBackupValidateStatus::Status::FAILED:
      case ObBackupValidateStatus::Status::CANCELED: {
        break;
      }
      case ObBackupValidateStatus::Status::CANCELING: {
        if (OB_FAIL(do_cancel_())) {
          LOG_WARN("[BACKUP_VALIDATE]failed to cancel backup validate", K(ret), K(task_attr_));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("[BACKUP_VALIDATE]unknown backup validate status", K(ret), K(task_attr_));
      }
    }
    if (OB_FAIL(ret) && !is_can_retry_(ret)) {
      if (ObBackupValidateStatus::Status::INIT == task_attr_.status_.status_
          || ObBackupValidateStatus::Status::DOING == task_attr_.status_.status_) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(deal_failed_task(ret))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to advance task status", K(tmp_ret), K(ret), K(task_attr_.status_));
        }
      }
    }
  }

  return ret;
}

int ObBackupValidateTaskMgr::check_task_running(bool &is_running)
{
  int ret = OB_SUCCESS;
  is_running = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]not init", K(ret));
  } else {
    is_running = ObBackupValidateStatus::Status::DOING == task_attr_.status_.status_;
  }
  return ret;
}

int ObBackupValidateTaskMgr::prepare_backup_validate_task_()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObLSID> ls_ids;
  share::ObBackupValidateStatus next_status;
  common::ObMySQLTransaction trans;
  share::ObBackupDest backup_dest;
  share::ObBackupDest backup_set_dest;
  share::ObBackupPathString raw_path;
  next_status.status_ = share::ObBackupValidateStatus::Status::DOING;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*sql_proxy_, task_attr_.tenant_id_,
                                                                                task_attr_.dest_id_, backup_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest", K(ret), K(task_attr_));
  } else if (OB_FAIL(ObBackupUtils::get_raw_path(task_attr_.validate_path_.ptr(), raw_path.ptr(), raw_path.capacity()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get raw path", K(ret), K(task_attr_));
  } else if (OB_FAIL(backup_set_dest.set(raw_path.ptr(), backup_dest.get_storage_info()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to set backup set dest", K(ret), K(raw_path), K(backup_dest));
  } else if (OB_FAIL(get_ls_ids_and_total_bytes_(backup_set_dest, ls_ids, task_attr_.total_bytes_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get ls ids", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(generate_and_insert_ls_tasks_(trans, ls_ids))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to generate and insert ls tasks", K(ret));
  } else if (FALSE_IT(task_attr_.total_ls_count_ = ls_ids.count())) {
  } else if (OB_FAIL(ObBackupValidateTaskOperator::update_ls_count_and_bytes(trans, task_attr_, true/*is_total*/))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to update ls count and bytes", K(ret), K_(task_attr));
  } else if (OB_FAIL(advance_task_status_(trans, next_status, task_attr_.result_, task_attr_.end_ts_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to advance status to DOING", K(ret), K(ls_ids));
  } else {
    task_attr_.status_.status_ = next_status.status_;
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to end trans", KR(ret), K(tmp_ret));
    } else if (OB_SUCC(ret)) {
      validate_service_->wakeup();
    }
  }

  return ret;
}

int ObBackupValidateTaskMgr::generate_and_insert_ls_tasks_(
    common::ObMySQLTransaction &trans,
    const ObArray<share::ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]not init", K(ret));
  } else if (0 == ls_ids.count()) {
  } else {
    share::ObBackupValidateLSTaskAttr new_ls_attr;
    for (int i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i) {
      const share::ObLSID &ls_id = ls_ids.at(i);
      if (!ls_id.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[BACKUP_VALIDATE]invalid ls id", K(ret), K(ls_id), K(i));
      } else {
        new_ls_attr.reset();
        if (OB_FAIL(generate_ls_task_(ls_id, new_ls_attr))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to generate log stream tasks", K(ret), K(ls_ids));
        } else if (OB_FAIL(validate_service_->check_leader())) {
          LOG_WARN("[BACKUP_VALIDATE]failed to check leader", K(ret));
        } else if (OB_FAIL(share::ObBackupValidateLSTaskOperator::insert_ls_task(trans, new_ls_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to insert backup validate log stream task", K(ret), K(new_ls_attr));
        }
      }
    }
  }

  return ret;
}

int ObBackupValidateTaskMgr::deal_failed_task(const int error)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupValidateLSTaskAttr> ls_tasks;
  int result = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t success_ls_count = 0;
  const bool for_update = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]ObBackupValidateTaskMgr not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", K(ret));
  } else if (OB_FAIL(ObBackupValidateLSTaskOperator::get_ls_tasks_from_task_id(trans, task_attr_.task_id_,
                                                                                task_attr_.tenant_id_,
                                                                                for_update, ls_tasks))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get log stream tasks", K(ret));
  } else {
    if (OB_FAIL(do_failed_ls_task_(trans, ls_tasks, success_ls_count, result))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to do failed backup validate task", K(ret), K(ls_tasks));
    }
    result = (OB_SUCCESS == result) ? error : result;
    if (OB_SUCC(ret)) {
      ObBackupValidateStatus next_status;
      if (OB_SUCCESS == result) {
        next_status.status_ = ObBackupValidateStatus::Status::CANCELED;
        result = OB_CANCELED;
      } else {
        next_status.status_ = ObBackupValidateStatus::Status::FAILED;
      }
      int64_t end_ts = ObTimeUtil::current_time();
      task_attr_.finish_ls_count_ = success_ls_count;
      if (OB_FAIL(ObBackupValidateTaskOperator::update_ls_count_and_bytes(trans, task_attr_, false/*is_total*/))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to update finish ls count", K(ret), K_(task_attr));
      } else if (OB_FAIL(advance_task_status_(trans, next_status, result, end_ts))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to advance task status to finish", K(ret), K_(task_attr));
      } else {
        task_attr_.status_.status_ = next_status.status_;
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to end trans", KR(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObBackupValidateTaskMgr::do_cancel_()
{
  int ret = OB_SUCCESS;
  int64_t finish_cnt = 0;
  int64_t success_ls_count = 0;
  ObArray<share::ObBackupValidateLSTaskAttr> ls_tasks;
  ObMySQLTransaction trans;
  const bool for_update = true;
  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret));
  } else {
    if (OB_FAIL(ObBackupValidateLSTaskOperator::get_ls_tasks_from_task_id(trans, task_attr_.task_id_,
                                                                            task_attr_.tenant_id_,
                                                                            for_update, ls_tasks))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get log stream tasks", KR(ret));
    } else if (ls_tasks.empty()) {
    } else {
      ObBackupValidateLSTaskMgr ls_task_mgr;
      for (int i = 0; OB_SUCC(ret) && i < ls_tasks.count(); ++i) {
        ls_task_mgr.reset();
        share::ObBackupValidateLSTaskAttr &ls_attr = ls_tasks.at(i);
        if (ObBackupTaskStatus::Status::FINISH == ls_attr.status_.status_) {
          ++finish_cnt;
          if (OB_SUCCESS == ls_attr.result_) {
            success_ls_count++;
          }
        } else if (OB_FAIL(ls_task_mgr.init(*job_attr_, task_attr_, ls_attr, *task_scheduler_, trans, *validate_service_))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to init task advancer", K(ret), K(ls_attr), KPC(job_attr_), K(task_attr_));
        } else if (OB_FAIL(ls_task_mgr.cancel(finish_cnt))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to process log stream task", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && ls_tasks.count() == finish_cnt) {
      ObBackupValidateStatus next_status;
      next_status.status_ = ObBackupValidateStatus::Status::CANCELED;
      int64_t end_ts = ObTimeUtil::current_time();
      task_attr_.finish_ls_count_ = success_ls_count;
      if (OB_FAIL(advance_task_status_(trans, next_status, OB_CANCELED, end_ts))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to advance validate task status to CANCELED", K(ret));
      } else if (OB_FAIL(ObBackupValidateTaskOperator::update_ls_count_and_bytes(trans, task_attr_, false/*is_total*/))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to update finish ls count", KR(ret), K(task_attr_));
      } else {
        task_attr_.status_.status_ = next_status.status_;
        FLOG_INFO("[BACKUP_VALIDATE]advance validate status CANCELED success", K(next_status));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(false))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to roll back status", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateTaskMgr::advance_task_status_(
    common::ObMySQLTransaction &trans,
    const share::ObBackupValidateStatus &next_status,
    const int result,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  if (!next_status.is_valid() || !task_attr_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid next_status or invalid task", K(ret), K(task_attr_));
  } else if (OB_FAIL(validate_service_->check_leader())) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check leader", KR(ret));
  } else if (OB_FAIL(ObBackupValidateTaskOperator::advance_task_status(trans, task_attr_,
                                                                          next_status, result, end_ts))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to advance task status", K(ret), K(task_attr_));
  }
  return ret;
}

int ObBackupValidateTaskMgr::do_backup_validate_ls_tasks_(
    ObArray<share::ObBackupValidateLSTaskAttr> &ls_tasks,
    int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  finish_cnt = 0;
  int retry_count = 0;
  if (ls_tasks.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(ret), K(ls_tasks));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ls_tasks.count(); ++i) {
      share::ObBackupValidateLSTaskAttr &ls_attr = ls_tasks.at(i);
      ObBackupValidateLSTaskMgr ls_task_mgr;
      if (ls_attr.retry_id_ >= OB_BACKUP_VALIDATE_MAX_RETRY_TIMES && OB_SUCCESS != ls_attr.result_) {
        job_attr_->can_retry_ = false;
        ret = ls_attr.result_;
        LOG_WARN("[BACKUP_VALIDATE]retry times exceeds the limit", K(ret), K(ls_attr.retry_id_), K(ls_attr.ls_id_),
                    K(task_attr_), K(job_attr_->can_retry_));
      } else if (OB_FAIL(ls_task_mgr.init(*job_attr_, task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, *validate_service_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to init task advancer", K(ret), K(ls_attr), KPC(job_attr_), K(task_attr_));
      } else if (OB_FAIL(ls_task_mgr.process(finish_cnt))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to process log stream task", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateTaskMgr::backup_validate_ls_tasks_()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupValidateLSTaskAttr> ls_tasks;
  int64_t finish_cnt = 0;
  bool need_do_ls_task = true;
  int tmp_ret = OB_SUCCESS;
  share::ObBackupValidateStatus next_status;
  int64_t finish_cnt_before_schedule = 0;
  if (task_attr_.type_.is_backupset() && OB_FAIL(read_meta_files_())) {
    LOG_WARN("[BACKUP_VALIDATE]failed to read meta files", KR(ret));
  } else if (OB_FAIL(share::ObBackupValidateLSTaskOperator::get_ls_tasks_from_task_id(*sql_proxy_, task_attr_.task_id_,
                                                              task_attr_.tenant_id_, false/*need_lock*/, ls_tasks))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get log stream tasks", KR(ret));
  } else if (ls_tasks.count() != task_attr_.total_ls_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]ls task count error", KR(ret), K(ls_tasks), K(task_attr_));
  } else if (ls_tasks.empty() && 0 == task_attr_.total_ls_count_) {
    // do nothing
  } else if (FALSE_IT(finish_cnt_before_schedule = task_attr_.finish_ls_count_)) {
  } else if (OB_FAIL(do_backup_validate_ls_tasks_(ls_tasks, finish_cnt))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to do backup ls task", KR(ret), K_(task_attr));
  } else if (finish_cnt_before_schedule != finish_cnt) {
    if (OB_FAIL(share::ObBackupValidateTaskOperator::update_ls_count_and_bytes(*sql_proxy_, task_attr_, false/*is_total*/))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update finish ls count", KR(ret), K_(task_attr));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ls_tasks.count() == finish_cnt) {
    task_attr_.end_ts_ = ObTimeUtil::current_time();
    next_status = share::ObBackupValidateStatus::Status::COMPLETED;
    common::ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret));
    } else if (OB_FAIL(advance_task_status_(trans, next_status, OB_SUCCESS, task_attr_.end_ts_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to advance task status", KR(ret), K_(task_attr));
    } else {
      task_attr_.status_.status_ = next_status.status_;
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to end trans", KR(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupValidateTaskMgr::generate_ls_task_(const share::ObLSID &ls_id, share::ObBackupValidateLSTaskAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]ls id is invalid", K(ret), K(ls_id));
  } else {
    const int64_t current_time = ObTimeUtility::current_time();
    ls_attr.task_id_ = task_attr_.task_id_;
    ls_attr.tenant_id_ = task_attr_.tenant_id_;
    ls_attr.ls_id_ = ls_id;
    ls_attr.job_id_ = task_attr_.job_id_;
    ls_attr.validate_id_ = task_attr_.validate_id_;
    ls_attr.round_id_ = task_attr_.round_id_;
    ls_attr.task_type_.type_ = task_attr_.type_.type_;
    ls_attr.status_.status_ = share::ObBackupTaskStatus::Status::INIT;
    ls_attr.start_ts_ = current_time;
    ls_attr.end_ts_ = 0;
    ls_attr.result_ = OB_SUCCESS;
    ls_attr.retry_id_ = 0;
    ls_attr.validate_path_ = task_attr_.validate_path_;
    ls_attr.dest_id_ = task_attr_.dest_id_;
    ls_attr.validate_level_ = task_attr_.validate_level_;
    ls_attr.total_object_count_ = 0;
    ls_attr.finish_object_count_ = 0;
    ls_attr.validated_bytes_ = 0;
  }
  return ret;
}

int ObBackupValidateTaskMgr::do_failed_ls_task_(
    common::ObISQLClient &sql_proxy,
    ObIArray<share::ObBackupValidateLSTaskAttr> &ls_task,
    int64_t &success_ls_count,
    int &result)
{
  int ret = OB_SUCCESS;
  int64_t finish_cnt = 0;
  ObBackupValidateStatus next_status;
  next_status.status_ = ObBackupValidateStatus::Status::COMPLETED;
  result = OB_SUCCESS;
  if (ls_task.empty()) {
    LOG_INFO("[BACKUP_VALIDATE]ls tasks is empty", K(ls_task));
    // do nothing
  } else {
    ObBackupValidateLSTaskMgr ls_task_mgr;
    for (int i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
      ls_task_mgr.reset();
      share::ObBackupValidateLSTaskAttr &ls_attr = ls_task.at(i);
      if (ObBackupTaskStatus::Status::FINISH == ls_attr.status_.status_) {
        if (OB_SUCCESS == ls_attr.result_) {
          success_ls_count++;
        } else if (OB_SUCCESS == result) {
          result = ls_attr.result_;
        }
      } else if (OB_FAIL(ls_task_mgr.init(*job_attr_, task_attr_, ls_attr, *task_scheduler_, sql_proxy, *validate_service_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to init task advancer", K(ret), K(ls_attr), KPC(job_attr_), K(task_attr_));
      } else if (OB_FAIL(ls_task_mgr.cancel(finish_cnt))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to process log stream task", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateTaskMgr::read_meta_files_()
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;
  share::ObBackupDest backup_set_dest;
  share::ObBackupPathString raw_path;
  ObBackupDataStore store;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*sql_proxy_, task_attr_.tenant_id_,
    task_attr_.dest_id_, backup_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest", K(ret), K(task_attr_));
  } else if (OB_FAIL(ObBackupUtils::get_raw_path(task_attr_.validate_path_.ptr(), raw_path.ptr(), raw_path.capacity()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get raw path", K(ret), K(task_attr_));
  } else if (OB_FAIL(backup_set_dest.set(raw_path.ptr(), backup_dest.get_storage_info()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to set backup set dest", K(ret), K(raw_path), K(backup_dest));
  } else if (OB_FAIL(store.init(backup_set_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init store", K(ret), K(backup_set_dest));
  } else {
    ObExternTenantLocalityInfoDesc locality_info;
    ObExternTenantDiagnoseInfoDesc diagnose_info;
    ObBackupLSMetaInfosDesc ls_meta_infos;
    ObExternParamInfoDesc param_info;
    ObBackupMajorCompactionMViewDepTabletListDesc mview_dep_tablet_list;
    if (OB_FAIL(store.read_tenant_locality_info(locality_info))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to read locality info", K(ret), K(backup_set_dest));
    } else if (OB_FAIL(store.read_tenant_diagnose_info(diagnose_info))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to read diagnose info", K(ret), K(backup_set_dest));
    } else if (OB_FAIL(store.read_ls_meta_infos(ls_meta_infos))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to read ls meta infos", K(ret), K(backup_set_dest));
    } else if (OB_FAIL(store.read_tenant_param_info(param_info))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to read param info", K(ret), K(backup_set_dest));
    } else if (OB_FAIL(store.read_major_compaction_mview_dep_tablet_list(mview_dep_tablet_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to read major compaction mview dep tablet list", K(ret), K(backup_set_dest));
    }
  }
  return ret;
}

int ObBackupValidateTaskMgr::do_clean_up()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]not init", KR(ret));
  } else if (OB_FAIL(validate_service_->check_leader())) {
    LOG_WARN("[BACKUP_VALIDATE]failed to check leader", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(task_attr_.tenant_id_)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret));
  } else if (OB_FAIL(share::ObBackupValidateLSTaskOperator::move_ls_task_to_history(trans, task_attr_.tenant_id_,
                                                                                        task_attr_.task_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to move ls task to history", KR(ret), K(task_attr_));
  } else if (OB_FAIL(ObBackupValidateTaskOperator::move_task_to_history(trans, task_attr_.tenant_id_,
                                                                            task_attr_.task_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to move task to history", KR(ret), K(task_attr_));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to end trans", KR(ret), K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

bool ObBackupValidateTaskMgr::is_can_retry_(const int error)
{
  return job_attr_->can_retry_ && job_attr_->retry_count_ < OB_MAX_RETRY_TIMES
            && ObBackupUtils::is_need_retry_error(error);
}

int ObBackupValidateTaskMgr::get_ls_ids_and_total_bytes_(
    const share::ObBackupDest &backup_set_dest,
    ObIArray<ObLSID> &ls_ids,
    int64_t &total_bytes)
{
  int ret = OB_SUCCESS;
  total_bytes = 0;
  if (task_attr_.type_.is_backupset()) {
    if (OB_FAIL(get_backup_set_ls_ids_and_total_bytes_(backup_set_dest, ls_ids, total_bytes))) {
      LOG_WARN("failed to get set ls ids", K(ret), K(backup_set_dest));
    }
  } else if (task_attr_.type_.is_archivelog()) {
    if (OB_FAIL(get_archive_piece_ls_ids_and_total_bytes_(backup_set_dest, ls_ids, total_bytes))) {
      LOG_WARN("failed to get piece ls ids", K(ret), K(backup_set_dest));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("validate task type is invalid", K(ret), K_(task_attr));
  }
  return ret;
}

int ObBackupValidateTaskMgr::get_backup_set_ls_ids_and_total_bytes_(
    const share::ObBackupDest &backup_set_dest,
    ObIArray<ObLSID> &ls_ids,
    int64_t &total_bytes)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  storage::ObBackupDataLSAttrDesc ls_attr;
  storage::ObExternBackupSetInfoDesc backup_set_info;
  if (OB_FAIL(store.init(backup_set_dest))) {
    LOG_WARN("failed to init store", K(ret), K_(task_attr), K(backup_set_dest));
  } else if (OB_FAIL(store.read_backup_set_info(backup_set_info))) {
    LOG_WARN("failed to read backup set info", K(ret), K_(task_attr), K(backup_set_dest));
  } else if (OB_FAIL(store.read_ls_attr_info(backup_set_info.backup_set_file_.meta_turn_id_, ls_attr))) {
    LOG_WARN("failed to read ls attr info", K(ret), K_(task_attr));
  } else if (!ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls attr", K(ret), K(ls_attr));
  } else {
    total_bytes = backup_set_info.backup_set_file_.stats_.output_bytes_;
    for (int i = 0; OB_SUCC(ret) && i < ls_attr.ls_attr_array_.count(); ++i) {
      const ObLSID &ls_id = ls_attr.ls_attr_array_.at(i).get_ls_id();
      if (!ls_id.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ls id", K(ret), K(ls_id));
      } else if (OB_FAIL(ls_ids.push_back(ls_id))) {
        LOG_WARN("failed to push back ls id", K(ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObBackupValidateTaskMgr::get_archive_piece_ls_ids_(
    const share::ObBackupDest &backup_piece_dest,
    const share::ObBackupPath &piece_path,
    ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;

  if (!backup_piece_dest.is_valid() || piece_path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(backup_piece_dest), K(piece_path));
  } else {
    const common::ObStorageIdMod mod(task_attr_.dest_id_, common::ObStorageUsedMod::STORAGE_USED_BACKUP);
    ls_ids.reset();
    ObArchiveStore store;
    bool is_file_list_exist = false;
    if (OB_FAIL(store.init(backup_piece_dest))) {
      LOG_WARN("failed to init store", K(ret), K_(task_attr));
    } else if (OB_FAIL(store.is_file_list_file_exist(piece_path, share::ObBackupFileSuffix::ARCHIVE, is_file_list_exist))) {
      LOG_WARN("failed to check file list exist", K(ret), K(piece_path));
    } else if (is_file_list_exist) {
      if (OB_FAIL(backup::ObBackupFileListReaderUtil::get_ls_id_list(backup_piece_dest.get_storage_info(),
                                                        piece_path, share::ObBackupFileSuffix::ARCHIVE, mod, ls_ids))) {
        LOG_WARN("failed to get piece ls ids from file list, fallback to traverse", K(ret), K(piece_path), K(mod));
      }
    } else if (OB_FAIL(ObBackupUtil::get_ls_ids_from_traverse(piece_path, backup_piece_dest.get_storage_info(), ls_ids))) {
      LOG_WARN("failed to get piece ls ids from traverse", K(ret), K(piece_path), K(backup_piece_dest));
    }
  }
  return ret;
}

int ObBackupValidateTaskMgr::get_archive_piece_ls_ids_and_total_bytes_(
    const share::ObBackupDest &backup_piece_dest,
    ObIArray<ObLSID> &ls_ids,
    int64_t &total_bytes)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObArchiveStore store;
  ObPieceInfoDesc desc;
  total_bytes = 0;
  ObBackupPath piece_path;
  if (OB_FAIL(piece_path.init(backup_piece_dest.get_root_path()))) {
    LOG_WARN("failed to init piece path", K(ret), K(backup_piece_dest));
  } else if (OB_FAIL(store.init(backup_piece_dest))) {
    LOG_WARN("failed to init store", K(ret), K_(task_attr));
  } else if (task_attr_.plus_archivelog_) {
    if (OB_FAIL(get_archive_piece_ls_ids_(backup_piece_dest, piece_path, ls_ids))) {
      LOG_WARN("failed to get piece ls ids", K(ret), K(piece_path), K(backup_piece_dest));
    }
  } else if (OB_FAIL(store.read_piece_info(desc))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(get_archive_piece_ls_ids_(backup_piece_dest, piece_path, ls_ids))) {
        LOG_WARN("failed to get piece ls ids", K(ret), K(piece_path), K(backup_piece_dest));
      }
    } else {
      LOG_WARN("failed to read piece info", K(ret), K_(task_attr), K(backup_piece_dest));
    }
  } else if (!desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls info", K(ret), K(desc));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < desc.filelist_.count(); ++i) {
      const ObLSID &ls_id = desc.filelist_.at(i).ls_id_;
      if (!ls_id.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ls info", K(ret), K(ls_id));
      } else if (OB_FAIL(ls_ids.push_back(ls_id))) {
        LOG_WARN("failed to push back ls_id", K(ret), K(ls_id));
      }
      for (int j = 0; OB_SUCC(ret) && j < desc.filelist_.at(i).filelist_.count(); ++j) {
        total_bytes += desc.filelist_.at(i).filelist_.at(j).size_bytes_;
      }
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
