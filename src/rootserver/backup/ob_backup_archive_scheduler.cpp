/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "ob_backup_archive_scheduler.h"
#include "ob_backup_data_scheduler.h"
#include "ob_backup_service.h"
#include "ob_backup_task_scheduler.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "share/backup/ob_archive_mode.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_share_util.h"
#include "share/ob_cluster_version.h"
#include "share/ob_rs_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config.h"
#include "share/backup/ob_backup_config.h"  // for ENABLE_BACKUP_ARCHIVE_VERSION
#include "share/backup/ob_backup_clean_operator.h"  // for ObBackupCleanJobOperator
#include "storage/tablelock/ob_lock_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver
{

ObBackupArchiveScheduler::ObBackupArchiveScheduler()
  : ObIBackupJobScheduler(BackupJobType::BACKUP_ARCHIVE_JOB),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    sql_proxy_(nullptr),
    rpc_proxy_(nullptr),
    schema_service_(nullptr),
    task_scheduler_(nullptr),
    backup_service_(nullptr)
{
}

int ObBackupArchiveScheduler::init(
    const uint64_t tenant_id,
    ObMySQLProxy &sql_proxy,
    ObSrvRpcProxy &rpc_proxy,
    schema::ObMultiVersionSchemaService &schema_service,
    ObBackupTaskScheduler &task_scheduler,
    ObBackupArchiveService &backup_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_ARCHIVE]init twice", K(ret));
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

int ObBackupArchiveScheduler::add_job(
    const uint64_t target_tenant_id,
    const ObBackupArchiveLogAllArg &arg)
{
  int ret = OB_SUCCESS;
  ObBackupJobAttr job_attr;
  ObBackupPathString backup_archive_path;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]not init", K(ret));
  } else if (!arg.is_valid() || !is_user_tenant(target_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid argument", K(ret), K(arg), K(target_tenant_id));
  } else if (OB_FAIL(check_can_start_(target_tenant_id, backup_archive_path))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to check can start", K(ret), K(target_tenant_id), K(arg));
  } else if (OB_FAIL(fill_job_attr_(target_tenant_id, arg, backup_archive_path, job_attr))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to fill job attr", K(ret), K(target_tenant_id), K(arg));
  } else if (OB_FAIL(insert_job_(job_attr))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to insert job", K(ret), K(job_attr));
  } else {
    FLOG_INFO("[BACKUP_ARCHIVE]insert backup archive job succeed", K(job_attr));
  }
  return ret;
}

int ObBackupArchiveScheduler::check_can_start_(
    const uint64_t target_tenant_id,
    ObBackupPathString &backup_archive_path)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  bool is_valid = false;
  int64_t archive_dest_id = 0;
  ObArray<ObBackupJobAttr> jobs;

  if (OB_FAIL(GET_MIN_DATA_VERSION(target_tenant_id, data_version))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get data version", K(ret), K(target_tenant_id));
  } else if (data_version < ENABLE_BACKUP_ARCHIVE_VERSION) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[BACKUP_ARCHIVE]data version not supported", K(ret), K(target_tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup archivelog all with data version less than 4.4.2.3 is");
  } else if (OB_FAIL(ObBackupDataScheduler::check_tenant_status(*schema_service_, target_tenant_id, is_valid))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to check tenant status", K(ret), K(target_tenant_id));
  } else if (!is_valid) {
    ret = OB_BACKUP_CAN_NOT_START;
    LOG_WARN("[BACKUP_ARCHIVE]tenant status is not valid", K(ret), K(target_tenant_id));
    LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, "tenant status is not normal.");
  } else if (OB_FAIL(ObBackupJobOperator::get_jobs(*sql_proxy_, target_tenant_id, false /*need_lock*/, jobs))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get jobs", K(ret), K(target_tenant_id));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
    const ObBackupJobAttr &job = jobs.at(i);
    if (job.backup_type_.is_backup_archive() && !job.status_.is_backup_finish()) {
      ret = OB_BACKUP_IN_PROGRESS;
      LOG_WARN("[BACKUP_ARCHIVE]previous backup archivelog job is not finished", K(ret), K(job));
      LOG_USER_ERROR(OB_BACKUP_IN_PROGRESS, "previous backup archivelog job is not finished");
    }
  }

  ObArchivePersistHelper helper;
  ObArchiveMode archive_mode;
  ObTenantArchiveRoundAttr round_attr;
  if (FAILEDx(helper.init(target_tenant_id))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to init archive helper", K(ret), K(target_tenant_id));
  } else if (OB_FAIL(helper.get_backup_archive_dest(*sql_proxy_, false /*need_lock*/, backup_archive_path))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_BACKUP_CAN_NOT_START;
      LOG_WARN("[BACKUP_ARCHIVE]backup archive dest is not configured", K(ret), K(target_tenant_id));
      LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, "backup archive dest is not configured.");
    } else {
      LOG_WARN("[BACKUP_ARCHIVE]failed to get backup archive dest", K(ret), K(target_tenant_id));
    }
  } else if (backup_archive_path.is_empty()) {
    ret = OB_BACKUP_CAN_NOT_START;
    LOG_WARN("[BACKUP_ARCHIVE]backup archive dest is empty", K(ret), K(target_tenant_id));
    LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, "backup archive dest is not configured.");
  } else if (OB_FAIL(helper.get_archive_mode(*sql_proxy_, archive_mode))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get archive mode", K(ret), K(target_tenant_id));
  } else if (archive_mode.is_noarchivelog()) {
    ret = OB_BACKUP_CAN_NOT_START;
    LOG_WARN("[BACKUP_ARCHIVE]archive mode is not enabled", K(ret), K(target_tenant_id));
    LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, "backup archivelog requires archive mode and active round.");
  } else if (OB_FAIL(ObTenantArchiveMgr::get_dest_round_by_dest_no(target_tenant_id, 0 /*dest_no*/, round_attr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_BACKUP_CAN_NOT_START;
      LOG_WARN("[BACKUP_ARCHIVE]archive round does not exist", K(ret), K(target_tenant_id));
      LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, "backup archivelog requires archive mode and active round.");
    } else {
      LOG_WARN("[BACKUP_ARCHIVE]failed to get archive round", K(ret), K(target_tenant_id));
    }
  } else if (!round_attr.state_.is_doing() && !round_attr.state_.is_suspend()) {
    ret = OB_BACKUP_CAN_NOT_START;
    LOG_WARN("[BACKUP_ARCHIVE]archive round is not active", K(ret), K(target_tenant_id), K(round_attr));
    LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, "backup archivelog requires archive mode and active round.");
  }
  return ret;
}

int ObBackupArchiveScheduler::fill_job_attr_(
    const uint64_t target_tenant_id,
    const ObBackupArchiveLogAllArg &arg,
    const ObBackupPathString &backup_archive_path,
    ObBackupJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  if (OB_FAIL(backup_dest.set(backup_archive_path.ptr()))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to set backup dest", K(ret), K(target_tenant_id), K(backup_dest));
  } else if (OB_FAIL(backup_dest.get_backup_path_str(job_attr.backup_path_.ptr(), job_attr.backup_path_.capacity()))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get backup path str", K(ret));
  } else if (OB_FAIL(job_attr.description_.assign(arg.description_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to assign description", K(ret), K(arg));
  } else if (OB_FAIL(job_attr.executor_tenant_id_.push_back(target_tenant_id))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to push executor tenant id", K(ret), K(target_tenant_id));
  } else {
    job_attr.tenant_id_ = target_tenant_id;
    job_attr.initiator_tenant_id_ = arg.tenant_id_;
    job_attr.initiator_job_id_ = 0;
    job_attr.incarnation_id_ = 1;
    job_attr.backup_set_id_ = 0;
    job_attr.start_ts_ = ObTimeUtility::current_time();
    job_attr.end_ts_ = 0;
    job_attr.result_ = OB_SUCCESS;
    job_attr.plus_archivelog_ = false;
    job_attr.backup_type_.type_ = arg.delete_input_
                                ? ObBackupType::BACKUP_ARCHIVE_DELETE_INPUT
                                : ObBackupType::BACKUP_ARCHIVE;
    job_attr.encryption_mode_ = ObBackupEncryptionMode::EncryptionMode::NONE;
    job_attr.status_.status_ = ObBackupStatus::INIT;
    job_attr.backup_level_.level_ = ObBackupLevel::Level::USER_TENANT;
  }
  return ret;
}

int ObBackupArchiveScheduler::insert_job_(const ObBackupJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObBackupJobAttr new_job_attr;
  ObMySQLTransaction trans;
  ObArray<ObBackupCleanJobAttr> clean_jobs;
  if (!job_attr.is_tmplate_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid job template", K(ret), K(job_attr));
  } else if (OB_FAIL(new_job_attr.assign(job_attr))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to assign job attr", K(ret), K(job_attr));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(new_job_attr.tenant_id_)))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to start trans", K(ret), K(new_job_attr));
  } else if (OB_FAIL(ObBackupDataScheduler::get_next_job_id(trans, new_job_attr.tenant_id_, new_job_attr.job_id_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get next job id", K(ret), K(new_job_attr));
  } else if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to check leader", K(ret));
  } else if (OB_FAIL(transaction::tablelock::ObInnerTableLockUtil::lock_inner_table_in_trans(
                     trans, gen_meta_tenant_id(new_job_attr.tenant_id_), share::OB_ALL_BACKUP_DELETE_POLICY_TID,
                     transaction::tablelock::SHARE_ROW_EXCLUSIVE, false))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to acquire archive-clean coordination lock", K(ret), K(new_job_attr));
  } else if (OB_FAIL(ObBackupCleanJobOperator::get_jobs(trans, new_job_attr.tenant_id_, false /*need_lock*/, clean_jobs))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get clean jobs", K(ret), K(new_job_attr));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < clean_jobs.count(); ++i) {
    const ObBackupCleanJobAttr &job = clean_jobs.at(i);
    if (job.is_delete_obsolete_backup() && !job.status_.is_finish()) {
      ret = OB_BACKUP_CAN_NOT_START;
      LOG_WARN("[BACKUP_ARCHIVE] backup clean job is running, rollback", K(ret), K(job), K(new_job_attr));
      LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, "backup archive job can't start because backup clean job is running");
    }
  }

  if (FAILEDx(ObBackupJobOperator::insert_job(trans, new_job_attr))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to insert job", K(ret), K(new_job_attr));
  } else if (OB_FAIL(trans.end(true))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to commit trans", K(ret));
  } else {
    LOG_INFO("[BACKUP_ARCHIVE]succeed insert backup archive job", K(new_job_attr));
  }
  if (OB_FAIL(ret) && trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to rollback trans", K(tmp_ret));
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::process()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupJobAttr> jobs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]not init", K(ret));
  } else if (OB_FAIL(ObBackupJobOperator::get_jobs(*sql_proxy_, tenant_id_, false /*need_lock*/, jobs))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get jobs", K(ret), K_(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
      ObBackupJobAttr &job_attr = jobs.at(i);
      if (!job_attr.backup_type_.is_backup_archive()) {
        // do nothing
      } else if (!job_attr.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_ARCHIVE]invalid job attr", K(ret), K(job_attr));
      } else if (OB_FAIL(process_job_(job_attr))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to process job", K(ret), K(job_attr));
        backup_service_->wakeup();
      }
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::process_job_(ObBackupJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("[BACKUP_ARCHIVE]not leader, skip process job", K(ret), K(job_attr));
  } else if (ObBackupStatus::INIT == job_attr.status_.status_) {
    if (OB_FAIL(start_job_(job_attr))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to start job", K(ret), K(job_attr));
    }
  } else if (ObBackupStatus::DOING == job_attr.status_.status_) {
    if (OB_FAIL(process_doing_job_(job_attr))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to process doing job", K(ret), K(job_attr));
    }
  } else if (ObBackupStatus::CANCELING == job_attr.status_.status_) {
    if (OB_FAIL(cancel_job_(job_attr))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to cancel job", K(ret), K(job_attr));
    }
  } else if (job_attr.status_.is_backup_finish()) { // move piece tasks and job to history table
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(job_attr.tenant_id_)))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to start trans", K(ret), K(job_attr));
    } else if (OB_FAIL(ObBackupArchivePieceTaskOperator::move_tasks_to_his(trans, job_attr.tenant_id_, job_attr.job_id_))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to move piece tasks to history", K(ret), K(job_attr));
    } else if (OB_FAIL(ObBackupJobOperator::move_job_to_his(trans, job_attr.tenant_id_, job_attr.job_id_))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to move job to history", K(ret), K(job_attr));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to end trans", K(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::advance_job_status_(
    ObISQLClient &proxy,
    const ObBackupJobAttr &job_attr,
    const ObBackupStatus &next_status,
    const int result,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to check leader", K(ret), K(job_attr), K(next_status));
  } else if (OB_FAIL(ObBackupJobOperator::advance_job_status(proxy, job_attr, next_status, result, end_ts))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to advance job status", K(ret), K(job_attr), K(next_status), K(result), K(end_ts));
  }
  return ret;
}

int ObBackupArchiveScheduler::start_job_(ObBackupJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  int64_t archive_dest_id = 0;
  ObArray<ObTenantArchivePieceAttr> pieces;
  ObArray<ObBackupArchivePieceTaskAttr> piece_tasks;
  ObMySQLTransaction trans;
  ObArchivePersistHelper helper;
  ObBackupStatus next_status(ObBackupStatus::DOING);

  if (OB_FAIL(helper.init(job_attr.tenant_id_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to init archive helper", K(ret), K(job_attr));
  } else if (OB_FAIL(helper.get_dest_id(*sql_proxy_, false /*need_lock*/, 0 /*dest_no*/, archive_dest_id))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get archive dest id", K(ret), K(job_attr));
  } else if (OB_FAIL(helper.get_unbackuped_frozen_pieces(*sql_proxy_, archive_dest_id, pieces))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get unbackuped pieces", K(ret), K(job_attr), K(archive_dest_id));
  } else if (pieces.empty()) {
    next_status.status_ = ObBackupStatus::COMPLETED;
    // no piece needs to backup, no need to schedule clean job
    if (OB_FAIL(advance_job_status_(*sql_proxy_, job_attr, next_status, OB_SUCCESS, ObTimeUtility::current_time()))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to advance job to completed", K(ret), K(job_attr));
    }
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(job_attr.tenant_id_)))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to start trans", K(ret), K(job_attr));
  } else if (OB_FAIL(build_piece_tasks_(job_attr, archive_dest_id, pieces, piece_tasks))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to build piece tasks", K(ret), K(job_attr), K(pieces));
  } else if (OB_FAIL(ObBackupArchivePieceTaskOperator::insert_tasks(trans, piece_tasks))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to insert piece tasks", K(ret), K(job_attr), K(piece_tasks));
  } else if (OB_FAIL(advance_job_status_(trans, job_attr, next_status))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to advance job to doing", K(ret), K(job_attr));
  } else if (OB_FAIL(trans.end(true))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to commit trans", K(ret), K(job_attr));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ret) && trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(false))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to rollback trans", K(tmp_ret));
    }
    ObBackupStatus failed_status(ObBackupStatus::FAILED);
    if (OB_TMP_FAIL(advance_job_status_(*sql_proxy_, job_attr, failed_status, ret, ObTimeUtility::current_time()))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to advance job to failed", K(tmp_ret), K(job_attr));
    }
  }
  if (OB_SUCC(ret) && ObBackupStatus::DOING == next_status.status_) {
    if (OB_TMP_FAIL(dispatch_init_piece_tasks_(job_attr))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to dispatch piece tasks, will retry in doing", K(tmp_ret), K(job_attr));
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::build_piece_tasks_(
    const ObBackupJobAttr &job_attr,
    const int64_t archive_dest_id,
    const ObIArray<ObTenantArchivePieceAttr> &pieces,
    ObIArray<ObBackupArchivePieceTaskAttr> &tasks)
{
  int ret = OB_SUCCESS;
  tasks.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < pieces.count(); ++i) {
    const ObTenantArchivePieceAttr &piece = pieces.at(i);
    ObBackupArchivePieceTaskAttr task;
    task.key_.tenant_id_ = job_attr.tenant_id_;
    task.key_.job_id_ = job_attr.job_id_;
    task.key_.round_id_ = piece.key_.round_id_;
    task.key_.piece_id_ = piece.key_.piece_id_;
    task.archive_dest_id_ = archive_dest_id;
    task.task_status_.status_ = ObBackupTaskStatus::INIT;
    task.retry_cnt_ = 0;
    task.result_ = OB_SUCCESS;
    if (OB_FAIL(tasks.push_back(task))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to push piece task", K(ret), K(task));
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::dispatch_init_piece_tasks_(const ObBackupJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupArchivePieceTaskAttr> piece_tasks;
  if (OB_FAIL(ObBackupArchivePieceTaskOperator::get_tasks(
      *sql_proxy_, job_attr.tenant_id_, job_attr.job_id_, false /*need_lock*/, piece_tasks))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get piece tasks", K(ret), K(job_attr));
  }

  // if failed, retry in the later round
  const ObBackupTaskStatus pending_status(ObBackupTaskStatus::PENDING);
  for (int64_t i = 0; OB_SUCC(ret) && i < piece_tasks.count(); ++i) {
    int tmp_ret = OB_SUCCESS;
    ObBackupArchivePieceTaskAttr task = piece_tasks.at(i);
    if (ObBackupTaskStatus::INIT == task.task_status_.status_) {
      if (OB_TMP_FAIL(ObBackupArchivePieceTaskOperator::update_task_status(
          *sql_proxy_, task.key_, task.task_status_.status_, pending_status))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to persist piece task pending, will retry", K(tmp_ret), K(task));
      } else {
        task.task_status_ = pending_status;
      }
    }
    if (task.task_status_.is_pending()) {
      ObBackupArchivePieceTask piece_task;
      if (OB_TMP_FAIL(piece_task.build(task, job_attr.backup_path_))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to build piece task", K(tmp_ret), K(piece_task), K(job_attr), K(task));
      } else if (OB_TMP_FAIL(task_scheduler_->add_task(piece_task))) {
        if (OB_ENTRY_EXIST != tmp_ret) {
          LOG_WARN("[BACKUP_ARCHIVE]failed to add task to scheduler", K(tmp_ret), K(task));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::process_doing_job_(ObBackupJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupArchivePieceTaskAttr> piece_tasks;
  int64_t failed_cnt = 0;
  int first_failed_result = OB_SUCCESS;
  bool all_reached_terminal = true;
  if (OB_FAIL(ObBackupArchivePieceTaskOperator::get_tasks(
      *sql_proxy_, job_attr.tenant_id_, job_attr.job_id_, false /*need_lock*/, piece_tasks))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get piece tasks", K(ret), K(job_attr));
  } else if (piece_tasks.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]doing job has no piece tasks", K(ret), K(job_attr));
  } else {
    for (int64_t i = 0; i < piece_tasks.count(); ++i) {
      const ObBackupArchivePieceTaskAttr &task = piece_tasks.at(i);
      if (ObBackupTaskStatus::FINISH == task.task_status_.status_) {
        if (OB_SUCCESS != task.result_) {
          ++failed_cnt;
          if (OB_SUCCESS == first_failed_result) {
            first_failed_result = task.result_; // keep the real error code of the first failed piece task
          }
        }
      } else {
        all_reached_terminal = false;
        break;
      }
    }
    if (all_reached_terminal) {
      ObBackupStatus next_status;
      const int64_t end_ts = ObTimeUtility::current_time();
      if (failed_cnt > 0) {
        next_status.status_ = ObBackupStatus::FAILED;
        if (OB_FAIL(advance_job_status_(*sql_proxy_, job_attr, next_status, first_failed_result, end_ts))) {
          LOG_WARN("[BACKUP_ARCHIVE]failed to advance job to failed", K(ret), K(job_attr), K(failed_cnt), K(first_failed_result));
        }
      } else {
        next_status.status_ = ObBackupStatus::COMPLETED;
        if (OB_FAIL(schedule_delete_backed_up_archive_pieces_(job_attr))) {
          LOG_WARN("[BACKUP_ARCHIVE]failed to schedule delete backed up archive pieces, will retry", K(ret), K(job_attr));
        } else if (OB_FAIL(advance_job_status_(*sql_proxy_, job_attr, next_status, OB_SUCCESS, end_ts))) {
          LOG_WARN("[BACKUP_ARCHIVE]failed to advance job to completed", K(ret), K(job_attr));
        }
      }
    } else if (OB_FAIL(dispatch_init_piece_tasks_(job_attr))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to dispatch init piece tasks", K(ret), K(job_attr));
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::schedule_delete_backed_up_archive_pieces_(const ObBackupJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  int64_t archive_dest_id = 0;
  ObArchivePersistHelper helper;
  ObAddr rs_addr;
  obrpc::ObBackupCleanArg clean_arg;
  if (!job_attr.backup_type_.is_backup_archive_delete_input()) {
    // not a delete input archive job, no clean job to schedule
  } else if (OB_FAIL(helper.init(job_attr.tenant_id_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to init archive helper", K(ret), K(job_attr));
  } else if (OB_FAIL(helper.get_dest_id(*sql_proxy_, false /*need_lock*/, 0 /*dest_no*/, archive_dest_id))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get archive dest id", K(ret), K(job_attr));
  } else {
    clean_arg.type_ = ObNewBackupCleanType::DELETE_BACKED_UP_ARCHIVE_PIECE;
    clean_arg.tenant_id_ = job_attr.tenant_id_;
    clean_arg.initiator_tenant_id_ = job_attr.tenant_id_;
    clean_arg.initiator_job_id_ = job_attr.job_id_;
    clean_arg.dest_id_ = archive_dest_id;
    clean_arg.dest_type_ = ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG;
    if (!clean_arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[BACKUP_ARCHIVE]invalid clean arg", K(ret), K(clean_arg), K(job_attr));
    } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("[BACKUP_ARCHIVE]rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to get rootservice address", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).backup_delete(clean_arg))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to schedule delete backed up archive pieces", K(ret), K(clean_arg));
    } else {
      LOG_INFO("[BACKUP_ARCHIVE]succeed schedule delete backed up archive pieces", K(clean_arg), K(job_attr));
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::cancel_job_(ObBackupJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupArchivePieceTaskAttr> piece_tasks;
  bool all_reached_terminal = true;
  if (OB_FAIL(ObBackupArchivePieceTaskOperator::get_tasks(
      *sql_proxy_, job_attr.tenant_id_, job_attr.job_id_, false /*need_lock*/, piece_tasks))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get piece tasks when canceling", K(ret), K(job_attr));
  } else {
    for (int64_t i = 0; i < piece_tasks.count(); ++i) {
      const ObBackupArchivePieceTaskAttr &task = piece_tasks.at(i);
      if (ObBackupTaskStatus::FINISH != task.task_status_.status_) {
        all_reached_terminal = false;
        break;
      }
    }
    if (all_reached_terminal) {
      ObBackupStatus next_status(ObBackupStatus::CANCELED);
      if (OB_FAIL(advance_job_status_(
          *sql_proxy_, job_attr, next_status, OB_CANCELED, ObTimeUtility::current_time()))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to advance job to canceled", K(ret), K(job_attr));
      }
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::force_cancel(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObBackupStatus canceling_status(ObBackupStatus::CANCELING);
  ObBackupStatus init_status(ObBackupStatus::INIT);
  ObBackupStatus doing_status(ObBackupStatus::DOING);
  ObBackupType backup_archive_type;
  backup_archive_type.type_ = ObBackupType::BACKUP_ARCHIVE;
  ObBackupType backup_archive_delete_input_type;
  backup_archive_delete_input_type.type_ = ObBackupType::BACKUP_ARCHIVE_DELETE_INPUT;
  const uint64_t exec_tenant_id = is_user_tenant(tenant_id) ? gen_meta_tenant_id(tenant_id) : tenant_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]not init", K(ret));
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set %s='%s' where %s=%lu and %s in ('%s','%s') and %s in ('%s','%s')",
      OB_ALL_BACKUP_JOB_TNAME, OB_STR_STATUS, canceling_status.get_str(),
      OB_STR_TENANT_ID, tenant_id, OB_STR_BACKUP_TYPE, backup_archive_type.get_backup_type_str(),
      backup_archive_delete_input_type.get_backup_type_str(),
      OB_STR_STATUS, init_status.get_str(), doing_status.get_str()))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to build cancel sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql_proxy_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to cancel backup archive jobs", K(ret), K(sql), K(tenant_id));
  } else {
    backup_service_->wakeup();
    LOG_INFO("[BACKUP_ARCHIVE]force cancel backup archive jobs", K(tenant_id), K(affected_rows));
  }
  return ret;
}

// The following scenes will call this function:
// 1. ObService::report_backup_archive_over, observer return a rpc to tell task scheduler task finish (success or fail)
// 2. ObBackupTaskScheduler::check_alive, task scheduler find a task not on the dest observer
// 3. ObBackupTaskScheduler::execute_task_, task scheduler execute task failed
int ObBackupArchiveScheduler::handle_execute_over(
    const ObBackupScheduleTask *task,
    const share::ObHAResultInfo &result_info,
    bool &can_remove)
{
  int ret = OB_SUCCESS;
  can_remove = false;
  bool is_tenant_valid = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]not init", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid argument", K(ret));
  } else if (OB_FAIL(ObBackupDataScheduler::check_tenant_status(*schema_service_, task->get_tenant_id(), is_tenant_valid))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to check tenant status", K(ret), KPC(task));
  } else if (OB_UNLIKELY(!is_tenant_valid)) {
    can_remove = true;
    LOG_WARN("[BACKUP_ARCHIVE]tenant is not valid, remove task", KPC(task));
  } else {
    const ObBackupArchivePieceTaskAttr::Key key(task->get_tenant_id(), task->get_job_id(), task->get_round_id(), task->get_piece_id());
    ObBackupArchivePieceTaskAttr cur_task;
    if (OB_FAIL(ObBackupArchivePieceTaskOperator::get_task(*sql_proxy_, key, false /*need_lock*/, cur_task))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        can_remove = true; // the task has been removed from inner table
      } else {
        LOG_WARN("[BACKUP_ARCHIVE]failed to get piece task", K(ret), K(key));
      }
    } else if (!cur_task.task_trace_id_.equals(task->get_trace_id())
            || ObBackupTaskStatus::DOING != cur_task.task_status_.status_) {
      // stale or concurrently changed result, drop the in-memory task; reload will readd it if still pending
      can_remove = true;
      LOG_INFO("[BACKUP_ARCHIVE]piece task changed concurrently, remove stale in-memory task", K(key), K(cur_task), KPC(task));
    } else {
      const ObBackupTaskStatus cur_status(ObBackupTaskStatus::DOING);
      const ObBackupTaskStatus next_status = (OB_SUCCESS != result_info.result_ && cur_task.retry_cnt_ < MAX_PIECE_TASK_RETRY_CNT)
                                           ? ObBackupTaskStatus(ObBackupTaskStatus::PENDING)
                                           : ObBackupTaskStatus(ObBackupTaskStatus::FINISH);
      const int64_t new_retry_cnt = OB_SUCCESS == result_info.result_ ? cur_task.retry_cnt_ : cur_task.retry_cnt_ + 1;
      if (OB_FAIL(ObBackupArchivePieceTaskOperator::report_result(*sql_proxy_, key, cur_status, next_status, new_retry_cnt, result_info.result_))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
          can_remove = true;
          LOG_INFO("[BACKUP_ARCHIVE]piece task status changed concurrently, ignore report", K(key));
        } else {
          LOG_WARN("[BACKUP_ARCHIVE]failed to report piece task result", K(ret), K(key), K(next_status));
        }
      } else {
        can_remove = true;
        backup_service_->wakeup();
      }
    }
  }
  return ret;
}

int ObBackupArchiveScheduler::get_need_reload_task(
    ObIAllocator &allocator,
    ObIArray<ObBackupScheduleTask *> &tasks)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObBackupJobAttr, 4> jobs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]not init", K(ret));
  } else if (OB_FAIL(ObBackupJobOperator::get_jobs(*sql_proxy_, tenant_id_, false /*need_lock*/, jobs))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get jobs", K(ret), K_(tenant_id));
  }

  // tenant only has one backup archive job
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
    const ObBackupJobAttr &job = jobs.at(i);
    ObArray<ObBackupArchivePieceTaskAttr> piece_tasks;
    if (!job.backup_type_.is_backup_archive() || ObBackupStatus::DOING != job.status_.status_) {
      // only doing archive jobs have tasks to rebuild
    } else if (OB_UNLIKELY(job.backup_path_.is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_ARCHIVE]doing job has empty backup path", K(ret), K(job));
    } else if (OB_FAIL(ObBackupArchivePieceTaskOperator::get_tasks(
        *sql_proxy_, job.tenant_id_, job.job_id_, false /*need_lock*/, piece_tasks))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to get piece tasks", K(ret), K(job));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < piece_tasks.count(); ++j) {
        const ObBackupArchivePieceTaskAttr &piece_task = piece_tasks.at(j);
        ObBackupArchivePieceTask tmp_task;
        ObBackupScheduleTask *clone_task = nullptr;
        if (ObBackupTaskStatus::PENDING != piece_task.task_status_.status_ &&
            ObBackupTaskStatus::DOING != piece_task.task_status_.status_) {
          // INIT/FINISH task, do nothing
        } else if (OB_FAIL(tmp_task.build(piece_task, job.backup_path_))) {
          LOG_WARN("[BACKUP_ARCHIVE]failed to build piece task", K(ret), K(piece_task), K(job));
        } else if (OB_FAIL(tmp_task.clone(allocator, clone_task))) {
          LOG_WARN("[BACKUP_ARCHIVE]failed to clone piece task", K(ret), K(piece_task));
        } else if (OB_ISNULL(clone_task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[BACKUP_ARCHIVE]cloned task is null", K(ret), K(piece_task));
        } else if (OB_FAIL(tasks.push_back(clone_task))) {
          LOG_WARN("[BACKUP_ARCHIVE]failed to push back task", K(ret), K(*clone_task));
        }
      }
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
