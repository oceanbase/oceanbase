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

#define USING_LOG_PREFIX ARCHIVE
#include "ob_archive_mgr.h"
#include "lib/oblog/ob_log_module.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "ob_archive_util.h"
#include "clog/ob_i_log_engine.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_group.h"
#include "lib/thread/ob_thread_name.h"

using namespace oceanbase::common;
using namespace oceanbase::clog;
using namespace oceanbase::share;
namespace oceanbase {
namespace archive {
ObArchiveMgr::ObArchiveMgr()
    : inited_(false),
      last_check_stop_ts_(OB_INVALID_TIMESTAMP),
      server_start_archive_tstamp_(OB_INVALID_TIMESTAMP),
      self_(),
      allocator_(),
      archive_scheduler_(),
      ilog_fetcher_(),
      archive_sender_(),
      clog_split_engine_(),
      log_wrapper_(),
      pg_mgr_(),
      log_engine_(NULL),
      ext_log_service_(NULL),
      partition_service_(NULL)
{}

ObArchiveMgr::~ObArchiveMgr()
{
  destroy();

  ARCHIVE_LOG(INFO, "=============archive mgr destroy==========");
}

int ObArchiveMgr::init(ObILogEngine* log_engine, logservice::ObExtLogService* ext_log_service,
    ObPartitionService* partition_service, const ObAddr& addr)
{
  ARCHIVE_LOG(INFO, "ObArchiveMgr begin init");
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveMgr has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(log_engine) || OB_ISNULL(ext_log_service) || OB_ISNULL(partition_service) ||
             OB_UNLIKELY(!addr.is_valid())) {
    ARCHIVE_LOG(WARN, "invalid argument", K(log_engine), K(ext_log_service), K(partition_service), K(addr));
  } else {
    log_engine_ = log_engine;
    ext_log_service_ = ext_log_service;
    partition_service_ = partition_service;
    self_ = addr;
    if (OB_FAIL(init_components_())) {
      ARCHIVE_LOG(WARN, "ObArchiveMgr init fail", KR(ret));
    } else {
      inited_ = true;

      ARCHIVE_LOG(INFO, "ObArchiveMgr init succ", KR(ret));
    }
  }
  if (OB_SUCCESS != ret && !inited_) {
    destroy();
  }

  return ret;
}

void ObArchiveMgr::destroy()
{
  inited_ = false;

  destroy_components_();
}

int ObArchiveMgr::start()
{
  ARCHIVE_LOG(INFO, "ObArchiveMgr threads begin start");
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(start_components_())) {
    ARCHIVE_LOG(WARN, "start_components_ fail", KR(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    ARCHIVE_LOG(WARN, "ObArchiveMgr start fail", KR(ret));
  } else {
    ARCHIVE_LOG(INFO, "ObArchiveMgr threads start succ");
  }

  return ret;
}

void ObArchiveMgr::stop()
{
  ARCHIVE_LOG(INFO, "ObArchiveMgr stop begin");

  pg_mgr_.stop();
  ilog_fetcher_.stop();
  archive_scheduler_.stop();
  clog_split_engine_.stop();
  archive_sender_.stop();
  ObThreadPool::stop();

  ARCHIVE_LOG(INFO, "ObArchiveMgr stop end");
}

void ObArchiveMgr::wait()
{
  ARCHIVE_LOG(INFO, "ObArchiveMgr wait begin");

  pg_mgr_.wait();
  ilog_fetcher_.wait();
  archive_scheduler_.wait();
  clog_split_engine_.wait();
  archive_sender_.wait();
  ObThreadPool::wait();

  ARCHIVE_LOG(INFO, "ObArchiveMgr wait end");
}

int ObArchiveMgr::add_pg_log_archive_task(ObIPartitionGroup* partition)
{
  int ret = OB_SUCCESS;
  bool enable_log_archive = GCONF.enable_log_archive;
  bool unused_added = false;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init", KR(ret), KPC(partition));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "patition is NULL", KR(ret), KP(partition));
  } else if (OB_SYS_TENANT_ID == partition->get_partition_key().get_tenant_id()) {
    // skip sys pgs
  } else if (!enable_log_archive) {
    // do nothing
  } else if (OB_FAIL(pg_mgr_.add_pg_archive_task(partition, unused_added))) {
    ARCHIVE_LOG(WARN, "add_pg_archive_task fail");
  }

  return ret;
}

int ObArchiveMgr::delete_pg_log_archive_task(ObIPartitionGroup* partition)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init", KR(ret), KPC(partition));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "patition is NULL", KR(ret), KP(partition));
  } else if (OB_SYS_TENANT_ID == partition->get_partition_key().get_tenant_id()) {
    // skip sys pgs
  } else if (!is_in_archive_status()) {
    // do nothing
  } else if (OB_FAIL(pg_mgr_.delete_pg_archive_task(partition))) {
    ARCHIVE_LOG(WARN, "delete_pg_archive_task fail");
  }

  return ret;
}

int ObArchiveMgr::submit_checkpoint_task(const common::ObPartitionKey& pg_key, const uint64_t max_log_id,
    const int64_t max_log_submit_ts, const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveMgr not init");
    ret = OB_NOT_INIT;
  } else if (!archive_round_mgr_.is_in_archive_doing_status()) {
    ret = OB_LOG_ARCHIVE_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      ARCHIVE_LOG(WARN, "archive not run, skip checkpoint task", KR(ret), K(pg_key));
    }
  } else if (OB_FAIL(ilog_fetcher_.generate_and_submit_checkpoint_task(
                 pg_key, max_log_id, max_log_submit_ts, checkpoint_ts))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN,
          "generate_and_submit_checkpoint_task fail",
          KR(ret),
          K(pg_key),
          K(max_log_id),
          K(max_log_submit_ts),
          K(checkpoint_ts));
    } else {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
        ARCHIVE_LOG(WARN, "pg not exist", K(ret), K(pg_key), K(max_log_id), K(max_log_submit_ts), K(checkpoint_ts));
      }
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObArchiveMgr::get_log_archive_status(common::ObPartitionKey& pg_key, ObPGLogArchiveStatus& status, int64_t& epoch)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTaskGuard guard(&pg_mgr_);
  ObPGArchiveTask* task = NULL;
  status.reset();

  int64_t archive_incarnation = -1;
  int64_t archive_round = -1;
  bool has_encount_error = false;
  ObArchiveRoundMgr::LogArchiveStatus log_archive_status;
  archive_round_mgr_.get_archive_round_info(archive_incarnation, archive_round, log_archive_status, has_encount_error);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveMgr not init", KR(ret), K(pg_key));
  } else if (ObArchiveRoundMgr::LogArchiveStatus::LOG_ARCHIVE_STOPPED == log_archive_status) {
    status.archive_incarnation_ = archive_incarnation;
    status.log_archive_round_ = archive_round;
    status.status_ = share::ObLogArchiveStatus::STOP;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ARCHIVE_LOG(INFO, "log archive stopped", KR(ret), K(pg_key), K(status));
    }
  } else if (has_encount_error) {
    status.archive_incarnation_ = archive_incarnation;
    status.log_archive_round_ = archive_round;
    status.status_ = share::ObLogArchiveStatus::INTERRUPTED;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ARCHIVE_LOG(WARN, "log archive has encount fatal error", KR(ret), K(pg_key), K(status));
    }
  } else if (ObArchiveRoundMgr::LogArchiveStatus::LOG_ARCHIVE_BEGINNING == log_archive_status ||
             ObArchiveRoundMgr::LogArchiveStatus::LOG_ARCHIVE_INVALID_STATUS == log_archive_status) {
    // return invalid status
    ret = OB_SUCCESS;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ARCHIVE_LOG(INFO,
          "log archive start prepare not ready",
          K(pg_key),
          K(archive_incarnation),
          K(archive_round),
          K(log_archive_status));
    }
  } else if (OB_FAIL(pg_mgr_.get_pg_archive_task_guard(pg_key, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // return invalid status
      ret = OB_SUCCESS;
    } else {
      ARCHIVE_LOG(WARN, "get_pg_archive_task_guard fail", KR(ret), K(pg_key));
    }
  } else if (OB_ISNULL(task = guard.get_pg_archive_task())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(pg_key));
  } else {
    task->get_pg_log_archive_status(status, epoch);

    if (REACH_TIME_INTERVAL(1000 * 1000L)) {
      ARCHIVE_LOG(INFO, "get_log_archive_status succ", K(pg_key), K(status));
    }
  }

  return ret;
}

int ObArchiveMgr::get_archive_pg_map(PGArchiveMap*& map)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveMgr not init", KR(ret));
  } else if (OB_FAIL(pg_mgr_.get_archive_pg_map(map))) {
    ARCHIVE_LOG(WARN, "get_archive_pg_map fail", KR(ret));
  }

  return ret;
}

bool ObArchiveMgr::is_in_archive_status() const
{
  return archive_round_mgr_.is_in_archive_status();
}

bool ObArchiveMgr::is_in_archive_beginning_status() const
{
  return archive_round_mgr_.is_in_archive_beginning_status();
}

bool ObArchiveMgr::is_in_archive_doing_status() const
{
  return archive_round_mgr_.is_in_archive_doing_status();
}

bool ObArchiveMgr::is_in_archive_stopping_status() const
{
  return archive_round_mgr_.is_in_archive_stopping_status();
}

bool ObArchiveMgr::is_in_archive_stop_status() const
{
  return archive_round_mgr_.is_in_archive_stopped_status();
}

bool ObArchiveMgr::is_server_archive_stop(const int64_t incarnation, const int64_t round)
{
  return archive_round_mgr_.is_server_archive_stop(incarnation, round);
}

void ObArchiveMgr::notify_all_archive_round_started()
{
  ilog_fetcher_.notify_start_archive_round();
  archive_round_mgr_.update_log_archive_status(ObArchiveRoundMgr::LogArchiveStatus::LOG_ARCHIVE_DOING);
}

// rs controls archive start/stop, while archive modules only stop archive data when encounter fatal error
int ObArchiveMgr::mark_encounter_fatal_err(
    const common::ObPartitionKey& pg_key, const int64_t incarnation, const int64_t log_archive_round)
{
  return archive_round_mgr_.mark_fatal_error(pg_key, incarnation, log_archive_round);
}

bool ObArchiveMgr::has_encounter_fatal_err(const int64_t incarnation, const int64_t log_archive_round)
{
  return archive_round_mgr_.has_encounter_fatal_error(incarnation, log_archive_round);
}

//================================== end of public functions ================================//
//
//================================= begin of private functions ================================//

int ObArchiveMgr::handle_start_archive_(const int64_t incarnation, const int64_t archive_round)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pg_mgr_.set_archive_round_info(archive_round, incarnation))) {
    ARCHIVE_LOG(ERROR, "failed to set_archive_round_info", KR(ret), K(archive_round), K(incarnation));
  } else if (OB_FAIL(ilog_fetcher_.set_archive_round_info(archive_round, incarnation))) {
    ARCHIVE_LOG(ERROR, "failed to set_archive_round_info", KR(ret), K(archive_round), K(incarnation));
  } else if (OB_FAIL(archive_sender_.notify_start(archive_round, incarnation))) {
    ARCHIVE_LOG(ERROR, "archive_sender_ notify_start fail", KR(ret));
  } else if (OB_FAIL(clog_split_engine_.notify_start(archive_round, incarnation))) {
    ARCHIVE_LOG(ERROR, "failed to notify_start", KR(ret), K(archive_round), K(incarnation));
  } else if (OB_FAIL(pg_mgr_.add_all_pg_on_start_archive_task(incarnation, archive_round))) {
    ARCHIVE_LOG(WARN, "add_all_pg_on_start_archive_task fail", KR(ret), K(incarnation), K(archive_round));
  }

  return ret;
}

int ObArchiveMgr::init_components_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(archive_round_mgr_.init())) {
    ARCHIVE_LOG(WARN, "archive_round_mgr_ init fail", KR(ret));
  } else if (OB_FAIL(allocator_.init())) {
    ARCHIVE_LOG(WARN, "allocator_ init fail", KR(ret));
  } else if (OB_FAIL(ilog_fetch_task_mgr_.init(&allocator_))) {
    ARCHIVE_LOG(WARN, "ilog_fetch_task_mgr_ init fail", KR(ret));
  } else if (OB_FAIL(log_wrapper_.init(partition_service_, log_engine_))) {
    ARCHIVE_LOG(WARN, "log_wrapper_ init fail", KR(ret));
  } else if (OB_FAIL(pg_mgr_.init(&allocator_, &log_wrapper_, partition_service_, &archive_round_mgr_, this))) {
    ARCHIVE_LOG(WARN, "pg_mgr_ init fail", KR(ret));
  } else if (OB_FAIL(archive_sender_.init(&allocator_, &pg_mgr_, &archive_round_mgr_, this))) {
    ARCHIVE_LOG(WARN, "archive_sender_ init fail", KR(ret));
  } else if (OB_FAIL(clog_split_engine_.init(ext_log_service_, &allocator_, &archive_sender_, this))) {
    ARCHIVE_LOG(WARN, "clog_split_engine_ init fail", KR(ret));
  } else if (OB_FAIL(archive_scheduler_.init(&allocator_, &clog_split_engine_, &archive_sender_))) {
    ARCHIVE_LOG(WARN, "archive_scheduler_ init fail", KR(ret));
  } else if (OB_FAIL(ilog_fetcher_.init(
                 &allocator_, log_engine_, &log_wrapper_, &pg_mgr_, &clog_split_engine_, &ilog_fetch_task_mgr_))) {
    ARCHIVE_LOG(ERROR, "ilog_fetcher_ init fail", KR(ret));
  } else {
    ARCHIVE_LOG(INFO, "components init succ");
  }

  return ret;
}

void ObArchiveMgr::destroy_components_()
{
  (void)ilog_fetch_task_mgr_.destroy();
  (void)archive_round_mgr_.destroy();
  pg_mgr_.destroy();
  ilog_fetcher_.destroy();
  clog_split_engine_.destroy();
  archive_sender_.destroy();
}

int ObArchiveMgr::start_archive_(share::ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  int64_t start_archive_ts = ObTimeUtility::current_time();
  const int64_t incarnation = info.status_.incarnation_;
  const int64_t archive_round = info.status_.round_;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(check_and_set_start_archive_ts_(incarnation, archive_round, start_archive_ts))) {
    ARCHIVE_LOG(WARN, "check_and_set_start_archive_ts_ fail", KR(ret), K(start_archive_ts), K(archive_round));
  } else if (OB_FAIL(pg_mgr_.set_server_start_archive_ts(server_start_archive_tstamp_))) {
    ARCHIVE_LOG(WARN, "set_start_archive_ts fail", KR(ret), K(server_start_archive_tstamp_));
  } else if (OB_FAIL(handle_start_archive_(incarnation, archive_round))) {
    ARCHIVE_LOG(WARN, "handle_start_archive_ fail", KR(ret));
  } else if (OB_FAIL(set_log_archive_info_(info))) {
    ARCHIVE_LOG(INFO, "set_log_archive_info_ fail", KR(ret));
  } else {
    ARCHIVE_LOG(INFO, "start archive succ");
  }

  return ret;
}

// first stop ilog_fetcher when stop archive
int ObArchiveMgr::stop_archive_()
{
  int ret = OB_SUCCESS;
  int64_t archive_round = archive_round_mgr_.get_current_archive_round();
  int64_t incarnation = archive_round_mgr_.get_current_archive_incarnation();

  ilog_fetcher_.notify_stop();
  ilog_fetch_task_mgr_.reset();
  (void)clog_split_engine_.notify_stop(archive_round, incarnation);
  (void)archive_sender_.notify_stop(archive_round, incarnation);

  archive_round_mgr_.update_log_archive_status(ObArchiveRoundMgr::LogArchiveStatus::LOG_ARCHIVE_IN_STOPPING);
  last_check_stop_ts_ = ObTimeUtility::current_time();
  server_start_archive_tstamp_ = OB_INVALID_TIMESTAMP;

  ARCHIVE_LOG(INFO, "archive into stopping", K(incarnation), K(archive_round));
  return ret;
}

// archive_status migrate path:
//
//                                   all_pg_start
//    invalid ======> beginning =================> doing
//       ||              ^                           ||
//       ||             ||                           ||
//       ||             ||start_archive              || stop_archive
//       ||             ||                           ||
//       ||  special    ||      all_task/pg clean     V
//        ==========> stopped  <=================== stopping
//
// sp: force stop when rs is in doing and server restart and lost its status
int ObArchiveMgr::set_log_archive_stop_status_()
{
  int ret = OB_SUCCESS;

  if (!archive_round_mgr_.is_in_archive_stopping_status()) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "invalid archive_status switch", KR(ret));
  } else {
    archive_round_mgr_.update_log_archive_status(ObArchiveRoundMgr::LogArchiveStatus::LOG_ARCHIVE_STOPPED);
  }

  return ret;
}

int ObArchiveMgr::clear_pg_archive_task_()
{
  int ret = OB_SUCCESS;

  pg_mgr_.reset_tasks();

  return ret;
}

int ObArchiveMgr::clear_archive_info_()
{
  int ret = OB_SUCCESS;

  pg_mgr_.clear_archive_info();
  ilog_fetcher_.clear_archive_info();
  clog_split_engine_.clear_archive_info();
  archive_sender_.clear_archive_info();

  return ret;
}

bool ObArchiveMgr::check_archive_task_empty_()
{
  bool bret = false;
  int64_t clog_split_task_num = clog_split_engine_.get_total_task_num();
  int64_t sender_task_num = archive_sender_.get_total_task_num();

  if (0 == clog_split_task_num && 0 == sender_task_num) {
    bret = true;
    ARCHIVE_LOG(INFO, "pendding task is clear");
  } else {
    ARCHIVE_LOG(INFO, "pendding tasks have not been cleared", K(clog_split_task_num), K(sender_task_num));
  }

  return bret;
}

int ObArchiveMgr::start_components_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(archive_sender_.start())) {
    ARCHIVE_LOG(WARN, "archive_sender_ start fail", KR(ret));
  } else if (OB_FAIL(clog_split_engine_.start())) {
    ARCHIVE_LOG(WARN, "clog_split_engine_ start fail", KR(ret));
  } else if (OB_FAIL(ilog_fetcher_.start())) {
    ARCHIVE_LOG(WARN, "ilog_fetcher_ start fail", KR(ret));
  } else if (OB_FAIL(pg_mgr_.start())) {
    ARCHIVE_LOG(ERROR, "pg_mgr_ start fail", KR(ret));
  } else if (OB_FAIL(archive_scheduler_.start())) {
    ARCHIVE_LOG(WARN, "archive_scheduler_ start fail", KR(ret));
  } else {
    ARCHIVE_LOG(INFO, "archive components start succ");
  }

  return ret;
}

int ObArchiveMgr::set_log_archive_info_(ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObBackupDest dest;
  uint64_t root_path_len = 0;
  uint64_t storage_info_len = 0;

  ilog_fetch_task_mgr_.reset();

  if (OB_FAIL(dest.set(info.backup_dest_))) {
    ARCHIVE_LOG(WARN, "ObBackupDest set fail", KR(ret), K(info));
  } else if (OB_UNLIKELY(OB_MAX_BACKUP_PATH_LENGTH < (root_path_len = sizeof(dest.root_path_) / sizeof(char))) ||
             OB_UNLIKELY(
                 OB_MAX_BACKUP_STORAGE_INFO_LENGTH < (storage_info_len = sizeof(dest.storage_info_) / sizeof(char)))) {
    _ARCHIVE_LOG(ERROR,
        "invalid dest, root_path_=%s, root_path_len=%lu, storage_info_=%s,"
        "storage_info_len=%lu",
        dest.root_path_,
        root_path_len,
        dest.storage_info_,
        storage_info_len);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(archive_round_mgr_.set_archive_start(
                 info.status_.incarnation_, info.status_.round_, info.status_.compatible_))) {
    ARCHIVE_LOG(WARN, "set archive start fail", KR(ret), K(info));
  } else {
    strncpy(archive_round_mgr_.root_path_, dest.root_path_, OB_MAX_BACKUP_PATH_LENGTH);
    strncpy(archive_round_mgr_.storage_info_, dest.storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
  }

  return ret;
}

void ObArchiveMgr::run1()
{
  ARCHIVE_LOG(INFO, "ObArchiveMgr thread run");

  lib::set_thread_name("ObArchiveMgr");

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init");
  } else {
    while (!has_set_stop()) {
      do_thread_task_();
      usleep(THREAD_INTERVAL);
    }
  }
}

void ObArchiveMgr::do_thread_task_()
{
  if (need_check_switch_archive_()) {
    do_check_switch_archive_();
  }

  if (need_check_switch_stop_status_()) {
    do_check_switch_stop_status_();
  }

  if (archive_round_mgr_.need_handle_error()) {
    do_force_stop_archive_work_threads_();
  }

  if (need_print_archive_status_()) {
    print_archive_status_();
  }
}

bool ObArchiveMgr::need_check_switch_archive_()
{
  return true;
}

// archive status and incarnation/round must be atomic
//
// 1. start/stop: normal start/stop archive
// 2. force_stop: unnormal stop archive, only server restart while rs is in stopping status
void ObArchiveMgr::do_check_switch_archive_()
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfo info;
  bool need_stop = false;
  bool need_start = false;
  bool need_force_stop = false;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init");
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_backup_info(info))) {
    if (REACH_TIME_INTERVAL(30 * 1000 * 1000L)) {
      ARCHIVE_LOG(WARN, "get_log_archive_backup_info fail", KR(ret));
    }
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ObLogArchiveBackupInfo is not valid", KR(ret), K(info));
  } else if (OB_LIKELY(!check_if_need_switch_log_archive_(info, need_stop, need_start, need_force_stop))) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      ARCHIVE_LOG(DEBUG, "check_log_archive_status_, not need start, stop or force_stop", K(info));
    }
  } else if (need_start) {
    // start a new archive round
    if (OB_FAIL(start_archive_(info))) {
      ARCHIVE_LOG(WARN, "start_archive_ fail", KR(ret));
    }
  } else if (need_stop) {
    // stop current archive round
    if (OB_FAIL(stop_archive_())) {
      ARCHIVE_LOG(WARN, "stop_archive_ fail", KR(ret));
    }
  } else if (need_force_stop) {
    // force stop archive
    archive_round_mgr_.set_archive_force_stop(info.status_.incarnation_, info.status_.round_);
    ARCHIVE_LOG(INFO, "force set log_archive_status STOPPED");
  }
}

bool ObArchiveMgr::need_check_switch_stop_status_()
{
  bool bret = false;
  int64_t now = ObTimeUtility::current_time();

  if (now - last_check_stop_ts_ > CHECK_ARCHIVE_STOP_INTERVAL && archive_round_mgr_.is_in_archive_stopping_status()) {
    bret = true;
  }

  return bret;
}

void ObArchiveMgr::do_check_switch_stop_status_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init", KR(ret));
  } else if (!is_in_archive_stopping_status()) {
    // skip
  } else if (!check_archive_task_empty_()) {
    // skip
  } else if (OB_FAIL(clear_pg_archive_task_())) {
    ARCHIVE_LOG(WARN, "clear_pg_archive_task_ fail", KR(ret));
  } else if (OB_FAIL(clear_archive_info_())) {
    ARCHIVE_LOG(WARN, "clear_archive_info_ fail", KR(ret));
  } else if (OB_FAIL(set_log_archive_stop_status_())) {
    ARCHIVE_LOG(WARN, "set_log_archive_stop_status_ fail", KR(ret));
  } else {
    // succ
    ARCHIVE_LOG(INFO, "switch log archive status from in_stopping to stopped succ");
  }

  last_check_stop_ts_ = ObTimeUtility::current_time();
}

void ObArchiveMgr::do_force_stop_archive_work_threads_()
{
  int64_t archive_round = archive_round_mgr_.get_current_archive_round();
  int64_t incarnation = archive_round_mgr_.get_current_archive_incarnation();
  ilog_fetcher_.notify_stop();
  (void)clog_split_engine_.notify_stop(archive_round, incarnation);
  (void)archive_sender_.notify_stop(archive_round, incarnation);
  archive_round_mgr_.set_has_handle_error(true);
  ARCHIVE_LOG(ERROR, "force stop archive work threads succ", K(archive_round), K(incarnation));
}

bool ObArchiveMgr::need_print_archive_status_()
{
  bool bret = false;
  const static int64_t PRINT_ARCHIVE_INTERVAL = 10 * 1000 * 1000L;
  if (GCONF.enable_log_archive && REACH_TIME_INTERVAL(PRINT_ARCHIVE_INTERVAL)) {
    bret = true;
  }
  return bret;
}

void ObArchiveMgr::print_archive_status_()
{
  int64_t archive_incarnation = -1;
  int64_t archive_round = -1;
  bool has_encount_error = false;
  ObArchiveRoundMgr::LogArchiveStatus log_archive_status;
  int64_t clog_split_task_num = clog_split_engine_.get_total_task_num();
  int64_t send_task_num = archive_sender_.get_total_task_num();
  int64_t pre_send_task_capacity = archive_sender_.get_pre_archive_task_capacity();
  int64_t pg_archive_task_count = pg_mgr_.get_pg_count();
  archive_round_mgr_.get_archive_round_info(archive_incarnation, archive_round, log_archive_status, has_encount_error);

  ARCHIVE_LOG(INFO,
      "print_archive_status",
      K(clog_split_task_num),
      K(send_task_num),
      K(pre_send_task_capacity),
      K(pg_archive_task_count),
      K(archive_incarnation),
      K(archive_round),
      K(log_archive_status),
      K(has_encount_error));
}

// start archive requires:
//   1) rs.is_in_backup = true
//   2) ob.in_archive = false
//   3) diff_ir = true
//
// normal start/stop archive requires:
//   1) rs.is_in_backup = false
//   2) ob.in_archive = true
//
//   1') ob.in_archive = true
//   2') rs.is_in_backup = true
//   3') ob.incarnation/round != rs.incarnation/round
//
// force stop archive requires:
//   1) ob.in_archive = false
//   2) rs.is_in_stop_status = true
//   3) ob.incarnation/round != rs.incarnation/round
//
// additional:
//   ob stopping status transfer to stop automatic, no matter what rs status is
bool ObArchiveMgr::check_if_need_switch_log_archive_(
    ObLogArchiveBackupInfo& info, bool& need_stop, bool& need_start, bool& need_force_stop)
{
  bool bret = false;
  const int64_t incarnation = archive_round_mgr_.get_current_archive_incarnation();
  const int64_t round = archive_round_mgr_.get_current_archive_round();
  const int64_t rs_incarnation = info.status_.incarnation_;
  const int64_t rs_round = info.status_.round_;
  ObLogArchiveStatus::STATUS status = info.status_.status_;
  const bool is_in_backup =
      (ObLogArchiveStatus::STATUS::BEGINNING == status || ObLogArchiveStatus::STATUS::DOING == status) &&
      GCONF.enable_log_archive;
  const bool is_in_stop_status =
      (ObLogArchiveStatus::STATUS::BEGINNING != status && ObLogArchiveStatus::STATUS::DOING != status);
  const bool in_archive = is_in_archive_status();
  const bool diff_ir = incarnation != rs_incarnation || round != rs_round;

  need_stop = false;
  need_start = false;
  need_force_stop = false;

  if (is_in_archive_stopping_status()) {
    bret = false;
  } else if (!in_archive && is_in_backup && diff_ir) {
    // inital incarnation/round = -1, incarnation/round retain the values when archive stop
    need_start = true;
    bret = true;
    ARCHIVE_LOG(INFO, "need_start", K(incarnation), K(round), K(info), K(in_archive), K(is_in_backup));
  } else if ((!is_in_backup && in_archive) || (is_in_backup && in_archive && diff_ir)) {
    need_stop = true;
    bret = true;
    ARCHIVE_LOG(INFO, "need_stop", K(incarnation), K(round), K(info), K(in_archive), K(diff_ir), K(is_in_backup));
  } else if (is_in_stop_status && !in_archive && diff_ir) {
    need_force_stop = true;
    bret = true;
    ARCHIVE_LOG(
        INFO, "need_force_stop", K(incarnation), K(round), K(info), K(in_archive), K(is_in_stop_status), K(diff_ir));
  }

  return bret;
}

int ObArchiveMgr::check_and_set_start_archive_ts_(const int64_t incarnation, const int64_t round, int64_t& start_ts)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(archive_sender_.check_server_start_ts_exist(incarnation, round, start_ts)) && OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "check_server_start_archive_ts_record_exist_ fail", KR(ret), K(round));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    if (OB_FAIL(archive_sender_.save_server_start_archive_ts(incarnation, round, start_ts))) {
      ARCHIVE_LOG(WARN, "save_server_start_archive_ts fail", KR(ret), K(incarnation), K(round), K(start_ts));
    }
  }

  if (OB_SUCC(ret)) {
    server_start_archive_tstamp_ = start_ts;
    ARCHIVE_LOG(INFO, "check_and_set_start_archive_ts succ", K(server_start_archive_tstamp_));
  }

  return ret;
}

}  // namespace archive
}  // namespace oceanbase
