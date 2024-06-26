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

#include "ob_ls_task.h"
#include <cstdint>
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_archive_define.h"                // ArchiveKey
#include "share/backup/ob_archive_piece.h"    // ObArchivePiece
#include "share/backup/ob_archive_struct.h"   // ObLSArchivePersistInfo
#include "ob_archive_task.h"                  // Archive.*Task
#include "ob_ls_mgr.h"                        // ObArchiveLSMgr
#include "ob_archive_worker.h"                // ObArchiveWorker
#include "ob_archive_allocator.h"             // ObArchiveAllocator
#include "ob_archive_task_queue.h"            // ObArchiveTaskStatus

namespace oceanbase
{
namespace archive
{
using namespace oceanbase::share;
using namespace oceanbase::palf;

ObLSArchiveTask::ObLSArchiveTask() :
  id_(),
  tenant_id_(OB_INVALID_TENANT_ID),
  station_(),
  round_start_scn_(),
  dest_(),
  allocator_(NULL),
  rwlock_(common::ObLatchIds::LS_ARCHIVE_TASK_LOCK)
{}

ObLSArchiveTask::~ObLSArchiveTask()
{
  destroy();
}

int ObLSArchiveTask::init(const StartArchiveHelper &helper, ObArchiveAllocator *allocator)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  if (OB_UNLIKELY(! helper.is_valid()) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(helper), K(allocator));
  } else {
    allocator_ = allocator;
    ret = update_unlock_(helper, allocator_);
  }
  return ret;
}

int ObLSArchiveTask::update_ls_task(const StartArchiveHelper &helper)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  if (OB_UNLIKELY(! helper.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(helper));
  } else if (OB_UNLIKELY(! is_task_stale_(helper.get_station()))) {
    ARCHIVE_LOG(INFO, "ls archive task exist, skip it", K(ret), K(helper));
  } else {
    ret = update_unlock_(helper, allocator_);
  }
  return ret;
}

bool ObLSArchiveTask::check_task_valid(const ArchiveWorkStation &station)
{
  RLockGuard guard(rwlock_);
  return ! is_task_stale_(station);
}

void ObLSArchiveTask::destroy()
{
  WLockGuard guard(rwlock_);
  id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  station_.reset();
  round_start_scn_.reset();
  dest_.destroy();
  allocator_ = NULL;
}

int ObLSArchiveTask::get_sequencer_progress(const ArchiveKey &key,
    ArchiveWorkStation &station,
    LSN &offset)
{
  RLockGuard guard(rwlock_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key != station_.get_round())) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "stale task, just skip", K(ret), K(key), KPC(this));
  } else {
    station = station_;
    dest_.get_sequencer_progress(offset);
  }
  return ret;
}

int ObLSArchiveTask::update_sequencer_progress(const ArchiveWorkStation &station,
    const int64_t size,
    const LSN &offset)
{
  WLockGuard guard(rwlock_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! station.is_valid() || size <= 0 || ! offset.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(size), K(station), K(offset));
  } else if (OB_UNLIKELY(is_task_stale_(station))) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "stale task, just skip", K(ret), K(size), K(station), KPC(this));
  } else if (OB_FAIL(dest_.update_sequencer_progress(size, offset))) {
    ARCHIVE_LOG(WARN, "update sequencer progress failed", K(ret), K(id_));
  }
  return ret;
}

int ObLSArchiveTask::push_fetch_log(ObArchiveLogFetchTask &task)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "send task is not valid", K(ret), KPC(this));
  } else if (OB_UNLIKELY(is_task_stale_(task.get_station()))) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "stale task, just skip", K(ret), K(task), KPC(this));
  } else if (OB_FAIL(dest_.push_fetch_log(task))) {
    ARCHIVE_LOG(WARN, "push fetch log failed", K(ret), K(task), KPC(this));
  } else {
    ARCHIVE_LOG(TRACE, "push fetch log succ", K(task), KPC(this));
  }
  return ret;
}

int ObLSArchiveTask::get_fetcher_progress(const ArchiveWorkStation &station,
    LSN &offset,
    SCN &scn,
    int64_t &last_fetch_timestamp)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(! station.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(station));
  } else if (OB_UNLIKELY(is_task_stale_(station))) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
     ARCHIVE_LOG(WARN, "stale task, just skip", K(ret), K(station), KPC(this));
  } else {
    LogFileTuple tuple;
    dest_.get_fetcher_progress(tuple, last_fetch_timestamp);
    offset = tuple.get_lsn();
    scn= tuple.get_scn();
  }
  return ret;
}

int ObLSArchiveTask::get_sorted_fetch_log(ObArchiveLogFetchTask *&task)
{
  int ret = OB_SUCCESS;
  int64_t unused_timestamp = OB_INVALID_TIMESTAMP;
  WLockGuard guard(rwlock_);
  ObArchiveLogFetchTask *tmp_task = NULL;
  LogFileTuple tuple;
  task = NULL;
  dest_.get_fetcher_progress(tuple, unused_timestamp);
  const LSN &cur_offset = tuple.get_lsn();

  if (OB_FAIL(dest_.get_top_fetch_log(tmp_task))) {
    ARCHIVE_LOG(WARN, "get top fetch log failed", K(ret));
  } else if (NULL == tmp_task) {
    // no task exist, just skip
    ARCHIVE_LOG(TRACE, "no task exist, just skip", K(tenant_id_), K(id_), K(tmp_task));
  }
  // 1. 同一个归档文件, 相同piece或者下一个piece
  else if (tmp_task->get_end_offset() > cur_offset && tmp_task->get_start_offset() <= cur_offset) {
    task = tmp_task;
    ARCHIVE_LOG(INFO, "the same file", K(tuple), KPC(tmp_task));
  }
  // 2. 下一个归档文件
  else if (tmp_task->get_start_offset() == cur_offset) {
    task = tmp_task;
    ARCHIVE_LOG(INFO, "the next file", K(tuple), KPC(tmp_task));
  } else {
    ARCHIVE_LOG(INFO, "first task is not in turn, just skip it", K(tuple), KPC(tmp_task));
  }

  if (OB_SUCC(ret) && NULL != task) {
    ObArchiveLogFetchTask *tmp_task = NULL;
    if (OB_FAIL(dest_.pop_fetch_log(tmp_task))) {
      ARCHIVE_LOG(ERROR, "pop failed", K(ret), KPC(this));
    }
  }
  ARCHIVE_LOG(TRACE, "print get_sorted_fetch_log", KPC(task), KPC(this));
  return ret;
}

int ObLSArchiveTask::update_fetcher_progress(const ArchiveWorkStation &station,
    const LogFileTuple &next_tuple)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  LogFileTuple tuple;
  if (OB_UNLIKELY(! next_tuple.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(next_tuple), K(id_));
  } else if (OB_UNLIKELY(is_task_stale_(station))) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "stale task, just skip", K(ret), K(station), KPC(this));
  } else if (OB_FAIL(dest_.update_fetcher_progress(round_start_scn_, next_tuple))) {
    ARCHIVE_LOG(WARN, "update fetch progress failed", K(ret), K(station), K(id_));
  } else {
    ARCHIVE_LOG(TRACE, "update_fetcher_progress succ", KPC(this));
  }
  return ret;
}

int ObLSArchiveTask::push_send_task(ObArchiveSendTask &task, ObArchiveWorker &worker)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "allocator_ is NULL", K(ret));
  } else if (OB_UNLIKELY(! task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "send_task is invalid", K(ret), K(task));
  } else if (OB_UNLIKELY(station_ != task.get_station())) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "stale task, just skip it", K(ret), K(task));
  } else if (OB_FAIL(dest_.push_send_task(task, worker))) {
    ARCHIVE_LOG(WARN, "push send task failed", K(ret), K(task));
  }
  return ret;
}

int ObLSArchiveTask::compensate_piece(const ArchiveWorkStation &station,
    const int64_t next_compensate_piece_id)
{
  WLockGuard guard(rwlock_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(station != station_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "stale task, just skip it", K(ret), K(station));
  } else {
    ret = dest_.compensate_piece(next_compensate_piece_id);
  }
  return ret;
}

int ObLSArchiveTask::get_max_archive_info(const ArchiveKey &key,
    ObLSArchivePersistInfo &info)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  LSN piece_min_lsn;
  LSN lsn;
  SCN scn;
  SCN lower_limit_scn;
  ObArchivePiece piece;
  int64_t file_id = 0;
  int64_t file_offset = 0;
  bool error_exist = false;

  if (OB_UNLIKELY(key != station_.get_round())) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "diff incarnation or round", K(ret), K(id_), K(key), K(station_));
  } else if (FALSE_IT(dest_.get_max_archive_progress(piece_min_lsn, lsn, scn,
          piece, file_id, file_offset, error_exist))) {
  } else if (OB_UNLIKELY(! piece.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "piece is not valid", K(ret), KPC(this));
  } else if (OB_FAIL(piece.get_piece_lower_limit(lower_limit_scn))) {
    ARCHIVE_LOG(WARN, "failed to get_piece_lower_limit", K(ret), K(piece), KPC(this));
  } else {
    const int64_t piece_id = piece.get_piece_id();
    const SCN piece_start_scn = (!round_start_scn_.is_valid() || (lower_limit_scn.is_valid() && lower_limit_scn > round_start_scn_))
                                 ? lower_limit_scn : round_start_scn_;
    ObArchiveRoundState state;
    state.status_ = error_exist ?
      ObArchiveRoundState::Status::INTERRUPTED : ObArchiveRoundState::Status::DOING;

    // set archive persist info
    info.key_.tenant_id_ = tenant_id_;
    info.key_.dest_id_ = key.dest_id_;
    info.key_.round_id_ = key.round_;
    info.key_.piece_id_ = piece_id;
    info.key_.ls_id_ = id_;
    info.incarnation_ = key.incarnation_;
    info.start_lsn_ = piece_min_lsn.val_;
    info.start_scn_ = piece_start_scn;
    info.checkpoint_scn_ = scn;
    info.lsn_ = lsn.val_;
    info.input_bytes_ = static_cast<int64_t>(info.lsn_ - info.start_lsn_);
    info.output_bytes_ = info.input_bytes_;
    info.archive_file_id_ = file_id;
    info.archive_file_offset_ = file_offset;
    info.state_ = state;
  }

  ARCHIVE_LOG(TRACE, "print get persist info", K(tenant_id_), K(id_), K(info), KPC(this));
  return ret;
}

int ObLSArchiveTask::get_archive_progress(const ArchiveWorkStation &station,
    int64_t &file_id,
    int64_t &file_offset,
    LogFileTuple &tuple)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (OB_UNLIKELY(station != station_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "stale task, just skip it", K(ret), K(station), K(station_), K(id_));
  } else {
    dest_.get_archive_progress(file_id, file_offset, tuple);
  }
  return ret;
}

int ObLSArchiveTask::get_send_task_count(const ArchiveWorkStation &station, int64_t &count)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (OB_UNLIKELY(station != station_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "stale task, just skip it", K(ret), K(station), K(station_), K(id_));
  } else {
    dest_.get_send_task_count(count);
  }
  return ret;
}

int ObLSArchiveTask::get_archive_send_arg(const ArchiveWorkStation &station, ObArchiveSendDestArg &arg)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (OB_UNLIKELY(station != station_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "stale task, just skip it", K(ret), K(station), K(station_), K(id_));
  } else {
    dest_.get_archive_send_arg(arg);
  }
  return ret;
}

int ObLSArchiveTask::update_archive_progress(const ArchiveWorkStation &station,
    const int64_t file_id,
    const int64_t file_offset,
    const LogFileTuple &tuple)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  if (OB_UNLIKELY(is_task_stale_(station))) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "stale task, just skip it", K(ret), K(station), K(station_), K(id_));
  } else if (OB_UNLIKELY(file_id <= 0 || file_offset <= 0 || ! tuple.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(file_id), K(file_offset), K(tuple));
  } else if (OB_FAIL(dest_.update_archive_progress(round_start_scn_, file_id, file_offset, tuple))) {
    ARCHIVE_LOG(WARN, "update archive progress failed", K(ret),
        K(id_), K(station), K(tuple), K(file_id), K(file_offset));
  } else {
    ARCHIVE_LOG(INFO, "update archive progress succ", K(ret), K(id_),
        K(station), K(tuple), K(file_id), K(file_offset));
  }
  return ret;
}

int ObLSArchiveTask::get_max_no_limit_lsn(const ArchiveWorkStation &station, LSN &lsn)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (OB_UNLIKELY(station != station_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "stale task, just skip it", K(ret), K(station), K(station_), K(id_));
  } else {
    dest_.get_max_no_limit_lsn(lsn);
  }
  return ret;
}

int ObLSArchiveTask::update_no_limit_lsn(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!lsn.is_valid())) {
    ARCHIVE_LOG(WARN, "lsn not valid", K(lsn));
  } else {
    dest_.update_no_limit_lsn(lsn);
  }
  return ret;
}

int ObLSArchiveTask::print_self()
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  ARCHIVE_LOG(INFO, "print ls archive task", K_(id), K_(tenant_id), K_(station), K_(round_start_scn), K_(dest));
  return ret;
}

int64_t ObLSArchiveTask::ArchiveDest::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(has_encount_error),
      K_(is_worm),
      K_(max_no_limit_lsn),
      K_(max_archived_info),
      K_(max_seq_log_offset),
      K_(max_fetch_info),
      K_(piece_min_lsn),
      K_(archive_file_id),
      K_(archive_file_offset),
      K_(piece_dir_exist),
      K_(wait_send_task_count));
  J_COMMA();
  J_NAME("tasks");
  J_COLON();
  for (int64_t i = 0; i < wait_send_task_count_; i++) {
    ObArchiveLogFetchTask *task = wait_send_task_array_[i];
    pos += task->to_string(buf + pos, buf_len - pos);
    J_COMMA();
    BUF_PRINTO(*task);
  }
  J_OBJ_END();
  return pos;
}

void ObLSArchiveTask::stat(LSArchiveStat &ls_stat) const
{
  RLockGuard guard(rwlock_);
  ls_stat.tenant_id_ = tenant_id_;
  ls_stat.ls_id_ = id_.id();
  ls_stat.dest_id_ = station_.get_round().dest_id_;
  ls_stat.incarnation_ = station_.get_round().incarnation_;
  ls_stat.round_ = station_.get_round().round_;
  ls_stat.lease_id_ = 0;
  ls_stat.round_start_scn_ = round_start_scn_;
  ls_stat.max_issued_log_lsn_ = static_cast<int64_t>(dest_.max_seq_log_offset_.val_);
  ls_stat.max_prepared_piece_id_ = dest_.max_fetch_info_.get_piece().get_piece_id();
  ls_stat.max_prepared_lsn_ = static_cast<int64_t>(dest_.max_fetch_info_.get_lsn().val_);
  ls_stat.max_prepared_scn_ = dest_.max_fetch_info_.get_scn();
  ls_stat.issued_task_count_ = ls_stat.max_issued_log_lsn_ / MAX_ARCHIVE_FILE_SIZE - ls_stat.max_prepared_lsn_ / MAX_ARCHIVE_FILE_SIZE;
  ls_stat.issued_task_size_ = ls_stat.max_issued_log_lsn_ - ls_stat.max_prepared_lsn_;
  ls_stat.wait_send_task_count_ = dest_.wait_send_task_count_;
  ls_stat.archive_piece_id_ = dest_.max_archived_info_.get_piece().get_piece_id();
  ls_stat.archive_lsn_ = static_cast<int64_t>(dest_.max_archived_info_.get_lsn().val_);
  ls_stat.archive_scn_ = dest_.max_archived_info_.get_scn();
  ls_stat.archive_file_id_ = dest_.archive_file_id_;
  ls_stat.archive_file_offset_ = dest_.archive_file_offset_;
}

int ObLSArchiveTask::mark_error(const ArchiveKey &key)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  if (key != station_.get_round()) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "station not match, skip mark error", K(ret), K(key), KPC(this));
  } else {
    dest_.mark_error();
  }
  return ret;
}

int ObLSArchiveTask::update_unlock_(const StartArchiveHelper &helper,
    ObArchiveAllocator *allocator)
{
  int ret = OB_SUCCESS;
  id_ = helper.get_ls_id();
  tenant_id_ = helper.get_tenant_id();
  station_ = helper.get_station();
  round_start_scn_ = helper.get_round_start_scn();
  ret = dest_.init(helper.get_max_no_limit_lsn(),
      helper.get_piece_min_lsn(), helper.get_offset(),
      helper.get_file_id(), helper.get_file_offset(),
      helper.get_piece(), helper.get_max_archived_scn(),
      helper.is_log_gap_exist(), allocator);
  ARCHIVE_LOG(INFO, "update_unlock_", KPC(this), K(helper));
  return ret;
}

bool ObLSArchiveTask::is_task_stale_(const ArchiveWorkStation &station) const
{
  return station_ != station;
}

void ObLSArchiveTask::mock_init(const ObLSID &id, ObArchiveAllocator *allocator)
{
  id_ = id;
  allocator_ = allocator;
}

// ========================================== //
ObLSArchiveTask::ArchiveDest::ArchiveDest() :
  has_encount_error_(false),
  is_worm_(false),
  max_no_limit_lsn_(),
  piece_min_lsn_(),
  max_archived_info_(),
  archive_file_id_(OB_INVALID_ARCHIVE_FILE_ID),
  archive_file_offset_(OB_INVALID_ARCHIVE_FILE_OFFSET),
  piece_dir_exist_(false),
  max_seq_log_offset_(),
  max_fetch_info_(),
  last_fetch_timestamp_(OB_INVALID_TIMESTAMP),
  wait_send_task_array_(),
  wait_send_task_count_(0),
  send_task_queue_(NULL),
  allocator_(NULL)
{}

ObLSArchiveTask::ArchiveDest::~ArchiveDest()
{
  destroy();
}

void ObLSArchiveTask::ArchiveDest::destroy()
{
  has_encount_error_ = false;
  is_worm_ = false;
  max_no_limit_lsn_.reset();
  piece_min_lsn_.reset();
  max_archived_info_.reset();
  archive_file_id_ = OB_INVALID_ARCHIVE_FILE_ID;
  archive_file_offset_ = OB_INVALID_ARCHIVE_FILE_OFFSET;
  piece_dir_exist_ = false;
  max_seq_log_offset_.reset();
  max_fetch_info_.reset();
  last_fetch_timestamp_ = OB_INVALID_TIMESTAMP;

  free_fetch_log_tasks_();
  free_send_task_status_();
  allocator_ = NULL;
}

int ObLSArchiveTask::ArchiveDest::init(const LSN &max_no_limit_lsn,
    const LSN &piece_min_lsn,
    const LSN &lsn,
    const int64_t file_id,
    const int64_t file_offset,
    const share::ObArchivePiece &piece,
    const SCN &max_archived_scn,
    const bool is_log_gap_exist,
    ObArchiveAllocator *allocator)
{
  int ret = OB_SUCCESS;
  const ObArchivePiece &cur_piece = max_archived_info_.get_piece();
  LogFileTuple tmp_tuple(lsn, max_archived_scn, piece);
  const bool renew_context = (!max_archived_info_.is_valid()) || max_archived_info_ < tmp_tuple;
  if (renew_context) {
    if (archive_file_id_ > file_id || (archive_file_id_ == file_id && archive_file_offset_ > file_offset)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "local cache archive progress is old, but file info is bigger", K(piece_min_lsn),
          K(tmp_tuple), K(piece), K(file_id), K(file_offset), KPC(this));
    } else {
      piece_min_lsn_ = piece_min_lsn;
      max_archived_info_ = tmp_tuple;
      archive_file_id_ = file_id;
      archive_file_offset_ = file_offset;
      has_encount_error_ = is_log_gap_exist;
      max_seq_log_offset_ = lsn;
      max_fetch_info_ = tmp_tuple;
      ARCHIVE_LOG(INFO, "update archive dest with remote info", K(piece_min_lsn),
          K(tmp_tuple), K(piece), K(file_id), K(file_offset), KPC(this));
    }
  } else {
    max_seq_log_offset_ = max_archived_info_.get_lsn();
    max_fetch_info_ = max_archived_info_;
    ARCHIVE_LOG(INFO, "update archive dest with local archive progress", K(piece_min_lsn),
        K(tmp_tuple), K(piece), K(file_id), K(file_offset), KPC(this));
  }
  max_no_limit_lsn_ = max_no_limit_lsn;
  wait_send_task_count_ = 0;
  free_fetch_log_tasks_();
  free_send_task_status_();
  allocator_ = allocator;
  return ret;
}

void ObLSArchiveTask::ArchiveDest::get_sequencer_progress(LSN &offset) const
{
  offset = max_seq_log_offset_;
}

int ObLSArchiveTask::ArchiveDest::update_sequencer_progress(const int64_t size, const LSN &offset)
{
  int ret = OB_SUCCESS;
  // sequencer分配任务一定是递增的
  if (OB_UNLIKELY(offset != max_seq_log_offset_ + size)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "sequencer lsn not continous", K(ret), K(size), K(offset), K(max_seq_log_offset_));
  } else {
    max_seq_log_offset_ = offset;
    ARCHIVE_LOG(TRACE, "update_sequencer_progress succ", K(max_seq_log_offset_));
  }
  return ret;
}

void ObLSArchiveTask::ArchiveDest::get_fetcher_progress(LogFileTuple &tuple,
    int64_t &last_fetch_timestamp) const
{
  tuple = max_fetch_info_;
  last_fetch_timestamp = last_fetch_timestamp_;
}

int ObLSArchiveTask::ArchiveDest::get_top_fetch_log(ObArchiveLogFetchTask *&task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  if (OB_UNLIKELY(wait_send_task_count_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "wait_send_task_count_ small than zero", K(ret), KPC(this));
  } else if (wait_send_task_count_ == 0) {
    // no task exist
  } else {
    task = wait_send_task_array_[0];
  }
  return ret;
}

int ObLSArchiveTask::ArchiveDest::update_fetcher_progress(const SCN &round_start_scn,
                                                          const LogFileTuple &tuple)
{
  int ret = OB_SUCCESS;
  ObArchivePiece piece = max_fetch_info_.get_piece();
  if (OB_UNLIKELY((tuple.get_scn() > round_start_scn && ! (max_fetch_info_ < tuple)))) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "fetcher progress rollback", K(ret), K(max_fetch_info_), K(tuple));
  } else {
    max_fetch_info_ = tuple;
    last_fetch_timestamp_ = common::ObTimeUtility::fast_current_time();
    ARCHIVE_LOG(TRACE, "update fetcher progress succ", K(max_fetch_info_));
  }
  return ret;
}

int ObLSArchiveTask::ArchiveDest::pop_fetch_log(ObArchiveLogFetchTask *&task)
{
  UNUSED(task);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(wait_send_task_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "wait_send_task_count_ small than zero", K(ret), KPC(this));
  } else if (1 == wait_send_task_count_) {
    wait_send_task_count_--;
  } else {
    wait_send_task_array_[0] = wait_send_task_array_[wait_send_task_count_ - 1];
    wait_send_task_count_--;
    lib::ob_sort(wait_send_task_array_, wait_send_task_array_ + wait_send_task_count_, LogFetchTaskCompare());
  }
  return ret;
}

int ObLSArchiveTask::ArchiveDest::compensate_piece(const int64_t piece_id)
{
  int ret = OB_SUCCESS;
  const int64_t archived_persist_id = max_archived_info_.get_piece().get_piece_id();
  if (piece_id < archived_persist_id) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "piece id rollback", K(ret), KPC(this));
  } else if (piece_id == archived_persist_id) {
    // do nothing, wait piece info persist
    ARCHIVE_LOG(INFO, "archived_log_info refresh, wait persistance", K(piece_id), KPC(this));
  } else if (piece_id == archived_persist_id + 1) {
    ARCHIVE_LOG(INFO, "compensate piece", K(piece_id), KPC(this));
    piece_min_lsn_ = max_archived_info_.get_lsn();
    max_archived_info_.compensate_piece();
  } else {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "piece id not continuous", K(ret), K(piece_id), KPC(this));
  }
  return ret;
}

void ObLSArchiveTask::ArchiveDest::get_max_archive_progress(LSN &piece_min_lsn,
    LSN &lsn, SCN &scn,
    ObArchivePiece &piece,
    int64_t &file_id,
    int64_t &file_offset,
    bool &error_exist)
{
  piece_min_lsn = piece_min_lsn_;
  lsn = max_archived_info_.get_lsn();
  scn = max_archived_info_.get_scn();
  piece = max_archived_info_.get_piece();
  file_id = archive_file_id_;
  file_offset = archive_file_offset_;
  error_exist = has_encount_error_;
}

void ObLSArchiveTask::ArchiveDest::free_fetch_log_tasks_()
{
  while (wait_send_task_count_ > 0) {
    if (NULL != wait_send_task_array_[wait_send_task_count_ - 1]) {
      // send_task will be free if the pointer is not NULL
      allocator_->free_log_fetch_task(wait_send_task_array_[wait_send_task_count_ - 1]);
      wait_send_task_array_[wait_send_task_count_ - 1] = NULL;
    }
    wait_send_task_count_--;
  }
}

// only free task_status if it is disacarded, or will be free after all send_tasks are handled
void ObLSArchiveTask::ArchiveDest::free_send_task_status_()
{
  // send_tasks only free by sender, here decrease ref of task_status and free it if ref count is zero
  if (NULL != send_task_queue_) {
    bool is_discarded = false;
    send_task_queue_->free(is_discarded);
    if (is_discarded && NULL != allocator_) {
      allocator_->free_send_task_status(send_task_queue_);
      send_task_queue_ = NULL;
    }
    // task_status is not reuse in any cases, and it will be free by worker thread
    send_task_queue_ = NULL;
  }
}

// 排序的队列
int ObLSArchiveTask::ArchiveDest::push_fetch_log(ObArchiveLogFetchTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(wait_send_task_count_ + 1 > MAX_FETCH_TASK_NUM)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "logh fetch tasks more than threshold is generated", K(ret), K(task));
  } else {
    wait_send_task_array_[wait_send_task_count_] = &task;
    wait_send_task_count_++;
    lib::ob_sort(wait_send_task_array_, wait_send_task_array_ + wait_send_task_count_, LogFetchTaskCompare());
  }
  ARCHIVE_LOG(INFO, "print push_fetch_log", K(task), KPC(this));
  return ret;
}

int ObLSArchiveTask::ArchiveDest::push_send_task(ObArchiveSendTask &task, ObArchiveWorker &worker)
{
  int ret = OB_SUCCESS;
  const ObLSID id = task.get_ls_id();
  if (NULL == send_task_queue_) {
    if (OB_ISNULL(send_task_queue_ = allocator_->alloc_send_task_status(id))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ARCHIVE_LOG(WARN, "alloc ObArchiveSendTaskStatus fail", K(ret), K(task));
    } else {
      send_task_queue_->inc_ref();
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(send_task_queue_)) {
    if (OB_FAIL(send_task_queue_->push(&task, worker))) {
      ARCHIVE_LOG(WARN, "push send taskfailed", K(ret), K(task));
    }
  }
  return ret;
}

int ObLSArchiveTask::ArchiveDest::update_archive_progress(const SCN &round_start_scn,
    const int64_t file_id,
    const int64_t file_offset,
    const LogFileTuple &tuple)
{
  int ret = OB_SUCCESS;
  if ((!tuple.is_valid()
       || !round_start_scn.is_valid()
       || (tuple.get_scn() > round_start_scn && max_archived_info_.is_valid() && ! (max_archived_info_ < tuple)))
      || file_id < archive_file_id_
      || (max_archived_info_.get_piece() == tuple.get_piece() && file_id == archive_file_id_ && file_offset <= archive_file_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "archive progress rollback", K(ret), K(file_id), K(archive_file_id_),
        K(file_offset), K(archive_file_offset_), K(tuple), K(max_archived_info_));
  } else {
    if (tuple.get_piece() > max_archived_info_.get_piece() && max_archived_info_.is_valid()) {
      piece_min_lsn_ = max_archived_info_.get_lsn();
    }
    archive_file_id_ = file_id;
    archive_file_offset_ = file_offset;
    max_archived_info_ = tuple;
    piece_dir_exist_ = true;
  }
  return ret;
}

void ObLSArchiveTask::ArchiveDest::get_archive_progress(int64_t &file_id,
    int64_t &file_offset,
    LogFileTuple &tuple)
{
  file_id = archive_file_id_;
  file_offset = archive_file_offset_;
  tuple = max_archived_info_;
}

void ObLSArchiveTask::ArchiveDest::get_send_task_count(int64_t &count)
{
  count = 0;
  if (NULL != send_task_queue_) {
    count = send_task_queue_->count();
  }
}

void ObLSArchiveTask::ArchiveDest::get_archive_send_arg(ObArchiveSendDestArg &arg)
{
  arg.cur_file_id_ = archive_file_id_;
  arg.cur_file_offset_ = archive_file_offset_;
  arg.tuple_ = max_archived_info_;
  arg.piece_dir_exist_ = piece_dir_exist_;
}

void ObLSArchiveTask::ArchiveDest::get_max_no_limit_lsn(LSN &lsn)
{
  lsn = max_no_limit_lsn_;
}

void ObLSArchiveTask::ArchiveDest::update_no_limit_lsn(const palf::LSN &lsn)
{
  if (lsn > max_no_limit_lsn_) {
    max_no_limit_lsn_ = lsn;
    ARCHIVE_LOG(INFO, "update max no limit lsn succ", K(lsn));
  } else {
    ARCHIVE_LOG(INFO, "lsn is smaller than max_no_limit_lsn_, just skip", K(lsn), K(max_no_limit_lsn_));
  }
}

void ObLSArchiveTask::ArchiveDest::mark_error()
{
  has_encount_error_ = true;
}

ObArchiveLSGuard::ObArchiveLSGuard(ObArchiveLSMgr *ls_mgr) :
  ls_task_(NULL),
  ls_mgr_(ls_mgr)
{
}

ObArchiveLSGuard::~ObArchiveLSGuard()
{
  revert_ls_task_();
  ls_mgr_ = NULL;
}

void ObArchiveLSGuard::set_ls_task(ObLSArchiveTask *ls_task)
{
  ls_task_ = ls_task;
}

ObLSArchiveTask* ObArchiveLSGuard::get_ls_task()
{
  return ls_task_;
}

void ObArchiveLSGuard::revert_ls_task_()
{
  if (OB_ISNULL(ls_mgr_) || OB_ISNULL(ls_task_)) {
    // nothing
  } else {
    ls_mgr_->revert_ls_task(ls_task_);
    ls_task_ = NULL;
  }
}
} // namespace archive
} // namespace oceanbas
