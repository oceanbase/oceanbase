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

#include "ob_archive_sender.h"
#include <algorithm>
#include "ob_archive_io.h"
#include "ob_archive_path.h"
#include "ob_archive_file_utils.h"
#include "ob_archive_mgr.h"
#include "share/ob_debug_sync.h"
#include "ob_archive_task_queue.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase {
namespace archive {

ObArchiveSender::ObArchiveSender()
    : log_archive_round_(-1),
      incarnation_(-1),
      pg_mgr_(NULL),
      archive_round_mgr_(NULL),
      archive_mgr_(NULL),
      allocator_(NULL),
      rwlock_()
{}

ObArchiveSender::~ObArchiveSender()
{
  ARCHIVE_LOG(INFO, "ObArchiveSender destroy");
  destroy();
}

int ObArchiveSender::init(ObArchiveAllocator* allocator, ObArchivePGMgr* pg_mgr, ObArchiveRoundMgr* archive_round_mgr,
    ObArchiveMgr* archive_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_ = allocator) || OB_ISNULL(pg_mgr_ = pg_mgr) ||
      OB_ISNULL(archive_round_mgr_ = archive_round_mgr) || OB_ISNULL(archive_mgr_ = archive_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(allocator), K(pg_mgr), K(archive_round_mgr), K(archive_mgr));
  } else if (OB_FAIL(ObArchiveThreadPool::init())) {
    ARCHIVE_LOG(WARN, "ObArchiveThreadPool init fail", KR(ret));
  } else {
    inited_ = true;

    ARCHIVE_LOG(INFO, "ObArchiveSender init succ");
  }

  return ret;
}

void ObArchiveSender::destroy()
{
  ObArchiveThreadPool::destroy();

  log_archive_round_ = -1;
  incarnation_ = -1;

  pg_mgr_ = NULL;
  archive_round_mgr_ = NULL;
  ;
  archive_mgr_ = NULL;
}

// calculate sender thread count by parameter log_archive_concurrency and ob mode
// 1. if ob is in mini mode, sender thread count is 1
// 2. if not in mini mode, sender thread count = 2/3 * log_archive_concurrency
int64_t ObArchiveSender::cal_work_thread_num()
{
  const int64_t log_archive_concurrency = GCONF.get_log_archive_concurrency();
  const int64_t total_cnt =
      log_archive_concurrency == 0 ? share::OB_MAX_LOG_ARCHIVE_THREAD_NUM : log_archive_concurrency;
  const int64_t normal_thread_num = total_cnt % 3 ? (total_cnt / 3 * 2 + 1) : (total_cnt / 3 * 2);
  const int64_t thread_num = !lib::is_mini_mode() ? std::max(normal_thread_num, 1L) : MINI_MODE_SENDER_THREAD_NUM;
  return thread_num;
}

void ObArchiveSender::set_thread_name_str(char* str)
{
  strncpy(str, "ObArchiveSender", MAX_ARCHIVE_THREAD_NAME_LENGTH);
}

int ObArchiveSender::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(INFO, "ObArchiveSender has not been initialized", KR(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    ARCHIVE_LOG(WARN, "start ObArchiveSender threads fail", KR(ret));
  } else {
    ARCHIVE_LOG(INFO, "start ObArchiveSender threads succ", KR(ret));
  }

  return ret;
}

void ObArchiveSender::stop()
{
  round_stop_flag_ = true;
  ObThreadPool::stop();

  ARCHIVE_LOG(INFO, "stop ObArchiveSender threads succ");
}

void ObArchiveSender::wait()
{
  ARCHIVE_LOG(INFO, "ObArchiveSender wait");
  ObThreadPool::wait();
}

int ObArchiveSender::notify_start(const int64_t archive_round, const int64_t incarnation)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(archive_round < 0 || incarnation < 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(archive_round), K(incarnation));
  } else {
    log_archive_round_ = archive_round;
    incarnation_ = incarnation;
    round_stop_flag_ = false;
    ARCHIVE_LOG(INFO, "ArchiveSender notify_start succ", K(archive_round), K(incarnation));
  }

  return ret;
}

int ObArchiveSender::notify_stop(const int64_t archive_round, const int64_t incarnation)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(archive_round != log_archive_round_ || incarnation != incarnation_)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(archive_round), K(incarnation));
  } else {
    round_stop_flag_ = true;
    ARCHIVE_LOG(INFO, "ArchiveSender notify_stop succ", K(archive_round), K(incarnation));
  }

  return ret;
}

int64_t ObArchiveSender::get_pre_archive_task_capacity()
{
  // memory hold by send tasks
  return allocator_->get_send_task_capacity();
}

int ObArchiveSender::get_send_task(const int64_t buf_len, ObArchiveSendTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;

  if (OB_ISNULL(task = allocator_->alloc_send_task(buf_len))) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      ARCHIVE_LOG(WARN, "alloc task is NULL, retry again", KR(ret));
    }
  }

  return ret;
}

void ObArchiveSender::release_send_task(ObArchiveSendTask* task)
{
  if (NULL == task || NULL == allocator_) {
    ARCHIVE_LOG(ERROR, "invalid arguments", K(task), K(allocator_));
  } else {
    allocator_->free_send_task(task);
  }
}

// PG task must be submit in order by log id and ts
// for example, log id 10 submited before log 9
int ObArchiveSender::submit_send_task(ObArchiveSendTask* task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveSender not init");
  } else if (OB_UNLIKELY(round_stop_flag_)) {
    ARCHIVE_LOG(WARN, "archive stopped");
    ret = OB_IN_STOP_STATE;
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "send task is NULL", KR(ret));
  } else if (OB_FAIL(check_task_valid_(task))) {
    ARCHIVE_LOG(WARN, "task is not valid", KR(ret), K(task));
  } else if (OB_FAIL(submit_send_task_(task))) {
    ARCHIVE_LOG(WARN, "push_task_ fail", KR(ret), KPC(task));
  } else {
    ATOMIC_INC(&total_task_count_);
  }

  return ret;
}

int ObArchiveSender::set_archive_round_info(const int64_t round, const int64_t incarnation)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(round <= 0) || OB_UNLIKELY(incarnation <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", K(round), K(incarnation), KR(ret));
  } else {
    log_archive_round_ = round;
    incarnation_ = incarnation;
  }

  return ret;
}

void ObArchiveSender::clear_archive_info()
{
  log_archive_round_ = -1;
  incarnation_ = -1;
}

int ObArchiveSender::check_task_valid_(ObArchiveSendTask* task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(task)) {
    ARCHIVE_LOG(WARN, "task in NULL", KR(ret), KPC(task));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(!task->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "task is not valid", KR(ret), KPC(task));
  }

  return ret;
}

int ObArchiveSender::submit_send_task_(ObArchiveSendTask* task)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTask* pg_archive_task = NULL;
  const ObPGKey& pg_key = task->pg_key_;
  ObPGArchiveTaskGuard guard(pg_mgr_);

  if (OB_ISNULL(pg_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "pg_mgr_ is NULL", KR(ret));
  } else if (OB_FAIL(pg_mgr_->get_pg_archive_task_guard_with_status(pg_key, guard)) && OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "get_pg_archive_task_guard_with_status fail", KR(ret), K(pg_key), KPC(task));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "pg leader has changed, skip it", KR(ret), KPC(task));
  } else if (OB_ISNULL(pg_archive_task = guard.get_pg_archive_task())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "pg_archive_task is NULL", KR(ret), KPC(task));
  } else if (OB_FAIL(pg_archive_task->push_send_task(*task, *this)) && OB_LOG_ARCHIVE_LEADER_CHANGED != ret) {
    ARCHIVE_LOG(WARN, "push_send_task fail", KR(ret), K(task));
  } else if (OB_LOG_ARCHIVE_LEADER_CHANGED == ret) {
    ARCHIVE_LOG(INFO, "pg leader has changed, skip it", KR(ret), K(task));
  } else {
    ARCHIVE_LOG(DEBUG, "push_send_task succ", KPC(task));
  }

  return ret;
}

int ObArchiveSender::handle_task_list(ObArchiveTaskStatus* status)
{
  int ret = OB_SUCCESS;
  SendTaskArray task_array;
  ObArchiveSendTaskStatus* task_status = static_cast<ObArchiveSendTaskStatus*>(status);

  if (OB_ISNULL(task_status)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(task_status));
  } else if (OB_FAIL(converge_send_task_(*task_status, task_array))) {
    ARCHIVE_LOG(WARN, "converge_send_task_ fail", KR(ret), KPC(task_status));
  } else if (0 == task_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "task_array count is 0", KR(ret), KPC(task_status));
  } else if (OB_SUCC(handle(*task_status, task_array))) {
    // do nothing
  } else if (is_not_leader_error_(ret)) {
    ret = OB_SUCCESS;
  } else {
    ARCHIVE_LOG(WARN, "handle task array fail", KR(ret), KPC(task_status));
  }

  if (OB_SUCC(ret)) {
    const int64_t task_num = task_array.count();
    if (OB_FAIL(task_status->pop_front(task_num))) {
      ARCHIVE_LOG(ERROR, "pop_front fail", KR(ret), K(task_num), KPC(task_status));
    } else if (OB_FAIL(release_task_array_(task_array))) {
      ARCHIVE_LOG(ERROR, "release_task_array_ fail", KR(ret), KPC(task_status));
    } else {
      ATOMIC_SAF(&total_task_count_, task_num);
    }
  }

  if (NULL != task_status) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_retire_task_status_(*task_status))) {
      ARCHIVE_LOG(ERROR, "try_retire_task_status_ fail", KR(tmp_ret), KPC(task_status));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }

  // if io error or alloc memory fail or io limit, sleep 100ms
  if (is_io_error_(ret) || OB_ALLOCATE_MEMORY_FAILED == ret) {
    usleep(100 * 1000L);
  }

  return ret;
}

int ObArchiveSender::converge_send_task_(ObArchiveSendTaskStatus& task_status, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  const int64_t task_limit = MAX_CONVERGE_TASK_COUNT;
  const int64_t max_task_size = MAX_CONVERGE_TASK_SIZE;
  int64_t total_task_size = 0;
  int64_t task_num = 0;
  ObLink* link = NULL;
  ObArchiveSendTask* task = NULL;
  bool task_exist = false;
  bool need_break = false;

  if (OB_FAIL(task_status.top(link, task_exist))) {
    ARCHIVE_LOG(WARN, "top fail", KR(ret), K(task_status));
  } else if (!task_exist) {
    // skip it
  } else {
    task = static_cast<ObArchiveSendTask*>(link);
    if (OB_FAIL(array.push_back(task))) {
      ARCHIVE_LOG(WARN, "push_back fail", KR(ret), KPC(task));
    } else {
      task_num++;
      total_task_size += task->get_data_len();
    }
  }

  while (OB_SUCC(ret) && NULL != task && !need_break && task_num < task_status.count()) {
    ObLink* next = NULL;
    if (OB_ISNULL(next = task_status.next(*task))) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "next is NULL", KR(ret), K(task_status), K(task_num));
    } else {
      ObArchiveSendTask* next_task = static_cast<ObArchiveSendTask*>(next);
      if (OB_FAIL(array.push_back(next_task))) {
        ARCHIVE_LOG(WARN, "push_back fail", KR(ret), KPC(task));
      } else {
        task = next_task;
        need_break = (++task_num) > task_limit || (total_task_size += task->get_data_len()) >= max_task_size;
      }
    }
  }

  return ret;
}

int ObArchiveSender::release_task_array_(SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  ObArchiveSendTask* task = NULL;

  while (OB_SUCC(ret) && 0 < array.count()) {
    if (OB_FAIL(array.pop_back(task))) {
      ARCHIVE_LOG(ERROR, "pop_back fail", KR(ret));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task is NULL", KR(ret));
    } else {
      release_send_task(task);
    }
  }

  return ret;
}

int ObArchiveSender::try_retire_task_status_(ObArchiveSendTaskStatus& task_status)
{
  int ret = OB_SUCCESS;
  bool is_queue_empty = false;
  bool is_discarded = false;

  if (OB_FAIL(task_status.retire(is_queue_empty, is_discarded))) {
    ARCHIVE_LOG(ERROR, "task_status retire fail", KR(ret), K(task_status));
  } else if (is_discarded && NULL != allocator_) {
    allocator_->free_send_task_status(&task_status);
  } else if (!is_queue_empty) {
    if (OB_FAIL(task_queue_.push(&task_status))) {
      ARCHIVE_LOG(ERROR, "push fail", KR(ret), K(task_status));
    }
  }

  return ret;
}

int ObArchiveSender::handle(ObArchiveSendTaskStatus& task_status, SendTaskArray& array)
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(LOG_ARCHIVE_SENDER_HANDLE);
  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveSender not init");
    ret = OB_NOT_INIT;
    on_fatal_error_(ret);
  } else if (OB_UNLIKELY(!in_normal_status_())) {
    // skip
    ARCHIVE_LOG(INFO, "not int normal status, skip it");
  } else if (OB_UNLIKELY(round_stop_flag_)) {
    // skip it
    ARCHIVE_LOG(INFO, "ObArchiveSender already stop, skip it");
  } else if (0 >= array.count()) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "array is empty", KR(ret));
  } else {
    ObArchiveSendTask* task = array[0];
    const ObPGKey& pg_key = task->pg_key_;
    const int64_t epoch = task->epoch_id_;
    const int64_t incarnation = task->incarnation_;
    const int64_t archive_round = task->log_archive_round_;
    ObPGArchiveTaskGuard guard(pg_mgr_);
    ObPGArchiveTask* pg_archive_task = NULL;
    uint64_t max_archived_log_id = OB_INVALID_ID;
    int64_t max_archived_log_ts = OB_INVALID_TIMESTAMP;
    int64_t unused_archived_checkpoint_ts = OB_INVALID_TIMESTAMP;
    int64_t unused_clog_epoch_id = OB_INVALID_TIMESTAMP;
    int64_t unused_accum_checksum = 0;

    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      ARCHIVE_LOG(INFO, "ObArchiveSender handle task");
    }

    if (OB_ISNULL(pg_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "pg_mgr_ is NULL", KR(ret), K(pg_key));
    } else if (OB_FAIL(pg_mgr_->get_pg_archive_task_guard_with_status(pg_key, guard)) && OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_pg_archive_task_guard_with_status fail", KR(ret), K(pg_key));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ARCHIVE_LOG(WARN, "pg archive task has beed deleted", K(pg_key));
      ret = OB_SUCCESS;
    } else if (OB_ISNULL(pg_archive_task = guard.get_pg_archive_task())) {
      ARCHIVE_LOG(ERROR, "get_pg_archive_task fail", K(pg_key), K(guard));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(pg_archive_task->get_max_archived_info(epoch,
                   incarnation,
                   archive_round,
                   max_archived_log_id,
                   max_archived_log_ts,
                   unused_archived_checkpoint_ts,
                   unused_clog_epoch_id,
                   unused_accum_checksum))) {
      ARCHIVE_LOG(WARN, "failed to get_max_archived_info", KR(ret), KPC(pg_archive_task));
    } else if (OB_FAIL(check_can_send_task_(
                   epoch, incarnation, archive_round, max_archived_log_id, max_archived_log_ts, array))) {
      ARCHIVE_LOG(WARN, "check_can_send_task_ can't send this task", KR(ret), KPC(pg_archive_task));
    } else if (OB_FAIL(archive_log_(pg_key, *pg_archive_task, array))) {
      ARCHIVE_LOG(WARN, "archive_log_ fail", KR(ret), KPC(pg_archive_task));
    } else if (OB_FAIL(archive_log_callback_(*pg_archive_task, array))) {
      ARCHIVE_LOG(WARN, "archive_log_callback_ fail", KR(ret), K(pg_key));
    }

    if (OB_FAIL(ret)) {
      handle_archive_error_(pg_key, epoch, incarnation, archive_round, ret, task_status);
    } else {
      task_status.clear_error_info();
    }
  }

  return ret;
}

// only filter stale or unordered tasks
int ObArchiveSender::check_can_send_task_(const int64_t epoch, const int64_t incarnation, const int64_t round,
    const uint64_t max_archived_log_id, const int64_t max_archived_log_ts, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  int64_t pre_log_id = max_archived_log_id;
  int64_t pre_log_ts = max_archived_log_ts;

  for (int64_t i = 0; i < array.count(); i++) {
    ObArchiveSendTask* task = NULL;
    if (OB_ISNULL(task = array[i])) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task is NULL", KR(ret));
    } else if (OB_UNLIKELY(0 >= task->get_data_len()) || OB_UNLIKELY(OB_INVALID_ID == task->start_log_id_) ||
               OB_UNLIKELY(epoch != task->epoch_id_) || OB_UNLIKELY(incarnation != task->incarnation_) ||
               OB_UNLIKELY(round != task->log_archive_round_)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "invalid send task", KR(ret), KPC(task));
    } else {
      const ObPGKey& pg_key = task->pg_key_;
      const uint64_t task_start_log_id = task->start_log_id_;
      const uint64_t task_end_log_id = task->end_log_id_;
      const int64_t task_timestamp = task->end_log_submit_ts_;
      const ObLogArchiveContentType task_type = task->task_type_;
      switch (task_type) {
        case OB_ARCHIVE_TASK_TYPE_KICKOFF:
          ret = check_start_archive_task_(pg_key, task_end_log_id);
          break;
        case OB_ARCHIVE_TASK_TYPE_CHECKPOINT:
          ret = check_checkpoint_task_(pg_key, task_end_log_id, task_timestamp, pre_log_id, pre_log_ts);
          break;
        case OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT:
          ret = check_clog_split_task_(
              pg_key, task_start_log_id, task_timestamp, pre_log_id, pre_log_ts, task->need_update_log_ts_);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          ARCHIVE_LOG(WARN, "invalid task type", KR(ret), KPC(task));
      }

      if (OB_FAIL(ret)) {
        ARCHIVE_LOG(WARN, "check task fail", KR(ret), K(i), KPC(task));
      } else {
        pre_log_id = OB_ARCHIVE_TASK_TYPE_KICKOFF == task_type ? task_end_log_id - 1 : task_end_log_id;
        pre_log_ts = OB_ARCHIVE_TASK_TYPE_KICKOFF == task_type ? pre_log_ts : task_timestamp;
      }
    }
  }

  return ret;
}

int ObArchiveSender::check_start_archive_task_(const ObPGKey& pg_key, const uint64_t end_log_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 >= end_log_id)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "start_archive_task invalid argument", K(pg_key), K(end_log_id), KR(ret));
  }

  return ret;
}

int ObArchiveSender::check_checkpoint_task_(const ObPGKey& pg_key, const uint64_t end_log_id, const int64_t task_log_ts,
    const uint64_t max_archive_log_id, const int64_t max_archive_log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(max_archive_log_ts > task_log_ts) || OB_UNLIKELY(max_archive_log_id != end_log_id)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN,
        "checkpoint_task invalid argument",
        KR(ret),
        K(pg_key),
        K(end_log_id),
        K(task_log_ts),
        K(max_archive_log_id),
        K(max_archive_log_ts));
  }

  return ret;
}

int ObArchiveSender::check_clog_split_task_(const ObPGKey& pg_key, const uint64_t start_log_id,
    const int64_t task_log_ts, const uint64_t max_archive_log_id, const int64_t max_archive_log_ts,
    const bool need_update_log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(start_log_id != (max_archive_log_id + 1)) ||
      OB_UNLIKELY(need_update_log_ts && task_log_ts <= max_archive_log_ts)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN,
        "clog_split_task invalid argument",
        KR(ret),
        K(pg_key),
        K(start_log_id),
        K(task_log_ts),
        K(max_archive_log_id),
        K(max_archive_log_ts),
        K(need_update_log_ts));
  }

  return ret;
}

bool ObArchiveSender::check_need_switch_file_(
    const int64_t log_size, const int64_t offset, const bool force_switch, const bool is_data_file)
{
  const int64_t file_size = is_data_file ? DEFAULT_ARCHIVE_DATA_FILE_SIZE : DEFAULT_ARCHIVE_INDEX_FILE_SIZE;
  return (log_size + offset) > file_size || force_switch;
}

// Fill block meta
//
// To accelerate archive start when partition leader switch, min_log/max_log
// are recorded in ObArchiveBlockMeta.
//
// Meanwhile an offset(int32_t) which record the start offset of the block in the file
// is reserved in the last four bytes of each block.
//
// So KICKOFF/CLOG log block need fill these info here
// block meta info is not need as CHECKPOINT no longer archived
//
// KICKOFF log is the first log in a file, so its BlockMeta is explicit,
// only CLOG block need set min_log/offset here
//
// As NOP log is speical in OB, submit_log_ts of its log_entry is filled with the one of the pre log,
// may be smaller than CHECKPOINT log. max_log_submit_ts of NOP is set with the partition's max.
//
// If no checkpoint(OB) log in block, block max_checkpoint_ts is filled with the partition's last max.
//
int ObArchiveSender::fill_and_build_block_meta_(const int64_t epoch, const int64_t incarnation, const int64_t round,
    ObPGArchiveTask& pg_archive_task, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  uint64_t unused_max_archived_log_id = OB_INVALID_ID;
  int64_t last_archived_log_submit_ts = OB_INVALID_TIMESTAMP;
  int64_t last_archived_checkpoint_ts = OB_INVALID_TIMESTAMP;
  int64_t unused_clog_epoch_id = OB_INVALID_TIMESTAMP;
  int64_t unused_accum_checksum = 0;
  uint64_t min_log_id_in_file = OB_INVALID_ID;
  int64_t min_log_ts_in_file = OB_INVALID_TIMESTAMP;
  bool min_log_exist_in_file = false;

  if (OB_FAIL(pg_archive_task.get_max_archived_info(epoch,
          incarnation,
          round,
          unused_max_archived_log_id,
          last_archived_log_submit_ts,
          last_archived_checkpoint_ts,
          unused_clog_epoch_id,
          unused_accum_checksum))) {
    ARCHIVE_LOG(WARN, "get_max_archived_info fail", KR(ret), K(pg_archive_task));
  } else if (OB_FAIL(pg_archive_task.get_current_data_file_min_log_info(
                 epoch, incarnation, round, min_log_id_in_file, min_log_ts_in_file, min_log_exist_in_file))) {
    ARCHIVE_LOG(WARN, "get_current_data_file_min_log_info fail", KR(ret), K(pg_archive_task));
  } else if (!min_log_exist_in_file) {
    ObArchiveSendTask* task = NULL;
    if (OB_FAIL(get_min_upload_archive_task_(task, array))) {
      ARCHIVE_LOG(WARN, "get_min_upload_archive_task_ fail", KR(ret));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task is NULL", KR(ret));
    } else {
      min_log_id_in_file =
          OB_ARCHIVE_TASK_TYPE_KICKOFF == task->task_type_ ? task->start_log_id_ - 1 : task->start_log_id_;
      min_log_ts_in_file = task->start_log_ts_;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_fill_and_build_block_meta_(
            min_log_id_in_file, min_log_ts_in_file, last_archived_checkpoint_ts, last_archived_log_submit_ts, array))) {
      ARCHIVE_LOG(WARN, "do_fill_and_build_block_meta_ fail", KR(ret));
    }
  }

  return ret;
}

int ObArchiveSender::do_fill_and_build_block_meta_(const uint64_t min_log_id, const int64_t min_log_ts,
    const int64_t max_checkpoint_ts, const int64_t max_log_ts, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  int64_t pre_checkpoint_ts = max_checkpoint_ts;
  int64_t pre_log_ts = max_log_ts;
  ObArchiveSendTask* task = NULL;

  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); i++) {
    if (OB_ISNULL(task = array[i])) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(i));
    } else {
      if (OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT == task->task_type_) {
        if (!task->need_update_log_ts_) {
          // all logs are nop, set bloack max log submit ts = max archived log submit ts
          task->block_meta_.max_log_submit_ts_ = pre_log_ts;
        } else {
          pre_log_ts = task->block_meta_.max_log_submit_ts_;
        }
        task->block_meta_.min_log_id_in_file_ = min_log_id;
        task->block_meta_.min_log_ts_in_file_ = min_log_ts;
      }

      // set block max checkpoint ts = max archived checkpoint ts if rollback
      if (pre_checkpoint_ts > task->block_meta_.max_checkpoint_ts_) {
        // if no checlpoint clog, set block max checkpoint ts = max archived checkpoint ts
        task->block_meta_.max_checkpoint_ts_ = pre_checkpoint_ts;
      } else {
        pre_checkpoint_ts = task->block_meta_.max_checkpoint_ts_;
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t block_meta_size = task->block_meta_.get_serialize_size();
      int64_t unused_pos = 0;
      if (OB_FAIL(task->block_meta_.build_serialized_block(
              task->buf_, block_meta_size, task->buf_ + block_meta_size, task->archive_data_len_, unused_pos))) {
        ARCHIVE_LOG(ERROR, "failed to build_serialized_block", KPC(task), KR(ret));
      }
    }

    task = NULL;
  }

  return ret;
}

int ObArchiveSender::fill_archive_block_offset_(const int64_t file_offset, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  const int64_t offset_size = ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE;
  int32_t offset = file_offset;

  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); i++) {
    int64_t pos = 0;
    ObArchiveSendTask* task = NULL;
    if (OB_ISNULL(task = array[i])) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(i));
    } else if (OB_UNLIKELY(0 > offset) || offset_size >= task->get_data_len()) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "invalid file offset", KR(ret), K(offset), K(i), KPC(task));
    } else if (OB_FAIL(serialization::encode_i32(
                   task->buf_ + task->get_data_len() - offset_size, offset_size, pos, offset))) {
      ARCHIVE_LOG(WARN, "encode_i32 offset fail", KR(ret), K(offset), K(i), KPC(task));
    } else {
      offset += task->get_data_len();
    }
  }

  return ret;
}

int ObArchiveSender::build_send_buf_(const int64_t buf_len, char* buf, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObArchiveSendTask* task = NULL;

  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); i++) {
    if (OB_ISNULL(task = array[i])) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(i));
    } else if (OB_ARCHIVE_TASK_TYPE_CHECKPOINT == task->task_type_) {
      // skip it
    } else if (OB_UNLIKELY(buf_len - pos < task->get_data_len())) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "no enough space to copy task buff", KR(ret), K(buf_len), K(pos), K(i), KPC(task));
    } else {
      MEMCPY(buf + pos, task->buf_, task->get_data_len());
      pos += task->get_data_len();
    }
  }

  return ret;
}

// locate the first no-checkpoint log in array
int ObArchiveSender::get_min_upload_archive_task_(ObArchiveSendTask*& task, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  bool done = false;

  for (int64_t i = 0; OB_SUCC(ret) && !done && i < array.count(); i++) {
    if (OB_ISNULL(array[i])) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "array element is NULL", KR(ret), K(i));
    } else if (OB_ARCHIVE_TASK_TYPE_KICKOFF == array[i]->task_type_ ||
               OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT == array[i]->task_type_) {
      task = array[i];
      done = true;
    }
  }

  return ret;
}

int ObArchiveSender::archive_log_(const ObPGKey& pg_key, ObPGArchiveTask& pg_archive_task, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  ObArchiveSendTask* task = NULL;
  int64_t buf_size = 0;
  bool only_checkpoint = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); i++) {
    if (OB_ISNULL(task = array[i])) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(i), K(pg_key), K(task));
    } else if (OB_ARCHIVE_TASK_TYPE_CHECKPOINT != task->task_type_) {
      only_checkpoint = false;
      buf_size += task->get_data_len();
    }
    task = NULL;
  }

  if (OB_SUCC(ret) && !only_checkpoint) {
    if (OB_FAIL(do_archive_log_(pg_archive_task, buf_size, array))) {
      ARCHIVE_LOG(WARN, "do_archive_log_ fail", KR(ret), K(pg_key));
    }
  }

  return ret;
}

int ObArchiveSender::do_archive_log_(ObPGArchiveTask& pg_archive_task, const int64_t buf_size, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  ObArchiveSendTask* task = array[0];
  const int64_t start_ts = ObTimeUtility::current_time();
  const int64_t epoch = task->epoch_id_;
  const int64_t incarnation = task->incarnation_;
  const int64_t round = task->log_archive_round_;
  char* buffer = NULL;
  int64_t file_offset = -1;
  char file_path[OB_MAX_ARCHIVE_PATH_LENGTH] = {'0'};
  bool force_switch_data_file = false;
  const bool is_data_file = true;
  bool need_switch_file = false;
  bool compatible = false;

  // 0. touch archive_key if not data archived in this pg
  if (OB_FAIL(try_touch_archive_key_(incarnation, round, pg_archive_task))) {
    ARCHIVE_LOG(WARN, "touch archive_key fail", KR(ret), K(incarnation), K(round), K(pg_archive_task));
  }

  // 1. get data file offset and check if need switch file
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_archive_task.get_current_file_offset(
            epoch, incarnation, round, LOG_ARCHIVE_FILE_TYPE_DATA, file_offset, force_switch_data_file))) {
      ARCHIVE_LOG(WARN, "get_current_file_offset fail", KR(ret), K(pg_archive_task));
    } else {
      need_switch_file = check_need_switch_file_(buf_size, file_offset, force_switch_data_file, is_data_file);
    }
  }

  // 2. record index file if needed
  if (OB_SUCC(ret) && need_switch_file) {
    if (OB_FAIL(record_index_info(pg_archive_task, epoch, incarnation, round))) {
      ARCHIVE_LOG(WARN, "record_index_info fail", KR(ret), K(pg_archive_task));
    }
  }

  // 3. switch data file
  if (OB_SUCC(ret) && need_switch_file) {
    if (OB_FAIL(switch_file_(pg_archive_task, epoch, incarnation, round, LOG_ARCHIVE_FILE_TYPE_DATA))) {
      ARCHIVE_LOG(WARN, "switch data file fail", KR(ret), K(pg_archive_task));
    }
  }

  // 4. get archive data file info -> min_log/offset/file_path
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_archive_task.get_current_file_info(epoch,
            incarnation,
            round,
            LOG_ARCHIVE_FILE_TYPE_DATA,
            compatible,
            file_offset,
            OB_MAX_ARCHIVE_PATH_LENGTH,
            file_path))) {
      ARCHIVE_LOG(WARN, "get_current_file_info fail", KR(ret), K(pg_archive_task));
    }
  }

  // 5. fill ArchiveBlockMeta and block offset
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fill_and_build_block_meta_(epoch, incarnation, round, pg_archive_task, array))) {
      ARCHIVE_LOG(WARN, "fill_and_build_block_meta_ fail", KR(ret), K(pg_archive_task));
    } else if (OB_FAIL(fill_archive_block_offset_(file_offset, array))) {
      ARCHIVE_LOG(WARN, "fill_archive_block_offset_ fail", KR(ret), K(pg_archive_task));
    }
  }

  // 6. batch send buffer
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(buffer = static_cast<char*>(ob_archive_malloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ARCHIVE_LOG(WARN, "ob_archive_malloc fail", KR(ret));
    } else if (OB_FAIL(build_send_buf_(buf_size, buffer, array))) {
      ARCHIVE_LOG(WARN, "build_send_buf_ fail", KR(ret), K(buf_size), K(buffer));
    }
  }

  bool has_inject_fault = false;
#ifdef ERRSIM
  if (OB_SUCC(ret) && file_offset > 0) {
    ret = E(EventTable::EN_LOG_ARCHIVE_DATA_BUFFER_NOT_COMPLETED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      // inject error
      has_inject_fault = true;
      const int64_t real_buf_size = buf_size - 50;
      ObString storage_info(archive_round_mgr_->storage_info_);
      ObArchiveIO archive_io(storage_info);
      if (OB_FAIL(archive_io.push_log(
              file_path, real_buf_size, buffer, need_switch_file, compatible, is_data_file, epoch))) {
        ARCHIVE_LOG(WARN, "push_log fail", KR(ret), K(pg_archive_task));
      } else {
        ret = OB_IO_ERROR;
      }

      if (is_io_error_(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS !=
            (tmp_ret = pg_archive_task.set_file_force_switch(epoch, incarnation_, round, LOG_ARCHIVE_FILE_TYPE_DATA))) {
          ARCHIVE_LOG(WARN, "set_file_force_switch fail", KR(tmp_ret), K(pg_archive_task));
          ret = tmp_ret;
          // rewrite ret
        } else {
          ARCHIVE_LOG(INFO, "fake archive data corrupted", KR(ret), K(pg_archive_task));
        }
      }
    }
  }
#endif

  // 7. write data file
  if (OB_SUCC(ret) && !has_inject_fault) {
    ObString storage_info(archive_round_mgr_->storage_info_);
    ObArchiveIO archive_io(storage_info);
    if (OB_FAIL(archive_io.push_log(file_path, buf_size, buffer, need_switch_file, compatible, is_data_file, epoch))) {
      ARCHIVE_LOG(WARN, "push_log fail", KR(ret), K(pg_archive_task));

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS !=
          (tmp_ret = pg_archive_task.set_file_force_switch(epoch, incarnation, round, LOG_ARCHIVE_FILE_TYPE_DATA))) {
        ARCHIVE_LOG(WARN, "set_file_force_switch fail", KR(tmp_ret), K(pg_archive_task));
        ret = tmp_ret;
      }
    }
  }

  if (NULL != buffer) {
    ob_archive_free(buffer);
  }

  // 8. record min_log/offset
  if (OB_SUCC(ret)) {
    ObArchiveSendTask* task = array[0];
    uint64_t min_log_id = task->start_log_id_;
    int64_t min_log_ts = task->start_log_ts_;
    if (OB_ARCHIVE_TASK_TYPE_KICKOFF == task->task_type_) {
      min_log_id = task->start_log_id_ - 1;
    } else if (OB_ARCHIVE_TASK_TYPE_CHECKPOINT == task->task_type_) {
      // need send only if non-checkpoint log is included
      min_log_id = task->start_log_id_ + 1;
    }
    if (OB_FAIL(update_data_file_info_(epoch, incarnation, round, min_log_id, min_log_ts, pg_archive_task, buf_size))) {
      ARCHIVE_LOG(WARN, "update_data_file_info_ fail", KR(ret), K(pg_archive_task));
    }
  }

  // 9. statistic
  if (OB_SUCC(ret)) {
    const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    statistic(array, cost_ts);
  }

  return ret;
}

int ObArchiveSender::record_index_info(
    ObPGArchiveTask& pg_archive_task, const int64_t epoch, const int64_t incarnation, const int64_t round)
{
  int ret = OB_SUCCESS;
  ObArchiveIndexFileInfo index_file_info;
  char* buffer = NULL;
  const int64_t buffer_len = index_file_info.get_serialize_size();
  int64_t pos = 0;

  if (OB_ISNULL(buffer = static_cast<char*>(ob_archive_malloc(buffer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "allocate memory fail", KR(ret), K(pg_archive_task));
  } else if (OB_FAIL(pg_archive_task.build_data_file_index_record(epoch, incarnation, round, index_file_info))) {
    ARCHIVE_LOG(WARN, "index_file_info reset fail", KR(ret), K(pg_archive_task), K(epoch), K(incarnation), K(round));
  } else if (OB_FAIL(index_file_info.serialize(buffer, buffer_len, pos))) {
    ARCHIVE_LOG(WARN, "index_file_info serialize fail", KR(ret), K(pg_archive_task), K(index_file_info));
  } else if (OB_FAIL(record_index_info_(pg_archive_task, epoch, incarnation, round, buffer, buffer_len))) {
    ARCHIVE_LOG(WARN,
        "record_index_info_ fail",
        KR(ret),
        K(pg_archive_task),
        K(index_file_info),
        K(epoch),
        K(incarnation),
        K(round),
        K(buffer_len),
        K(buffer_len));
  } else {
    ARCHIVE_LOG(INFO,
        "generate new index record succ",
        K(epoch),
        K(incarnation),
        K(round),
        K(pg_archive_task),
        K(index_file_info));
  }

  // 5. update index file offset
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_archive_task.update_pg_archive_file_offset(
            epoch, incarnation, round, buffer_len, LOG_ARCHIVE_FILE_TYPE_INDEX))) {
      ARCHIVE_LOG(WARN, "update_index_file_offset_ fail", KR(ret), K(pg_archive_task), K(buffer_len));
    } else {
      ARCHIVE_LOG(DEBUG, "record_index_info succ", K(index_file_info), K(pg_archive_task), K(incarnation), K(round));
    }
  }

  if (NULL != buffer) {
    ob_archive_free(buffer);
    buffer = NULL;
  }

  return ret;
}

// attention: NO retry
int ObArchiveSender::record_index_info_(ObPGArchiveTask& pg_archive_task, const int64_t epoch,
    const int64_t incarnation, const int64_t round, char* buffer, const int64_t buffer_len)
{
  int ret = OB_SUCCESS;
  int64_t file_offset = 0;
  char file_path[OB_MAX_ARCHIVE_PATH_LENGTH] = {'0'};
  bool need_switch_file = false;
  bool force_switch_index_file = false;
  const bool is_data_file = false;
  bool compatible = false;

  if (OB_ISNULL(archive_round_mgr_) || OB_ISNULL(buffer) || OB_UNLIKELY(0 >= buffer_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_archive_task), K(buffer), K(buffer_len), K(archive_round_mgr_));
  }

  // 0. get archive file offset
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_archive_task.get_current_file_offset(
            epoch, incarnation, round, LOG_ARCHIVE_FILE_TYPE_INDEX, file_offset, force_switch_index_file))) {
      ARCHIVE_LOG(WARN, "get_current_file_offset fail", KR(ret), K(pg_archive_task));
    }
  }

  // 1. check if need switch index file
  if (OB_SUCC(ret)) {
    need_switch_file = check_need_switch_file_(buffer_len, file_offset, force_switch_index_file, is_data_file);
  }

  // 2. switch index file
  if (OB_SUCC(ret) && need_switch_file) {
    if (OB_FAIL(switch_file_(pg_archive_task, epoch, incarnation, round, LOG_ARCHIVE_FILE_TYPE_INDEX))) {
      ARCHIVE_LOG(WARN, "switch index file fail", KR(ret), K(pg_archive_task));
    }
  }

  // 3. get archive file info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_archive_task.get_current_file_info(epoch,
            incarnation,
            round,
            LOG_ARCHIVE_FILE_TYPE_INDEX,
            compatible,
            file_offset,
            OB_MAX_ARCHIVE_PATH_LENGTH,
            file_path))) {
      ARCHIVE_LOG(WARN, "get_current_file_info fail", KR(ret), K(pg_archive_task));
    }
  }

  bool has_inject_fault = false;
#ifdef ERRSIM
  if (OB_SUCC(ret) && file_offset > 0) {
    ret = E(EventTable::EN_LOG_ARCHIVE_INDEX_BUFFER_NOT_COMPLETED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      // error injection
      has_inject_fault = true;
      const int64_t real_buffer_len = buffer_len - 5;
      ObString storage_info(archive_round_mgr_->storage_info_);
      ObArchiveIO archive_io(storage_info);
      if (OB_FAIL(archive_io.push_log(
              file_path, real_buffer_len, buffer, need_switch_file, compatible, is_data_file, epoch))) {
        ARCHIVE_LOG(WARN, "push_log fail", KR(ret), K(pg_archive_task));
      } else {
        ret = OB_IO_ERROR;
      }

      if (is_io_error_(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = pg_archive_task.set_file_force_switch(
                               epoch, incarnation_, round, LOG_ARCHIVE_FILE_TYPE_INDEX))) {
          ARCHIVE_LOG(WARN, "set_file_force_switch fail", KR(tmp_ret), K(pg_archive_task));
          ret = tmp_ret;
          // rewrite ret
        } else {
          ARCHIVE_LOG(INFO, "fake archive index corrupted", KR(ret), K(pg_archive_task));
        }
      }
    }
  }
#endif
  // 4. write index file
  if (OB_SUCC(ret) && !has_inject_fault) {
    ObString storage_info(archive_round_mgr_->storage_info_);
    ObArchiveIO archive_io(storage_info);
    if (OB_FAIL(
            archive_io.push_log(file_path, buffer_len, buffer, need_switch_file, compatible, is_data_file, epoch))) {
      ARCHIVE_LOG(WARN, "push_log fail", KR(ret), K(pg_archive_task));

      if (is_io_error_(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = pg_archive_task.set_file_force_switch(
                               epoch, incarnation_, round, LOG_ARCHIVE_FILE_TYPE_INDEX))) {
          ARCHIVE_LOG(WARN, "set_file_force_switch fail", KR(tmp_ret), K(pg_archive_task));
          ret = tmp_ret;
        }
      }
    }
  }

  return ret;
}

// check leader before switch file
int ObArchiveSender::switch_file_(ObPGArchiveTask& pg_archive_task, const int64_t epoch, const int64_t incarnation,
    const int64_t round, const LogArchiveFileType file_type)
{
  int ret = OB_SUCCESS;
  bool is_leader = true;
  const ObPGKey& pg_key = pg_archive_task.get_pg_key();

  if (OB_FAIL(check_is_leader(pg_key, epoch, is_leader))) {
    is_leader = false;
    ARCHIVE_LOG(WARN, "check_is_leader fail", KR(ret), K(pg_archive_task), K(file_type));
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
  } else if (OB_LIKELY(is_leader)) {
    if (OB_FAIL(pg_archive_task.switch_archive_file(epoch, incarnation, round, file_type))) {
      ARCHIVE_LOG(WARN, "switch index file fail", KR(ret), K(pg_archive_task), K(file_type));
    }
  } else if (OB_UNLIKELY(!is_leader)) {
    // mark delete pg_archive_task
    pg_archive_task.mark_pg_archive_task_del(epoch, incarnation, round);
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
  }

  return ret;
}

int ObArchiveSender::update_data_file_info_(const int64_t epoch, const int64_t incarnation, const int64_t round,
    const uint64_t min_log_id, const int64_t min_log_ts, ObPGArchiveTask& pg_archive_task, const int64_t buffer_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_archive_task.update_pg_archive_file_offset(
          epoch, incarnation, round, buffer_len, LOG_ARCHIVE_FILE_TYPE_DATA))) {
    ARCHIVE_LOG(WARN, "update_pg_archive_file_offset fail", KR(ret), K(pg_archive_task));
  } else if (OB_FAIL(pg_archive_task.set_pg_data_file_record_min_log_info(
                 epoch, incarnation, round, min_log_id, min_log_ts))) {
    ARCHIVE_LOG(WARN, "set_pg_data_file_record_min_log_info fail", KR(ret), K(pg_archive_task));
  }

  return ret;
}

int ObArchiveSender::archive_log_callback_(ObPGArchiveTask& pg_archive_task, SendTaskArray& array)
{
  int ret = OB_SUCCESS;
  ObArchiveSendTask* task = NULL;

  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); i++) {
    if (OB_ISNULL(task = array[i])) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(i));
    } else if (OB_FAIL(do_archive_log_callback_(pg_archive_task, *task))) {
      ARCHIVE_LOG(WARN, "do_archive_log_callback_ fail", KR(ret), KPC(task));
    }
  }

  return ret;
}

int ObArchiveSender::do_archive_log_callback_(ObPGArchiveTask& pg_archive_task, ObArchiveSendTask& task)
{
  int ret = OB_SUCCESS;
  ObLogArchiveContentType task_type = task.task_type_;

  ARCHIVE_LOG(DEBUG, "archive_log_callback_", K(task), K(pg_archive_task));

  switch (task_type) {
    case OB_ARCHIVE_TASK_TYPE_KICKOFF:
      ret = pg_archive_task.mark_pg_first_record_finish(task.epoch_id_, task.incarnation_, task.log_archive_round_);
      break;
    case OB_ARCHIVE_TASK_TYPE_CHECKPOINT:
      ret = pg_archive_task.update_pg_archive_checkpoint_ts(
          task.epoch_id_, task.incarnation_, task.log_archive_round_, task.end_log_submit_ts_, task.checkpoint_ts_);
      break;
    case OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT:
      ret = pg_archive_task.update_pg_archive_progress(task.need_update_log_ts_,
          task.epoch_id_,
          task.incarnation_,
          task.log_archive_round_,
          task.end_log_id_,
          task.end_log_submit_ts_,
          task.checkpoint_ts_,
          task.block_meta_.clog_epoch_id_,
          task.block_meta_.accum_checksum_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(WARN, "invalid task type", K(task), K(pg_archive_task));
  }

  return ret;
}

bool ObArchiveSender::in_normal_status_()
{
  return archive_mgr_->is_in_archive_status();
}

void ObArchiveSender::on_fatal_error_(const int err_ret)
{
  const int64_t err_ts = ObClockGenerator::getClock();
  while (!round_stop_flag_) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      ARCHIVE_LOG(ERROR, "fatal error happened", K(err_ret), K(err_ts));
    }
    usleep(1 * 1000 * 1000);  // sleep 1s
  }
}

int ObArchiveSender::handle_archive_error_(const ObPGKey& pg_key, const int64_t epoch, const int64_t incarnation,
    const int64_t round, const int ret_code, ObArchiveSendTaskStatus& task_status)
{
  int ret = OB_SUCCESS;
  bool io_error_trigger = false;

  // statistic IO error
  if (is_io_error_(ret_code)) {
    io_error_trigger = task_status.mark_io_error();
  }

  if (OB_UNLIKELY(!in_normal_status_())) {
    // skip it
    ARCHIVE_LOG(
        WARN, "error occur in unnormal status, skip it", KR(ret_code), K(pg_key), K(epoch), K(incarnation), K(round));
  } else if (round_stop_flag_) {
    ARCHIVE_LOG(WARN,
        "error occur when ObArchiveSender already stop, skip it",
        KR(ret),
        K(pg_key),
        K(epoch),
        K(incarnation),
        K(round));
  } else if ((is_not_leader_error_(ret_code) || is_io_error_(ret_code) || OB_ALLOCATE_MEMORY_FAILED == ret_code) &&
             !io_error_trigger) {
    // not leader or IO error, skip it
    ARCHIVE_LOG(WARN,
        "obsolete task or alloc memory fail or io error, skip it",
        KR(ret_code),
        K(pg_key),
        K(epoch),
        K(incarnation),
        K(round));
  } else if (OB_IO_ERROR == ret_code) {
    // nfs io error, print error only
    if (io_error_trigger && REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      ARCHIVE_LOG(ERROR,
          "pay ATTENTION!! archive continuous encounter error  more than 15mins",
          K(ret_code),
          K(pg_key),
          K(epoch),
          K(incarnation),
          K(round));
    }
  } else if (OB_FAIL((pg_mgr_->mark_fatal_error(pg_key, epoch, incarnation, round)))) {
    ARCHIVE_LOG(ERROR,
        "failed to mark_fatal_error",
        KR(ret),
        KR(ret_code),
        K(pg_key),
        K(epoch),
        K(incarnation),
        K(round),
        K(task_status));
  } else {
    ARCHIVE_LOG(
        INFO, "mark_fatal_error_ succ", KR(ret_code), K(pg_key), K(epoch), K(incarnation), K(round), K(task_status));
  }

  return ret;
}

bool ObArchiveSender::is_io_error_(const int ret_code)
{
  return OB_IO_ERROR == ret_code || OB_OSS_ERROR == ret_code;
}

bool ObArchiveSender::is_not_leader_error_(const int ret_code)
{
  return OB_LOG_ARCHIVE_LEADER_CHANGED == ret_code || OB_BACKUP_INFO_NOT_MATCH == ret_code ||
         OB_ENTRY_NOT_EXIST == ret_code;
}

void ObArchiveSender::statistic(SendTaskArray& array, const int64_t cost_ts)
{
  static __thread int64_t SEND_LOG_COUNT;
  static __thread int64_t SEND_BUF_SIZE;
  static __thread int64_t SEND_TASK_COUNT;
  static __thread int64_t SEND_COST_TS;

  for (int64_t i = 0; i < array.count(); i++) {
    ObArchiveSendTask* task = NULL;
    if (NULL == (task = array[i])) {
      break;
    } else if (OB_ARCHIVE_TASK_TYPE_CHECKPOINT == task->task_type_) {
      // skip
    } else {
      SEND_LOG_COUNT += (task->end_log_id_ - task->start_log_id_ + 1);
      SEND_BUF_SIZE += task->get_data_len();
    }
  }
  SEND_TASK_COUNT++;
  SEND_COST_TS += cost_ts;

  if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
    const int64_t total_send_buf_size = SEND_BUF_SIZE;
    const int64_t total_send_log_count = SEND_LOG_COUNT;
    const int64_t avg_log_size = SEND_BUF_SIZE / std::max(SEND_LOG_COUNT, 1L);
    const int64_t total_send_task_count = SEND_TASK_COUNT;
    const int64_t total_send_cost_ts = SEND_COST_TS;
    const int64_t avg_send_task_cost_ts = SEND_COST_TS / std::max(SEND_TASK_COUNT, 1L);
    ARCHIVE_LOG(INFO,
        "archive_sender statistic in 10s",
        K(total_send_buf_size),
        K(total_send_log_count),
        K(avg_log_size),
        K(total_send_task_count),
        K(total_send_cost_ts),
        K(avg_send_task_cost_ts));

    SEND_LOG_COUNT = 0;
    SEND_BUF_SIZE = 0;
    SEND_TASK_COUNT = 0;
    SEND_COST_TS = 0;
  }
}

int ObArchiveSender::build_archive_key_prefix_(const ObPGKey& pg_key, const int64_t incarnation, const int64_t round)
{
  int ret = OB_SUCCESS;
  ObArchivePathUtil util;
  const uint64_t tenant_id = pg_key.get_tenant_id();
  char path[OB_MAX_ARCHIVE_PATH_LENGTH] = {'0'};
  char info[OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH] = {'0'};

  if (OB_FAIL(util.build_archive_key_prefix(
          incarnation, round, tenant_id, OB_MAX_ARCHIVE_PATH_LENGTH, path, OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH, info))) {
    ARCHIVE_LOG(WARN, "build_archive_key_prefix fail", KR(ret), K(pg_key), K(incarnation), K(round));
  } else {
    ObString uri(path);
    ObString storage_info(info);
    ObStorageUtil storage_util(false /*need retry*/);
    if (OB_FAIL(storage_util.mkdir(uri, storage_info))) {
      ARCHIVE_LOG(WARN, "make archive_key dir fail", KR(ret), K(pg_key), K(uri), K(storage_info));
    }
  }

  return ret;
}

int ObArchiveSender::touch_archive_key_file_(const ObPGKey& pg_key, const int64_t incarnation, const int64_t round)
{
  int ret = OB_SUCCESS;
  ObArchivePathUtil util;
  char path[OB_MAX_ARCHIVE_PATH_LENGTH] = {'0'};
  char info[OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH] = {'0'};

  if (OB_FAIL(util.build_archive_key_path(
          pg_key, incarnation, round, OB_MAX_ARCHIVE_PATH_LENGTH, path, OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH, info))) {
    ARCHIVE_LOG(WARN, "build_archive_key_path fail", KR(ret), K(pg_key), K(incarnation), K(round));
  } else {
    ObString uri(path);
    ObString storage_info(info);
    ObArchiveFileUtils file_utils;
    if (OB_FAIL(file_utils.create_file(uri, storage_info))) {
      ARCHIVE_LOG(WARN, "create_file fail", KR(ret), K(uri), K(storage_info));
    }
  }

  return ret;
}

int ObArchiveSender::try_touch_archive_key_(const int64_t incarnation, const int64_t round, ObPGArchiveTask& pg_task)
{
  int ret = OB_SUCCESS;
  bool need_touch = false;
  const ObPGKey& pg_key = pg_task.get_pg_key();

  if (OB_FAIL(pg_task.need_record_archive_key(incarnation, round, need_touch))) {
    ARCHIVE_LOG(WARN, "get archive_key flag fail", KR(ret), K(incarnation), K(round), K(pg_key));
  } else if (!need_touch) {
    // skip it
  } else if (OB_FAIL(build_archive_key_prefix_(pg_key, incarnation, round))) {
    ARCHIVE_LOG(WARN, "build_archive_key_prefix fail", KR(ret), K(incarnation), K(round), K(pg_key));
  } else if (OB_FAIL(touch_archive_key_file_(pg_key, incarnation, round))) {
    ARCHIVE_LOG(WARN, "touch_archive_key_file fail", KR(ret));
  } else {
    ARCHIVE_LOG(DEBUG, "try_touch_archive_key_ succ", KR(ret), K(incarnation), K(round), K(pg_key));
  }

  return ret;
}

int ObArchiveSender::save_server_start_archive_ts(
    const int64_t incarnation, const int64_t round, const int64_t start_ts)
{
  int ret = OB_SUCCESS;
  char* buffer = NULL;
  int64_t pos = 0;
  ObArchivePathUtil util;
  char path[OB_MAX_ARCHIVE_PATH_LENGTH] = {'0'};
  char info[OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH] = {'0'};
  ObArchiveStartTimestamp start_tstamp;
  const int64_t buffer_len = start_tstamp.get_serialize_size();

  if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_ts) || OB_UNLIKELY(0 >= incarnation || 0 >= round)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(start_ts), K(incarnation), K(round));
  } else if (OB_FAIL(start_tstamp.set(round, start_ts))) {
    ARCHIVE_LOG(WARN, "ObArchiveStartTimestamp set fail", KR(ret), K(round), K(start_ts));
  } else if (OB_ISNULL(buffer = static_cast<char*>(ob_archive_malloc(buffer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "allocate memory fail", KR(ret), K(start_ts), K(buffer_len));
  } else if (OB_FAIL(check_and_mk_server_start_ts_dir_(incarnation, round))) {
    ARCHIVE_LOG(WARN, "check_and_mk_server_start_ts_dir_ fail", KR(ret));
  } else if (OB_FAIL(util.build_server_start_archive_path(
                 incarnation, round, OB_MAX_ARCHIVE_PATH_LENGTH, path, OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH, info))) {
    ARCHIVE_LOG(WARN, "build_server_start_archive_path fail", KR(ret), K(path));
  } else if (OB_FAIL(start_tstamp.serialize(buffer, buffer_len, pos))) {
    ARCHIVE_LOG(WARN, "ObArchiveStartTimestamp serialize fail", KR(ret), K(start_tstamp));
  } else {
    ObString uri(path);
    ObString storage_info(info);
    ObStorageUtil storage_util(false /*need retry*/);
    const int64_t start_ts = ObTimeUtility::current_time();
    int64_t interval = 0;
    do {
      if (OB_FAIL(storage_util.write_single_file(uri, storage_info, buffer, buffer_len))) {
        ARCHIVE_LOG(WARN, "write_single_file fail", KR(ret), K(uri), K(storage_info), K(buffer), K(buffer_len));
        interval = ObTimeUtility::current_time() - start_ts;
        usleep(WRITE_RETRY_INTERVAL);
      }
    } while (OB_FAIL(ret) && interval < WRITE_MAX_RETRY_TIME);
  }

  if (NULL != buffer) {
    ob_archive_free(buffer);
    buffer = NULL;
  }
  return ret;
}

int ObArchiveSender::check_and_mk_server_start_ts_dir_(const int64_t incarnation, const int64_t round)
{
  int ret = OB_SUCCESS;
  char prefix[OB_MAX_ARCHIVE_PATH_LENGTH] = {'0'};
  char info[OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH] = {'0'};
  ObArchivePathUtil path_util;

  if (OB_FAIL(path_util.build_server_start_archive_prefix(
          incarnation, round, OB_MAX_ARCHIVE_PATH_LENGTH, prefix, OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH, info))) {
    ARCHIVE_LOG(WARN, "build_server_start_archive_prefix fail", KR(ret), K(incarnation), K(round));
  } else {
    ObString uri(prefix);
    ObString storage_info(info);
    ObStorageUtil util(false /*need retry*/);
    if (uri.prefix_match(OB_OSS_PREFIX)) {
      // skip
    } else if (OB_FAIL(util.mkdir(uri, storage_info))) {
      ARCHIVE_LOG(WARN, "mkdir fail", K(ret), K(uri), K(storage_info));
    }
  }

  return ret;
}

int ObArchiveSender::check_server_start_ts_exist(
    const int64_t incarnation, const int64_t current_round, int64_t& start_ts)
{
  int ret = OB_SUCCESS;
  bool file_exist = false;
  int64_t archive_round = 0;
  int64_t timestamp = OB_INVALID_TIMESTAMP;
  char path[OB_MAX_ARCHIVE_PATH_LENGTH] = {'0'};
  char info[OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH] = {'0'};
  ObArchivePathUtil path_util;

  if (OB_FAIL(path_util.build_server_start_archive_path(
          incarnation, current_round, OB_MAX_ARCHIVE_PATH_LENGTH, path, OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH, info))) {
    ARCHIVE_LOG(WARN, "build_server_start_archive_path fail", KR(ret), K(incarnation), K(current_round), K(path));
  } else {
    ObString uri(path);
    ObString storage_info(info);
    ObArchiveFileUtils util;
    if (OB_FAIL(util.check_file_exist(uri, storage_info, file_exist))) {
      ARCHIVE_LOG(WARN, "check file is_exist fail", KR(ret), K(uri), K(storage_info));
    } else if (!file_exist) {
      ret = OB_ENTRY_NOT_EXIST;
      ARCHIVE_LOG(WARN, "server_start_archive_ts file not exist", KR(ret), K(uri), K(storage_info), K(file_exist));
    } else if (OB_FAIL(util.get_server_start_archive_ts(uri, storage_info, archive_round, timestamp))) {
      ARCHIVE_LOG(WARN, "get_server_start_archive_ts fail", KR(ret), K(uri), K(storage_info), K(current_round));
    } else if (current_round != archive_round) {
      ret = OB_ENTRY_NOT_EXIST;
      ARCHIVE_LOG(WARN,
          "not exist server_start_archive_ts in this archive_round",
          KR(ret),
          K(current_round),
          K(archive_round),
          K(timestamp));
    } else {
      start_ts = timestamp;
    }
  }

  return ret;
}

}  // namespace archive
}  // namespace oceanbase
