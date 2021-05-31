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

#include "ob_archive_clog_split_engine.h"
#include <algorithm>
#include "lib/compress/ob_compressor_pool.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "clog/ob_log_define.h"
#include "clog/ob_external_log_service.h"
#include "storage/transaction/ob_trans_log.h"
#include "ob_ilog_fetcher.h"
#include "ob_archive_pg_mgr.h"
#include "ob_archive_sender.h"
#include "ob_archive_mgr.h"
#include "ob_archive_task_queue.h"
#include "lib/thread/ob_thread_name.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::clog;
using namespace oceanbase::logservice;
using namespace oceanbase::storage;

namespace oceanbase {
namespace archive {

void ObArCLogSplitEngine::ObArchiveSplitStat::reset()
{
  send_task_count_ = 0;
  read_log_used_ = 0;
  read_log_size_ = 0;
  get_send_task_used_ = 0;
}

ObArCLogSplitEngine::ObArCLogSplitEngine()
    : log_archive_round_(-1),
      incarnation_(-1),
      ext_log_service_(NULL),
      archive_sender_(NULL),
      archive_mgr_(NULL),
      archive_pg_mgr_(NULL),
      allocator_()
{}

ObArCLogSplitEngine::~ObArCLogSplitEngine()
{
  destroy();
}

int ObArCLogSplitEngine::init(ObExtLogService* ext_log_service, ObArchiveAllocator* allocator,
    ObArchiveSender* archive_sender, ObArchiveMgr* archive_mgr)
{
  int ret = OB_SUCCESS;
  ObArchivePGMgr* archive_pg_mgr = NULL;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(ERROR, "ObArCLogSplitEngine has been inited twice", K(ret));
  } else if (OB_ISNULL(ext_log_service) || OB_ISNULL(allocator) || OB_ISNULL(archive_sender) ||
             OB_ISNULL(archive_mgr) || OB_ISNULL(archive_pg_mgr = archive_mgr->get_pg_mgr())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid arguments",
        K(ret),
        KP(archive_pg_mgr),
        K(allocator),
        KP(ext_log_service),
        KP(archive_sender),
        KP(archive_mgr));
  } else if (OB_FAIL(ObArchiveThreadPool::init())) {
    ARCHIVE_LOG(WARN, "ObArchiveThreadPool init fail", KR(ret));
  } else {
    round_stop_flag_ = true;
    archive_pg_mgr_ = archive_pg_mgr;
    ext_log_service_ = ext_log_service;
    archive_sender_ = archive_sender;
    archive_mgr_ = archive_mgr;
    allocator_ = allocator;
    inited_ = true;
  }

  ARCHIVE_LOG(INFO, "ObArCLogSplitEngine begin succ");
  return ret;
}

void ObArCLogSplitEngine::destroy()
{
  ObArchiveThreadPool::destroy();

  log_archive_round_ = -1;
  incarnation_ = -1;

  ext_log_service_ = NULL;
  archive_sender_ = NULL;
  archive_mgr_ = NULL;
  archive_pg_mgr_ = NULL;
  allocator_ = NULL;
}

int64_t ObArCLogSplitEngine::cal_work_thread_num()
{
  const int64_t log_archive_concurrency = GCONF.get_log_archive_concurrency();
  const int64_t total_cnt =
      log_archive_concurrency == 0 ? share::OB_MAX_LOG_ARCHIVE_THREAD_NUM : log_archive_concurrency;
  const int64_t normal_thread_num =
      total_cnt % 3 ? ((1 == total_cnt % 3) ? total_cnt / 3 : total_cnt / 3 + 1) : total_cnt / 3;
  const int64_t thread_num = !lib::is_mini_mode() ? std::max(normal_thread_num, 1L) : MINI_MODE_SPLITER_THREAD_NUM;
  return thread_num;
}

void ObArCLogSplitEngine::set_thread_name_str(char* str)
{
  strncpy(str, "ObArcCLogSplitEng", MAX_ARCHIVE_THREAD_NAME_LENGTH);
}

int ObArCLogSplitEngine::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(INFO, "ObArCLogSplitEngine has not been initialized", KR(ret));
  } else if (ObThreadPool::start()) {
    ARCHIVE_LOG(INFO, "start ObArCLogSplitEngine threads fail", KR(ret));
  } else {
    ARCHIVE_LOG(INFO, "start ObArCLogSplitEngine threads succ", KR(ret));
  }

  return ret;
}

void ObArCLogSplitEngine::wait()
{
  ARCHIVE_LOG(INFO, "Archive Clog Split Engine wait");
  ObThreadPool::wait();
}

int ObArCLogSplitEngine::notify_start(const int64_t archive_round, const int64_t incarnation)
{
  int ret = OB_SUCCESS;
  WLockGuard wlock_guard(rwlock_);
  if (OB_UNLIKELY(incarnation < incarnation_ || archive_round <= log_archive_round_)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(
        WARN, "invalid arguments", KR(ret), K(archive_round), K(incarnation), K(log_archive_round_), K(incarnation_));
  } else {
    log_archive_round_ = archive_round;
    incarnation_ = incarnation;
    round_stop_flag_ = false;
    ARCHIVE_LOG(INFO, "Archive Clog Split Engine notify_start", K(incarnation), K(archive_round));
  }
  return ret;
}

int ObArCLogSplitEngine::notify_stop(const int64_t archive_round, const int64_t incarnation)
{
  int ret = OB_SUCCESS;
  WLockGuard wlock_guard(rwlock_);
  if (OB_UNLIKELY(incarnation != incarnation_ || archive_round != log_archive_round_)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(
        WARN, "invalid arguments", KR(ret), K(archive_round), K(incarnation), K(log_archive_round_), K(incarnation_));
  } else {
    round_stop_flag_ = true;
    ARCHIVE_LOG(INFO, "Archive Clog Split Engine notify_stop", K(incarnation), K(archive_round));
  }
  return ret;
}

int ObArCLogSplitEngine::check_current_round_stopped_(
    const ObPGKey& pg_key, const int64_t incarnation, const int64_t archive_round, bool& is_current_round_stopped) const
{
  int ret = OB_SUCCESS;
  is_current_round_stopped = false;
  RLockGuard wlock_guard(rwlock_);
  if (OB_UNLIKELY(incarnation > incarnation_ || archive_round > log_archive_round_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR,
        "archive_round info is invalid",
        KR(ret),
        K(pg_key),
        K(archive_round),
        K(incarnation),
        K(log_archive_round_),
        K(incarnation_));
  } else if (archive_round < log_archive_round_ || incarnation < incarnation_) {
    is_current_round_stopped = true;
  } else {
    is_current_round_stopped = round_stop_flag_;
  }
  return ret;
}

void ObArCLogSplitEngine::clear_archive_info()
{
  // do nothing so far
}

void ObArCLogSplitEngine::stop()
{
  ARCHIVE_LOG(INFO, "Archive Clog Split Engine stop begin");
  ObThreadPool::stop();
  ARCHIVE_LOG(INFO, "Archive Clog Split Engine stop end");
}

void ObArCLogSplitEngine::handle(void* task)
{
  // submit_split_task() guarantees that tasks can not been pushed into queue after split_engine has
  // been inited, so handle do not check whether ptr members are NULL;
  int ret = OB_SUCCESS;
  ObPGArchiveCLogTask* clog_task = static_cast<ObPGArchiveCLogTask*>(task);
  bool is_current_round_stopped = false;
  if (OB_ISNULL(clog_task)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "archive log task is NULL", KP(task));
    on_fatal_error_(ret);
  } else if (has_set_stop()) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ARCHIVE_LOG(INFO, "clog split engine has been stopped", "clog_task", *clog_task, K(ret));
    }
  } else if (OB_UNLIKELY(!clog_task->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "archive log task is invalid", "task", *clog_task);
    mark_fatal_error_(clog_task->pg_key_, clog_task->epoch_id_, clog_task->incarnation_, clog_task->log_archive_round_);
  } else if (OB_FAIL(check_current_round_stopped_(clog_task->pg_key_,
                 clog_task->incarnation_,
                 clog_task->log_archive_round_,
                 is_current_round_stopped))) {
    ARCHIVE_LOG(ERROR, "failed to check_current_round_stopped_", KR(ret), K(log_archive_round_), "task", *clog_task);
    mark_fatal_error_(clog_task->pg_key_, clog_task->epoch_id_, clog_task->incarnation_, clog_task->log_archive_round_);
  } else if (is_current_round_stopped) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ARCHIVE_LOG(INFO, "current round has been stopped", "clog_task", *clog_task, K(ret));
    }
  } else {
    const ObLogArchiveContentType task_type = clog_task->task_type_;
    const bool is_inner_task = clog_task->is_inner_task();
    bool is_expired = false;
    bool need_update_progress = true;
    if (OB_FAIL(archive_pg_mgr_->check_if_task_expired(
            clog_task->pg_key_, clog_task->incarnation_, clog_task->log_archive_round_, is_expired))) {
      ARCHIVE_LOG(ERROR, "failed to check_if_task_expired", KR(ret), "task", *clog_task);
    } else if (is_expired) {
      // just skip
    } else if (is_inner_task) {
      need_update_progress = false;  // checkpoint task may be skipped directly
      if (OB_FAIL(handle_archive_inner_task_(*clog_task, need_update_progress))) {
        if (clog_task->is_archive_checkpoint_task()) {
          // A single cold partition advancement failure does not affect the entire backup,
          // so override ret with OB_SUCCESS
          ARCHIVE_LOG(
              WARN, "failed to handle archive_checkpoint and override with OB_SUCCESS", K(ret), "task", *clog_task);
          ret = OB_SUCCESS;
        } else if (OB_IN_STOP_STATE != ret) {
          ARCHIVE_LOG(ERROR, "failed to handle archive_kickoff task", K(ret), "task", *clog_task);
        }
      }
    } else if (OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT == task_type) {
      ret = handle_archive_log_task_(*clog_task);
      if (OB_IN_STOP_STATE == ret || OB_LOG_ARCHIVE_LEADER_CHANGED == ret) {
        // do not need mark error
      } else if (OB_FILE_RECYCLED == ret) {
        mark_fatal_error_(
            clog_task->pg_key_, clog_task->epoch_id_, clog_task->incarnation_, clog_task->log_archive_round_);
        ARCHIVE_LOG(ERROR, "log missing", K(ret), "task", *clog_task);
      } else if (OB_FAIL(ret)) {
        ARCHIVE_LOG(ERROR, "failed to handle task", K(ret), "task", *clog_task);
        mark_fatal_error_(
            clog_task->pg_key_, clog_task->epoch_id_, clog_task->incarnation_, clog_task->log_archive_round_);
      } else { /*do nothing*/
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "invalid task type", KR(ret), K(task_type), "task", *clog_task);
      mark_fatal_error_(
          clog_task->pg_key_, clog_task->epoch_id_, clog_task->incarnation_, clog_task->log_archive_round_);
    }

    if (OB_SUCC(ret) && need_update_progress &&
        (OB_ARCHIVE_TASK_TYPE_CHECKPOINT == task_type || OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT == task_type)) {
      // only clog_split and archive_checkpoint task need update
      if (OB_FAIL(archive_pg_mgr_->update_clog_split_progress(clog_task))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          ARCHIVE_LOG(ERROR, "failed to update_clog_split_progress_", "clog_task", *clog_task, K(ret));
          mark_fatal_error_(
              clog_task->pg_key_, clog_task->epoch_id_, clog_task->incarnation_, clog_task->log_archive_round_);
        } else {
          ARCHIVE_LOG(WARN, "pg leader may has been revoked, just skip this", "clog_task", *clog_task, K(ret));
          ret = OB_SUCCESS;
        }
      }
    }
  }

  // release clog_task
  if (NULL != clog_task) {
    if (OB_FAIL(release_clog_split_task_(clog_task))) {
      ARCHIVE_LOG(ERROR, "failed to release_clog_split_task_", KR(ret));
    } else {
      ATOMIC_DEC(&total_task_count_);
    }
  }
}

int ObArCLogSplitEngine::release_clog_split_task_(ObPGArchiveCLogTask*& task)
{
  int ret = OB_SUCCESS;
  ObArchiveIlogFetcher* ilog_fetcher = NULL;
  if (NULL != task) {
    // free read_buf
    if ((NULL != task->read_buf_)) {
      op_reclaim_free(task->read_buf_);
      task->read_buf_ = NULL;
    }

    // free task
    if (OB_ISNULL(archive_mgr_) || OB_ISNULL(ilog_fetcher = archive_mgr_->get_ilog_fetcher())) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task or archive_mgr_ or ilog_fetcher is NULL", KP(archive_mgr_), KP(ilog_fetcher));
      mark_fatal_error_(task->pg_key_, task->epoch_id_, task->incarnation_, task->log_archive_round_);
    } else {
      ilog_fetcher->free_clog_split_task(task);
      task = NULL;
    }
  }
  return ret;
}

//=============== start of funcs for external============//
int ObArCLogSplitEngine::submit_split_task(ObPGArchiveCLogTask* split_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "not init", KR(ret));
  } else if (OB_ISNULL(split_task)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "split_task is NULL", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_IN_STOP_STATE;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ARCHIVE_LOG(INFO, "clog split engine has been stopped", KR(ret));
    }
  } else if (OB_UNLIKELY(!split_task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "split_task is invalid", KR(ret), KPC(split_task));
  } else if (OB_FAIL(submit_split_task_(split_task)) && OB_ALLOCATE_MEMORY_FAILED != ret) {
    ARCHIVE_LOG(WARN, "submit_split_task_ fail", KR(ret), KPC(split_task));
  } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
    ARCHIVE_LOG(WARN, "allocate memory fail, rewrite ret and wait retry", KR(ret));
    ret = OB_EAGAIN;
  } else {
    ATOMIC_INC(&total_task_count_);
  }
  return ret;
}

int ObArCLogSplitEngine::submit_split_task_(ObPGArchiveCLogTask* task)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTask* pg_archive_task = NULL;
  const ObPGKey& pg_key = task->pg_key_;
  ObPGArchiveTaskGuard guard(archive_pg_mgr_);

  if (OB_ISNULL(archive_pg_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "archive_pg_mgr_ is NULL", KR(ret));
  } else if (OB_FAIL(archive_pg_mgr_->get_pg_archive_task_guard_with_status(pg_key, guard)) &&
             OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "get_pg_archive_task_guard_with_status fail", KR(ret), K(pg_key), KPC(task));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(INFO, "pg leader has changed, skip it", KR(ret), KPC(task));
  } else if (OB_ISNULL(pg_archive_task = guard.get_pg_archive_task())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "pg_archive_task is NULL", KR(ret), KPC(task));
  } else if (OB_FAIL(pg_archive_task->push_split_task(*task, *this)) && OB_LOG_ARCHIVE_LEADER_CHANGED != ret) {
    ARCHIVE_LOG(WARN, "push_send_task fail", KR(ret), KPC(task));
  } else if (OB_LOG_ARCHIVE_LEADER_CHANGED == ret) {
    ARCHIVE_LOG(INFO, "pg leader has changed, skip it", KR(ret), KPC(task));
  }

  return ret;
}

int ObArCLogSplitEngine::handle_task_list(ObArchiveTaskStatus* status)
{
  int ret = OB_SUCCESS;
  const int64_t task_limit = 5;
  int64_t task_num = 0;
  ObLink* link = NULL;
  bool jump = false;
  bool task_exist = false;
  ObPGArchiveCLogTask* task = NULL;
  ObArchiveCLogTaskStatus* task_status = static_cast<ObArchiveCLogTaskStatus*>(status);

  if (OB_ISNULL(task_status)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(task_status));
  } else {
    while (OB_SUCC(ret) && !jump) {
      if (OB_FAIL(task_status->pop(link, task_exist))) {
        ARCHIVE_LOG(WARN, "ObArchiveCLogTaskStatus pop fail", KR(ret), KPC(task_status));
      } else if (!task_exist) {
        jump = true;
      } else if (OB_ISNULL(link)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "link is NULL", KR(ret), K(link));
      } else {
        task = static_cast<ObPGArchiveCLogTask*>(link);
        handle(task);
        task_num++;
        link = NULL;
        jump = (++task_num) > task_limit;
      }
    }
  }

  if (NULL != task_status) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_retire_task_status_(*task_status))) {
      ARCHIVE_LOG(WARN, "try_retire_task_status_ fail", KR(ret), KPC(task_status));
    }
  }

  return ret;
}

//=============== end of funcs for external============//

//=============== start of private funcs============//

int ObArCLogSplitEngine::try_retire_task_status_(ObArchiveCLogTaskStatus& task_status)
{
  int ret = OB_SUCCESS;
  bool is_queue_empty = false;
  bool is_discarded = false;

  if (OB_FAIL(task_status.retire(is_queue_empty, is_discarded))) {
    ARCHIVE_LOG(ERROR, "task_status retire fail", KR(ret), K(task_status));
  } else if (is_discarded && NULL != allocator_) {
    allocator_->free_clog_task_status(&task_status);
  } else if (!is_queue_empty) {
    if (OB_FAIL(task_queue_.push(&task_status))) {
      ARCHIVE_LOG(WARN, "push fail", KR(ret), K(task_status));
    }
  }

  return ret;
}

int ObArCLogSplitEngine::handle_archive_inner_task_(ObPGArchiveCLogTask& clog_task, bool& need_update_progress)
{
  int ret = OB_SUCCESS;
  need_update_progress = false;
  uint64_t log_id = clog_task.archive_checkpoint_log_id_;
  int64_t log_ts = clog_task.log_submit_ts_;
  int64_t checkpoint_ts = clog_task.checkpoint_ts_;
  uint64_t last_split_log_id = OB_INVALID_ID;
  int64_t last_split_log_submit_ts = OB_INVALID_TIMESTAMP;
  int64_t last_split_checkpoint_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(archive_pg_mgr_->get_clog_split_info(clog_task.pg_key_,
          clog_task.epoch_id_,
          clog_task.incarnation_,
          clog_task.log_archive_round_,
          last_split_log_id,
          last_split_log_submit_ts,
          last_split_checkpoint_ts))) {
    if (OB_ENTRY_NOT_EXIST != ret && OB_LOG_ARCHIVE_LEADER_CHANGED != ret) {
      ARCHIVE_LOG(ERROR, "failed to get_clog_split_info", K(clog_task), K(ret));
    } else {
      // partition archive task is deleted, just skip the task
      ARCHIVE_LOG(WARN, "pg leader may has been revoked, just skip this", K(clog_task), KR(ret));
      ret = OB_SUCCESS;
    }
  } else if (OB_ARCHIVE_TASK_TYPE_CHECKPOINT == clog_task.task_type_ &&
             (last_split_log_id != log_id || log_ts <= last_split_log_submit_ts ||
                 checkpoint_ts <= last_split_checkpoint_ts)) {
    // just skip the checkpoint task
    ARCHIVE_LOG(INFO,
        "just skip the checkpoint task",
        K(clog_task),
        K(last_split_log_id),
        K(last_split_log_submit_ts),
        K(last_split_checkpoint_ts),
        K(ret));
  } else {
    ObArchiveBlockMeta block_meta;
    if (OB_ARCHIVE_TASK_TYPE_KICKOFF == clog_task.task_type_) {
      // clog_epoch_id_ and accum_checksum_ of archive_checkpoint will be set in sender
      block_meta.clog_epoch_id_ = clog_task.clog_epoch_id_;
      block_meta.accum_checksum_ = clog_task.accum_checksum_;
      block_meta.min_log_id_in_file_ = log_id - 1;
      block_meta.min_log_ts_in_file_ = log_ts;
      block_meta.max_log_id_ = log_id - 1;
    } else {
      block_meta.max_log_id_ = log_id;
      clog_task.need_update_log_ts_ = true;
    }

    block_meta.max_checkpoint_ts_ = checkpoint_ts;
    block_meta.max_log_submit_ts_ = log_ts;

    ObLogArchiveInnerLog archive_inner_log;
    archive_inner_log.set_checkpoint_ts(clog_task.checkpoint_ts_);
    // round_start_ts and round_snapshot_version are only valid in kickoff log
    archive_inner_log.set_round_start_ts(clog_task.round_start_ts_);
    archive_inner_log.set_round_snapshot_version(clog_task.round_snapshot_version_);
    ObLogEntryHeader header;
    const int64_t block_meta_size = block_meta.get_serialize_size();
    int64_t header_pos = block_meta_size;
    int64_t log_pos = header_pos + header.get_serialize_size();
    int64_t old_log_pos = log_pos;
    common::ObVersion unused_version = ObVersion(1, 0);
    ObProposalID fake_proposal_id;
    int64_t fake_epoch_id = 0;
    const bool is_trans_log = false;
    ObLogType log_type = clog_task.get_log_type();
    const int64_t buf_size = block_meta_size + header.get_serialize_size() + archive_inner_log.get_serialize_size() +
                             ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE;
    ObArchiveSendTask* send_task = NULL;
    if (clog::ObLogType::OB_LOG_UNKNOWN == log_type) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "unexpected log type", K(clog_task), KR(ret));
    } else if (OB_FAIL(get_send_task_(clog_task, buf_size, send_task))) {
      ARCHIVE_LOG(WARN, "failed to get send_task", KR(ret), K(buf_size), K(clog_task));
    } else if (OB_ISNULL(send_task)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "got send buf is NULL", K(clog_task), KR(ret));
    } else if (OB_FAIL(archive_inner_log.serialize(
                   send_task->buf_, send_task->get_buf_len() - ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE, log_pos))) {
      ARCHIVE_LOG(ERROR, "failed to serialize archive_inner_log", K(clog_task), KR(ret));
    } else if (OB_FAIL(header.generate_header(log_type,  // log_type
                   clog_task.pg_key_,                    // partition_key
                   log_id,
                   send_task->buf_ + old_log_pos,  // buff
                   log_pos - old_log_pos,          // data_len
                   ObClockGenerator::getClock(),   // generation_timestamp
                   fake_epoch_id,                  // epoch_id,
                   fake_proposal_id,               // proposal_id,
                   log_ts,                         // submit_timestamp
                   unused_version,                 // ObVersion
                   is_trans_log))) {
      ARCHIVE_LOG(ERROR, "failed to generate_header", K(clog_task), KR(ret));
    } else if (OB_FAIL(header.serialize(send_task->buf_, old_log_pos, header_pos))) {
      ARCHIVE_LOG(ERROR, "failed to serialize header", K(clog_task), KR(ret));
    } else {
      // data len in an archive block
      send_task->archive_data_len_ = log_pos - block_meta_size;
      // total data buffer include archive block and offset: block_size + sizeof(int32_t)
      send_task->set_data_len(log_pos + ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE);
      send_task->start_log_id_ = log_id;
      send_task->start_log_ts_ = log_ts;
      send_task->end_log_id_ = log_id;
      send_task->end_log_submit_ts_ = log_ts;
      send_task->checkpoint_ts_ = checkpoint_ts;
      send_task->block_meta_ = block_meta;
      if (OB_FAIL((submit_send_task_(send_task)))) {
        if (OB_LOG_ARCHIVE_LEADER_CHANGED == ret) {
          // partition is deleted, just skip it
          ARCHIVE_LOG(WARN, "pg leader may has been revoked, just skip this", K(clog_task), KR(ret));
          ret = OB_SUCCESS;
        } else {
          ARCHIVE_LOG(WARN, "failed to submit send_task", K(clog_task), K(send_task), K(ret));
        }
      } else {
        need_update_progress = (OB_ARCHIVE_TASK_TYPE_CHECKPOINT == clog_task.task_type_);
        ARCHIVE_LOG(TRACE, "succ to submit send_task", K(clog_task), K(send_task), K(ret));
      }
    }

    if (NULL != send_task) {
      release_send_task_(send_task);
      send_task = NULL;
    }
  }

  return ret;
}

int ObArCLogSplitEngine::build_original_block_(ObPGArchiveCLogTask& clog_task)
{
  int ret = OB_SUCCESS;
  ObTSIArchiveReadBuf* tsi_read_buf = NULL;
  if (OB_FAIL(get_tsi_read_buf_(clog_task, tsi_read_buf))) {
    ARCHIVE_LOG(WARN, "failed to get_tsi_read_buf", K(clog_task), KR(ret));
  } else if (OB_ISNULL(tsi_read_buf)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "got tsi_read_buf is NULL ", K(clog_task), KR(ret));
  }

  ObArchiveSplitStat stat;

  const bool need_decrypted_log = false;
  while (OB_SUCC(ret) && !has_set_stop() && !(clog_task.is_finished())) {
    tsi_read_buf->reset_log_related_info();
    ObArchiveBlockMeta& block_meta = tsi_read_buf->block_meta_;
    char* read_buf = tsi_read_buf->get_buf();
    int64_t log_read_buf_len = tsi_read_buf->get_buf_len();
    int64_t read_buf_pos = 0;
    const int64_t before_read_log_ts = ObTimeUtility::fast_current_time();
    if (OB_FAIL(fill_read_buf_(clog_task, read_buf, log_read_buf_len, read_buf_pos, *tsi_read_buf))) {
      ARCHIVE_LOG(WARN, "failed to fill_read_buf", K(clog_task), K(*tsi_read_buf), KR(ret));
    } else if (read_buf_pos > 0) {
      tsi_read_buf->pos_ = read_buf_pos;
      // update clog_task meta
    } else {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "read nothing when clog_task is not finished", K(clog_task), KR(ret));
    }
    const int64_t after_read_log_ts = ObTimeUtility::fast_current_time();
    stat.read_log_used_ += after_read_log_ts - before_read_log_ts;
    stat.read_log_size_ += tsi_read_buf->get_data_len();

    if (OB_SUCC(ret)) {
      ObArchiveSendTask* send_task = NULL;
      const int64_t archive_data_len = tsi_read_buf->get_data_len();
      const int64_t block_meta_size = block_meta.get_serialize_size();
      const int64_t demand_send_buf_size = block_meta_size + archive_data_len + ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE;
      if (OB_FAIL(get_send_task_(clog_task, demand_send_buf_size, send_task))) {
        ARCHIVE_LOG(WARN, "failed to get_send_task_", K(clog_task), KR(ret));
      } else if (OB_ISNULL(send_task)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "got send task is NULL", K(clog_task), KR(ret));
      } else if (OB_FAIL(send_task->assign_meta(*tsi_read_buf))) {
        ARCHIVE_LOG(WARN, "failed to assign send_task with tsi send buf", K(clog_task), KR(ret));
      } else {
        const int64_t send_task_data_len = archive_data_len + block_meta_size + ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE;
        MEMCPY(send_task->get_buf() + block_meta_size, read_buf, read_buf_pos);
        send_task->set_archive_data_len(archive_data_len);
        send_task->set_data_len(send_task_data_len);
        if (OB_FAIL((submit_send_task_(send_task)))) {
          ARCHIVE_LOG(WARN, "failed to init send_task", K(clog_task), K(ret));
        } else {
          send_task = NULL;
        }
      }

      if (NULL != send_task) {
        release_send_task_(send_task);
        send_task = NULL;
      }
    }

    const int64_t after_get_send_task_ts = ObTimeUtility::fast_current_time();
    stat.get_send_task_used_ += (after_get_send_task_ts - after_read_log_ts);
    stat.send_task_count_++;
  }

  statistic(stat);
  return ret;
}

int ObArCLogSplitEngine::get_tsi_read_buf_(const ObPGArchiveCLogTask& clog_task, ObTSIArchiveReadBuf*& read_buf)
{
  int ret = OB_SUCCESS;
  do {
    if (OB_ISNULL(read_buf = GET_TSI(ObTSIArchiveReadBuf))) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        ARCHIVE_LOG(WARN, "failed to get tsi send buf, need retry", K(clog_task), KR(ret));
      }
    } else if (OB_FAIL(read_buf->set_archive_meta_info(clog_task))) {
      ARCHIVE_LOG(ERROR, "failed to set_archive_meta_info", K(clog_task), KR(ret));
    } else { /*do nothing*/
    }

    if (OB_EAGAIN == ret) {
      if (!has_set_stop()) {
        int tmp_ret =
            check_if_task_is_expired_(clog_task.pg_key_, clog_task.incarnation_, clog_task.log_archive_round_);
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
        } else {
          usleep(WAIT_TIME_AFTER_EAGAIN);  // sleep 100us
        }
      }
    }

  } while (OB_EAGAIN == ret && (!has_set_stop()));

  if (OB_EAGAIN == ret && has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }
  return ret;
}

int ObArCLogSplitEngine::get_tsi_compress_buf_(
    const ObPGArchiveCLogTask& clog_task, ObTSIArchiveCompressBuf*& compress_buf)
{
  int ret = OB_SUCCESS;
  do {
    if (OB_ISNULL(compress_buf = GET_TSI(ObTSIArchiveCompressBuf))) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        ARCHIVE_LOG(WARN, "failed to get tsi compress buf, need retry", K(clog_task), KR(ret));
      }
    }

    if (OB_EAGAIN == ret) {
      if (!has_set_stop()) {
        int tmp_ret =
            check_if_task_is_expired_(clog_task.pg_key_, clog_task.incarnation_, clog_task.log_archive_round_);
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
        } else {
          usleep(WAIT_TIME_AFTER_EAGAIN);  // sleep 100us
        }
      }
    }

  } while (OB_EAGAIN == ret && (!has_set_stop()));

  if (OB_EAGAIN == ret && has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }
  return ret;
}

int ObArCLogSplitEngine::get_send_task_(
    const ObPGArchiveCLogTask& clog_task, const int64_t buf_len, ObArchiveSendTask*& send_task)
{
  int ret = OB_SUCCESS;
  do {
    if (OB_FAIL((archive_sender_->get_send_task(buf_len, send_task)))) {
      if (OB_EAGAIN == ret) {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          ARCHIVE_LOG(WARN, "failed to get send buf, need retry", K(buf_len), K(clog_task), K(ret));
        }
      } else if (OB_IN_STOP_STATE != ret) {
        ARCHIVE_LOG(ERROR, "failed to get send buf", K(buf_len), K(clog_task), K(ret));
      }
    } else if (OB_FAIL(send_task->set_archive_meta_info(clog_task))) {
      ARCHIVE_LOG(ERROR, "failed to set_archive_meta_info", K(clog_task), KR(ret));
    } else { /*do nothing*/
    }

    if (OB_EAGAIN == ret) {
      if (!has_set_stop()) {
        int tmp_ret =
            check_if_task_is_expired_(clog_task.pg_key_, clog_task.incarnation_, clog_task.log_archive_round_);
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
        } else {
          usleep(WAIT_TIME_AFTER_EAGAIN);  // sleep 100us
        }
      }
    }
  } while (OB_EAGAIN == ret && (!has_set_stop()));

  if (OB_EAGAIN == ret && has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

void ObArCLogSplitEngine::release_send_task_(ObArchiveSendTask* send_task)
{
  archive_sender_->release_send_task(send_task);
}

int ObArCLogSplitEngine::submit_send_task_(ObArchiveSendTask*& send_task)
{
  // this function should not fail
  int ret = OB_SUCCESS;
  do {
    if (OB_FAIL((archive_sender_->submit_send_task(send_task)))) {
      if (OB_EAGAIN == ret) {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          ARCHIVE_LOG(WARN, "submitting send_task needs retry", K(ret), "send_task", *send_task);
        }

        int tmp_ret =
            check_if_task_is_expired_(send_task->pg_key_, send_task->incarnation_, send_task->log_archive_round_);
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
        } else {
          usleep(WAIT_TIME_AFTER_EAGAIN);  // sleep 100us
        }
      } else if (OB_IN_STOP_STATE != ret && OB_LOG_ARCHIVE_LEADER_CHANGED != ret) {
        ARCHIVE_LOG(ERROR, "failed to submit send task", K(ret), "send_task", *send_task);
      }
    }
  } while (OB_EAGAIN == ret && !has_set_stop());

  if (OB_SUCC(ret)) {
    send_task = NULL;
  }
  if (OB_EAGAIN == ret && has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }
  return ret;
}

int ObArCLogSplitEngine::fill_read_buf_(ObPGArchiveCLogTask& clog_task, char* read_buf, const int64_t read_buf_len,
    int64_t& read_buf_pos, ObArchiveSendTaskMeta& meta)
{
  int ret = OB_SUCCESS;
  const int64_t log_buf_base_pos = read_buf_pos;
  int64_t left_buf_len = read_buf_len - read_buf_pos;
  int64_t base_pos = clog_task.processed_log_count_;
  const int64_t total_log_count = clog_task.get_clog_count();
  bool is_buf_full = false;
  int64_t last_log_pos = read_buf_pos;
  int64_t max_checkpoint_ts = OB_INVALID_TIMESTAMP;
  clog::ObLogEntry log_entry;
  clog::ObLogEntryHeader last_log_entry_header;
  for (; OB_SUCC(ret) && !is_buf_full && base_pos < total_log_count; base_pos++) {
    log_entry.reset();
    int64_t old_read_buf_pos = read_buf_pos;
    ObArchiveLogCursor& log_cursor = clog_task.clog_pos_list_.at(base_pos);
    if (has_set_stop()) {
      ret = OB_IN_STOP_STATE;
    } else if (left_buf_len < log_cursor.size_) {
      is_buf_full = true;
    } else if (OB_UNLIKELY(log_cursor.log_id_ != clog_task.archive_base_log_id_ + base_pos)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "invalid log_curosr", K(ret), K(log_cursor), K(clog_task));
    } else if (OB_FAIL(fetch_log_with_retry_(clog_task, log_cursor, read_buf, read_buf_len, read_buf_pos, log_entry)) &&
               OB_FILE_RECYCLED != ret && OB_BUF_NOT_ENOUGH != ret && OB_IN_STOP_STATE != ret) {
      ARCHIVE_LOG(ERROR, "failed to fetch log", K(ret), K(log_cursor), K(clog_task));
    } else if (OB_IN_STOP_STATE == ret) {
      ARCHIVE_LOG(WARN, "observer is in stop state", K(ret), K(log_cursor), K(clog_task));
    } else if (OB_FILE_RECYCLED == ret) {
      ARCHIVE_LOG(WARN, "failed to fetch log, log has been recycled", K(ret), K(log_cursor), K(clog_task));
      // do nothing
    } else if (OB_BUF_NOT_ENOUGH == ret) {
      // when clog is compressed, buffer not enough may be returned
      is_buf_full = true;
      ret = OB_SUCCESS;
      if (REACH_TIME_INTERVAL(30 * 1000 * 1000)) {
        ARCHIVE_LOG(WARN, "buf is not enough", K(ret), K(log_cursor), K(clog_task));
      }
    } else if (OB_FAIL(last_log_entry_header.shallow_copy(log_entry.get_header()))) {
      ARCHIVE_LOG(WARN, "failed to shallow_copy log_header", K(ret), K(log_entry), K(clog_task), K(meta));
    } else {
      last_log_pos = old_read_buf_pos;
      left_buf_len = read_buf_len - read_buf_pos;
      if (OB_INVALID_ID == meta.start_log_id_) {
        meta.start_log_id_ = log_cursor.log_id_;
        meta.start_log_ts_ = log_cursor.log_submit_ts_;
      }

      // log deserialization for some info, like checkpoint_ts in checkpoint log
      // distinguish and set whether all are nop logs or not
      clog::ObLogType log_type = log_entry.get_header().get_log_type();
      if (!clog::is_nop_or_truncate_log(log_type)) {
        meta.need_update_log_ts_ = true;
        clog_task.need_update_log_ts_ = true;
      }

      if (OB_LOG_SUBMIT == log_type) {
        ObStorageLogType storage_log_type = storage::OB_LOG_UNKNOWN;
        int64_t log_type_in_buf = storage::OB_LOG_UNKNOWN;
        int64_t pos = 0;
        int64_t log_buf_len = log_entry.get_header().get_data_len();
        if (OB_FAIL(serialization::decode_i64(log_entry.get_buf(), log_buf_len, pos, &log_type_in_buf))) {
          ARCHIVE_LOG(WARN, "failed to decode storage_log_type", KR(ret), K(log_entry), K(clog_task), K(meta));
        } else {
          storage_log_type = static_cast<ObStorageLogType>(log_type_in_buf);
          if (OB_LOG_TRANS_CHECKPOINT == storage_log_type) {
            transaction::ObCheckpointLog log;
            if (OB_FAIL(log.deserialize(log_entry.get_buf(), log_buf_len, pos))) {
              ARCHIVE_LOG(WARN, "deserialize checkpoint log failed", KR(ret), K(log_entry), K(clog_task), K(meta));
            } else {
              if (log.get_checkpoint() > max_checkpoint_ts) {
                max_checkpoint_ts = log.get_checkpoint();
              }
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        clog_task.processed_log_count_++;
      }
    }
  }

  if (OB_SUCC(ret)) {
    // set block_meta and SendTaskMeta info
    ObArchiveLogCursor& archive_log_cursor = clog_task.clog_pos_list_.at(clog_task.processed_log_count_ - 1);
    if (OB_UNLIKELY(last_log_entry_header.get_log_id() != archive_log_cursor.log_id_)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "invalid log entry", K(clog_task), K(last_log_entry_header), K(archive_log_cursor), KR(ret));
    } else {
      meta.block_meta_.clog_epoch_id_ = last_log_entry_header.get_epoch_id();
      meta.block_meta_.accum_checksum_ = archive_log_cursor.accum_checksum_;
      meta.block_meta_.max_log_id_ = archive_log_cursor.log_id_;
      meta.block_meta_.max_log_submit_ts_ = archive_log_cursor.get_submit_timestamp();
      meta.end_log_id_ = archive_log_cursor.log_id_;
      meta.end_log_submit_ts_ = archive_log_cursor.get_submit_timestamp();
      if (max_checkpoint_ts > meta.checkpoint_ts_) {
        meta.checkpoint_ts_ = max_checkpoint_ts;
        meta.block_meta_.max_checkpoint_ts_ = max_checkpoint_ts;
        clog_task.checkpoint_ts_ = max_checkpoint_ts;
      }
      clog_task.log_submit_ts_ = archive_log_cursor.get_submit_timestamp();
    }
  }
  return ret;
}

int ObArCLogSplitEngine::fetch_log_with_retry_(const ObPGArchiveCLogTask& clog_task, ObArchiveLogCursor& log_cursor,
    char* read_buf, int64_t read_buf_len, int64_t& read_buf_pos, clog::ObLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  do {
    if (OB_FAIL(fetch_log_(clog_task.pg_key_, log_cursor, read_buf, read_buf_len, read_buf_pos, log_entry))) {
      // handle these two ret here
      if (OB_TIMEOUT == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          ARCHIVE_LOG(WARN, "failed to get tsi send buf, need retry", K(clog_task), KR(ret));
        }
        ret = OB_EAGAIN;
      }
    }

    if (OB_EAGAIN == ret) {
      if (!has_set_stop()) {
        int tmp_ret =
            check_if_task_is_expired_(clog_task.pg_key_, clog_task.incarnation_, clog_task.log_archive_round_);
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
        } else {
          usleep(WAIT_TIME_AFTER_EAGAIN);  // sleep 100us
        }
      }
    }
  } while (OB_EAGAIN == ret && (!has_set_stop()));

  if (OB_EAGAIN == ret && has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }
  return ret;
}

int ObArCLogSplitEngine::fetch_log_(const ObPGKey& pg_key, ObArchiveLogCursor& log_cursor, char* read_buf,
    int64_t read_buf_len, int64_t& read_buf_pos, clog::ObLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  ObReadParam param;
  ObReadBuf rbuf;
  ObReadRes res;
  param.timeout_ = 5 * 1024 * 1024L;  // fetch_log_timeout:5s
  param.file_id_ = log_cursor.file_id_;
  param.offset_ = log_cursor.offset_;
  param.read_len_ = log_cursor.size_;

  rbuf.buf_ = read_buf + read_buf_pos;
  rbuf.buf_len_ = read_buf_len - read_buf_pos;
  res.reset();
  ObReadCost cost;
  if (OB_FAIL(ext_log_service_->archive_fetch_log(pg_key, param, rbuf, res))) {
    if (OB_FILE_RECYCLED != ret && OB_BUF_NOT_ENOUGH != ret) {
      ARCHIVE_LOG(ERROR, "read_data_direct fail", K(ret), K(pg_key), K(param), K(rbuf), K(res));
    }
  } else if (OB_UNLIKELY(res.data_len_ < param.read_len_) || OB_UNLIKELY(rbuf.buf_ != res.buf_)) {
    ret = OB_INVALID_DATA;
    ARCHIVE_LOG(ERROR, "got data is invalid", K(ret), K(pg_key), K(param), K(rbuf), K(res));
  } else {
    if (log_cursor.is_batch_committed()) {
      ObLogEntryHeader header;
      // set submit_timestamp here
      header.set_submit_timestamp(log_cursor.get_submit_timestamp());
      // NOTE: set batch commit flag
      header.set_trans_batch_commit_flag();
      // get submit_timestamp serialization position
      int64_t submit_ts_serialize_pos = header.get_submit_ts_serialize_pos();
      char* submit_ts_buf = rbuf.buf_ + submit_ts_serialize_pos;
      int64_t submit_ts_buf_size = serialization::encoded_length_i64(header.get_submit_timestamp());

      // modify submit_timestamp serialization content
      int64_t serialize_pos = 0;
      if (OB_FAIL(header.serialize_submit_timestamp(submit_ts_buf, submit_ts_buf_size, serialize_pos))) {
        ARCHIVE_LOG(WARN,
            "header serialize_submit_timestamp fail",
            K(ret),
            K(header),
            K(submit_ts_buf_size),
            KP(submit_ts_buf),
            K(serialize_pos));
      }
    }
    read_buf_pos += res.data_len_;
    if (OB_SUCC(ret)) {
      int64_t pos = 0;
      if (OB_FAIL(log_entry.deserialize(res.buf_, res.data_len_, pos))) {
        ARCHIVE_LOG(WARN, "failed to deserialize log_entry", K(ret), K(pg_key), K(log_cursor), K(res));
      }
    }
  }
  return ret;
}

void ObArCLogSplitEngine::on_fatal_error_(int err_ret)
{
  const int64_t err_ts = ObClockGenerator::getClock();
  while (!has_set_stop()) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      ARCHIVE_LOG(ERROR, "fatal error happened", K(err_ret), K(err_ts));
    }
    usleep(1 * 1000 * 1000);  // sleep 1s
  }
}

int ObArCLogSplitEngine::mark_fatal_error_(
    const ObPGKey& pg_key, const int64_t epoch_id, const int64_t incarnation, const int64_t log_archive_round)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((archive_pg_mgr_->mark_fatal_error(pg_key, epoch_id, incarnation, log_archive_round)))) {
    ARCHIVE_LOG(ERROR, "failed to mark_fatal_error", K(pg_key), K(incarnation), K(log_archive_round));
  }
  return ret;
}

void ObArCLogSplitEngine::statistic(const ObArchiveSplitStat& stat)
{
  static __thread int64_t READ_LOG_USED;
  static __thread int64_t GET_SEND_TASK_USED;  // get_and_submit_send_task
  static __thread int64_t READ_LOG_SIZE;
  static __thread int64_t SEND_TASK_COUNT;
  static __thread int64_t SPLIT_TASK_COUNT;

  READ_LOG_USED += stat.read_log_used_;
  GET_SEND_TASK_USED += stat.get_send_task_used_;
  READ_LOG_SIZE += stat.read_log_size_;
  SEND_TASK_COUNT += stat.send_task_count_;
  SPLIT_TASK_COUNT++;

  if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    const int64_t avg_split_task_used = (READ_LOG_USED + GET_SEND_TASK_USED) / (SPLIT_TASK_COUNT + 1);
    const int64_t avg_read_log_used = READ_LOG_USED / (SPLIT_TASK_COUNT + 1);
    const int64_t avg_read_log_size = READ_LOG_SIZE / (SPLIT_TASK_COUNT + 1);
    const int64_t avg_get_send_task_used = GET_SEND_TASK_USED / (SPLIT_TASK_COUNT + 1);
    const int64_t avg_send_task_per_task = SEND_TASK_COUNT / (SPLIT_TASK_COUNT + 1);
    ARCHIVE_LOG(INFO,
        "archive_clog_split_engine statistics",
        K(avg_split_task_used),
        K(avg_read_log_used),
        K(avg_read_log_size),
        K(avg_get_send_task_used),
        K(avg_send_task_per_task),
        "split_task_count",
        SPLIT_TASK_COUNT);
    READ_LOG_USED = 0;
    READ_LOG_SIZE = 0;
    GET_SEND_TASK_USED = 0;
    SEND_TASK_COUNT = 0;
    SPLIT_TASK_COUNT = 0;
  }
}

int ObArCLogSplitEngine::handle_archive_log_task_(ObPGArchiveCLogTask& clog_task)
{
  int ret = OB_SUCCESS;
  bool need_compress = false;
  ObCompressorType compressor_type = INVALID_COMPRESSOR;
  // TODO: Map to cache tenant compress and encryption info?
  if (OB_FAIL(get_compress_config_(clog_task, need_compress, compressor_type))) {
    ARCHIVE_LOG(WARN, "failed to get_compress_config_", K(clog_task), KR(ret));
  } else if (need_compress) {
    ret = build_compressed_block_(clog_task, compressor_type);
  } else {
    ret = build_original_block_(clog_task);
  }

  if (OB_FAIL(ret)) {
    ARCHIVE_LOG(WARN, "failed to handle_archive_log_task_", KR(ret), K(need_compress), K(compressor_type));
  } else if (clog_task.is_finished()) {
    // do nothing
  } else if (has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  } else { /*do nothing*/
  }
  return ret;
}

int ObArCLogSplitEngine::build_compressed_block_(
    ObPGArchiveCLogTask& clog_task, const common::ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  ObTSIArchiveReadBuf* tsi_read_buf = NULL;
  ObTSIArchiveCompressBuf* tsi_compress_buf = NULL;
  ObCompressor* compressor = NULL;
  if (OB_FAIL(get_tsi_read_buf_(clog_task, tsi_read_buf))) {
    ARCHIVE_LOG(WARN, "failed to get_tsi_read_buf", K(clog_task), KR(ret));
  } else if (OB_FAIL(get_tsi_compress_buf_(clog_task, tsi_compress_buf))) {
    ARCHIVE_LOG(WARN, "failed to get_tsi_compress_buf", K(clog_task), KR(ret));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_type, compressor))) {
    ARCHIVE_LOG(ERROR, "failed to get compressor", KR(ret), K(clog_task));
  } else if (OB_ISNULL(tsi_read_buf) || OB_ISNULL(tsi_compress_buf) || OB_ISNULL(compressor)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(
        ERROR, "got buf is NULL ", KP(tsi_read_buf), KP(tsi_compress_buf), KP(compressor), K(clog_task), KR(ret));
  } else { /*do nothing*/
  }

  ObArchiveSplitStat stat;
  while (OB_SUCC(ret) && !has_set_stop() && !(clog_task.is_finished())) {
    tsi_read_buf->reset_log_related_info();
    if (OB_FAIL(build_single_compressed_block_(
            clog_task, tsi_read_buf, tsi_compress_buf, compressor_type, compressor, stat))) {
      ARCHIVE_LOG(WARN, "failed to build_single_compressed_block_", KR(ret), K(clog_task));
    }
  }

  statistic(stat);
  return ret;
}

int ObArCLogSplitEngine::check_if_task_is_expired_(
    const ObPartitionKey& pkey, const int64_t incarnation, const int64_t log_archive_round) const
{
  int ret = OB_SUCCESS;
  // check if current round has been stopped
  bool is_current_round_stopped = false;
  if (OB_FAIL(check_current_round_stopped_(pkey, incarnation, log_archive_round, is_current_round_stopped))) {
    ARCHIVE_LOG(
        ERROR, "failed to check_current_round_stopped_", KR(ret), K(pkey), K(incarnation), K(log_archive_round));
  } else if (is_current_round_stopped) {
    ret = OB_IN_STOP_STATE;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ARCHIVE_LOG(WARN, "current_round has been stopped", KR(ret), K(pkey), K(incarnation), K(log_archive_round));
    }
  }
  return ret;
}

int ObArCLogSplitEngine::get_compress_config_(
    const ObPGArchiveCLogTask& clog_task, bool& need_compress, ObCompressorType& compressor_type) const
{
  int ret = OB_SUCCESS;
  UNUSED(clog_task);
  need_compress = GCONF.backup_log_archive_option.need_compress();
  if (need_compress) {
    compressor_type = GCONF.backup_log_archive_option.get_compressor_type();
    need_compress = ObCompressorPool::need_common_compress(compressor_type);
  }

  return ret;
}

int ObArCLogSplitEngine::get_chunk_header_serialize_size_(
    const ObPartitionKey& pg_key, const bool need_compress, int64_t& chunk_header_size)
{
  int ret = OB_SUCCESS;
  chunk_header_size = 0;
  if (need_compress) {
    ObArchiveCompressedChunkHeader compressed_chunk_header;
    chunk_header_size = compressed_chunk_header.get_serialize_size();
  } else {
    chunk_header_size = 0;
  }

  if (OB_FAIL(ret)) {
    ARCHIVE_LOG(WARN, "failed to get_chunk_header_serialize_size", KR(ret), K(need_compress), K(pg_key));
  }
  return ret;
}

int ObArCLogSplitEngine::build_single_compressed_block_(ObPGArchiveCLogTask& clog_task,
    ObTSIArchiveReadBuf* tsi_read_buf, ObTSIArchiveCompressBuf* tsi_compress_buf,
    const ObCompressorType compressor_type, ObCompressor* compressor, ObArchiveSplitStat& stat)

{
  int ret = OB_SUCCESS;
  char* read_buf = tsi_read_buf->get_buf();
  int64_t log_read_buf_len = tsi_read_buf->get_buf_len();
  int64_t read_buf_pos = 0;
  const int64_t before_read_log_ts = ObTimeUtility::fast_current_time();

  if (OB_FAIL(fill_read_buf_(clog_task, read_buf, log_read_buf_len, read_buf_pos, *tsi_read_buf))) {
    ARCHIVE_LOG(WARN, "failed to fill_read_buf", K(clog_task), K(*tsi_read_buf), KR(ret));
  } else if (read_buf_pos > 0) {
    tsi_read_buf->pos_ = read_buf_pos;
  } else {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "read nothing when clog_task is not finished", K(clog_task), KR(ret));
  }
  const int64_t after_read_log_ts = ObTimeUtility::fast_current_time();
  stat.read_log_used_ += after_read_log_ts - before_read_log_ts;
  stat.read_log_size_ += tsi_read_buf->get_data_len();
  bool has_compressed = false;
  int64_t compressed_data_len = 0;
  // 1. compress data
  if (OB_SUCC(ret)) {
    if (OB_FAIL(compressor->compress(read_buf,
            read_buf_pos,
            tsi_compress_buf->get_buf(),
            tsi_compress_buf->get_buf_len(),
            compressed_data_len))) {
      ARCHIVE_LOG(ERROR, "failed to compress data", KR(ret), K(clog_task));
    } else if (compressed_data_len < read_buf_pos) {
      has_compressed = true;
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    ObArchiveBlockMeta& block_meta = tsi_read_buf->block_meta_;
    const int64_t block_meta_size = block_meta.get_serialize_size();
    int64_t chunk_header_size = 0;
    if (OB_FAIL(get_chunk_header_serialize_size_(clog_task.pg_key_, has_compressed, chunk_header_size))) {
      ARCHIVE_LOG(ERROR, "failed to get_chunk_header_serialize_size_", KR(ret), K(clog_task));
    } else {
      const int64_t total_header_size = block_meta_size + chunk_header_size;
      ObArchiveSendTask* send_task = NULL;
      // guarantee buf_size enough for all situations
      const int64_t buf_size = total_header_size + tsi_read_buf->get_data_len() + ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE;
      if (OB_FAIL(get_send_task_(clog_task, buf_size, send_task))) {
        ARCHIVE_LOG(WARN, "failed to get_send_task_", K(clog_task), KR(ret));
      } else if (OB_ISNULL(send_task)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "got send task is NULL", K(clog_task), KR(ret));
      } else if (OB_FAIL(send_task->assign_meta(*tsi_read_buf))) {
        ARCHIVE_LOG(WARN, "failed to assign send_task meta with tsi send buf", K(clog_task), KR(ret));
      } else {
        const int64_t after_get_send_task_ts = ObTimeUtility::fast_current_time();
        stat.get_send_task_used_ += (after_get_send_task_ts - after_read_log_ts);
        stat.send_task_count_++;

        if (OB_SUCC(ret)) {
          if (OB_FAIL(fill_compressed_block_(clog_task,
                  tsi_read_buf,
                  tsi_compress_buf,
                  block_meta_size,
                  chunk_header_size,
                  compressor_type,
                  has_compressed,
                  read_buf_pos,
                  compressed_data_len,
                  send_task))) {
            ARCHIVE_LOG(WARN, "failed to fill_compressed_block", K(clog_task), KR(ret));
          } else if (OB_FAIL(submit_send_task_(send_task))) {
            ARCHIVE_LOG(WARN, "failed to submit_send_task", K(clog_task), K(ret));
          } else {
            send_task = NULL;
          }
        }
      }

      if (NULL != send_task) {
        release_send_task_(send_task);
        send_task = NULL;
      }
    }
  }

  return ret;
}

int ObArCLogSplitEngine::fill_compressed_block_(const ObPGArchiveCLogTask& clog_task,
    const ObTSIArchiveReadBuf* tsi_read_buf, const ObTSIArchiveCompressBuf* tsi_compress_buf,
    const int64_t block_meta_size, const int64_t chunk_header_size, const ObCompressorType compressor_type,
    const bool has_compressed, const int64_t orig_data_len, const int64_t compressed_data_len,
    ObArchiveSendTask* send_task)
{
  int ret = OB_SUCCESS;
  int64_t chunk_header_pos = block_meta_size;
  int64_t archive_data_len = 0;
  if (has_compressed) {
    ObArchiveCompressedChunkHeader compressed_chunk_header;
    if (OB_FAIL(compressed_chunk_header.generate_header(compressor_type, orig_data_len, compressed_data_len))) {
      ARCHIVE_LOG(ERROR, "failed to generate_header", KR(ret), K(clog_task), KPC(send_task));
    } else if (OB_FAIL(compressed_chunk_header.serialize(
                   send_task->get_buf(), send_task->get_buf_len(), chunk_header_pos))) {
      ARCHIVE_LOG(WARN, "failed to serialize chunk_header", K(clog_task), K(compressed_chunk_header), KR(ret));
    } else {
      archive_data_len = compressed_data_len + chunk_header_size;
      // TODO:maybe this memory could be saved
      MEMCPY(
          send_task->get_buf() + block_meta_size + chunk_header_size, tsi_compress_buf->get_buf(), compressed_data_len);
    }
  } else {
    archive_data_len = tsi_read_buf->get_data_len();
    MEMCPY(send_task->get_buf() + block_meta_size, tsi_read_buf->get_buf(), orig_data_len);
  }

  if (OB_SUCC(ret)) {
    const int64_t send_task_data_len = block_meta_size + archive_data_len + ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE;
    send_task->set_archive_data_len(archive_data_len);
    send_task->set_data_len(send_task_data_len);
  }
  return ret;
}

//=============== end of private funcs============//
}  // end of namespace archive
}  // namespace oceanbase
