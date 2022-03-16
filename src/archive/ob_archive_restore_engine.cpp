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

#define USING_LOG_PREFIX RESTORE_ARCHIVE
#include "ob_archive_restore_engine.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_small_allocator.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "share/ob_tenant_mgr.h"
#include "storage/ob_pg_storage.h"
#include "storage/ob_storage_log_type.h"
#include "storage/ob_partition_service.h"
#include "clog/ob_partition_log_service.h"
#include "ob_log_archive_struct.h"
#include "ob_archive_entry_iterator.h"
#include "ob_archive_util.h"  // is_io_error
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::storage;
using namespace oceanbase::clog;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;
using namespace oceanbase::transaction;

namespace oceanbase {
namespace archive {
using oceanbase::clog::ObLogEntry;

void ObPhysicalRestoreCLogStat::reset()
{
  retry_sleep_interval_ = 0;
  fetch_log_entry_count_ = 0;
  fetch_log_entry_cost_ = 0;
  io_cost_ = 0;
  io_count_ = 0;
  limit_bandwidth_cost_ = 0;
  total_cost_ = 0;
}

void ObPGArchiveRestoreTask::reset()
{
  restore_pg_key_.reset();
  archive_pg_key_.reset();
  is_expired_ = false;
  has_located_log_ = false;
  start_log_id_ = OB_INVALID_ID;
  start_log_ts_ = OB_INVALID_TIMESTAMP;
  end_snapshot_version_ = OB_INVALID_TIMESTAMP;
  leader_takeover_ts_ = OB_INVALID_TIMESTAMP;
  last_fetched_log_id_ = OB_INVALID_ID;
  last_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  last_fetched_log_submit_ts_ = OB_INVALID_TIMESTAMP;

  total_piece_cnt_ = 0;
  start_piece_idx_ = -1;
  cur_piece_idx_ = -1;
  end_piece_idx_ = -1;
  start_file_id_in_cur_piece_ = OB_INVALID_ARCHIVE_FILE_ID;
  end_file_id_in_cur_piece_ = OB_INVALID_ARCHIVE_FILE_ID;
  cur_file_id_in_cur_piece_ = OB_INVALID_ARCHIVE_FILE_ID;
  cur_file_offset_ = 0;
  cur_file_store_ = NULL;
  tenant_restore_meta_ = NULL;

  retry_cnt_ = 0;
  io_fail_cnt_ = 0;
  fetch_log_result_ = OB_ARCHIVE_FETCH_LOG_INIT;
  stat_.reset();
  file_store_ = NULL;
}

int ObPGArchiveRestoreTask::switch_file()
{
  // this func may return OB_IO_ERROR,
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("task not init", K(ret));
  } else {
    const uint64_t next_file_id = cur_file_id_in_cur_piece_ + 1;
    const int64_t next_piece_idx = cur_piece_idx_ + 1;
    if (next_file_id > end_file_id_in_cur_piece_ && next_piece_idx > end_piece_idx_) {
      ret = OB_ITER_END;
    } else if (next_file_id <= end_file_id_in_cur_piece_) {
      // switch file in the same piece
      cur_file_id_in_cur_piece_ = next_file_id;
      cur_file_offset_ = 0;
    } else {
      // switch piece
      int64_t target_piece_idx = next_piece_idx;
      uint64_t min_file_id = OB_INVALID_ARCHIVE_FILE_ID;
      uint64_t max_file_id = OB_INVALID_ARCHIVE_FILE_ID;
      ObArchiveLogFileStore* file_store = NULL;
      do {
        if (OB_FAIL(tenant_restore_meta_->get_pg_piece_file_store_info(
                archive_pg_key_, target_piece_idx, min_file_id, max_file_id, file_store))) {
          LOG_WARN("failed to get_pg_piece_file_store_info", KPC(this), K(ret));
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("this piece has no archive log files", KPC(this), K(target_piece_idx));
            if (target_piece_idx < end_piece_idx_) {
              target_piece_idx++;
              ret = OB_NEED_RETRY;
            } else {
              ret = OB_ITER_END;
            }
          }
        } else {
          cur_piece_idx_ = target_piece_idx;
          start_file_id_in_cur_piece_ = min_file_id;
          cur_file_id_in_cur_piece_ = min_file_id;
          end_file_id_in_cur_piece_ = max_file_id;
          cur_file_offset_ = 0;
          cur_file_store_ = file_store;
        }
      } while (OB_NEED_RETRY == ret);
    }
  }
  return ret;
}

bool ObPGArchiveRestoreTask::is_finished() const
{
  return (OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH == fetch_log_result_ ||
          OB_ARCHIVE_FETCH_LOG_FINISH_AND_NOT_ENOUGH == fetch_log_result_);
}

int ObPGArchiveRestoreTask::reconfirm_fetch_log_result()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("task is not inited", KR(ret));
  } else if (last_checkpoint_ts_ >= end_snapshot_version_) {
    // 1. restore has already exceeded restore_point, ENOUGH
    fetch_log_result_ = OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH;
    LOG_INFO("hot partition, self log checkpoint_ts bigger than restore point", KPC(this));
  } else if (start_log_id_ > last_fetched_log_id_) {
    // 2. has not iteratored any data, find and compare max_archived_info to start_log_id_
    uint64_t max_archived_log_id = OB_INVALID_ID;
    int64_t max_archived_checkpoint_ts = OB_INVALID_TIMESTAMP;
    const uint64_t real_tenant_id = restore_pg_key_.get_tenant_id();
    int64_t unused_max_log_ts = OB_INVALID_TIMESTAMP;
    if (should_fetched_log_()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("gap exist from max_archived_log and restore start_log", K(max_archived_log_id), KPC(this));
    } else if (OB_FAIL(cur_file_store_->get_pg_max_archived_info(archive_pg_key_,
                   real_tenant_id,
                   max_archived_log_id,
                   max_archived_checkpoint_ts,
                   unused_max_log_ts))) {
      LOG_WARN("get_pg_max_archived_info fail", KR(ret), KPC(this));
    } else if (max_archived_log_id < start_log_id_ - 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("gap exist from max_archived_log and restore start_log", K(max_archived_log_id), KPC(this));
    } else {
      fetch_log_result_ = OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH;
      LOG_INFO("cold partition, self log checkpoint_ts smaller than restore point", KPC(this));
    }
  }
  // 3. some data fetched, but checkpoint not crossed restore point
  else {
    LOG_INFO("cold partition, self log checkpoint_ts smaller than restore point", KPC(this));
    fetch_log_result_ = OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH;
  }
  return ret;
}

int ObPGArchiveRestoreTask::check_and_locate_start_log_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited");
  } else if (has_located_log_) {
    // just skip
  } else {
    int64_t total_piece_cnt = 0;
    int64_t start_piece_idx = -1;
    int64_t end_piece_idx = -1;
    uint64_t start_file_id = OB_INVALID_ARCHIVE_FILE_ID;
    uint64_t target_file_id = OB_INVALID_ARCHIVE_FILE_ID;
    uint64_t end_file_id = OB_INVALID_ARCHIVE_FILE_ID;
    ObArchiveLogFileStore* file_store = NULL;
    if (OB_FAIL(tenant_restore_meta_->locate_log(restore_pg_key_,
            archive_pg_key_,
            start_log_id_,
            file_store,
            total_piece_cnt,
            start_piece_idx,
            end_piece_idx,
            target_file_id,
            start_file_id,
            end_file_id)) &&
        OB_ITER_END != ret) {
      LOG_WARN("failed to locate start_log_info", K(ret), K(*this));
    } else if (OB_ITER_END == ret) {
      if (OB_ISNULL(file_store) || OB_UNLIKELY(start_piece_idx < 0 || end_piece_idx < 0 || total_piece_cnt < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid piece info",
            K(ret),
            KP(file_store),
            K(start_piece_idx),
            K(end_piece_idx),
            K(total_piece_cnt),
            K(*this));
      } else {
        LOG_INFO("cold partition, no need iterator log", K(*this));
        ret = OB_SUCCESS;
        total_piece_cnt_ = total_piece_cnt;
        start_piece_idx_ = start_piece_idx;
        cur_piece_idx_ = start_piece_idx;
        end_piece_idx_ = end_piece_idx;
        has_located_log_ = true;
        fetch_log_result_ = OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH;
      }
    } else if (OB_ISNULL(file_store) ||
               OB_UNLIKELY(start_piece_idx < 0 || end_piece_idx < 0 || total_piece_cnt < 1 ||
                           OB_INVALID_ARCHIVE_FILE_ID == target_file_id ||
                           OB_INVALID_ARCHIVE_FILE_ID == start_file_id || OB_INVALID_ARCHIVE_FILE_ID == end_file_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid log location info",
          K(ret),
          KP(file_store),
          K(start_piece_idx),
          K(end_piece_idx),
          K(total_piece_cnt),
          K(target_file_id),
          K(start_file_id),
          K(end_file_id),
          K(*this));
    } else {
      total_piece_cnt_ = total_piece_cnt;
      start_piece_idx_ = start_piece_idx;
      cur_piece_idx_ = start_piece_idx;
      end_piece_idx_ = end_piece_idx;
      cur_file_id_in_cur_piece_ = target_file_id;
      start_file_id_in_cur_piece_ = start_file_id;
      end_file_id_in_cur_piece_ = end_file_id;
      cur_file_store_ = file_store;
      has_located_log_ = true;
      LOG_INFO("success to locate start_log_info", K(*this));
    }
  }

  return ret;
}

int ObPGArchiveRestoreTask::init_basic_info(const ObPGKey& restore_pg_key, const ObPGKey& archive_pg_key,
    ObTenantPhysicalRestoreMeta* tenant_restore_meta, uint64_t start_log_id, int64_t start_log_ts,
    int64_t snapshot_version, int64_t leader_takeover_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!restore_pg_key.is_valid() || !archive_pg_key.is_valid() || OB_ISNULL(tenant_restore_meta) ||
                  start_log_id <= 0 || snapshot_version <= 0 || leader_takeover_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to init task",
        K(ret),
        K(restore_pg_key),
        K(archive_pg_key),
        KP(tenant_restore_meta),
        K(start_log_id),
        K(snapshot_version),
        K(leader_takeover_ts));
  } else {
    is_expired_ = false;
    has_located_log_ = false;

    start_log_id_ = start_log_id;
    start_log_ts_ = start_log_ts;
    end_snapshot_version_ = snapshot_version;
    leader_takeover_ts_ = leader_takeover_ts;
    last_fetched_log_id_ = start_log_id_ - 1;
    last_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
    last_fetched_log_submit_ts_ = start_log_ts - 1;
    fetch_log_result_ = OB_ARCHIVE_FETCH_LOG_INIT;

    tenant_restore_meta_ = tenant_restore_meta;

    retry_cnt_ = 0;
    io_fail_cnt_ = 0;
    restore_pg_key_ = restore_pg_key;
    archive_pg_key_ = archive_pg_key;
    is_inited_ = true;
  }
  return ret;
}

void ObPGArchiveRestoreTask::record_io_stat(const ObPhysicalRestoreCLogStat& stat)
{
  if (stat.io_count_ > 0) {
    stat_.fetch_log_entry_count_ += stat.fetch_log_entry_count_;  // iterated logs count
    stat_.fetch_log_entry_cost_ += stat.fetch_log_entry_cost_;    // iterated logs cost
    stat_.io_cost_ += stat.io_cost_;                              // total io cost
    stat_.io_count_ += stat.io_count_;                            // total io count
  }
}

void ObPGArchiveRestoreTask::report_io_stat()
{
  if (stat_.io_count_ > 0) {
    const int64_t io_total_size = ObArchiveEntryIterator::MAX_READ_BUF_SIZE * stat_.io_count_;
    const uint64_t tenant_id = restore_pg_key_.get_tenant_id();
    SERVER_EVENT_ADD("clog_restore",
        "report_io_stat",
        "tenant_id",
        tenant_id,
        "partition",
        restore_pg_key_,
        "fetch_log_entry_count",
        stat_.fetch_log_entry_count_,
        "io_cost",
        stat_.io_cost_,
        "io_count",
        stat_.io_count_,
        "io_total_size",
        io_total_size);
  }
}

int ObPGArchiveRestoreTask::set_last_fetched_log_info(
    const clog::ObLogType log_type, const uint64_t log_id, const int64_t checkpoint_ts, const int64_t log_submit_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((!clog::is_nop_or_truncate_log(log_type) && log_submit_ts <= last_fetched_log_submit_ts_) ||
                  (!is_archive_kickoff_log(log_type) && log_id <= last_fetched_log_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(log_type), K(log_id), K(log_submit_ts), KPC(this));
  } else {
    last_fetched_log_id_ = log_id;
    if (!clog::is_nop_or_truncate_log(log_type)) {
      last_fetched_log_submit_ts_ = log_submit_ts;
    }

    if (checkpoint_ts > last_checkpoint_ts_) {
      last_checkpoint_ts_ = checkpoint_ts;
    }
  }
  return ret;
}

bool ObPGArchiveRestoreTask::is_dropped_pg_() const
{
  // end_piece_idx_ is neither the last piece nor secord to last
  return 0 <= end_piece_idx_ && total_piece_cnt_ > 1 && end_piece_idx_ < total_piece_cnt_ - 2;
}

bool ObPGArchiveRestoreTask::should_fetched_log_() const
{
  // in this situation, log entry with log_id >= start_log_id_ exist,
  // need guarantee last_fetched_log_id_ >= start_log_id_ after fetching all logs
  /*
   * 1. end_piece_idx_ > start_piece_idx_: max_archived_log_id of archive_pg_key in end_piece must be
  greater than max_archived_log_id of archive_pg_key in start_piece, that means archived log exist
  in start_piece.
  */
  return 0 <= start_piece_idx_ && (end_piece_idx_ > start_piece_idx_);
}

/*
int ObPGArchiveRestoreTask::reconfirm_fetch_log_result()
{
  int ret = OB_SUCCESS;
  uint64_t max_archived_log_id = OB_INVALID_ID;
  int64_t max_archived_checkpoint_ts = OB_INVALID_TIMESTAMP;
  int64_t unused_max_log_ts = OB_INVALID_TIMESTAMP;
  if (OB_ISNULL(file_store_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("task is not inited", KR(ret));
  } else if (OB_FAIL(file_store_->get_pg_max_archived_info(archive_pg_key_,
                                                           max_archived_log_id,
                                                           max_archived_checkpoint_ts,
                                                           unused_max_log_ts))) {
    LOG_WARN("failed to get_max_archived_info", KR(ret), K(restore_pg_key_),
                K(archive_pg_key_), K(start_log_id_), K(start_log_ts_),
                K(end_snapshot_version_), K(leader_takeover_ts_));
  } else if (last_fetched_log_id_ < max_archived_log_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("has not fetched all log", KPC(this),
                K(max_archived_log_id), K(max_archived_checkpoint_ts));
  } else if (max_archived_checkpoint_ts >= end_snapshot_version_) {
    LOG_INFO("ATTENTION!!!!: checkpoint log is smaller than last_fetched_log_id"
                "when checkpoint_ts is no smaller than end_snapshot_version",
                KPC(this), K(max_archived_log_id), K(max_archived_checkpoint_ts));
    set_fetch_log_result(OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH);
  } else {
    set_fetch_log_result(OB_ARCHIVE_FETCH_LOG_FINISH_AND_NOT_ENOUGH);
  }
  return ret;
}
*/
int ObArchiveRestoreEngine::init(ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  const int64_t TASK_OBJ_SIZE = sizeof(ObPGArchiveRestoreTask);
  const int64_t RESTORE_INFO_OBJ_SIZE = sizeof(ObTenantPhysicalRestoreMeta);
  const int64_t TASK_BLOCK_SIZE = 8 * 1024LL;           // 8K
  const int64_t RESOTRE_INFO_BLOCK_SIZE = 32 * 1024LL;  // 32K
  const uint64_t ALLOCATOR_TENANT_ID = OB_SERVER_TENANT_ID;
  int64_t thread_num = !lib::is_mini_mode() ? GCONF.get_log_restore_concurrency() : MINI_MODE_RESTORE_THREAD_NUM;

  if (OB_ISNULL(partition_service) || OB_UNLIKELY(thread_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(partition_service), K(thread_num));
  } else if (OB_FAIL(task_allocator_.init(TASK_OBJ_SIZE, "AR_task", ALLOCATOR_TENANT_ID, TASK_BLOCK_SIZE))) {
    LOG_WARN("failed to init task allocator", KR(ret));
  } else if (OB_FAIL(restore_meta_allocator_.init(
                 RESTORE_INFO_OBJ_SIZE, "AR_info", ALLOCATOR_TENANT_ID, RESOTRE_INFO_BLOCK_SIZE))) {
    LOG_WARN("failed to init task allocator", KR(ret));
  } else if (OB_FAIL(restore_meta_map_.init("AR_map", ALLOCATOR_TENANT_ID))) {
    LOG_WARN("failed to init task allocator", KR(ret));
  } else if (OB_FAIL(ObSimpleThreadPool::init(thread_num, TASK_NUM_LIMIT, "AR_Threads"))) {
    LOG_WARN("failed to init thread pool", KR(ret));
  } else {
    partition_service_ = partition_service;
    is_inited_ = true;
    is_stopped_ = false;
    LOG_INFO("success to init ObArchiveRestoreEngine", K(thread_num), K(TASK_OBJ_SIZE), K(RESTORE_INFO_OBJ_SIZE));
  }
  return ret;
}

void ObArchiveRestoreEngine::stop()
{
  LOG_INFO("Archive Restore Engine stop begin");
  is_stopped_ = true;
  LOG_INFO("Archive Restore Engine stop finish");
  return;
}

void ObArchiveRestoreEngine::wait()
{
  LOG_INFO(" wait begin");
  while (get_queue_num() > 0 || (!task_queue_.is_empty())) {
    PAUSE();
  }
  LOG_INFO("Archive Restore Engine SimpleQueue empty");
  ObSimpleThreadPool::destroy();
  LOG_INFO("Archive Restore Engine thread pool destroy finish");
  LOG_INFO("Archive Restore Engine wait finish");
  return;
}

void ObArchiveRestoreEngine::destroy()
{
  is_inited_ = false;
  partition_service_ = NULL;
  restore_meta_map_.destroy();
  task_allocator_.destroy();
  restore_meta_allocator_.destroy();
}

int ObArchiveRestoreEngine::submit_restore_task(
    common::ObPGKey& pg_key, uint64_t start_log_id, int64_t start_log_ts, int64_t leader_takeover_ts)
{
  int ret = OB_SUCCESS;
  ObPGArchiveRestoreTask* task = NULL;
  ObTenantPhysicalRestoreMeta* restore_meta = NULL;
  char* buf = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("archive restore engine is not inited",
        KR(ret),
        K(pg_key),
        K(start_log_id),
        K(start_log_ts),
        K(leader_takeover_ts));
  } else if (OB_UNLIKELY(!pg_key.is_valid() || OB_INVALID_ID == start_log_id || OB_INVALID_TIMESTAMP == start_log_ts ||
                         OB_INVALID_TIMESTAMP == leader_takeover_ts)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(pg_key), K(start_log_id), K(start_log_ts), K(leader_takeover_ts));
  } else if (is_stopped_) {
    ret = OB_IN_STOP_STATE;
    if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
      LOG_WARN("archive restore engine has been stopeed", K(pg_key), K(start_log_id), KR(ret));
    }
  } else if (OB_FAIL(get_tenant_restore_meta_(pg_key.get_tenant_id(), restore_meta))) {
    LOG_WARN("failed to get_tenant_restore_meta", KR(ret), K(pg_key), K(start_log_id), K(leader_takeover_ts));
  } else if (OB_ISNULL(restore_meta) || OB_UNLIKELY(!restore_meta->is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("restore_meta is NULL or not inited",
        KR(ret),
        K(pg_key),
        K(start_log_id),
        K(leader_takeover_ts),
        KP(restore_meta));
  } else if (OB_UNLIKELY(NULL == (buf = (char*)task_allocator_.alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN(
        "failed to alloc buf for task", KR(ret), K(pg_key), K(start_log_id), K(start_log_ts), K(leader_takeover_ts));
  } else {
    task = new (buf) ObPGArchiveRestoreTask();
    const int64_t snapshot_version = restore_meta->get_snapshot_version();
    ObPGKey archive_pg_key;
    if (OB_FAIL(convert_pkey_(pg_key, restore_meta->get_tenant_id(), archive_pg_key))) {
      LOG_WARN("failed to convert_pkey", KR(ret), K(pg_key));
    } else if (OB_FAIL(task->init_basic_info(pg_key,
                   archive_pg_key,
                   restore_meta,
                   start_log_id,
                   start_log_ts,
                   snapshot_version,
                   leader_takeover_ts))) {
      LOG_WARN("failed to init task",
          KR(ret),
          K(pg_key),
          K(start_log_id),
          K(start_log_ts),
          K(snapshot_version),
          K(leader_takeover_ts));
    } else if (OB_FAIL(push_into_task_queue_(task))) {
      LOG_WARN("failed to push task", KR(ret), KPC(task));
    } else {
      LOG_INFO("success to submit restore task", KPC(task));
    }

    if (OB_FAIL(ret)) {
      task->~ObPGArchiveRestoreTask();
      task_allocator_.free(task);
      task = NULL;
      buf = NULL;
    }
  }

  if (OB_FAIL(ret)) {
    // release ref of tenant_restore_meta when fail
    (void)revert_tenant_restore_meta_(restore_meta);
  }
  return ret;
}

int ObArchiveRestoreEngine::try_advance_restoring_clog()
{
  int ret = OB_SUCCESS;
  if (!task_queue_.is_empty()) {
    // single thread pop
    if (OB_FAIL(lock_.try_wrlock(ObLatchIds::ARCHIVE_RESTORE_QUEUE_LOCK))) {
      if (OB_EAGAIN != ret) {
        LOG_ERROR("failed to lock", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      while (OB_SUCC(ret) && (!task_queue_.is_empty())) {
        ObLink* link = NULL;
        if (OB_ISNULL(link = task_queue_.top())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("popped task is null when task queue is not empty", KR(ret));
        } else {
          ObPGArchiveRestoreTask* restore_task = static_cast<ObPGArchiveRestoreTask*>(link);
          const ObPartitionKey& restore_pg_key = restore_task->get_restore_pg_key();
          const uint64_t tenant_id = restore_pg_key.get_tenant_id();
          ObTenantPhysicalRestoreMeta* restore_meta = restore_task->get_tenant_restore_meta();
          int64_t old_restore_concurrency = 0;
          if (OB_ISNULL(restore_meta)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("restore meta is NULL", KR(ret), K(restore_pg_key));
          } else if (restore_meta->get_cur_restore_concurrency() <
                     (old_restore_concurrency = restore_meta->get_restore_concurrency_threshold())) {
            // here we must first pop before push, to avoid restore_task been accessed with
            // task.queue.push() after freed when there is only one element in queue.
            (void)task_queue_.pop();
            if (OB_FAIL(ObSimpleThreadPool::push(restore_task))) {
              (void)release_restore_task_(restore_task);
              LOG_ERROR("failed to push task into inner_queue", "restore_task", *restore_task, KR(ret));
            } else {
              restore_meta->inc_cur_restore_concurrency();
              LOG_INFO("success to push task into inner_queue", K(restore_pg_key));
              // update restore_concurrency_threshold_, memstore limit of tenant may has changed
              if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
                int64_t new_restore_concurrency_threshold = 0;
                if (OB_FAIL(calc_restore_concurrency_threshold_(tenant_id, new_restore_concurrency_threshold))) {
                  LOG_WARN("failed to calc_restore_concurrency_threshold_", KR(ret), K(tenant_id));
                } else if (new_restore_concurrency_threshold != old_restore_concurrency) {
                  LOG_INFO("restore_concurrency has been changed",
                      K(old_restore_concurrency),
                      K(new_restore_concurrency_threshold),
                      K(tenant_id));
                  restore_meta->set_restore_concurrency_threshold(new_restore_concurrency_threshold);
                }
              }
            }
          } else {
            // break when can not submit task
            break;
          }
        }
      }  // end while

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = lock_.unlock())) {
        LOG_ERROR("unlock failed", K(tmp_ret));
      }
    }
  }
  return ret;
}

void ObArchiveRestoreEngine::revert_tenant_restore_meta_(ObTenantPhysicalRestoreMeta*& restore_meta)
{
  if (NULL != restore_meta) {
    restore_meta_map_.revert(restore_meta);
    restore_meta = NULL;
  }
}

void ObArchiveRestoreEngine::handle(void* task)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(MYADDR);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObArchiveRestoreEngine is not inited", KR(ret));
  } else {
    bool encount_fatal_err = false;
    ObPhysicalRestoreCLogStat stat;
    const int64_t begin_time = ObClockGenerator::getClock();
    ObPGArchiveRestoreTask* fetch_task = static_cast<ObPGArchiveRestoreTask*>(task);
    if (OB_ISNULL(fetch_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("FATAL ERROR: fetch task is NULL");
    } else {
      const ObPartitionKey restore_pg_key = fetch_task->get_restore_pg_key();
      const ObPartitionKey archive_pg_key = fetch_task->get_archive_pg_key();
      if (OB_FAIL(handle_single_task_(*fetch_task, stat))) {
        if (!need_retry_ret_code_(ret)) {
          encount_fatal_err = true;
          LOG_ERROR("failed to handle_single_task_", KR(ret), "task", *fetch_task);
        } else {
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
            LOG_WARN("handle_single_task need retry", "task", *fetch_task, KR(ret));
          }
          fetch_task->inc_retry_cnt();
          if (is_io_error(ret)) {
            fetch_task->inc_io_fail_cnt();
            if (fetch_task->get_io_fail_cnt() > MAX_FETCH_LOG_IO_FAIL_CNT) {
              // encount_fatal_err = true;
              LOG_ERROR("io fail too many times", "task", *fetch_task, KR(ret));
            }
          }
        }
      } else {
        // reset retry_cnt_ and io_fail_cnt_
        fetch_task->reset_retry_fail_cnt();
      }
#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = E(EventTable::EN_RESTORE_FETCH_CLOG_ERROR) OB_SUCCESS;
        if (OB_FAIL(ret)) {
          encount_fatal_err = true;
          STORAGE_LOG(INFO, "ERRSIM restore_fetch_clog", K(ret));
        }
      }
#endif
      (void)fetch_task->record_io_stat(stat);
      if ((OB_SUCC(ret) && (fetch_task->is_finished() || fetch_task->is_expired())) || encount_fatal_err) {
        //释放任务内存
        LOG_INFO("free restore task", KR(ret), "task", *fetch_task, K(encount_fatal_err));
        (void)fetch_task->report_io_stat();
        (void)release_restore_task_(fetch_task);
      } else if (OB_FAIL(push(fetch_task))) {
        LOG_ERROR("failed to push task task", KR(ret), KPC(fetch_task));
        (void)fetch_task->report_io_stat();
        (void)release_restore_task_(fetch_task);
        encount_fatal_err = true;
      } else { /*do nothing*/
      }

      int err_ret = ret;
      if (NULL == fetch_task) {
        // task has been freed, try submit new task
        ObTenantPhysicalRestoreMeta* restore_meta = NULL;
        if (OB_FAIL(get_tenant_restore_meta_(restore_pg_key.get_tenant_id(), restore_meta))) {
          encount_fatal_err = true;
          err_ret = ret;
          LOG_ERROR(
              "failed to get_tenant_restore_meta", KR(ret), K(restore_pg_key), K(archive_pg_key), K(encount_fatal_err));
        } else {
          restore_meta->dec_cur_restore_concurrency();
          if (OB_FAIL(try_advance_restoring_clog())) {
            encount_fatal_err = true;
            err_ret = ret;
            LOG_ERROR("failed to try_submit_task", K(ret), K(restore_pg_key), K(archive_pg_key));
          }
        }
        (void)revert_tenant_restore_meta_(restore_meta);
      }

      if (encount_fatal_err) {
        // report fatal error
        if (OB_FAIL(report_fatal_error_(restore_pg_key, archive_pg_key, err_ret))) {
          LOG_WARN(
              "failed to report restore fetch clog error", K(ret), K(restore_pg_key), K(archive_pg_key), K(err_ret));
        }
      }
    }
    const int64_t end_time = ObClockGenerator::getClock();
    stat.total_cost_ += end_time - begin_time;
    statistic_(stat);
  }
}

//---------------start of private funcs-----------//
int ObArchiveRestoreEngine::try_replace_tenant_id_(const uint64_t new_tenant_id, ObLogEntryHeader& log_entry_header)
{
  int ret = OB_SUCCESS;
  ObPartitionKey new_pkey;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (new_tenant_id == log_entry_header.get_partition_key().get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(
                 log_entry_header.get_partition_key(), new_tenant_id, new_pkey))) {
  } else {
    log_entry_header.set_partition_key(new_pkey);
    log_entry_header.update_header_checksum();
  }

  return ret;
}

// =======================================
int ObArchiveRestoreEngine::handle_single_task_(ObPGArchiveRestoreTask& task, ObPhysicalRestoreCLogStat& stat)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionLogService* log_service = NULL;
  ObIPartitionGroupGuard guard;
  bool is_current_leader = false;
  int64_t leader_takeover_ts = OB_INVALID_TIMESTAMP;
  const ObPartitionKey& restore_pg_key = task.get_restore_pg_key();
  int64_t job_id = 0;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition service is NULL", KR(ret), K(task));
  } else if (is_stopped_) {
    task.set_is_expired(true);
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_restore_job_id(restore_pg_key.get_tenant_id(), job_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("failed to get restore info", K(ret), K(restore_pg_key));
    } else {
      task.set_is_expired(true);
      ret = OB_SUCCESS;
      LOG_INFO("physical restore info not exist", K(restore_pg_key));
    }
  } else if (OB_FAIL(partition_service_->get_partition(restore_pg_key, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      task.set_is_expired(true);
      ret = OB_SUCCESS;
      LOG_INFO("partition may has been dropped", KR(ret), K(task));
    } else {
      LOG_ERROR("failed to get_partition", KR(ret), K(task));
    }
  } else if (OB_ISNULL(partition = guard.get_partition_group()) || (!partition->is_valid()) ||
             OB_ISNULL(log_service = partition->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition is invalid", KR(ret), KP(partition), KP(log_service), K(task));
  } else if (OB_FAIL(log_service->get_restore_leader_info(is_current_leader, leader_takeover_ts))) {
    LOG_WARN("failed to get_restore_leader_info", KR(ret), K(task));
  } else {
    // Determine whether the split task needs to continue according to the tenant recovery status.
    const bool is_expired = !is_current_leader || leader_takeover_ts > task.get_leader_takeover_ts();
    task.set_is_expired(is_expired);
    if (task.get_last_checkpoint_ts() >= task.get_end_snapshot_version()) {
      task.set_fetch_log_result(OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH);
    }

    if (OB_SUCC(ret) && !task.is_expired() && !task.is_finished()) {  // start to fetch log
      if (task.get_retry_cnt() > 0) {
        // check_can_receive_batch_log before fetching log to save net bandwidch
        if (OB_FAIL(log_service->check_can_receive_batch_log(task.get_last_fetched_log_id() + 1))) {
          ret = OB_EAGAIN;
          if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
            LOG_WARN("failed to check_can_receive_batch_log", KR(ret), K(task));
          }
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(task.check_and_locate_start_log_info()) && OB_ITER_END != ret) {
        LOG_WARN("failed to check_and_locate_start_log_info", K(ret), K(task));
      }

      if (OB_SUCC(ret) && (!task.is_finished()) && OB_FAIL(fetch_and_submit_archive_log_(task, stat, log_service))) {
        if (need_retry_ret_code_(ret)) {
          if (REACH_TIME_INTERVAL(5 * 1000 * 1000L)) {
            LOG_WARN("failed to fetch_and_submit_archive_log_", KR(ret), K(task));
          }
        } else {
          LOG_WARN("failed to fetch_and_submit_archive_log_", KR(ret), K(task));
        }
      }
    }  // end of fetch log

    // set restore finish status
    if (OB_SUCC(ret) && task.is_finished()) {
      if (OB_FAIL(log_service->set_restore_fetch_log_finished(task.get_fetch_log_result()))) {
        LOG_WARN("failed to set_restore_fetch_log_finished", KR(ret), K(task));
      } else {
        LOG_INFO("succ to set_restore_fetch_log_finished", K(task));
      }
    }
  }

  if (need_retry_ret_code_(ret) && !task.is_expired() && !task.is_finished()) {
    const int64_t sleep_interval = 1000;
    usleep(sleep_interval);
    stat.retry_sleep_interval_ += sleep_interval;
  }
  return ret;
}

int ObArchiveRestoreEngine::fetch_and_submit_archive_log_(
    ObPGArchiveRestoreTask& task, ObPhysicalRestoreCLogStat& stat, ObIPartitionLogService* log_service)

{
  int ret = OB_SUCCESS;
  ObArchiveEntryIterator iter;
  const bool need_limit_bandwidth = true;
  const uint64_t real_tenant_id = task.get_restore_pg_key().get_tenant_id();
  if (OB_FAIL(iter.init(task.get_cur_file_store(),
          task.get_archive_pg_key(),
          task.get_cur_file_id(),
          task.get_cur_file_offset(),
          ACCESS_LOG_FILE_STORE_TIMEOUT,
          need_limit_bandwidth,
          real_tenant_id))) {
    LOG_WARN("failed to init ObArchiveEntryIterator", KR(ret), K(task));
  } else {
    const int64_t LOG_BUF_SIZE = OB_MAX_LOG_ALLOWED_SIZE;
    char* log_buf = NULL;
    if (NULL == (log_buf = static_cast<char*>(ob_malloc(LOG_BUF_SIZE, "archRestoLogBuf")))) {
      ret = OB_EAGAIN;
      LOG_WARN("alloc memory failed", K(task), K(ret));
    }

    bool is_accum_checksum_valid = false;
    int64_t accum_checksum = 0;
    ObLogEntry log_entry;
    int64_t begin_time = ObClockGenerator::getClock();
    while (OB_SUCC(ret) && (!is_stopped_) && (!task.is_finished()) && (!task.is_expired()) &&
           OB_SUCC(iter.next_entry(log_entry, is_accum_checksum_valid, accum_checksum))) {
      const int64_t cur_time = ObClockGenerator::getClock();
      stat.fetch_log_entry_count_++;
      stat.fetch_log_entry_cost_ += cur_time - begin_time;

      begin_time = cur_time;
      // log_buf is not cleared before reuse, reducing cpu overhead
      ObLogEntryHeader& log_header = const_cast<ObLogEntryHeader&>(log_entry.get_header());
      const uint64_t log_id = log_header.get_log_id();
      const int64_t log_ts = log_header.get_submit_timestamp();
      ObLogType log_type = log_header.get_log_type();
      const bool is_batch_committed = log_header.is_batch_committed();
      log_header.clear_trans_batch_commit_flag();
      if (!log_entry.check_integrity()) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("invalid log entry ", KR(ret), K(task), K(log_entry));
      } else if (is_archive_kickoff_log(log_type)) {
        // skip it
      } else if (OB_FAIL(try_replace_tenant_id_(real_tenant_id, log_header))) {
        LOG_ERROR("try_replace_tenant_id_ failed", KR(ret), K(task), K(log_header));
      } else {
        ObLogEntry* final_log_entry = &log_entry;
        if (OB_FAIL(process_normal_clog_(
                *final_log_entry, is_accum_checksum_valid, accum_checksum, is_batch_committed, log_service, task))) {
          LOG_WARN("failed to process_normal_clog_", KR(ret), K(task), "log_entry", *final_log_entry);
        }
      }

      if (OB_STATE_NOT_MATCH == ret) {
        // expire if not master
        task.set_is_expired(true);
        ret = OB_SUCCESS;
      }
    }  // end while
    stat.io_cost_ = iter.get_io_cost();
    stat.io_count_ = iter.get_io_count();
    stat.limit_bandwidth_cost_ = iter.get_limit_bandwidth_cost();

    if (is_stopped_) {
      task.set_is_expired(true);
    }
    // free log_buf
    if (NULL != log_buf) {
      (void)ob_free(log_buf);
    }

    if (OB_ITER_END == ret) {
      // finish ieterating cur_file
      if (OB_FAIL(task.switch_file())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to switch_file", K(ret), K(task));
        } else {
          LOG_INFO("task has iteratored all archive log files", K(task));
          // finish iterating all files in all piece
          if (OB_FAIL(task.reconfirm_fetch_log_result())) {
            LOG_WARN("failed to reconfirm_fetch_log_result ", KR(ret), K(task));
          } else {
            LOG_INFO("finish reconfirm_fetch_log_result", K(task));
          }
        }
      }
    }

    if (need_retry_ret_code_(ret)) {
      task.set_cur_file_offset(iter.get_cur_block_start_offset());
    }
  }
  return ret;
}

int ObArchiveRestoreEngine::process_normal_clog_(const clog::ObLogEntry& log_entry, const bool is_accum_checksum_valid,
    const int64_t accum_checksum, const bool is_batch_committed, ObIPartitionLogService* log_service,
    ObPGArchiveRestoreTask& task)
{
  // process clog
  int ret = OB_SUCCESS;
  const uint64_t expected_log_id = task.get_last_fetched_log_id() + 1;
  const ObLogEntryHeader& log_header = log_entry.get_header();
  const uint64_t log_id = log_header.get_log_id();
  const int64_t log_submit_ts = log_header.get_submit_timestamp();

  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_service is NULL", KR(ret), K(task), K(log_header));
  } else if (log_id < expected_log_id) {
    // just skip the log
  } else if (log_id > expected_log_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is gap in archive log file", KR(ret), K(log_id), K(expected_log_id), K(task), K(log_entry));
  } else { /*log_id == expected_log_id*/
    int64_t log_checkpoint_ts = OB_INVALID_TIMESTAMP;
    const clog::ObLogType log_type = log_header.get_log_type();
    if (OB_LOG_SUBMIT == log_type) {
      ObStorageLogType storage_log_type = storage::OB_LOG_UNKNOWN;
      int64_t log_type_in_buf = storage::OB_LOG_UNKNOWN;
      int64_t log_buf_len = log_header.get_data_len();
      int64_t pos = 0;
      if (OB_FAIL(serialization::decode_i64(log_entry.get_buf(), log_buf_len, pos, &log_type_in_buf))) {
        LOG_WARN("failed to decode storage_log_type", KR(ret), K(log_entry), K(task));
      } else {
        storage_log_type = static_cast<ObStorageLogType>(log_type_in_buf);
        if (OB_LOG_TRANS_CHECKPOINT == storage_log_type) {
          transaction::ObCheckpointLog log;
          if (OB_FAIL(log.deserialize(log_entry.get_buf(), log_buf_len, pos))) {
            LOG_WARN("deserialize checkpoint log failed", KR(ret), K(log_entry), K(task));
          } else {
            log_checkpoint_ts = log.get_checkpoint();
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t accum_checksum_new = accum_checksum;
#ifdef ERRSIM
      int tmp_ret = E(EventTable::EN_LOG_ARCHIVE_RESTORE_ACCUM_CHECKSUM_TAMPERED) OB_SUCCESS;
      if (OB_SUCCESS != tmp_ret) {
        // tamper accum checksum if signal sign
        accum_checksum_new += 1;
      }
#endif
      if (OB_FAIL(log_service->receive_archive_log(
              log_entry, is_accum_checksum_valid, accum_checksum_new, is_batch_committed))) {
        if (OB_EAGAIN == ret) {
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            LOG_WARN("failed to receive_archive_log", KR(ret), K(log_entry), K(task));
          } else {
            LOG_WARN("failed to receive_archive_log", KR(ret), K(log_entry), K(task));
          }
        }
      } else if (OB_FAIL(task.set_last_fetched_log_info(log_type, expected_log_id, log_checkpoint_ts, log_submit_ts))) {
        LOG_WARN("failed to set_last_fetched_log_info", KR(ret), K(log_entry), K(task));
      } else { /*do nothing*/
      }

      // restore end is log whose checkpoint ts >= resotre point and is the end of archive block
      // end log of archive block returned with accum chekcsum, is_accum_checksum_valid is true
      if (OB_SUCC(ret) && task.get_last_checkpoint_ts() >= task.get_end_snapshot_version() && is_accum_checksum_valid) {
        task.set_fetch_log_result(OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH);
      }
    }
  }
  return ret;
}

bool ObArchiveRestoreEngine::need_retry_ret_code_(const int ret)
{
  return (OB_EAGAIN == ret || is_io_error(ret) || OB_ALLOCATE_MEMORY_FAILED == ret || OB_IO_LIMIT == ret ||
          OB_BACKUP_IO_PROHIBITED == ret);
}

int ObArchiveRestoreEngine::get_tenant_restore_meta_(
    const uint64_t tenant_id, ObTenantPhysicalRestoreMeta*& restore_meta)
{
  int ret = OB_SUCCESS;
  restore_meta = NULL;
  ObTenantID tid(tenant_id);
  if (OB_FAIL(restore_meta_map_.get(tid, restore_meta))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("failed to get_restore_meta from map", KR(ret), K(tenant_id));
    } else {
      // restore_meta not exist, generate and insert into map
      ret = OB_SUCCESS;
      char* buf = NULL;
      ObTenantPhysicalRestoreMeta* new_restore_meta = NULL;
      if (OB_UNLIKELY(NULL == (buf = (char*)restore_meta_allocator_.alloc()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc buf", KR(ret), K(tenant_id));
      } else {
        bool need_free_new_meta = false;
        new_restore_meta = new (buf) ObTenantPhysicalRestoreMeta();
        if (OB_FAIL(new_restore_meta->init(tenant_id))) {
          LOG_WARN("failed to init new_restore_meta", KR(ret), K(tenant_id));
          need_free_new_meta = true;
        } else if (OB_FAIL(restore_meta_map_.insert_and_get(tid, new_restore_meta))) {
          need_free_new_meta = true;
          if (OB_ENTRY_EXIST == ret) {
            if (OB_FAIL(restore_meta_map_.get(tid, restore_meta))) {
              LOG_WARN("failed to get restore_meta", KR(ret), K(tenant_id));
            }
          } else {
            LOG_ERROR("failed to insert_and_get restore_meta", KR(ret), K(tenant_id));
          }
        } else {
          restore_meta = new_restore_meta;
          LOG_INFO("succ to insert restore meta into map", K(tenant_id), K(*new_restore_meta));
        }

        if (need_free_new_meta) {
          new_restore_meta->~ObTenantPhysicalRestoreMeta();
          restore_meta_allocator_.free(new_restore_meta);
          new_restore_meta = NULL;
          buf = NULL;
        }
      }
    }
  }
  return ret;
}

int ObArchiveRestoreEngine::convert_pkey_(const ObPGKey& src_pkey, const uint64_t dest_tenant_id, ObPGKey& dest_pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src_pkey.is_valid() || !is_valid_id(dest_tenant_id))) {
    LOG_WARN("invalid arguments", KR(ret), K(src_pkey), K(dest_tenant_id));
  } else {
    uint64_t src_table_id = src_pkey.get_table_id();
    uint64_t dest_table_id = combine_id(dest_tenant_id, src_table_id);
    int64_t src_partition_id = src_pkey.get_partition_id();
    int32_t src_partition_cnt = src_pkey.get_partition_cnt();
    ObPartitionKey new_pkey(dest_table_id, src_partition_id, src_partition_cnt);
    dest_pkey = new_pkey;
  }
  return ret;
}

int ObArchiveRestoreEngine::push_into_task_queue_(ObPGArchiveRestoreTask* task)
{
  int ret = OB_SUCCESS;
  (void)task_queue_.push(task);
  if (OB_FAIL(try_advance_restoring_clog())) {
    LOG_ERROR("failed to try_advance_restoring_clog", KR(ret), "task", *task);
  }
  return ret;
}

int ObArchiveRestoreEngine::calc_restore_concurrency_threshold_(
    const uint64_t tenant_id, int64_t& new_restore_concurrency_threshold)
{
  int ret = OB_SUCCESS;
  int64_t tenant_memstore_limit = 0;
  if (OB_FAIL(ObTenantManager::get_instance().get_tenant_memstore_limit(tenant_id, tenant_memstore_limit))) {
    LOG_WARN("failed to get_tenant_memstore_limit", KR(ret), K(tenant_id));
  } else if (INT64_MAX == tenant_memstore_limit) {
    // do not update temporarily as no memstore limit is set for the tenant
  } else {
    const int64_t concurrency_with_memstore = tenant_memstore_limit / MEMSTORE_RESERVED_PER_PG;
    const int64_t concurrency_with_thread_cnt = ObSimpleThreadPool::get_total_thread_num() * 2;
    new_restore_concurrency_threshold = std::min(concurrency_with_thread_cnt, concurrency_with_memstore);
  }
  return ret;
}

int ObArchiveRestoreEngine::report_fatal_error_(
    const ObPartitionKey& restore_pg_key, const ObPartitionKey& archive_pg_key, const int err_ret)
{
  int ret = OB_SUCCESS;
  int64_t job_id = 0;
  const uint64_t tenant_id = restore_pg_key.get_tenant_id();
  SERVER_EVENT_ADD("clog_restore", "fetch clog failed", "partition", restore_pg_key, "ret", err_ret);
  LOG_ERROR("report fatal error", K(err_ret), K(archive_pg_key), K(restore_pg_key));
  if (OB_FAIL(ObBackupInfoMgr::get_instance().get_restore_job_id(tenant_id, job_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get restore info", KR(err_ret), K(ret), K(tenant_id), K(archive_pg_key), K(restore_pg_key));
    } else {
      LOG_WARN("physical restore info not exist", KR(ret), K(err_ret), K(archive_pg_key), K(restore_pg_key));
    }
  } else if (OB_FAIL((ObRestoreFatalErrorReporter::get_instance().add_restore_error_task(
                 tenant_id, PHYSICAL_RESTORE_MOD_CLOG, err_ret, job_id, MYADDR)))) {
    LOG_WARN("failed to report restore fetch clog error",
        KR(ret),
        K(tenant_id),
        K(err_ret),
        K(archive_pg_key),
        K(restore_pg_key));
  } else { /*do nothing*/
  }
  return ret;
}

void ObArchiveRestoreEngine::statistic_(const ObPhysicalRestoreCLogStat& other)
{
  static int64_t __thread thread_retry_sleep_interval;
  static int64_t __thread thread_fetch_log_entry_count;
  static int64_t __thread thread_fetch_log_entry_cost;
  static int64_t __thread thread_io_cost;
  static int64_t __thread thread_io_count;
  static int64_t __thread thread_limit_bandwidth_cost;
  static int64_t __thread thread_total_cost;

  thread_retry_sleep_interval += other.retry_sleep_interval_;
  thread_fetch_log_entry_count += other.fetch_log_entry_count_;
  thread_fetch_log_entry_cost += other.fetch_log_entry_cost_;
  thread_io_cost += other.io_cost_;
  thread_io_count += other.io_count_;
  thread_limit_bandwidth_cost += other.limit_bandwidth_cost_;
  thread_total_cost += other.total_cost_;
  if (TC_REACH_TIME_INTERVAL(5 * 1000 * 1000L)) {
    LOG_INFO("[STATISTIC] archive_restore_engine",
        K(thread_retry_sleep_interval),
        K(thread_fetch_log_entry_count),
        K(thread_fetch_log_entry_cost),
        K(thread_io_cost),
        K(thread_io_count),
        K(thread_limit_bandwidth_cost),
        K(thread_total_cost),
        "cost_per_log_entry",
        thread_fetch_log_entry_cost / (thread_fetch_log_entry_count + 1),
        "cost_per_io",
        thread_io_cost / (thread_io_count + 1));
    thread_retry_sleep_interval = 0;
    thread_fetch_log_entry_count = 0;
    thread_fetch_log_entry_cost = 0;
    thread_io_cost = 0;
    thread_io_count = 0;
    thread_limit_bandwidth_cost = 0;
    thread_total_cost = 0;
  }
}

int ObTenantPhysicalRestoreMeta::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_restore_info(tenant_id, restore_info_))) {
    LOG_WARN("failed to get_restore_info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_file_store_array_())) {
    LOG_WARN("failed to init file_store_array", KR(ret), K(tenant_id), K(restore_info_));
  } else {
    is_inited_ = true;
    cur_restore_concurrency_ = 0;
    restore_info_.set_array_label("PhyReListInfo");
    restore_concurrency_threshold_ = DEFAULT_RESTORE_CONCURRENCY_THRESHOLD;
    file_store_array_.set_label("PhyReFileStore");
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObTenantPhysicalRestoreMeta::init(ObPhysicalRestoreInfo& info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(info));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(info));
  } else if (OB_FAIL(restore_info_.assign(info))) {
    LOG_WARN("assign fail", KR(ret), K(info));
  } else if (OB_FAIL(init_file_store_array_())) {
    LOG_WARN("failed to init file_store_array", KR(ret), K(info));
  } else {
    is_inited_ = true;
    cur_restore_concurrency_ = 0;
    restore_info_.set_array_label("PhyReListInfo");
    restore_concurrency_threshold_ = DEFAULT_RESTORE_CONCURRENCY_THRESHOLD;
    file_store_array_.set_label("PhyReFileStore");
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObTenantPhysicalRestoreMeta::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    cur_restore_concurrency_ = 0;
    restore_concurrency_threshold_ = -1;
  }
  restore_info_.reset();

  const int64_t array_cnt = file_store_array_.count();
  ObArchiveLogFileStore* file_store = NULL;
  for (int64_t idx = 0; idx < array_cnt; idx++) {
    if (OB_ISNULL(file_store = file_store_array_.at(idx))) {
      LOG_ERROR("file_store is NULL", K(idx), K(array_cnt));
    } else {
      file_store->~ObArchiveLogFileStore();
    }
  }

  if (NULL != file_store_buf_) {
    ob_free(file_store_buf_);
    file_store_buf_ = NULL;
  }
  file_store_array_.destroy();
}

int ObTenantPhysicalRestoreMeta::get_pg_piece_file_store_info(const ObPartitionKey& pg_key, const int64_t cur_piece_idx,
    uint64_t& start_file_id, uint64_t& end_file_id, ObArchiveLogFileStore*& file_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_store_array_.at(cur_piece_idx, file_store))) {
    LOG_WARN("failed to access get_file_store_array", K(ret), K(cur_piece_idx), K(pg_key));
  } else if (OB_ISNULL(file_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("file_store is NULL", K(ret), K(cur_piece_idx), K(pg_key));
  } else if (OB_FAIL(file_store->get_data_file_id_range(pg_key, start_file_id, end_file_id))) {
    LOG_WARN("failed to get_data_file_range", K(ret), K(cur_piece_idx), K(pg_key));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTenantPhysicalRestoreMeta::locate_log(const ObPGKey& restore_pg_key, const ObPGKey& archive_pg_key,
    const uint64_t start_log_id, ObArchiveLogFileStore*& file_store, int64_t& total_piece_cnt, int64_t& start_piece_idx,
    int64_t& end_piece_idx, uint64_t& target_file_id, uint64_t& start_file_id, uint64_t& end_file_id) const
{
  int ret = OB_SUCCESS;
  uint64_t last_piece_base_log_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant restore meta is not inited", K(ret), K(archive_pg_key), K(archive_pg_key), K(start_log_id));
  } else if (OB_FAIL(locate_piece_(restore_pg_key,
                 archive_pg_key,
                 start_log_id,
                 start_piece_idx,
                 end_piece_idx,
                 last_piece_base_log_id,
                 total_piece_cnt,
                 file_store))) {
    LOG_WARN("failed to locate_piece", K(ret), K(archive_pg_key), K(archive_pg_key), K(start_log_id));
  } else if (OB_FAIL(locate_start_file_id_in_piece_(restore_pg_key,
                 archive_pg_key,
                 file_store,
                 start_log_id,
                 start_piece_idx,
                 end_piece_idx,
                 last_piece_base_log_id,
                 target_file_id,
                 start_file_id,
                 end_file_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN(
          "failed to locate_start_file_id_in_piece_", K(ret), K(archive_pg_key), K(archive_pg_key), K(start_log_id));
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObTenantPhysicalRestoreMeta::get_last_file_store(ObArchiveLogFileStore*& file_store)
{
  int ret = OB_SUCCESS;
  const int64_t piece_cnt = file_store_array_.count();
  if (OB_FAIL(file_store_array_.at(piece_cnt - 1, file_store))) {
    LOG_WARN("failed to get file_store", KR(ret), K(piece_cnt));
  }
  return ret;
}

int ObTenantPhysicalRestoreMeta::check_need_fetch_archived_log(const ObPGKey& pg_key, bool& is_need)
{
  int ret = OB_SUCCESS;
  const uint64_t unused_tenant_id = pg_key.get_tenant_id();
  is_need = false;
  if (OB_UNLIKELY(!pg_key.is_valid() || !is_inner_table_with_partition(pg_key.get_table_id()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pg_key));
  } else {
    const int64_t file_store_count = file_store_array_.count();
    uint64_t max_log_id = OB_INVALID_ID;
    int64_t max_checkpoint_ts = OB_INVALID_TIMESTAMP;
    int64_t max_log_ts = OB_INVALID_TIMESTAMP;
    for (int64_t index = 0; index < file_store_count && OB_SUCC(ret) && !is_need; index++) {
      // check if need fetch log based on interface get_pg_max_archived_info
      // 1. if pg max archive info exist and max log id > 0, return true
      // 2. if pg max archive info exist and max log id = 0, means no log archived, return false
      // 3. if pg max archive info not exist, return false
      // 4. other error code, return error code
      ObArchiveLogFileStore* file_store = file_store_array_.at(index);
      if (OB_ISNULL(file_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file_store is NULL", K(ret), K(index), K(file_store_count), KPC(this));
      } else if (OB_FAIL(file_store->get_pg_max_archived_info(
                     pg_key, unused_tenant_id, max_log_id, max_checkpoint_ts, max_log_ts))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_TRACE("pg max info not exist in this piece", KR(ret), K(pg_key), K(file_store_count), KPC(file_store));
        } else {
          LOG_WARN("get pg max archived info fail", KR(ret), K(pg_key), K(file_store_count), KPC(file_store));
        }
      } else if (OB_UNLIKELY(0 == max_log_id)) {
        LOG_INFO("no data exist in this piece, check next piece", KR(ret), K(pg_key), K(max_log_id), KPC(file_store));
      } else {
        is_need = true;
        LOG_TRACE("get pg max archived info succ", K(pg_key), K(is_need), K(max_log_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("check need fetch archived log succ", K(pg_key), K(is_need));
  }
  return ret;
}

int ObTenantPhysicalRestoreMeta::init_file_store_array_()
{
  int ret = OB_SUCCESS;
  const int64_t array_piece_count = restore_info_.get_backup_piece_path_list().count();
  const int64_t piece_count = (0 == array_piece_count) ? 1 : array_piece_count;
  const int64_t file_store_size = sizeof(ObArchiveLogFileStore);
  if (OB_UNLIKELY(piece_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("piece_count should not be zero", K(ret), K(piece_count), K(restore_info_));
  } else if (OB_FAIL(file_store_array_.reserve(piece_count))) {
    LOG_WARN("failed to reserve file_store_array_", K(ret), K(restore_info_), K(piece_count));
  } else if (NULL == (file_store_buf_ = (char*)ob_malloc(file_store_size * piece_count, "ReFilestore"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for file store", K(ret), K(restore_info_), K(piece_count));
  } else {
    int64_t pos = 0;
    if (0 == array_piece_count) {
      ObArchivePathUtil util;
      ObBackupDest dest;
      char path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
      ObSimpleBackupPiecePath simple_path;
      simple_path.backup_piece_id_ = OB_BACKUP_NO_SWITCH_PIECE_ID;
      if (OB_FAIL(dest.set(restore_info_.backup_dest_))) {
        ARCHIVE_LOG(WARN, "failed to set dest", K(ret), KPC(this));
      } else if (OB_FAIL(util.build_base_path(dest,
                     restore_info_.cluster_name_,
                     restore_info_.cluster_id_,
                     restore_info_.tenant_id_,
                     restore_info_.incarnation_,
                     restore_info_.log_archive_round_,
                     OB_BACKUP_NO_SWITCH_PIECE_ID,
                     1,
                     OB_MAX_BACKUP_DEST_LENGTH,
                     path))) {
        ARCHIVE_LOG(WARN, "failed to build_base_path", K(ret), KPC(this));
      } else if (ObStorageType::OB_STORAGE_FILE != dest.device_type_) {
        const int64_t str_len = strlen(path);
        if (OB_FAIL(databuff_printf(path + str_len, OB_MAX_BACKUP_DEST_LENGTH - str_len, "?%s", dest.storage_info_))) {
          ARCHIVE_LOG(WARN, "failed to copy backup dest", K(ret), K(dest));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(simple_path.backup_dest_.assign(path))) {
        ARCHIVE_LOG(WARN, "failed to build_base_path", K(ret), KPC(this));
      } else if (OB_FAIL(restore_info_.get_backup_piece_path_list().push_back(simple_path))) {
        ARCHIVE_LOG(WARN, "failed to push_back", K(ret), K(simple_path), KPC(this));
      } else {
        ARCHIVE_LOG(INFO, "push_back piece info", K(simple_path), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < piece_count; ++idx) {
        ObArchiveLogFileStore* file_store = new (file_store_buf_ + pos) ObArchiveLogFileStore();
        if (OB_ISNULL(file_store)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("file_store is NULL", K(ret), K(idx), K(piece_count), KPC(this));
        } else if (OB_FAIL(file_store->init(restore_info_.get_backup_piece_path_list().at(idx).backup_dest_))) {
          LOG_WARN("failed to init file_store", K(ret), K(restore_info_), K(piece_count));
        } else if (OB_FAIL(file_store_array_.push_back(file_store))) {
          LOG_WARN("failed to push_back file_store", K(ret), K(restore_info_), K(piece_count), K(file_store));
        } else {
          pos += file_store_size;
          LOG_INFO("success to push_back file_store", K(restore_info_), K(piece_count), K(idx), K(file_store));
        }

        if (OB_FAIL(ret) && NULL != file_store) {
          file_store->~ObArchiveLogFileStore();
        }
      }
    }
  }
  return ret;
}

int ObTenantPhysicalRestoreMeta::locate_piece_(const ObPGKey& restore_pg_key, const ObPGKey& archive_pg_key,
    const uint64_t start_log_id, int64_t& start_piece_idx, int64_t& end_piece_idx, uint64_t& last_piece_base_log_id,
    int64_t& total_piece_cnt, ObArchiveLogFileStore*& file_store) const
{
  int ret = OB_SUCCESS;
  start_piece_idx = -1;
  end_piece_idx = -1;
  last_piece_base_log_id = OB_INVALID_ID;
  total_piece_cnt = file_store_array_.count();
  if (OB_UNLIKELY(0 >= total_piece_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "invalid total_piece_cnt", K(ret), K(total_piece_cnt), K(restore_pg_key), K(archive_pg_key), K(start_log_id));
  } else if (!is_switch_piece_mode()) {
    start_piece_idx = 0;
    end_piece_idx = 0;
  } else {
    // situation with multi_piece
    bool has_data_in_prev_piece = false;
    // assign value to start_piece_idx
    for (int64_t idx = 0; OB_SUCC(ret) && 0 > start_piece_idx && idx < total_piece_cnt; ++idx) {
      ObArchiveLogFileStore* file_store = file_store_array_.at(idx);
      ObArchiveKeyContent archive_key_content;
      if (OB_FAIL(file_store->get_archive_key_content(archive_pg_key, archive_key_content)) &&
          OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get_archive_key_content", K(ret), K(restore_pg_key), K(archive_pg_key), K(start_log_id));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        // archive_key_file not exists
        if (!has_data_in_prev_piece) {
          if (idx == total_piece_cnt - 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR(
                "has no archive data among all pieces", K(ret), K(restore_pg_key), K(archive_pg_key), K(start_log_id));
          } else { /*check next piece*/
            ret = OB_SUCCESS;
            LOG_INFO("next piece with not exist",
                K(ret),
                K(restore_pg_key),
                K(archive_pg_key),
                K(start_log_id),
                K(idx),
                K(total_piece_cnt));
          }
        } else {
          start_piece_idx = idx - 1;
          end_piece_idx = idx - 1;
          ret = OB_SUCCESS;
          if (idx != total_piece_cnt - 1) {
            LOG_INFO("pg is dropped during previous piece",
                K(ret),
                K(restore_pg_key),
                K(archive_pg_key),
                K(start_log_id),
                K(idx),
                K(total_piece_cnt));
          } else {
            LOG_INFO("finish locating piece, pg has not finish switching from previous piece to cur piece",
                K(ret),
                K(restore_pg_key),
                K(archive_pg_key),
                K(start_log_id),
                K(total_piece_cnt),
                K(start_piece_idx),
                K(end_piece_idx));
          }
        }
      } else {
        // archive_key_file exists
        if (OB_UNLIKELY(!archive_key_content.check_integrity())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("archive data is corrupted",
              K(ret),
              K(restore_pg_key),
              K(archive_pg_key),
              K(start_log_id),
              K(idx),
              K(archive_key_content));
        } else {
          last_piece_base_log_id = archive_key_content.get_max_archived_log_id();
          if (start_log_id <= last_piece_base_log_id) {
            if (!has_data_in_prev_piece) {
              // first piece with archive data
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("archive_key_content is corrupted",
                  K(ret),
                  K(restore_pg_key),
                  K(archive_pg_key),
                  K(start_log_id),
                  K(archive_key_content),
                  K(idx),
                  K(total_piece_cnt),
                  K(start_piece_idx),
                  K(end_piece_idx));
            } else {
              start_piece_idx = idx - 1;
              LOG_INFO("finish locating piece info",
                  K(ret),
                  K(restore_pg_key),
                  K(archive_pg_key),
                  K(start_piece_idx),
                  K(end_piece_idx),
                  K(total_piece_cnt),
                  K(start_log_id));
            }
          } else {
            if (idx == total_piece_cnt - 1) {
              start_piece_idx = idx;
              end_piece_idx = idx;
              LOG_INFO("finish locating piece info",
                  K(ret),
                  K(restore_pg_key),
                  K(archive_pg_key),
                  K(start_piece_idx),
                  K(end_piece_idx),
                  K(total_piece_cnt),
                  K(start_log_id));
            } else { /*iterating next piece*/
              LOG_INFO("YYY next piece with exist",
                  K(ret),
                  K(restore_pg_key),
                  K(archive_pg_key),
                  K(start_log_id),
                  K(idx),
                  K(total_piece_cnt));
            }
          }
          has_data_in_prev_piece = true;  // should be set here
        }
      }
    }

    if (OB_SUCC(ret) && start_piece_idx >= 0 && end_piece_idx < 0) {
      // assign value to end_piece_idx
      for (int64_t idx = total_piece_cnt - 1; OB_SUCC(ret) && end_piece_idx < 0 && idx >= 0; --idx) {
        ObArchiveKeyContent archive_key_content;
        ObArchiveLogFileStore* file_store = file_store_array_.at(idx);
        if (0 == idx) {
          end_piece_idx = 0;
        } else if (OB_FAIL(file_store->get_archive_key_content(archive_pg_key, archive_key_content)) &&
                   OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get_archive_key_content", K(ret), K(restore_pg_key), K(archive_pg_key), K(start_log_id));
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          /*iterator next piece*/
          ret = OB_SUCCESS;
        } else if (OB_UNLIKELY(!archive_key_content.check_integrity())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("archive data is corrupted",
              K(ret),
              K(restore_pg_key),
              K(archive_pg_key),
              K(start_log_id),
              K(idx),
              K(archive_key_content));
        } else {
          end_piece_idx = idx;
          last_piece_base_log_id = archive_key_content.get_max_archived_log_id();
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (start_piece_idx < 0 || end_piece_idx < start_piece_idx || total_piece_cnt <= end_piece_idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid piece info",
          K(ret),
          K(restore_pg_key),
          K(archive_pg_key),
          K(start_log_id),
          K(start_piece_idx),
          K(end_piece_idx),
          K(total_piece_cnt));
    } else {
      file_store = file_store_array_.at(start_piece_idx);
    }
  }
  return ret;
}

int ObTenantPhysicalRestoreMeta::locate_start_file_id_in_piece_(const ObPGKey& restore_pg_key,
    const ObPGKey& archive_pg_key, ObArchiveLogFileStore* file_store, const uint64_t start_log_id,
    const int64_t start_piece_idx, const int64_t end_piece_idx, const uint64_t last_piece_base_log_id,
    uint64_t& target_file_id, uint64_t& start_file_id, uint64_t& end_file_id) const
{
  int ret = OB_SUCCESS;
  uint64_t min_file_id = 0;
  uint64_t max_file_id = 0;
  bool archive_file_exists = true;
  if (OB_ISNULL(file_store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file_store is NULL", KR(ret), K(restore_pg_key), K(archive_pg_key), K(start_piece_idx));
  } else if (OB_FAIL(file_store->locate_file_by_log_id(
                 archive_pg_key, start_log_id, target_file_id, archive_file_exists))) {
    if (OB_ENTRY_NOT_EXIST == ret && !archive_file_exists && is_switch_piece_mode() &&
        (start_piece_idx == end_piece_idx) && (start_log_id == last_piece_base_log_id + 1)) {
      ret = OB_ITER_END;
      LOG_INFO("cold partition has no archive data",
          KR(ret),
          K(restore_pg_key),
          K(archive_pg_key),
          K(start_piece_idx),
          K(end_piece_idx),
          K(start_log_id),
          K(last_piece_base_log_id));
    } else {
      LOG_WARN("failed to locate_file_by_log_id",
          KR(ret),
          K(restore_pg_key),
          K(archive_pg_key),
          K(start_piece_idx),
          K(end_piece_idx),
          K(archive_file_exists),
          K(start_log_id),
          K(last_piece_base_log_id));
    }
  } else if (OB_FAIL(file_store->get_data_file_id_range(archive_pg_key, min_file_id, max_file_id))) {
    LOG_WARN("failed to get_data_file_id_range", KR(ret), K(restore_pg_key), K(archive_pg_key), K(start_piece_idx));
  } else {
    start_file_id = min_file_id;
    end_file_id = max_file_id;
  }
  return ret;
}

void ObArchiveRestoreEngine::release_restore_task_(ObPGArchiveRestoreTask*& task)
{
  if (NULL != task) {
    ObTenantPhysicalRestoreMeta* tenant_restore_meta = task->get_tenant_restore_meta();
    (void)revert_tenant_restore_meta_(tenant_restore_meta);
    task->reset_tenant_restore_meta();
    task->~ObPGArchiveRestoreTask();
    task_allocator_.free(task);
    task = NULL;
  }
}

//---------------end of private funcs-----------//
}  // end of namespace archive
}  // end of namespace oceanbase
