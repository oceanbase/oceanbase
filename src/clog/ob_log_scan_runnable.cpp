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

#include "ob_log_scan_runnable.h"
#include "share/ob_thread_mgr.h"
#include "storage/ob_partition_service.h"
#include "ob_info_block_handler.h"
#include "ob_i_log_engine.h"
#include "clog/ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace clog {
ObLogScanRunnable::LocateFileRangeTimerTask::LocateFileRangeTimerTask()
    : host_(NULL),
      thread_index_(-1),
      start_file_id_(OB_INVALID_FILE_ID),
      last_file_id_(OB_INVALID_FILE_ID),
      result_file_id_(OB_INVALID_FILE_ID)
{}

int ObLogScanRunnable::LocateFileRangeTimerTask::init(
    ObLogScanRunnable* host, const int64_t thread_index, const file_id_t start_file_id, const file_id_t last_file_id)
{
  int ret = OB_SUCCESS;
  if (NULL != host_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "LocateFileRangeTimerTask init twice", K(ret));
  } else if (NULL == host || thread_index < 0 || thread_index >= host->file_range_th_cnt_ ||
             !is_valid_file_id(start_file_id) || !is_valid_file_id(last_file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(host), K(thread_index), K(start_file_id), K(last_file_id));
  } else {
    host_ = host;
    thread_index_ = thread_index;
    start_file_id_ = start_file_id;
    last_file_id_ = last_file_id;
    result_file_id_ = last_file_id;
  }
  return ret;
}

void ObLogScanRunnable::LocateFileRangeTimerTask::runTimerTask()
{
  if (NULL == host_) {
    CLOG_LOG(ERROR, "ScanTimerTask is not inited");
  } else {
    host_->do_locate_file_range_(thread_index_, start_file_id_, last_file_id_);
  }
}

ObLogScanRunnable::ScanTimerTask::ScanTimerTask()
    : host_(NULL), scan_thread_index_(-1), start_file_id_(OB_INVALID_FILE_ID), last_file_id_(OB_INVALID_FILE_ID)
{}

int ObLogScanRunnable::ScanTimerTask::init(ObLogScanRunnable* host, const int64_t scan_thread_index,
    const file_id_t start_file_id, const file_id_t last_file_id)
{
  int ret = OB_SUCCESS;
  if (NULL != host_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "ScanTimerTask init twice", K(ret));
  } else if (NULL == host || scan_thread_index < 0 || scan_thread_index >= host->scan_th_cnt_ ||
             !is_valid_file_id(start_file_id) || !is_valid_file_id(last_file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(host), K(scan_thread_index), K(start_file_id), K(last_file_id));
  } else {
    host_ = host;
    scan_thread_index_ = scan_thread_index;
    start_file_id_ = start_file_id;
    last_file_id_ = last_file_id;
  }
  return ret;
}

void ObLogScanRunnable::ScanTimerTask::runTimerTask()
{
  if (NULL == host_) {
    CLOG_LOG(ERROR, "ScanTimerTask is not inited");
  } else {
    host_->do_scan_files_(scan_thread_index_, start_file_id_, last_file_id_);
  }
}

ObLogScanRunnable::ObLogScanRunnable()
    : is_inited_(false),
      is_stopped_(false),
      bkg_task_ret_(common::OB_SUCCESS),
      task_finished_cnt_(0),
      scan_state_(BEFORE_SCAN),
      partition_service_(NULL),
      log_engine_(NULL),
      file_range_th_cnt_(!lib::is_mini_mode() ? FILE_RANGE_THREAD_CNT : MINI_MODE_FILE_RANGE_THREAD_CNT),
      scan_th_cnt_(!lib::is_mini_mode() ? SCAN_THREAD_CNT : MINI_MODE_SCAN_THREAD_CNT)
{}

ObLogScanRunnable::~ObLogScanRunnable()
{
  destroy();
}

int ObLogScanRunnable::init(ObPartitionService* partition_service, ObILogEngine* log_engine)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "ObLogScanRunnable init twice", K(ret));
  } else if (OB_ISNULL(partition_service) || OB_ISNULL(log_engine)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(partition_service), KP(log_engine));
  } else {
    if (OB_FAIL(TG_START(lib::TGDefIDs::LogScan))) {
      CLOG_LOG(ERROR, "timer init failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      is_stopped_ = false;
      scan_state_ = BEFORE_SCAN;
      partition_service_ = partition_service;
      log_engine_ = log_engine;

      is_inited_ = true;
    }
  }
  if (!is_inited_) {
    destroy();
  }
  CLOG_LOG(INFO, "ObLogScanRunnable init finished", K(ret));
  return ret;
}

int ObLogScanRunnable::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObThreadPool::start())) {
    CLOG_LOG(ERROR, "log scan runnable thread failed to start", K(ret));
  }
  return ret;
}

void ObLogScanRunnable::stop()
{
  TG_STOP(lib::TGDefIDs::LogScan);
  share::ObThreadPool::stop();
  is_stopped_ = true;
}

void ObLogScanRunnable::wait()
{
  TG_WAIT(lib::TGDefIDs::LogScan);
  share::ObThreadPool::wait();
}

void ObLogScanRunnable::destroy()
{
  stop();
  wait();
  TG_DESTROY(lib::TGDefIDs::LogScan);
  ATOMIC_STORE(&scan_state_, BEFORE_SCAN);
  partition_service_ = NULL;
  log_engine_ = NULL;

  is_inited_ = false;
  CLOG_LOG(INFO, "log scan runnable destroy");
}

void ObLogScanRunnable::run1()
{
  lib::set_thread_name("LogScanRunnable");
  do_scan_log_();
}

bool ObLogScanRunnable::is_before_scan() const
{
  return BEFORE_SCAN == ATOMIC_LOAD(&scan_state_);
}

bool ObLogScanRunnable::is_scan_finished() const
{
  return SCAN_FINISHED == ATOMIC_LOAD(&scan_state_);
}

void ObLogScanRunnable::do_scan_log_()
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  file_id_t start_file_id = OB_INVALID_FILE_ID;
  file_id_t last_file_id = OB_INVALID_FILE_ID;
  ATOMIC_STORE(&scan_state_, SCANNING);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogScanRunnable is not inited", K(ret));
  } else if (OB_FAIL(fill_file_id_cache_())) {
    CLOG_LOG(ERROR, "fill_file_id_cache_ failed", K(ret));
  } else if (OB_FAIL(set_next_index_log_id_())) {
    CLOG_LOG(ERROR, "set_next_index_log_id_ failed", K(ret));
  } else if (OB_FAIL(get_scan_file_range_(start_file_id, last_file_id))) {
    CLOG_LOG(ERROR, "get_scan_file_range_ failed", K(ret));
  } else if (OB_FAIL(scan_all_files_(start_file_id, last_file_id))) {
    CLOG_LOG(WARN, "scan_all_files_ failed", K(ret));
  } else if (OB_FAIL(wait_scan_task_finished_())) {
    CLOG_LOG(WARN, "wait_scan_task_finished_ failed", K(ret));
  } else if (OB_FAIL(keep_file_max_submit_timestamp_inc_())) {
    CLOG_LOG(ERROR, "keep_file_max_submit_timestamp_inc_ failed", K(ret));
  } else if (OB_FAIL(notify_scan_finished_())) {
    CLOG_LOG(ERROR, "notify_scan_finished_ failed", K(ret));
  }

  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  if (OB_SUCC(ret)) {
    CLOG_LOG(INFO, "scan runnable success", K(ret), K(cost_time));
  } else if (OB_LSR_THREAD_STOPPED != ret) {
    CLOG_LOG(ERROR, "log scan runnable exit error", K(ret));
    on_fatal_error(ret);
  }
}

int ObLogScanRunnable::fill_file_id_cache_()
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  if (OB_FAIL(log_engine_->fill_file_id_cache())) {
    CLOG_LOG(WARN, "fill_file_id_cache failed", K(ret));
  }
  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  CLOG_LOG(INFO, "fill_file_id_cache finished", K(ret), K(cost_time));
  return ret;
}

int ObLogScanRunnable::set_next_index_log_id_()
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  ObIPartitionGroup* partition = NULL;
  ObIPartitionGroupIterator* partition_iter = NULL;
  ObIPartitionLogService* pls = NULL;
  const int64_t scan_thread_index = 0;

  if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "partition_service_ alloc partition iter failed", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(partition_iter->get_next(partition))) {
      // do nothing
    } else if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "partition is NULL", K(ret));
    } else if (!partition->is_valid() || NULL == (pls = partition->get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "partition is invalid when scanning", "partition_key", partition->get_partition_key());
    } else {
      const common::ObPartitionKey& partition_key = partition->get_partition_key();
      uint64_t max_ilog_id = OB_INVALID_ID;
      ObLogCursorExt log_cursor_ext;
      const uint64_t last_replay_log_id = pls->get_last_replay_log_id();
      if (OB_FAIL(log_engine_->ensure_log_continuous_in_file_id_cache(partition_key, last_replay_log_id))) {
        CLOG_LOG(ERROR, "failed to ensure_log_continuous", K(ret), K(partition_key));
      } else if (OB_FAIL(log_engine_->query_max_ilog_id(partition_key, max_ilog_id)) && OB_PARTITION_NOT_EXIST != ret) {
        CLOG_LOG(ERROR, "query_max_ilog_id failed", K(ret), K(partition_key));
      } else if (OB_PARTITION_NOT_EXIST == ret) {
        // do nothing;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(get_cursor_with_retry_(scan_thread_index, pls, partition_key, max_ilog_id, log_cursor_ext))) {
        CLOG_LOG(ERROR, "get_cursor_with_retry_ failed", K(ret), K(partition_key), K(max_ilog_id));
      } else if (OB_FAIL(pls->set_next_index_log_id(max_ilog_id + 1, log_cursor_ext.get_accum_checksum()))) {
        CLOG_LOG(ERROR, "set_next_index_log_id failed", K(ret), K(partition_key), K(max_ilog_id));
      } else {
        CLOG_LOG(INFO, "success to set_next_index_log_id", K(partition_key), K(max_ilog_id));
      }
    }
  }
  if (NULL != partition_iter) {
    partition_service_->revert_pg_iter(partition_iter);
    partition_iter = NULL;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  CLOG_LOG(INFO, "set_next_index_log_id_ finished", K(ret), K(cost_time));
  return ret;
}

int ObLogScanRunnable::get_scan_file_range_(file_id_t& start_file_id, file_id_t& last_file_id)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  if (OB_FAIL(get_scan_file_range_based_on_ts_(start_file_id, last_file_id))) {
    CLOG_LOG(ERROR, "get_scan_file_range_based_on_ts_ failed", K(ret));
  } else if (OB_FAIL(get_scan_file_range_based_on_log_id_(start_file_id, last_file_id))) {
    CLOG_LOG(ERROR, "get_scan_file_range_based_on_log_id_ failed", K(ret));
  }
  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  CLOG_LOG(INFO, "get_scan_file_range_ finished", K(ret), K(cost_time), K(start_file_id), K(last_file_id));
  return ret;
}

int ObLogScanRunnable::get_scan_file_range_based_on_ts_(file_id_t& start_file_id, file_id_t& last_file_id)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  bool can_binary_search = false;
  if (OB_FAIL(log_engine_->get_clog_file_id_range(min_file_id, max_file_id))) {
    CLOG_LOG(ERROR, "get_clog_file_id_range failed", K(ret));
  } else if (OB_INVALID_FILE_ID == min_file_id || OB_INVALID_FILE_ID == max_file_id || min_file_id > max_file_id) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get invalid file id range", K(ret), K(min_file_id), K(max_file_id));
  } else if (max_file_id - min_file_id >= 1 && OB_FAIL(check_can_binary_search_(min_file_id, can_binary_search))) {
    CLOG_LOG(ERROR, "check_can_binary_search failed", K(ret), K(min_file_id));
  } else if (can_binary_search) {
    file_id_t low = min_file_id - 1;
    file_id_t high = max_file_id;
    file_id_t mid = OB_INVALID_FILE_ID;
    int64_t min_submit_timestamp = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(ObIInfoBlockHandler::get_min_submit_timestamp(partition_service_, min_submit_timestamp))) {
      CLOG_LOG(WARN, "get_min_submit_timestamp failed", K(ret), K(min_submit_timestamp));
    }
    // a[low] <= target < a[high]
    bool can_skip = false;
    while ((low + 1 < high) && OB_SUCC(ret) && !is_stopped_) {
      mid = (low + high) / 2;
      if (OB_FAIL(compare_file_submit_timestamp_(min_submit_timestamp, mid, can_skip))) {
        CLOG_LOG(WARN, "compare file submit_timestamp failed", K(ret), K(mid), K(min_submit_timestamp));
      } else {
        if (can_skip) {
          // a[mid] <= target
          low = mid;
        } else {
          high = mid;
        }
      }
      CLOG_LOG(INFO, "binary search start file id", K(low), K(high), K(mid), K(can_skip));
    }  // end while
    start_file_id = high;
    last_file_id = max_file_id;
  } else {
    start_file_id = min_file_id;
    last_file_id = max_file_id;
  }
  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  CLOG_LOG(INFO, "get_scan_file_range_based_on_ts_ finished", K(ret), K(cost_time), K(start_file_id), K(last_file_id));
  return ret;
}

int ObLogScanRunnable::get_scan_file_range_based_on_log_id_(file_id_t& start_file_id, const file_id_t last_file_id)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  for (int64_t i = 0; OB_SUCC(ret) && i < file_range_th_cnt_; i++) {
    if (OB_FAIL(locate_tasks_[i].init(this, i, start_file_id, last_file_id))) {
      CLOG_LOG(ERROR, "ScanTimerTask init failed", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::LogScan, i, locate_tasks_[i], 0, false))) {
      CLOG_LOG(ERROR, "timer schedule failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(wait_locate_task_finished_())) {
      CLOG_LOG(ERROR, "failed to wait_locate_task_finished", K(ret));
    } else {
      file_id_t result_file_id = last_file_id;
      for (int64_t i = 0; OB_SUCC(ret) && i < file_range_th_cnt_; i++) {
        if (locate_tasks_[i].get_result_file_id() < result_file_id) {
          result_file_id = locate_tasks_[i].get_result_file_id();
        }
      }
      start_file_id = result_file_id;
    }
  }
  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  CLOG_LOG(
      INFO, "get_scan_file_range_based_on_log_id_ finished", K(ret), K(cost_time), K(start_file_id), K(last_file_id));
  return ret;
}

int ObLogScanRunnable::scan_all_files_(const file_id_t start_file_id, const file_id_t last_file_id)
{
  int ret = OB_SUCCESS;
  ATOMIC_SET(&task_finished_cnt_, 0);
  for (int64_t i = 0; i < scan_th_cnt_; i++) {
    scan_process_coordinator_[i] = start_file_id;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_th_cnt_; i++) {
    if (OB_FAIL(scan_file_tasks_[i].init(this, i, start_file_id, last_file_id))) {
      CLOG_LOG(ERROR, "ScanTimerTask init failed", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::LogScan, i, scan_file_tasks_[i], 0, false))) {
      CLOG_LOG(ERROR, "timer schedule failed", K(ret));
    }
  }
  return ret;
}

int ObLogScanRunnable::do_scan_files_(
    const int64_t scan_thread_index, const file_id_t start_file_id, const file_id_t last_file_id)
{
  int ret = OB_SUCCESS;
  const int64_t scan_begin_time = ObTimeUtility::current_time();
  file_id_t curr_file_id = start_file_id + static_cast<file_id_t>(scan_thread_index);
  while (OB_SUCC(ret)) {
    if (curr_file_id <= last_file_id) {
      if (OB_FAIL(handle_process_coordinate_(curr_file_id, scan_thread_index))) {
        CSR_LOG(ERROR, "handle_process_coordinate_ failed", K(ret), K(scan_thread_index), K(curr_file_id));
      } else if (OB_FAIL(scan_one_file_(scan_thread_index, curr_file_id, last_file_id))) {
        CSR_LOG(ERROR, "scan_one_file_ failed", K(ret), K(scan_thread_index), K(curr_file_id), K(last_file_id));
      } else {
        if (1 == scan_thread_index) {
          (void)update_process_bar_(curr_file_id, start_file_id, last_file_id, scan_begin_time);
        }
        curr_file_id += static_cast<file_id_t>(scan_th_cnt_);
        ATOMIC_STORE(&scan_process_coordinator_[scan_thread_index], curr_file_id);
      }
    } else {
      break;
    }
  }
  if (OB_SUCCESS != ret) {
    ATOMIC_STORE(&bkg_task_ret_, ret);
  }
  (void)mark_task_finished_(scan_thread_index);
  return ret;
}

int ObLogScanRunnable::scan_one_file_(
    const int64_t scan_thread_index, const file_id_t curr_file_id, const file_id_t last_file_id)
{
  int ret = OB_SUCCESS;
  ObIRawLogIterator* log_iterator = NULL;
  const offset_t start_offset = 0;
  const int64_t TIMEOUT = 10 * 1000 * 1000;

  if (scan_thread_index < 0 || scan_thread_index >= scan_th_cnt_ || !is_valid_file_id(curr_file_id) ||
      !is_valid_file_id(last_file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(scan_thread_index), K(curr_file_id), K(last_file_id));
  } else if (NULL ==
             (log_iterator = log_engine_->alloc_raw_log_iterator(curr_file_id, curr_file_id, start_offset, TIMEOUT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "alloc_raw_log_iterator failed", K(ret), K(curr_file_id));
  } else {
    ObLogEntry entry;
    ObReadParam param;
    int64_t persist_len = 0;  // not used here
    while (!is_stopped_ && OB_SUCCESS == ret &&
           OB_SUCCESS == (ret = log_iterator->next_entry(entry, param, persist_len))) {
      if (param.file_id_ != curr_file_id) {
        break;
      }
      const ObPartitionKey& partition_key = entry.get_header().get_partition_key();
      if (param.file_id_ == last_file_id) {
        const uint64_t log_id = entry.get_header().get_log_id();
        const ObLogType log_type = entry.get_header().get_log_type();
        const int64_t submit_timestamp = entry.get_header().get_submit_timestamp();
        if (!is_prepared_log(log_type)) {
          log_engine_->update_clog_info(partition_key, log_id, submit_timestamp);
        }
      }
      bool need_submit = true;
      int64_t accum_checksum = 0;
      bool batch_committed = false;
      bool is_confirmed_log = false;
      ObIPartitionLogService* pls = NULL;
      ObIPartitionGroupGuard guard;
      if (OB_SUCCESS != partition_service_->get_partition(partition_key, guard) ||
          NULL == guard.get_partition_group() || NULL == (pls = guard.get_partition_group()->get_log_service())) {
        ret = OB_SUCCESS;
        CLOG_LOG(INFO, "partition is not exist", K(ret), K(partition_key), K(entry));
      } else if (!guard.get_partition_group()->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "partition is invalid", K(ret), K(partition_key), K(entry));
      } else if (OB_FAIL(check_need_submit_(scan_thread_index,
                     pls,
                     entry,
                     param,
                     need_submit,
                     accum_checksum,
                     batch_committed,
                     is_confirmed_log))) {
        CLOG_LOG(WARN, "check_need_submit_ failed", K(ret), K(entry), K(param));
      } else if (!need_submit) {
        CLOG_LOG(DEBUG, "no need submit", K(ret), K(partition_key), K(entry), K(param));
      } else {
        ObLogCursor log_cursor;
        log_cursor.file_id_ = param.file_id_;
        log_cursor.offset_ = param.offset_;
        log_cursor.size_ = static_cast<int32_t>(entry.get_header().get_total_len());
        int64_t retry_count = 0;
        const int64_t APPEND_LOG_RETRY_TIME_US = 1000;
        const int64_t APPEND_LOG_RETRY_WARN_THRESHOLD = 50;
        // CLOG_LOG(INFO, "append_disk_log", K(entry), K(log_cursor));
        while (
            OB_ERROR_OUT_OF_RANGE == (ret = pls->append_disk_log(entry, log_cursor, accum_checksum, batch_committed)) ||
            OB_EAGAIN == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
          usleep(APPEND_LOG_RETRY_TIME_US);
          if (++retry_count > APPEND_LOG_RETRY_WARN_THRESHOLD && REACH_TIME_INTERVAL(500 * 1000)) {
            CLOG_LOG(WARN, "retry submit clog to sliding window too many times", K(retry_count));
          }
        }
        if (OB_FAIL(ret)) {
          CLOG_LOG(WARN,
              "append disk log failed",
              K(ret),
              K(partition_key),
              "ObLogEntryHeader",
              entry.get_header(),
              K(log_cursor));
        }
      }
    }
    if (OB_SUCCESS == ret && is_stopped_) {
      ret = OB_LSR_THREAD_STOPPED;
      CLOG_LOG(INFO, "log scan runnable is stopped when scan_files_", K(ret), K(scan_thread_index), K(curr_file_id));
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCCESS == ret) {
      if (param.file_id_ > last_file_id) {
        CLOG_LOG(INFO, "reach the end of last clog file", K(ret), K(last_file_id), K(param));
        log_engine_->reset_clog_info_block();
      }
    }
  }
  if (NULL != log_iterator) {
    log_engine_->revert_raw_log_iterator(log_iterator);
    log_iterator = NULL;
  }
  return ret;
}

int ObLogScanRunnable::handle_process_coordinate_(const file_id_t curr_file_id, const int64_t scan_thread_index)
{
  int ret = OB_SUCCESS;
  bool can_goon = false;
  while (OB_SUCC(ret) && !can_goon) {
    if (ATOMIC_LOAD(&bkg_task_ret_) == OB_SUCCESS) {
      can_goon = true;
      for (int64_t i = 0; can_goon && i < scan_th_cnt_; i++) {
        if (curr_file_id <= ATOMIC_LOAD(&scan_process_coordinator_[i]) + 2 * scan_th_cnt_ + scan_thread_index) {
          // continue;
        } else {
          can_goon = false;
        }
      }
      if (!can_goon) {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(INFO, "handle_process_coordinate_ need to wait", K(curr_file_id));
          for (int64_t i = 0; i < scan_th_cnt_; i++) {
            CLOG_LOG(INFO, "handle_process_coordinate_  scan process:", K(scan_process_coordinator_[i]));
          }
        }
        usleep(1000);
      }
    } else {
      ret = ATOMIC_LOAD(&bkg_task_ret_);
    }
  }
  return ret;
}

int ObLogScanRunnable::mark_task_finished_(const int64_t thread_index)
{
  int ret = OB_SUCCESS;
  ATOMIC_INC(&task_finished_cnt_);
  CLOG_LOG(INFO, "mark_task_finished_", K(thread_index), K(task_finished_cnt_));
  return ret;
}

int ObLogScanRunnable::wait_scan_task_finished_()
{
  int ret = OB_SUCCESS;
  while (ATOMIC_LOAD(&task_finished_cnt_) < scan_th_cnt_) {
    usleep(1000 * 1000);
  }
  ret = bkg_task_ret_;
  return ret;
}

int ObLogScanRunnable::keep_file_max_submit_timestamp_inc_()
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  if (OB_FAIL(log_engine_->get_clog_file_id_range(min_file_id, max_file_id))) {
    CLOG_LOG(WARN, "get ilog file id range failed", K(ret));
  } else if (OB_INVALID_FILE_ID == min_file_id || OB_INVALID_FILE_ID == max_file_id) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get invalid file id range", K(ret), K(min_file_id), K(max_file_id));
  } else if (max_file_id - min_file_id >= 1) {
    int64_t prev_file_max_submit_timestamp = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(get_file_max_submit_timestamp_(max_file_id - 1, prev_file_max_submit_timestamp))) {
      CLOG_LOG(
          WARN, "get prev file max submit_timestamp failed", K(ret), K(max_file_id), K(prev_file_max_submit_timestamp));
    } else {
      // Handle upgrade from 1.x
      if (OB_INVALID_TIMESTAMP == prev_file_max_submit_timestamp) {
        prev_file_max_submit_timestamp = 0;
      }
      log_engine_->update_clog_info(prev_file_max_submit_timestamp);
      CLOG_LOG(INFO,
          "update clog info by prev_file_max_submit_timestamp success",
          K(prev_file_max_submit_timestamp),
          K(max_file_id),
          K(min_file_id));
    }
  }
  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  CLOG_LOG(INFO, "keep_file_max_submit_timestamp_inc_ finished", K(ret), K(cost_time));
  return ret;
}

int ObLogScanRunnable::notify_scan_finished_()
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  ObIPartitionGroup* partition = NULL;
  ObIPartitionGroupIterator* partition_iter = NULL;
  ObIPartitionLogService* pls = NULL;

  if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "partition_service_ alloc partition iter failed", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(partition_iter->get_next(partition))) {
      // do nothing
    } else if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "partition is NULL", K(ret));
    } else if (!partition->is_valid() || NULL == (pls = partition->get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "partition is invalid when scanning", "partition_key", partition->get_partition_key());
    } else {
      const ObPartitionKey& pkey = partition->get_partition_key();
      int64_t scan_confirmed_log_cnt = pls->get_scan_confirmed_log_cnt();
      uint64_t next_ilog_id = pls->get_next_index_log_id();
      uint64_t last_replay_log_id = pls->get_last_replay_log_id();
      if (OB_UNLIKELY((last_replay_log_id >= next_ilog_id) ||
                      (scan_confirmed_log_cnt != (next_ilog_id - 1 - last_replay_log_id)))) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR,
            "invalid scan_confirmed_log_cnt",
            KR(ret),
            K(scan_confirmed_log_cnt),
            K(next_ilog_id),
            K(last_replay_log_id),
            K(pkey));
      } else if (OB_FAIL(pls->set_scan_disk_log_finished())) {
        CLOG_LOG(ERROR, "set_scan_disk_log_finished failed", K(ret), K(pkey));
      } else {
        CLOG_LOG(INFO,
            "notify_scan_finished_ success",
            K(pkey),
            K(next_ilog_id),
            K(last_replay_log_id),
            K(scan_confirmed_log_cnt));
      }
    }
  }
  if (NULL != partition_iter) {
    partition_service_->revert_pg_iter(partition_iter);
    partition_iter = NULL;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    ATOMIC_STORE(&scan_state_, SCAN_FINISHED);
  }
  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  CLOG_LOG(INFO, "notify_scan_finished_ finished", K(ret), K(cost_time));
  return ret;
}

void ObLogScanRunnable::update_process_bar_(const file_id_t curr_file_id, const file_id_t start_file_id,
    const file_id_t last_file_id, const int64_t scan_begin_time)
{
  const int64_t total_file_cnt = last_file_id - start_file_id + 1;
  const int64_t processed_file_cnt = curr_file_id - start_file_id + 1;
  const int64_t rest_file_cnt = total_file_cnt - processed_file_cnt;
  const int64_t estimated_rest_time =
      (ObTimeUtility::current_time() - scan_begin_time) * rest_file_cnt / processed_file_cnt / 1000 / 1000;
  CLOG_LOG(INFO,
      "scan process bar",
      K(curr_file_id),
      K(start_file_id),
      K(last_file_id),
      K(total_file_cnt),
      K(processed_file_cnt),
      K(rest_file_cnt),
      "estimated_rest_time(second)",
      estimated_rest_time);
}

int ObLogScanRunnable::get_cursor_with_retry_(const int64_t scan_thread_index, ObIPartitionLogService* pls,
    const common::ObPartitionKey& partition_key, const uint64_t log_id, ObLogCursorExt& log_cursor_ext) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pls) || !partition_key.is_valid() || OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(pls), K(partition_key), K(log_id));
  } else {
    while (OB_NEED_RETRY == (ret = pls->get_cursor_with_cache(scan_thread_index, log_id, log_cursor_ext))) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "get_cursor_with_cache return OB_NEED_RETRY", K(ret), K(partition_key), K(log_id));
      }
      usleep(1000);
    }
  }
  return ret;
}

int ObLogScanRunnable::check_can_binary_search_(const file_id_t min_file_id, bool& can_binary_search)
{
  int ret = OB_SUCCESS;
  int64_t max_submit_timestamp = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(get_file_max_submit_timestamp_(min_file_id, max_submit_timestamp))) {
    CLOG_LOG(WARN, "get_file_max_submit_timestamp_ failed", K(ret), K(min_file_id));
  } else {
    can_binary_search = (max_submit_timestamp != OB_INVALID_TIMESTAMP);
  }
  return ret;
}

// file_id is not the last file, so we can always get info_block
int ObLogScanRunnable::get_file_max_submit_timestamp_(const file_id_t file_id, int64_t& max_submit_timestamp)
{
  int ret = OB_SUCCESS;
  if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(file_id));
  } else {
    ObCommitInfoBlockHandler handler;
    if (OB_FAIL(handler.init())) {
      CLOG_LOG(WARN, "clog info handler init error", K(ret), K(file_id));
    } else if (OB_FAIL(log_engine_->get_clog_info_handler(file_id, handler))) {
      CLOG_LOG(WARN, "get clog info handler error", K(ret), K(file_id));
    } else if (OB_FAIL(handler.get_max_submit_timestamp(max_submit_timestamp))) {
      CLOG_LOG(WARN, "get_max_submit_timestamp failed", K(ret), K(file_id), K(max_submit_timestamp));
    }
    handler.destroy();
  }
  return ret;
}

int ObLogScanRunnable::compare_file_submit_timestamp_(
    const int64_t min_submit_timestamp, const file_id_t file_id, bool& can_skip)
{
  int ret = OB_SUCCESS;
  if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(file_id));
  } else {
    ObCommitInfoBlockHandler handler;
    if (OB_FAIL(handler.init())) {
      CLOG_LOG(WARN, "clog info handler init error", K(ret), K(file_id));
    } else if (OB_FAIL(log_engine_->get_clog_info_handler(file_id, handler))) {
      CLOG_LOG(WARN, "get clog info handler error", K(ret), K(file_id));
    } else if (OB_FAIL(handler.can_skip_based_on_submit_timestamp(min_submit_timestamp, can_skip))) {
      CLOG_LOG(WARN, "can_skip_based_on_submit_timestamp error", K(ret), K(min_submit_timestamp));
    }
    handler.destroy();
  }
  return ret;
}

int ObLogScanRunnable::compare_file_based_on_log_id_(const file_id_t file_id, bool& can_skip)
{
  int ret = OB_SUCCESS;
  if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(file_id));
  } else {
    ObCommitInfoBlockHandler handler;
    if (OB_FAIL(handler.init())) {
      CLOG_LOG(WARN, "clog info handler init failed", K(ret), K(file_id));
    } else if (OB_FAIL(log_engine_->get_clog_info_handler(file_id, handler))) {
      CLOG_LOG(WARN, "get clog info handler failed", K(ret), K(file_id));
    } else if (OB_FAIL(handler.can_skip_based_on_log_id(partition_service_, can_skip))) {
      CLOG_LOG(WARN, "can_skip_based_on_log_id failed", K(ret));
    }
    handler.destroy();
  }
  return ret;
}

int ObLogScanRunnable::check_need_submit_(const int64_t scan_thread_index, ObIPartitionLogService* pls,
    const ObLogEntry& log_entry, const ObReadParam& read_param, bool& need_submit, int64_t& accum_checksum,
    bool& batch_committed, bool& is_confirmed_log) const
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& partition_key = log_entry.get_header().get_partition_key();
  const ObLogType log_type = log_entry.get_header().get_log_type();
  const uint64_t log_id = log_entry.get_header().get_log_id();
  uint64_t min_log_id = OB_INVALID_ID;
  uint64_t max_log_id = OB_INVALID_ID;
  ObLogCursorExt log_cursor_ext;

  bool is_obsoleted = false;
  is_confirmed_log = false;
  if (OB_FAIL(
          log_engine_->check_is_clog_obsoleted(partition_key, read_param.file_id_, read_param.offset_, is_obsoleted))) {
    CLOG_LOG(WARN, "check_is_clog_obsoleted failed", K(ret), K(partition_key), K(read_param));
  } else if (is_obsoleted) {
    need_submit = false;
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000L)) {
      CLOG_LOG(INFO, "clog is obsoleted, no need to submit", K(partition_key), K(read_param));
    }
  } else if (OB_FAIL(get_need_submit_log_range_(pls, min_log_id, max_log_id))) {
    CLOG_LOG(WARN, "get_need_submit_log_range_ failed", K(ret), K(partition_key));
  } else if (is_prepared_log(log_type)) {
    need_submit = true;
  } else if (log_id < min_log_id) {
    need_submit = false;
  } else if (log_id > max_log_id) {
    need_submit = true;
    accum_checksum = 0;
    batch_committed = false;
  } else if (OB_FAIL(get_cursor_with_retry_(scan_thread_index, pls, partition_key, log_id, log_cursor_ext))) {
    CLOG_LOG(ERROR, "get_cursor_with_retry_ failed", K(ret), K(partition_key), K(log_id));
  } else if (read_param.file_id_ == log_cursor_ext.get_file_id() && read_param.offset_ == log_cursor_ext.get_offset()) {
    need_submit = true;
    is_confirmed_log = true;
    accum_checksum = log_cursor_ext.get_accum_checksum();
    batch_committed = log_cursor_ext.is_batch_committed();
  } else {
    need_submit = false;
  }
  return ret;
}

int ObLogScanRunnable::get_need_submit_log_range_(
    const ObIPartitionLogService* pls, uint64_t& min_log_id, uint64_t& max_log_id) const
{
  int ret = OB_SUCCESS;
  min_log_id = pls->get_last_replay_log_id() + 1;
  max_log_id = pls->get_next_index_log_id() - 1;
  return ret;
}

int ObLogScanRunnable::do_locate_file_range_(
    const int64_t thread_index, const file_id_t start_file_id, const file_id_t last_file_id)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  file_id_t cur_file_id = start_file_id + static_cast<file_id_t>(thread_index);
  while (OB_SUCC(ret)) {
    if (cur_file_id < last_file_id) {
      if (OB_SUCCESS == ATOMIC_LOAD(&bkg_task_ret_)) {
        bool can_skip = false;
        if (OB_FAIL(compare_file_based_on_log_id_(cur_file_id, can_skip))) {
          CLOG_LOG(WARN, "compare_file_based_on_log_id_ failed", K(ret), K(cur_file_id));
        } else if (!can_skip) {
          locate_tasks_[thread_index].set_result_file_id(cur_file_id);
          break;
        } else {
          cur_file_id += static_cast<file_id_t>(file_range_th_cnt_);
        }
        if (OB_SUCCESS != ret) {
          ATOMIC_CAS(&bkg_task_ret_, OB_SUCCESS, ret);
        }
      } else {
        ret = ATOMIC_LOAD(&bkg_task_ret_);
      }
    } else {
      locate_tasks_[thread_index].set_result_file_id(last_file_id);
      break;
    }
  }

  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  CLOG_LOG(INFO,
      "finish locate file range",
      K(ret),
      K(thread_index),
      K(cost_time),
      K(start_file_id),
      K(last_file_id),
      K(cur_file_id));
  (void)mark_task_finished_(thread_index);
  return ret;
}

int ObLogScanRunnable::wait_locate_task_finished_() const
{
  int ret = OB_SUCCESS;
  while (ATOMIC_LOAD(&task_finished_cnt_) < file_range_th_cnt_) {
    usleep(10 * 1000);
  }
  ret = bkg_task_ret_;
  return ret;
}

}  // namespace clog
}  // namespace oceanbase
