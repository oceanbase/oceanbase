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

#include "ob_batch_submit_task.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/ob_define.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "ob_i_log_engine.h"
#include "ob_clog_mgr.h"

namespace oceanbase {
namespace clog {
void ObBatchSubmitDiskTask::reset()
{
  is_inited_ = false;
  ObIBufferTask::reset();
  trans_id_.reset();
  partition_array_.reset();
  log_info_array_.reset();
  clog_mgr_ = NULL;
  log_engine_ = NULL;
  need_callback_ = true;
}

int ObBatchSubmitDiskTask::init(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
    const ObLogInfoArray& log_info_array, ObICLogMgr* clog_mgr, ObILogEngine* log_engine)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(clog_mgr) || OB_ISNULL(log_engine) || !trans_id.is_valid() || partition_array.count() == 0 ||
      partition_array.count() != log_info_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(clog_mgr),
        K(log_engine),
        K(trans_id),
        K(partition_array),
        K(log_info_array));
  } else if (OB_FAIL(partition_array_.assign(partition_array))) {
    CLOG_LOG(WARN, "partition_array_ assign failed", K(ret));
  } else if (OB_FAIL(log_info_array_.assign(log_info_array))) {
    CLOG_LOG(WARN, "log_info_array_ assign failed", K(ret));
  } else {
    is_inited_ = true;
    trans_id_ = trans_id;
    clog_mgr_ = clog_mgr;
    log_engine_ = log_engine;
  }

  return ret;
}

int64_t ObBatchSubmitDiskTask::get_data_len() const
{
  int64_t ret_val = 0;
  for (int64_t index = 0; index < log_info_array_.count(); index++) {
    if (log_info_array_[index].is_need_flushed()) {
      ret_val += log_info_array_[index].get_size();
    }
  }
  return ret_val;
}

int64_t ObBatchSubmitDiskTask::get_entry_cnt() const
{
  int64_t cnt = 0;
  for (int64_t index = 0; index < log_info_array_.count(); index++) {
    if (log_info_array_[index].is_need_flushed()) {
      cnt += 1;
    }
  }
  return cnt;
}

int ObBatchSubmitDiskTask::fill_buffer(char* buf, const offset_t offset)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_IS_INVALID_OFFSET(offset)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(buf), K(offset));
  } else {
    offset_ = offset;
    offset_t tmp_offset = offset;
    for (int64_t index = 0; index < log_info_array_.count(); index++) {
      if (log_info_array_[index].is_need_flushed()) {
        MEMCPY(buf + tmp_offset, log_info_array_[index].get_buf(), log_info_array_[index].get_size());
        tmp_offset += static_cast<offset_t>(log_info_array_[index].get_size());
      }
    }
  }

  return ret;
}

int ObBatchSubmitDiskTask::st_after_consume(const int handle_err)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != handle_err) {
    CLOG_LOG(ERROR,
        "ObBatchSubmitDiskTask write disk error, the server will kill self",
        K(trans_id_),
        K(partition_array_),
        K(log_info_array_));
    on_fatal_error(handle_err);
  } else {
    for (int64_t index = 0; index < partition_array_.count(); index++) {
      if (log_info_array_[index].is_need_flushed()) {
        const common::ObPartitionKey& partition_key = partition_array_[index];
        const uint64_t log_id = log_info_array_[index].get_log_id();
        const int64_t submit_timestamp = log_info_array_[index].get_submit_timestamp();

        log_engine_->update_clog_info(partition_key, log_id, submit_timestamp);
      }
    }
  }

  return ret;
}

int ObBatchSubmitDiskTask::after_consume(const int handle_err, const void* arg, const int64_t before_push_cb_ts)
{
  int ret = OB_SUCCESS;

  static int64_t batch_submit_flush_cb_cnt = 0;
  static int64_t batch_submit_flush_cb_cost_time = 0;
  int64_t before_flush_cb = ObClockGenerator::getClock();
  ObLogCursor base_log_cursor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != handle_err) {
    CLOG_LOG(ERROR,
        "ObBatchSubmitDiskTask write disk error, the server will kill self",
        K(trans_id_),
        K(partition_array_),
        K(log_info_array_));
    on_fatal_error(handle_err);
  } else if (OB_FAIL(base_log_cursor.deep_copy(*(static_cast<const ObLogCursor*>(arg))))) {
    CLOG_LOG(WARN, "base_log_cursor deep_copy failed", K(ret));
  } else {
    EVENT_INC(CLOG_TASK_CB_COUNT);
    EVENT_ADD(CLOG_CB_QUEUE_TIME, ObTimeUtility::current_time() - before_push_cb_ts);
    base_log_cursor.offset_ += static_cast<offset_t>(offset_);
    if (OB_FAIL(clog_mgr_->batch_flush_cb(trans_id_, base_log_cursor))) {
      CLOG_LOG(ERROR, "flush cb failed", K(trans_id_), K(partition_array_), K(log_info_array_), K(base_log_cursor));
    }

    const int64_t after_flush_cb = ObClockGenerator::getClock();
    ATOMIC_INC(&batch_submit_flush_cb_cnt);
    ATOMIC_AAF(&batch_submit_flush_cb_cost_time, after_flush_cb - before_flush_cb);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO,
          "clog batch submit flush cb cost time",
          K(batch_submit_flush_cb_cnt),
          K(batch_submit_flush_cb_cost_time),
          "avg time",
          batch_submit_flush_cb_cost_time / (batch_submit_flush_cb_cnt + 1));
      ATOMIC_STORE(&batch_submit_flush_cb_cnt, 0);
      ATOMIC_STORE(&batch_submit_flush_cb_cost_time, 0);
    }
  }
  ObBatchSubmitDiskTaskFactory::free(this);
  return ret;
}

int64_t ObBatchSubmitDiskTaskFactory::alloc_cnt_ = 0;
int64_t ObBatchSubmitDiskTaskFactory::free_cnt_ = 0;

ObBatchSubmitDiskTask* ObBatchSubmitDiskTaskFactory::alloc(common::ObILogAllocator* alloc_mgr)
{
  ObBatchSubmitDiskTask* ret_val = NULL;
  if (NULL != alloc_mgr) {
    ATOMIC_INC(&alloc_cnt_);
    ret_val = alloc_mgr->alloc_batch_submit_dtask();
  }
  return ret_val;
}

void ObBatchSubmitDiskTaskFactory::free(ObBatchSubmitDiskTask* task)
{
  ATOMIC_INC(&free_cnt_);
  ob_slice_free_batch_submit_dtask(task);
}

void ObBatchSubmitDiskTaskFactory::statistics()
{
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    CLOG_LOG(INFO,
        "ObBatchSubmitDiskTaskFactory statistics",
        K(alloc_cnt_),
        K(free_cnt_),
        "using_cnt",
        alloc_cnt_ - free_cnt_);
  }
}
}  // namespace clog
}  // namespace oceanbase
