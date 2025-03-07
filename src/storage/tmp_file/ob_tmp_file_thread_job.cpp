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

#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_tmp_file_thread_job.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace tmp_file
{

int ObTmpFileSwapJob::init(int64_t expect_swap_size, uint32_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFileSwapJob init twice", KR(ret));
  } else if (timeout_ms <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(swap_cond_.init(ObWaitEventIds::THREAD_IDLING_COND_WAIT))) {
    STORAGE_LOG(WARN, "ObTmpFileSwapJob init cond failed", KR(ret));
  } else {
    is_inited_ = true;
    ret_code_ = OB_SUCCESS;
    is_finished_ = false;
    expect_swap_size_ = expect_swap_size;
    timeout_ms_ = timeout_ms;
    create_ts_ = ObTimeUtility::current_time();
    abs_timeout_ts_ = ObTimeUtility::current_time() + timeout_ms_ * 1000;
  }
  return ret;
}

void ObTmpFileSwapJob::reset()
{
  is_inited_ = false;
  ret_code_ = OB_SUCCESS;
  is_finished_ = false;
  expect_swap_size_ = 0;
  timeout_ms_ = DEFAULT_TIMEOUT_MS;
  create_ts_ = 0;
  abs_timeout_ts_ = 0;
  swap_cond_.destroy();
}

// waits for swap job to finish, loop inside until is_finished_ is true
int ObTmpFileSwapJob::wait_swap_complete()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileSwapJob not init", KR(ret));
  } else {
    bool is_finished = false;
    ObThreadCondGuard guard(swap_cond_);
    while (false == (is_finished = ATOMIC_LOAD(&is_finished_))) {
      if (OB_FAIL(swap_cond_.wait())) {
        if (OB_TIMEOUT == ret) {
          STORAGE_LOG(WARN, "fail to wait swap job complete", KR(ret), K(is_finished), K(timeout_ms_));
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(ERROR, "fail to wait swap job thread cond", KR(ret), K(is_finished), KPC(this));
        }
      }
    }
  }
  return ret;
}

// set swap job is_finished, wake up threads that invoke swap job
int ObTmpFileSwapJob::signal_swap_complete(int ret_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileSwapJob not init", KR(ret));
  } else {
    ObThreadCondGuard guard(swap_cond_);
    ATOMIC_SET(&ret_code_, ret_code);
    ATOMIC_SET(&is_finished_, true);
    if (OB_FAIL(swap_cond_.signal())) {
      STORAGE_LOG(WARN, "ObTmpFileSwapJob signal swap complete failed", KR(ret));
    }
  }
  return ret;
}

void ObTmpFileFlushMonitor::reset()
{
  flush_task_cnt_ = 0;
  total_flushing_data_length_ = 0;
  max_flush_data_length_ = -1;
  min_flush_data_length_ = INT64_MAX;
  f1_cnt_ = 0;
  f2_cnt_ = 0;
  f3_cnt_ = 0;
  f4_cnt_ = 0;
  f5_cnt_ = 0;
}

void ObTmpFileFlushMonitor::print_statistics()
{
  int64_t flush_task_cnt = flush_task_cnt_;
  int64_t avg_flush_data_len = total_flushing_data_length_ / max(flush_task_cnt, 1);
  int64_t max_flush_data_len = max_flush_data_length_;
  int64_t min_flush_data_len = min_flush_data_length_ == INT64_MAX ? -1 : min_flush_data_length_;
  int64_t f1_cnt = f1_cnt_;
  int64_t f2_cnt = f2_cnt_;
  int64_t f3_cnt = f3_cnt_;
  int64_t f4_cnt = f4_cnt_;
  int64_t f5_cnt = f5_cnt_;
  STORAGE_LOG(INFO, "tmp file flush statistics", K(flush_task_cnt), K(avg_flush_data_len),
    K(max_flush_data_len), K(min_flush_data_len), K(f1_cnt), K(f2_cnt), K(f3_cnt), K(f4_cnt), K(f5_cnt));
  reset();
}

void ObTmpFileFlushMonitor::record_flush_stage(const ObTmpFileGlobal::FlushCtxState flush_stage)
{
  switch(flush_stage) {
    case ObTmpFileGlobal::FSM_F1:
    f1_cnt_++;
    break;
    case ObTmpFileGlobal::FSM_F2:
    f2_cnt_++;
    break;
    case ObTmpFileGlobal::FSM_F3:
    f3_cnt_++;
    break;
    case ObTmpFileGlobal::FSM_F4:
    f4_cnt_++;
    break;
    case ObTmpFileGlobal::FSM_F5:
    f5_cnt_++;
    break;
    default:
    break;
  }
}

void ObTmpFileFlushMonitor::record_flush_task(const int64_t data_length)
{
  flush_task_cnt_ += 1;
  total_flushing_data_length_ += data_length;
  if (data_length > 0 && data_length > max_flush_data_length_) {
    max_flush_data_length_ = data_length;
  }
  if (data_length > 0 && data_length < min_flush_data_length_) {
    min_flush_data_length_ = data_length;
  }
}

void ObTmpFileSwapMonitor::reset()
{
  swap_task_cnt_ = 0;
  swap_total_response_time_= 0;
  swap_max_response_time_ = -1;
  swap_min_response_time_ = INT64_MAX;
}

void ObTmpFileSwapMonitor::print_statistics()
{
  int64_t swap_task_cnt = swap_task_cnt_;
  int64_t avg_swap_response_time = swap_total_response_time_ / max(swap_task_cnt, 1);
  int64_t max_swap_response_time = swap_max_response_time_;
  int64_t min_swap_response_time = swap_min_response_time_ == INT64_MAX ? -1 : swap_min_response_time_;
  STORAGE_LOG(INFO, "tmp file swap statistics", K(swap_task_cnt),
    K(avg_swap_response_time), K(max_swap_response_time), K(min_swap_response_time));
  reset();
}

void ObTmpFileSwapMonitor::record_swap_response_time(const int64_t response_time)
{
  swap_task_cnt_ += 1;
  swap_total_response_time_ += response_time;
  if (response_time > swap_max_response_time_) {
    swap_max_response_time_ = response_time;
  }
  if (response_time < swap_min_response_time_) {
    swap_min_response_time_ = response_time;
  }
}

}  // end namespace tmp_file
}  // end namespace oceanbase
