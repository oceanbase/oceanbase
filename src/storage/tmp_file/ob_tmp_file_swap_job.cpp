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

#include "storage/tmp_file/ob_tmp_file_swap_job.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace tmp_file
{

int ObTmpFileSwapJob::init(int64_t expect_swap_page_cnt, uint32_t timeout_ms)
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
    expect_swap_page_cnt_ = expect_swap_page_cnt;
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
  expect_swap_page_cnt_ = 0;
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

}  // end namespace tmp_file
}  // end namespace oceanbase
