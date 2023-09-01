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
 *
 * PartProgressController
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_part_progress_controller.h"

#include "share/ob_errno.h"                   // OB_SUCCESS
#include "lib/utility/ob_macro_utils.h"     // OB_UNLIKELY
#include "lib/oblog/ob_log_module.h"        // LOG_ERROR

#include "ob_log_utils.h"                   // TS_TO_STR

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{
//////////////////////////// PartProgressController ////////////////////////////

PartProgressController::PartProgressController() :
    inited_(false),
    max_progress_cnt_(0),
    progress_list_(NULL),
    recycled_indices_(),
    recycled_indices_lock_(common::ObLatchIds::OBCDC_PROGRESS_RECYCLE_LOCK),
    progress_cnt_(0),
    valid_progress_cnt_(0),
    thread_counter_(0),
    last_global_count_and_timeval_(),
    global_count_and_timeval_()
{
  last_global_count_and_timeval_.lo = 0;
  last_global_count_and_timeval_.hi = 0;
  global_count_and_timeval_.lo = 0;
  global_count_and_timeval_.hi = 0;
}

PartProgressController::~PartProgressController()
{
  destroy();
}

int PartProgressController::init(const int64_t max_progress_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(max_progress_cnt <= 0)) {
    LOG_ERROR("invalid progress cnt", K(max_progress_cnt));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t size = max_progress_cnt * static_cast<int64_t>(sizeof(Item));
    progress_list_ = static_cast<Item*>(ob_malloc(size, ObModIds::OB_LOG_PART_PROGRESS_CONTROLLER));

    if (OB_ISNULL(progress_list_)) {
      LOG_ERROR("alloc progress list fail", K(size), K(max_progress_cnt));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      max_progress_cnt_ = max_progress_cnt;

      for (int64_t idx = 0, cnt = max_progress_cnt; OB_SUCCESS == ret && (idx < cnt); ++idx) {
        Item &item = *(progress_list_ + idx);
        item.reset();
      }
    }

    if (OB_SUCCESS == ret) {
      thread_counter_ = 0;
      last_global_count_and_timeval_.lo = 0;
      last_global_count_and_timeval_.hi = 0;
      global_count_and_timeval_.lo = 0;
      global_count_and_timeval_.hi = 0;
      inited_ = true;
    }
  }
  return ret;
}

void PartProgressController::destroy()
{
  inited_ = false;

  if (NULL != progress_list_) {
    ob_free(progress_list_);
    progress_list_ = NULL;
  }

  max_progress_cnt_ = 0;
  progress_cnt_ = 0;
  valid_progress_cnt_ = 0;
  recycled_indices_.destroy();
  thread_counter_ = 0;
  last_global_count_and_timeval_.lo = 0;
  last_global_count_and_timeval_.hi = 0;
  global_count_and_timeval_.lo = 0;
  global_count_and_timeval_.hi = 0;
}

int PartProgressController::acquire_progress(int64_t &progress_id, const int64_t start_progress)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(recycled_indices_lock_);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(INVALID_PROGRESS == start_progress)) {
    LOG_ERROR("invalid start progress", K(start_progress));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(progress_list_)) {
    LOG_ERROR("invalid  progress list", K(progress_list_));
    ret = OB_ERR_UNEXPECTED;
  } else if ((0 == recycled_indices_.count()) && (max_progress_cnt_ <= progress_cnt_)) {
    LOG_WARN("progress id used up", K(progress_cnt_), K(max_progress_cnt_));
    ret = OB_NEED_RETRY;
  } else {
    if (0 < recycled_indices_.count()) {
      progress_id = recycled_indices_.at(recycled_indices_.count() - 1);
      recycled_indices_.pop_back();
    } else {
      progress_id = progress_cnt_;
      ATOMIC_INC(&progress_cnt_);
    }

    // Set the starting progress value
    progress_list_[progress_id].reset(start_progress);

    ATOMIC_INC(&valid_progress_cnt_);

    _LOG_INFO("[STAT] [PROGRESS_CONTROLLER] [ACQUIRE] progress_id=%ld start_progress=%ld(%s) "
        "progress_cnt=(total=%ld,valid=%ld,recycled=%ld,max=%ld)",
        progress_id, start_progress, NTS_TO_STR(start_progress),
        progress_cnt_, valid_progress_cnt_, recycled_indices_.count(), max_progress_cnt_);
  }

  return ret;
}

int PartProgressController::release_progress(const int64_t progress_id)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(recycled_indices_lock_);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(progress_list_)) {
    LOG_ERROR("invalid progress list", K(progress_list_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(progress_id < 0) || OB_UNLIKELY(progress_cnt_ <= progress_id)) {
    LOG_ERROR("invalid progress id", K(progress_id), K(progress_cnt_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(recycled_indices_.push_back(progress_id))) {
    LOG_ERROR("push back recycled index fail", KR(ret), K(progress_id));
  } else {
    Item &item = *(progress_list_ + progress_id);
    item.reset();

    ATOMIC_DEC(&valid_progress_cnt_);

    _LOG_INFO("[STAT] [PROGRESS_CONTROLLER] [RELEASE] progress_id=%ld progress_cnt=(total=%ld,"
        "valid=%ld,recycled=%ld,max=%ld)",
        progress_id, progress_cnt_, valid_progress_cnt_, recycled_indices_.count(),
        max_progress_cnt_);
  }
  return ret;

}

int PartProgressController::update_progress(const int64_t progress_id, const int64_t progress)
{
  int ret = OB_SUCCESS;
  const int64_t progress_cnt = ATOMIC_LOAD(&progress_cnt_);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(progress_list_)) {
    LOG_ERROR("invalid progress list", K(progress_list_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(progress_id < 0) || OB_UNLIKELY(progress_cnt <= progress_id)) {
    LOG_ERROR("invalid progress id", K(progress_id), K(progress_cnt));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(INVALID_PROGRESS == progress)) {
    LOG_ERROR("invalid progress value", K(progress));
    ret = OB_INVALID_ARGUMENT;
  } else {
    Item &item = *(progress_list_ + progress_id);
    item.update(progress);

    _LOG_DEBUG("[STAT] [PROGRESS_CONTROLLER] [UPDATE] progress_id=%ld progress=%ld(%s) "
        "delay=%s  progress_cnt=(total=%ld,valid=%ld,recycled=%ld,max=%ld)",
        progress_id, progress, NTS_TO_STR(progress), NTS_TO_DELAY(progress),
        progress_cnt_, valid_progress_cnt_, recycled_indices_.count(), max_progress_cnt_);
  }
  return ret;
}

// If there is no minimum, an invalid value is returned: INVALID_PROGRESS
int PartProgressController::get_min_progress(int64_t &progress)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(progress_list_)) {
    LOG_ERROR("invalid progress list", K(progress_list_));
    ret = OB_ERR_UNEXPECTED;
  } else if (ATOMIC_LOAD(&valid_progress_cnt_) <= 0) {
    progress = INVALID_PROGRESS;
    ret = OB_EMPTY_RESULT;
  } else {
    progress = INVALID_PROGRESS;
    const int64_t cnt = ATOMIC_LOAD(&progress_cnt_);
    int64_t execution_time = get_timestamp();
    int64_t min_progress_id = 0;

    for (int64_t idx = 0; OB_SUCCESS == ret && (idx < cnt); ++idx) {
      const int64_t this_progress = progress_list_[idx].get();
      if (INVALID_PROGRESS != this_progress) {
        if ((INVALID_PROGRESS == progress) || (this_progress < progress)) {
          progress = this_progress;
          min_progress_id = idx;
        }
      }
    }

    LOG_DEBUG("[FETCHER] [GET_MIN_PROGRESS] ", K(progress), K_(progress_cnt), K(min_progress_id));
    execution_time = get_timestamp() - execution_time ;
    // Update execution time, print execution time periodically
    update_execution_time_(execution_time);
  }

  return ret;
}

int64_t PartProgressController::get_itid_()
{
  static TLOCAL(int64_t, index) = -1;
  return index < 0 ? (index = ATOMIC_FAA(&thread_counter_, 1)) : index;
}

void PartProgressController::update_execution_time_(int64_t execution_time)
{
  // Multi-threaded resource seizure, recording the id number of the seized thread and subsequently having the thread periodically print the average execution time
  static int64_t tid_flag = -1;
  // get tid
  int64_t tid = get_itid_();

  if (OB_UNLIKELY(-1 == tid_flag)) {
    ATOMIC_CAS(&tid_flag, -1, tid);
  }

  while (true) {
    types::uint128_t old_v;
    types::uint128_t new_v;

    LOAD128(old_v, &global_count_and_timeval_);

    // Scan the array + 1, and add up the execution time
    new_v.lo = old_v.lo + 1;
    new_v.hi = old_v.hi + execution_time;

    if (CAS128(&global_count_and_timeval_, old_v, new_v)) {
      // success, break
      break;
    } else {
      PAUSE();
    }
  }

  if (tid_flag == tid) {
    if (REACH_TIME_INTERVAL(PRINT_GET_MIN_PROGRESS_INTERVAL)) {
      types::uint128_t global_count_and_timeval;
      LOAD128(global_count_and_timeval, &global_count_and_timeval_);
      // Calculate the number of times the array was scanned during this interval and the average time to scan an array
      const uint64_t scan_cnt = global_count_and_timeval.lo - last_global_count_and_timeval_.lo;
      uint64_t time = 0;
      if (0 != scan_cnt) {
        time = (global_count_and_timeval.hi - last_global_count_and_timeval_.hi) / scan_cnt;
      }
      // part count
      const int64_t part_cnt = ATOMIC_LOAD(&progress_cnt_);
      // Record current statistics
      last_global_count_and_timeval_.lo = global_count_and_timeval.lo;
      last_global_count_and_timeval_.hi = global_count_and_timeval.hi;

       _LOG_INFO("[STAT] [GET_MIN_PROGRESS] AVG_TIME=%lu LS_COUNT=%ld SCAN_COUNT=%lu",
                 time, part_cnt, scan_cnt);
    }
  }
}

}
}
