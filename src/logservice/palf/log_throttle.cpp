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
#define USING_LOG_PREFIX PALF

#include "log_throttle.h"
#include "share/ob_throttling_utils.h"                //ObThrottlingUtils
#include "palf_env_impl.h"                            // PalfEnvImpl

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
void LogThrottlingStat::reset()
{
  start_ts_ = OB_INVALID_TIMESTAMP;
  stop_ts_ = OB_INVALID_TIMESTAMP;
  total_throttling_interval_ = 0;
  total_throttling_size_ = 0;
  total_throttling_task_cnt_ = 0;
  total_skipped_size_ = 0;
  total_skipped_task_cnt_ = 0;
  max_throttling_interval_ = 0;
}
void LogWritingThrottle::reset()
{
  last_update_ts_ = OB_INVALID_TIMESTAMP;
  need_writing_throttling_notified_ = false;
  appended_log_size_cur_round_ = 0;
  decay_factor_ = 0;
  throttling_options_.reset();
  stat_.reset();
}

int LogWritingThrottle::update_throttling_options(IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(palf_env_impl)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("palf_env_impl is NULL", KPC(this));
  } else {
    bool unused_has_freed_up_space = false;
    if (check_need_update_throtting_options_guarded_by_lock_()
        && OB_FAIL(update_throtting_options_guarded_by_lock_(palf_env_impl, unused_has_freed_up_space))) {
      LOG_WARN("failed to update_throttling_info", KPC(this));
    }
  }
  return ret;
}

int LogWritingThrottle::throttling(const int64_t throttling_size,
                                   const NeedPurgingThrottlingFunc &need_purging_throttling_func,
                                   IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (!need_purging_throttling_func.is_valid()
      || OB_ISNULL(palf_env_impl)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (throttling_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid throttling_size", K(throttling_size), KPC(this));
  } else if (0 == throttling_size) {
    //no need throttling
  } else {
    // hold lock firstly.
    lock_.lock();
    if (need_throttling_not_guarded_by_lock_(need_purging_throttling_func)) {
      const int64_t cur_unrecyclable_size = throttling_options_.unrecyclable_disk_space_ + appended_log_size_cur_round_;
      const int64_t trigger_base_log_disk_size = throttling_options_.total_disk_space_ * throttling_options_.trigger_percentage_ /  100;
      int64_t time_interval = 0;
      if (OB_FAIL(ObThrottlingUtils::get_throttling_interval(THROTTLING_CHUNK_SIZE, throttling_size, trigger_base_log_disk_size,
                                                             cur_unrecyclable_size, decay_factor_, time_interval))) {
        LOG_WARN("failed to get_throttling_interval", KPC(this));
      }
      int64_t remain_interval_us = time_interval;
      bool has_freed_up_space = false;
      // release lock_ in progress of usleep, therefore, accessing shared members in LogWritingThrottle need be guarded by lock
      // in following code block.
      lock_.unlock();
      while (OB_SUCC(ret) && remain_interval_us > 0) {
        const int64_t real_interval = MIN(remain_interval_us, DETECT_INTERVAL_US);
        usleep(real_interval);
        remain_interval_us -= real_interval;
        if (remain_interval_us <= 0) {
          //do nothing
        } else if (OB_FAIL(update_throtting_options_guarded_by_lock_(palf_env_impl, has_freed_up_space))) {
          LOG_WARN("failed to update_throttling_info_", KPC(this), K(time_interval), K(remain_interval_us));
        } else if (!need_throttling_not_guarded_by_lock_(need_purging_throttling_func)
                   || has_freed_up_space) {
          LOG_TRACE("no need throttling or log disk has been freed up", KPC(this), K(time_interval), K(remain_interval_us), K(has_freed_up_space));
          break;
        }
      }
      // hold lock_ after ulseep, therefore, accessing shared members in LogWritingThrottle no need be guarded by lock.
      lock_.lock();
      stat_.after_throttling(time_interval - remain_interval_us, throttling_size);
    } else if (need_throttling_with_options_not_guarded_by_lock_()) {
      stat_.after_throttling(0, throttling_size);
    }

    if (stat_.has_ever_throttled()) {
      if (REACH_TIME_INTERVAL(2 * 1000 * 1000L)) {
         PALF_LOG(INFO, "[LOG DISK THROTTLING] [STAT]", KPC(this));
      }
    }
    // release lock lastly.
    lock_.unlock();
  }
  return ret;
}

int LogWritingThrottle::after_append_log(const int64_t log_size)
{
  SpinLockGuard guard(lock_);
  appended_log_size_cur_round_ += log_size;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(log_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(log_size));
  } else {/*do nothing*/}
  return ret;
}

int LogWritingThrottle::update_throtting_options_guarded_by_lock_(IPalfEnvImpl *palf_env_impl, bool &has_freed_up_space)
{
  int ret = OB_SUCCESS;
  SpinLockGuard guard(lock_);
  const int64_t cur_ts = ObClockGenerator::getClock();
  if (ATOMIC_LOAD(&need_writing_throttling_notified_)) {
    PalfThrottleOptions new_throttling_options;
    if (OB_FAIL(palf_env_impl->get_throttling_options(new_throttling_options))) {
      PALF_LOG(WARN, "failed to get_writing_throttling_option");
    } else if (OB_UNLIKELY(!new_throttling_options.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "options is invalid", K(new_throttling_options), KPC(this));
    } else {
      const bool need_throttling = new_throttling_options.need_throttling();
      const int64_t new_available_size_after_limit = new_throttling_options.get_available_size_after_limit();
      const int64_t new_maximum_duration = new_throttling_options.get_maximum_duration();
      bool need_update_decay_factor = false;
      bool need_start_throttling = false;

      if (need_throttling) {
        if (!throttling_options_.need_throttling()) {
          need_start_throttling = true;
          need_update_decay_factor = true;
        } else {
          need_update_decay_factor = (throttling_options_.get_available_size_after_limit() != new_available_size_after_limit)
              || throttling_options_.get_maximum_duration() != new_maximum_duration;
        }
        if (need_update_decay_factor) {
          if (OB_FAIL(ObThrottlingUtils::calc_decay_factor(new_available_size_after_limit, new_maximum_duration,
                  THROTTLING_CHUNK_SIZE, decay_factor_))) {
            PALF_LOG(ERROR, "failed to calc_decay_factor", K(throttling_options_), K(THROTTLING_CHUNK_SIZE));
          } else {
            PALF_LOG(INFO, "[LOG DISK THROTTLING] success to calc_decay_factor", K(decay_factor_), K(throttling_options_),
                     K(new_throttling_options), K(THROTTLING_CHUNK_SIZE));
          }
        }

        if (OB_SUCC(ret)) {
          // update other field
          has_freed_up_space = new_throttling_options.unrecyclable_disk_space_ < throttling_options_.unrecyclable_disk_space_;
          bool has_unrecyclable_space_changed = new_throttling_options.unrecyclable_disk_space_ != throttling_options_.unrecyclable_disk_space_;
          if (has_unrecyclable_space_changed || need_start_throttling) {
            // reset appended_log_size_cur_round_ when unrecyclable_disk_space_ changed
            appended_log_size_cur_round_ = 0;
          }
          throttling_options_ = new_throttling_options;
          if (need_start_throttling) {
            stat_.start_throttling();
            PALF_LOG(INFO, "[LOG DISK THROTTLING] [START]", KPC(this), K(THROTTLING_CHUNK_SIZE));
          }
        }
      } else {
        if (throttling_options_.need_throttling()) {
          PALF_LOG(INFO, "[LOG DISK THROTTLING] [STOP]", KPC(this), K(THROTTLING_CHUNK_SIZE));
          clean_up_not_guarded_by_lock_();
          stat_.stop_throttling();
        }
      }
    }
  } else {
    if (throttling_options_.need_throttling()) {
      PALF_LOG(INFO, "[LOG DISK THROTTLING] [STOP] no need throttling any more", KPC(this), K(THROTTLING_CHUNK_SIZE));
      clean_up_not_guarded_by_lock_();
      stat_.stop_throttling();
    }
  }
  if (OB_SUCC(ret)) {
    last_update_ts_ = cur_ts;
  }
  return ret;
}

void LogWritingThrottle::clean_up_not_guarded_by_lock_()
{
  //do not reset submitted_seq_  && handled_seq_ && last_update_ts_ && stat_
  appended_log_size_cur_round_ = 0;
  decay_factor_ = 0;
  throttling_options_.reset();
}

bool LogWritingThrottle::check_need_update_throtting_options_guarded_by_lock_()
{
  SpinLockGuard guard(lock_);
  int64_t cur_ts = ObClockGenerator::getClock();
  return cur_ts > last_update_ts_ + UPDATE_INTERVAL_US;
}
} // end namespace palf
} // end namespace oceanbase
