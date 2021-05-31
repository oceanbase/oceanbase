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

#include "ob_base_log_writer.h"
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <errno.h>
#include <stdio.h>
#include <sys/prctl.h>
#include <linux/prctl.h>
#include "lib/ob_errno.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_utility.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/coro/routine.h"
#include "lib/worker.h"

using namespace oceanbase::lib;

namespace oceanbase {
namespace common {
ObBaseLogWriter::ObBaseLogWriter()
    : is_inited_(false),
      has_stoped_(true),
      flush_tid_(0),
      log_items_(NULL),
      process_items_(NULL),
      max_buffer_item_cnt_(0),
      log_item_push_idx_(0),
      log_item_pop_idx_(0)
{}

ObBaseLogWriter::~ObBaseLogWriter()
{}

int ObBaseLogWriter::init(const ObBaseLogWriterCfg& log_cfg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_STDERR("The ObBaseLogWriter has been inited.\n");
  } else if (OB_UNLIKELY(!log_cfg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_STDERR("Invalid argument.\n");
  } else if (NULL == (log_items_ = (ObIBaseLogItem**)malloc(sizeof(ObIBaseLogItem*) * log_cfg.max_buffer_item_cnt_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_STDERR("Fail to allocate memory, max_buffer_item_cnt=%lu.\n", log_cfg.max_buffer_item_cnt_);
  } else if (NULL ==
             (process_items_ = (ObIBaseLogItem**)malloc(sizeof(ObIBaseLogItem*) * log_cfg.max_buffer_item_cnt_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_STDERR("Fail to allocate memory, max_buffer_item_cnt=%lu.\n", log_cfg.max_buffer_item_cnt_);
  } else if (0 != pthread_mutex_init(&log_mutex_, NULL)) {
    ret = OB_ERR_SYS;
  } else if (0 != pthread_cond_init(&log_write_cond_, NULL)) {
    ret = OB_ERR_SYS;
  } else if (0 != pthread_cond_init(&log_flush_cond_, NULL)) {
    ret = OB_ERR_SYS;
  } else {
    flush_tid_ = 0;
    log_item_push_idx_ = 0;
    log_item_pop_idx_ = 0;
    log_cfg_ = log_cfg;
    has_stoped_ = false;
    max_buffer_item_cnt_ = log_cfg.max_buffer_item_cnt_;
    memset((void*)log_items_, 0, sizeof(ObIBaseLogItem*) * max_buffer_item_cnt_);
    memset((void*)process_items_, 0, sizeof(ObIBaseLogItem*) * max_buffer_item_cnt_);

    pthread_attr_t attr;
    int err_code = 0;
    if (OB_UNLIKELY(0 != (err_code = pthread_attr_init(&attr)))) {
      ret = OB_ERR_SYS;
      LOG_STDERR("failed to pthread_attr_init, err_code=%d.\n", err_code);
      // PTHREAD_SCOPE_SYSTEM: The thread competes for resources with all other threads in
      //                      all processes on the system that are in the same scheduling
      //                      allocation domain (a group of one or more processors).
      // PTHREAD_SCOPE_PROCESS: The thread competes for resources with all other threads in
      //                       the same process that were also created with the
      //                       PTHREAD_SCOPE_PROCESS contention scope
    } else if (OB_UNLIKELY(0 != (err_code = pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM)))) {
      ret = OB_ERR_SYS;
      LOG_STDERR("failed to pthread_attr_setscope, err_code=%d.\n", err_code);
    } else if (OB_UNLIKELY(0 != pthread_create(&flush_tid_, NULL, ObBaseLogWriter::flush_log_thread, this))) {
      ret = OB_ERR_SYS;
      LOG_STDERR("Fail to create log flush thread.\n");
    } else {
      is_inited_ = true;
      LOG_STDOUT("Success to create thread %ld.\n", flush_tid_);
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObBaseLogWriter::destroy()
{
  is_inited_ = false;
  if (0 != flush_tid_) {
    bool is_empty = false;
    static const int64_t max_try_cnt = 100;
    int64_t try_cnt = max_try_cnt;
    LOG_STDOUT("Stop thread, %ld.\n", flush_tid_);

    while (!is_empty && try_cnt > 0) {
      pthread_mutex_lock(&log_mutex_);
      if (log_item_push_idx_ - log_item_pop_idx_ > 0) {
        pthread_cond_signal(&log_flush_cond_);
      } else {
        is_empty = true;
      }
      pthread_mutex_unlock(&log_mutex_);

      if (!is_empty) {
        this_routine::usleep(MAX_STOP_WAIT_TIME_US / max_try_cnt);
        --try_cnt;
      }
    }

    has_stoped_ = true;
    pthread_join(flush_tid_, NULL);
    flush_tid_ = 0;

    pthread_mutex_destroy(&log_mutex_);
    pthread_cond_destroy(&log_write_cond_);
    pthread_cond_destroy(&log_flush_cond_);
  }

  if (NULL != log_items_) {
    free(log_items_);
    log_items_ = NULL;
  }
  if (NULL != process_items_) {
    free(process_items_);
    process_items_ = NULL;
  }
  max_buffer_item_cnt_ = 0;
}

int ObBaseLogWriter::append_log(ObIBaseLogItem& log_item, const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  struct timespec abstime;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_STDERR("The ObBaseLogWriter has not been inited.\n");
  } else if (OB_FAIL(ob_get_abs_timeout(timeout_us, abstime))) {
    LOG_STDERR("Failed to get abstime.\n");
  } else {
    pthread_mutex_lock(&log_mutex_);
    while (OB_SUCC(ret) && log_item_push_idx_ - log_item_pop_idx_ >= max_buffer_item_cnt_) {
      if (ETIMEDOUT == pthread_cond_timedwait(&log_write_cond_, &log_mutex_, &abstime)) {
        ret = OB_TIMEOUT;
      } else if (!is_inited_) {
        ret = OB_CANCELED;
      }
    }

    if (OB_SUCC(ret)) {
      log_items_[log_item_push_idx_ % max_buffer_item_cnt_] = &log_item;
      ++log_item_push_idx_;
      if (need_flush()) {
        pthread_cond_signal(&log_flush_cond_);
      }
    }
    pthread_mutex_unlock(&log_mutex_);
  }
  return ret;
}

int ObBaseLogWriter::reconfig(const ObBaseLogWriterCfg& log_cfg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_STDERR("The ObBaseLogWriter has not been inited.\n");
  } else if (OB_UNLIKELY(!log_cfg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_STDERR("Invalid argument!\n");
  } else if (OB_UNLIKELY(log_cfg.max_buffer_item_cnt_ != max_buffer_item_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_STDERR("The max buffer item cnt CAN NOT be changed!\n");
  } else {
    pthread_mutex_lock(&log_mutex_);
    log_cfg_ = log_cfg;
    pthread_mutex_unlock(&log_mutex_);
  }
  return ret;
}

void ObBaseLogWriter::wait()
{
  if (0 != flush_tid_ && has_stoped_) {
    pthread_join(flush_tid_, NULL);
    flush_tid_ = 0;
  }
}

void cleanup_log_thread(void* arg)
{
  if (OB_ISNULL(arg)) {
    LOG_STDERR("invalid argument, arg = %p\n", arg);
  } else {
    ObBaseLogWriter* log_writer = reinterpret_cast<ObBaseLogWriter*>(arg);
    log_writer->set_stoped();
    LOG_STDERR("async thread exited.\n");
  }
}

void* ObBaseLogWriter::flush_log_thread(void* arg)
{
  int err_code = 0;
  if (OB_ISNULL(arg)) {
    LOG_STDERR("invalid argument, arg = %p\n", arg);
  } else if (OB_UNLIKELY(0 != (err_code = prctl(PR_SET_NAME, "OB_ALOG", 0, 0, 0)))) {
    LOG_STDERR("failed to prctl PR_SET_NAME, ret=%d.\n", err_code);
  } else {
    pthread_cleanup_push(cleanup_log_thread, arg);
    ObBaseLogWriter* log_writer = reinterpret_cast<ObBaseLogWriter*>(arg);
    void* buf = reinterpret_cast<void*>(lib::get_default_runtime_context());
    new (buf) lib::ObRuntimeContext();
    log_writer->flush_log();
    pthread_cleanup_pop(1);
  }
  return NULL;
}

void ObBaseLogWriter::flush_log()
{
  int64_t process_item_cnt = 0;
  int64_t finish_item_cnt = 0;
  int64_t item_cnt = 0;
  int64_t start_idx = 0;
  int64_t end_idx = 0;
  struct timespec group_wait_abstime;
  while (!has_stoped_) {
    ob_get_abs_timeout(log_cfg_.group_commit_max_wait_us_, group_wait_abstime);
    pthread_mutex_lock(&log_mutex_);
    if (!need_flush()) {
      (void)pthread_cond_timedwait(&log_flush_cond_, &log_mutex_, &group_wait_abstime);
    }

    if ((process_item_cnt = log_item_push_idx_ - log_item_pop_idx_) > 0) {
      if (process_item_cnt > log_cfg_.group_commit_max_item_cnt_) {
        process_item_cnt = log_cfg_.group_commit_max_item_cnt_;
      }
      start_idx = log_item_pop_idx_ % max_buffer_item_cnt_;
      end_idx = (log_item_pop_idx_ + process_item_cnt) % max_buffer_item_cnt_;
      if (end_idx > start_idx) {
        memcpy((void*)process_items_, (void*)(log_items_ + start_idx), process_item_cnt * sizeof(ObIBaseLogItem*));
      } else {
        memcpy((void*)process_items_,
            (void*)(log_items_ + start_idx),
            (max_buffer_item_cnt_ - start_idx) * sizeof(ObIBaseLogItem*));
        memcpy((void*)(process_items_ + max_buffer_item_cnt_ - start_idx),
            (void*)log_items_,
            end_idx * sizeof(ObIBaseLogItem*));
      }
      log_item_pop_idx_ += process_item_cnt;
      pthread_cond_broadcast(&log_write_cond_);
    }
    pthread_mutex_unlock(&log_mutex_);

    if (process_item_cnt > 0) {
      finish_item_cnt = 0;
      while (process_item_cnt > 0 && !has_stoped_) {
        item_cnt = 0;
        process_log_items(process_items_ + finish_item_cnt, process_item_cnt, item_cnt);
        if (item_cnt > 0) {
          process_item_cnt -= item_cnt;
          finish_item_cnt += item_cnt;
        }
      }
    }
  }
}

bool ObBaseLogWriter::need_flush()
{
  return log_item_push_idx_ - log_item_pop_idx_ >= log_cfg_.group_commit_min_item_cnt_;
}

}  // namespace common
}  // namespace oceanbase
