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
#include <linux/prctl.h>
#include "lib/ob_errno.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_scond.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_utility.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/worker.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread.h"
#include "lib/thread/protected_stack_allocator.h"

using namespace oceanbase::lib;
extern "C" {
int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
void ob_pthread_join(void *ptr);
}

namespace oceanbase
{
namespace common
{
ObBaseLogWriter::ObBaseLogWriter()
  : has_stopped_(true),
    is_inited_(false),
    flush_tid_(NULL),
    log_items_(NULL),
    max_buffer_item_cnt_(0),
    log_item_push_idx_(0),
    log_item_pop_idx_(0),
    log_write_cond_(nullptr),
    log_flush_cond_(nullptr)
{
}

ObBaseLogWriter::~ObBaseLogWriter()
{
}

int ObBaseLogWriter::init(
    const ObBaseLogWriterCfg &log_cfg,
    const char *thread_name,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "BaseLogWriter");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_STDERR("The ObBaseLogWriter has been inited.\n");
  } else if (OB_UNLIKELY(!log_cfg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_STDERR("Invalid argument.\n");
  } else if (NULL == (log_items_ = (ObIBaseLogItem**) ob_malloc(sizeof(ObIBaseLogItem*) * log_cfg.max_buffer_item_cnt_,
                                                                attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_STDERR("Fail to allocate memory, max_buffer_item_cnt=%lu.\n", log_cfg.max_buffer_item_cnt_);
  } else if (0 != pthread_mutex_init(&thread_mutex_, NULL)) {
    ret = OB_ERR_SYS;
  } else if (OB_ISNULL(log_write_cond_ = OB_NEW(SimpleCond, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_STDERR("Fail to allocate memory, max_buffer_item_cnt=%lu.\n", log_cfg.max_buffer_item_cnt_);
  } else if (OB_ISNULL(log_flush_cond_ = OB_NEW(SimpleCond, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_STDERR("Fail to allocate memory, max_buffer_item_cnt=%lu.\n", log_cfg.max_buffer_item_cnt_);
  } else {
    log_item_push_idx_ = 0;
    log_item_pop_idx_ = 0;
    log_cfg_ = log_cfg;
    max_buffer_item_cnt_ = log_cfg.max_buffer_item_cnt_;
    memset((void*) log_items_, 0, sizeof(ObIBaseLogItem*) * max_buffer_item_cnt_);
    thread_name_ = thread_name;
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      LOG_STDOUT("successfully init ObBaseLogWriter\n");
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

int ObBaseLogWriter::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_STDERR("ObBaseLogWriter hasn't been inited.");
  } else if (!has_stopped_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_STDERR("ObBaseLogWriter has started");
  } else {
    has_stopped_ = false;
    if (0 != ob_pthread_create(&flush_tid_, ObBaseLogWriter::flush_log_thread, this)) {
      ret = OB_ERR_SYS;
      LOG_STDERR("Fail to create log flush thread.\n");
    }
  }
  if (OB_FAIL(ret)) {
    has_stopped_ = true;
    wait();
  }

  return ret;
}

void ObBaseLogWriter::stop()
{
  if (!has_stopped_) {
    bool is_empty = false;
    static const int64_t max_try_cnt = 100;
    int64_t try_cnt = max_try_cnt;
    LOG_STDOUT("Stop thread\n");

    while (!is_empty && try_cnt > 0) {
      if (ATOMIC_LOAD(&log_item_push_idx_) - ATOMIC_LOAD(&log_item_pop_idx_) > 0) {
        log_flush_cond_->signal(1);
      } else {
        is_empty = true;
      }

      if (!is_empty) {
        ::usleep(MAX_STOP_WAIT_TIME_US / max_try_cnt);
        --try_cnt;
      }
    }
  }
  has_stopped_ = true;
}

void ObBaseLogWriter::wait()
{
  if (has_stopped_ && is_inited_) {
    if (NULL != flush_tid_) {
      ob_pthread_join(flush_tid_);
      flush_tid_ = NULL;
    }
  }
}

void ObBaseLogWriter::destroy()
{
  if (is_inited_) {
    if (!has_stopped_) {
      stop();
      wait();
    }
    log_write_cond_->signal(UINT32_MAX);
    log_flush_cond_->signal(UINT32_MAX);
  }
  is_inited_ = false;

  if (NULL != log_items_) {
    ob_free(log_items_);
    log_items_ = NULL;
  }
  if (OB_NOT_NULL(log_write_cond_)) {
    OB_DELETE(SimpleCond, "BaseLogWriter", log_write_cond_);
  }
  if (OB_NOT_NULL(log_flush_cond_)) {
    OB_DELETE(SimpleCond, "BaseLogWriter", log_flush_cond_);
  }
  max_buffer_item_cnt_ = 0;
  has_stopped_ = true;
}

int ObBaseLogWriter::append_log(ObIBaseLogItem &log_item, const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_STDERR("The ObBaseLogWriter has not been inited.\n");
  } else {
    int64_t abs_time = ObTimeUtility::current_time() + timeout_us;
    while (OB_SUCC(ret)) {
      auto key = log_write_cond_->get_key();
      int64_t push_idx = ATOMIC_LOAD(&log_item_push_idx_);
      int64_t pop_idx = ATOMIC_LOAD(&log_item_pop_idx_);
      if (push_idx - pop_idx < max_buffer_item_cnt_) {
        if (OB_LIKELY(ATOMIC_BCAS(&log_item_push_idx_, push_idx, push_idx + 1))) {
          ATOMIC_STORE(log_items_ + push_idx % max_buffer_item_cnt_, &log_item);
          if (need_flush()) {
            log_flush_cond_->signal(UINT32_MAX);
          }
          break;
        }
      } else {
        int64_t current_time = ObTimeUtility::current_time();
        if (OB_UNLIKELY(!ATOMIC_LOAD(&is_inited_))) {
          ret = OB_CANCELED;
        } else if (current_time >= abs_time && timeout_us != UINT64_MAX) {
          ret = OB_TIMEOUT;
        } else {
          log_write_cond_->wait(key, abs_time - current_time);
        }
      }
    }
  }
  return ret;
}

void cleanup_log_thread(void *arg)
{
  if (OB_ISNULL(arg)) {
    LOG_STDERR("invalid argument, arg = %p\n", arg);
  } else {
    ObBaseLogWriter *log_writer = reinterpret_cast<ObBaseLogWriter*> (arg);
    log_writer->stop();
    LOG_STDERR("async thread exited.\n");
  }
}

void *ObBaseLogWriter::flush_log_thread(void *arg)
{
  int err_code = 0;
  if (OB_ISNULL(arg)) {
    LOG_STDERR("invalid argument, arg = %p\n", arg);
  } else {
    pthread_cleanup_push(cleanup_log_thread, arg);
    ObBaseLogWriter *log_writer = reinterpret_cast<ObBaseLogWriter*> (arg);
    lib::set_thread_name(log_writer->thread_name_);
    log_writer->flush_log();
    pthread_cleanup_pop(1);
  }
  return NULL;
}

void ObBaseLogWriter::flush_log()
{
  while (!has_stopped_) {
    IGNORE_RETURN lib::Thread::update_loop_ts(ObTimeUtility::fast_current_time());
    pthread_mutex_lock(&thread_mutex_);
    // 每个线程执行16次再重新抢占, 对cpu cache hit有利
    for (int64_t i = 0; i < 16; i++) {
      do_flush_log();
    }
    pthread_mutex_unlock(&thread_mutex_);
  }
}

void ObBaseLogWriter::do_flush_log()
{
  int64_t process_item_cnt = 0;
  int64_t item_cnt = 0;
  auto key = log_flush_cond_->get_key();
  if (!need_flush()) {
    log_flush_cond_->wait(key, log_cfg_.group_commit_max_wait_us_);
  }
  while (OB_LIKELY(need_flush() && !has_stopped_)) {
    // flush log will not block append any more, so there is no need to limit process_item_cnt
    //if (process_item_cnt > log_cfg_.group_commit_max_item_cnt_) {
    //  process_item_cnt = log_cfg_.group_commit_max_item_cnt_;
    //}
    int64_t pop_idx = ATOMIC_LOAD(&log_item_pop_idx_) % max_buffer_item_cnt_;
    int64_t i = pop_idx;
    // process to the end of array at most
    while (i < max_buffer_item_cnt_
           && i - pop_idx < log_cfg_.group_commit_max_item_cnt_
           && OB_NOT_NULL(ATOMIC_LOAD(log_items_ + i))
           ) {
      ++i;
    }
    process_item_cnt = i - pop_idx;
    // guarantee all item in process was not null.
    if (process_item_cnt > 0) {
      item_cnt = 0;
      process_log_items(log_items_ + pop_idx, process_item_cnt, item_cnt);
      if (item_cnt > 0) {
        memset((void*)(log_items_+ pop_idx), 0, sizeof(ObIBaseLogItem*) * item_cnt);
        IGNORE_RETURN ATOMIC_FAA(&log_item_pop_idx_, item_cnt);
        log_write_cond_->signal(UINT32_MAX);
      }
    }
  }
}

bool ObBaseLogWriter::need_flush()
{
  return ATOMIC_LOAD(&log_item_push_idx_) - ATOMIC_LOAD(&log_item_pop_idx_) >= log_cfg_.group_commit_min_item_cnt_;
}

}
}
