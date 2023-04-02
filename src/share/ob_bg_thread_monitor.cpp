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

#include "ob_bg_thread_monitor.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/atomic/ob_atomic.h"                 // ATOMIC_STORE
#include "lib/thread/ob_thread_name.h"            // set_thread_name
#include "common/ob_i_callback.h"                 // ObICallback
#include "common/ob_clock_generator.h"            // ObClockGenerator

namespace oceanbase {
namespace share {
using namespace common;

ObTSIBGMonitorMemory::~ObTSIBGMonitorMemory()
{
  unpin_memory();
}

int ObTSIBGMonitorMemory::pin_memory()
{
  int ret = OB_SUCCESS;
  if (NULL == ptr_
      && NULL == (ptr_ = static_cast<char *>(ob_malloc(MEMORY_SIZE, "BGMonitor")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

void ObTSIBGMonitorMemory::unpin_memory()
{
  if (NULL != ptr_) {
    ob_free(ptr_);
    ptr_ = NULL;
  }
}

MonitorCallbackWrapper::MonitorCallbackWrapper() : is_idempotent_(false),
                                                   has_called_(false),
                                                   callback_(NULL) {}

MonitorCallbackWrapper::MonitorCallbackWrapper(common::ObICallback *callback,
                                               bool is_idempotent) : is_idempotent_(is_idempotent),
                                                                     has_called_(false),
                                                                     callback_(callback)
{}

MonitorCallbackWrapper::~MonitorCallbackWrapper()
{
  is_idempotent_ = false;
  has_called_ = false;
  callback_ = NULL;
}

int MonitorCallbackWrapper::handle_callback()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(callback_)) {
    // do_nothing
  } else if (false == is_idempotent_
             && true == has_called_) {
  } else if (!FALSE_IT(has_called_ = true)
              && OB_FAIL(callback_->callback())) {
    SHARE_LOG(ERROR, "execute callback failed", K(ret));
  }
  return ret;
}

void MonitorCallbackWrapper::reset()
{
  has_called_ = false;
  is_idempotent_ = false;
  callback_ = NULL;
}

MonitorEntry::MonitorEntry() : start_ts_(OB_INVALID_TIMESTAMP),
                               warn_ts_(OB_INVALID_TIMESTAMP),
                               physical_thread_idx_(-1),
                               last_handle_callback_ts_(OB_INVALID_TIMESTAMP),
                               callback_()
{}

void MonitorEntry::set(const int64_t start_ts,
                       const int64_t warn_ts,
                       const MonitorCallbackWrapper &callback)
{
  start_ts_ = start_ts;
  warn_ts_ = warn_ts;
  physical_thread_idx_ = ob_gettid();
  last_handle_callback_ts_ = OB_INVALID_TIMESTAMP;
  callback_ = callback;
}

void MonitorEntry::reset()
{
  start_ts_ = OB_INVALID_TIMESTAMP;
  warn_ts_ = OB_INVALID_TIMESTAMP;
  physical_thread_idx_ = -1;
  last_handle_callback_ts_ = OB_INVALID_TIMESTAMP;
  callback_.reset();
}

MonitorEntry::~MonitorEntry()
{
  reset();
}

bool MonitorEntry::check_is_cost_too_much_time(const int64_t current_ts) const
{
  bool bool_ret = false;
  if (OB_INVALID_TIMESTAMP == start_ts_) {
    // do nothing
  } else if (OB_INVALID_TIMESTAMP == last_handle_callback_ts_
      && start_ts_ + warn_ts_ <= current_ts) {
    bool_ret = true;
  } else if (OB_INVALID_TIMESTAMP != last_handle_callback_ts_
      && last_handle_callback_ts_ + warn_ts_ <= current_ts) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int MonitorEntry::callback()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(callback_.handle_callback())) {
    SHARE_LOG(ERROR, "handle_callback failed", K(ret), K(*this));
  }
  return ret;
}

MonitorEntryStack::MonitorEntryStack() : curr_idx_(0),
                                         inner_entry_(),
                                         lock_(ObLatchIds::BG_THREAD_MONITOR_LOCK)
{
}

MonitorEntryStack::~MonitorEntryStack()
{
}

int MonitorEntryStack::push(const int64_t start_ts,
                            const int64_t warn_ts,
                            const MonitorCallbackWrapper &callback)
{
  int ret = OB_SUCCESS;
  LockGuard guard(lock_);
  if (curr_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
  } else if (curr_idx_ >= NEST_LIMIT) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    inner_entry_[curr_idx_++].set(start_ts, warn_ts, callback);
  }
  return ret;
}

void MonitorEntryStack::pop()
{
  LockGuard guard(lock_);
  if (curr_idx_ <= 0 || curr_idx_ > NEST_LIMIT) {
    SHARE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected error, code mustn't go into this branch");
  } else {
    inner_entry_[--curr_idx_].reset();
  }
}

void MonitorEntryStack::check_and_handle_timeout_task(const int64_t current_ts)
{
  int ret = OB_SUCCESS;
  for (int64_t i = NEST_LIMIT - 1; i >= 0; i--) {
    MonitorEntry &inner_entry = inner_entry_[i];
    if (true == inner_entry.check_is_cost_too_much_time(current_ts)) {
      LockGuard guard(lock_);
      if (true == inner_entry.check_is_cost_too_much_time(current_ts)) {
        if (OB_FAIL(inner_entry.callback())) {
          SHARE_LOG(ERROR, "inner_entry callback failed", K(ret));
        } else {
          const int64_t cost_ts = current_ts - inner_entry.start_ts_;
          // set hanlde callback timestamp
          inner_entry.last_handle_callback_ts_ = current_ts;
          SHARE_LOG(WARN, "timeout entry status", K(inner_entry),
                    K(current_ts), K(cost_ts));
        }
      }
    }
  }
}

ObBGThreadMonitor::ObBGThreadMonitor() : is_inited_(false), seq_(-1),
                                         monitor_entry_stack_(NULL),
                                         allocator_("BGTMonitor")
{
}

ObBGThreadMonitor::~ObBGThreadMonitor()
{
}

int ObBGThreadMonitor::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(ERROR, "ObBGThreadMonitor is inited twice");
  } else {
    monitor_entry_stack_ = static_cast<MonitorEntryStack *>(allocator_.alloc(
                     MONITOR_LIMIT*sizeof(MonitorEntryStack)));
    if (OB_UNLIKELY(OB_ISNULL(monitor_entry_stack_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t i = 0; i < MONITOR_LIMIT; i++) {
        new(&monitor_entry_stack_[i]) MonitorEntryStack();
      }
      is_inited_ = true;
    }
    SHARE_LOG(INFO, "init ObBGThreadMonitor success", K(ret), K(MONITOR_LIMIT));
  }
  return ret;
}

void ObBGThreadMonitor::run1()
{
  lib::set_thread_name("BGThreadMonitor");
  run_loop_();
}

int ObBGThreadMonitor::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    SHARE_LOG(ERROR, "ObBGThreadMonitor thread failed to start", K(ret));
  }
  return ret;
}

void ObBGThreadMonitor::wait()
{
  SHARE_LOG(INFO, "ObBGThreadMonitor wait");
  share::ObThreadPool::wait();
}

void ObBGThreadMonitor::stop()
{
  SHARE_LOG(INFO, "ObBGThreadMonitor stop");
  share::ObThreadPool::stop();
}

void ObBGThreadMonitor::destroy()
{
  SHARE_LOG(INFO, "ObBGThreadMonitor destroy");
  stop();
  wait();
  if (NULL != monitor_entry_stack_) {
    for (int64_t i = 0; i < MONITOR_LIMIT; i++) {
      monitor_entry_stack_[i].~MonitorEntryStack();
    }
    allocator_.free(monitor_entry_stack_);
    monitor_entry_stack_ = NULL;
  }
  share::ObThreadPool::destroy();
  is_inited_ = false;
}

int ObBGThreadMonitor::set(const int64_t start_ts,
                           const int64_t warn_ts,
                           const MonitorCallbackWrapper &callback)
{
  int ret = OB_SUCCESS;
  int64_t slot_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_slot_idx_(slot_idx))
             && OB_SIZE_OVERFLOW != ret) {
    SHARE_LOG(ERROR, "get_slot_idx_ failed", K(ret));
    // over monitor limit
  } else if (OB_SIZE_OVERFLOW == ret) {
    SHARE_LOG(INFO, "ObBGThreadMonitor set overflow", K(ret));
  } else if (OB_FAIL(monitor_entry_stack_[slot_idx].push(start_ts, warn_ts, callback))
             && OB_SIZE_OVERFLOW != ret) {
    SHARE_LOG(ERROR, "MonitorEntryStack set failed", K(ret));
  } else if (OB_SIZE_OVERFLOW == ret) {
    SHARE_LOG(INFO, "MonitorEntryStack set overflow", K(ret));
  }
  return ret;
}

void ObBGThreadMonitor::reset()
{
  int ret = OB_SUCCESS;
  int64_t slot_idx = -1;
  ObTSIBGMonitorSlotInfo *slot_info = GET_TSI(ObTSIBGMonitorSlotInfo);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_ISNULL(slot_info))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(get_slot_idx_(slot_idx))
             && OB_SIZE_OVERFLOW != ret) {
    SHARE_LOG(ERROR, "get_slot_idx_ failed", K(ret));
  } else if (OB_SIZE_OVERFLOW == ret) {
    SHARE_LOG(ERROR, "unexpected error, mustn't go into this branch", K(ret));
  } else {
    monitor_entry_stack_[slot_idx].pop();
  }
}

ObBGThreadMonitor &ObBGThreadMonitor::get_instance()
{
  static ObBGThreadMonitor global_monitor;
  return global_monitor;
}

void ObBGThreadMonitor::run_loop_()
{
  while (!has_set_stop()) {
    if (REACH_TIME_INTERVAL(1000*1000)) {
      SHARE_LOG(INFO, "current monitor number", K(seq_));
    }
    int64_t current_ts = common::ObClockGenerator::getClock();
    for (int64_t i = 0; i < MONITOR_LIMIT; i++) {
      monitor_entry_stack_[i].check_and_handle_timeout_task(current_ts);
    }
    int64_t cost_time = common::ObClockGenerator::getClock() - current_ts;
    if (cost_time > 100*1000) {
      SHARE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "ObBGThreadMonitor cost too much time", K(cost_time));
    }
    int32_t sleep_time = static_cast<int32_t>(CHECK_INTERVAL - cost_time);
    if (sleep_time < 0) {
      sleep_time = 0;
    }
    ob_usleep(sleep_time);
  }
}

int ObBGThreadMonitor::register_slot_idx_(ObTSIBGMonitorSlotInfo *&slot_info)
{
  int ret = OB_SUCCESS;
  int64_t slot_idx = -1;
  slot_info = GET_TSI(ObTSIBGMonitorSlotInfo);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_ISNULL(slot_info))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (true == slot_info->need_register()) {
    slot_idx = ATOMIC_AAF(&seq_, 1);
    slot_info->set_slot_idx(slot_idx);
  }
  return ret;
}

int ObBGThreadMonitor::get_slot_idx_(int64_t &slot_idx)
{
  int ret = OB_SUCCESS;
  ObTSIBGMonitorSlotInfo *slot_info = NULL;
  if (OB_FAIL(register_slot_idx_(slot_info))) {
    SHARE_LOG(ERROR, "register_slot_idx_ failed", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(slot_info))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    slot_idx = slot_info->get_slot_idx();
    if (slot_idx < 0) {
      ret = OB_ERR_UNEXPECTED;
    } else if (slot_idx >= MONITOR_LIMIT) {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

MonitorGuard::MonitorGuard(const int64_t warn_ts,
                           const char *function_name) :
                           ret_code_(OB_SUCCESS),
                           callback_(NULL)
{
  int64_t start_ts = ObClockGenerator::getClock();
  BG_NEW_CALLBACK(callback_, BGDummyCallback, function_name);
  MonitorCallbackWrapper wrapper(callback_, true);
  ret_code_ = ObBGThreadMonitor::get_instance().set(start_ts, warn_ts, wrapper);
}

MonitorGuard::MonitorGuard(const int64_t warn_ts,
                           const bool is_idempotent,
                           IBGCallback *callback) :
                           ret_code_(OB_SUCCESS),
                           callback_(callback)
{
  int64_t start_ts = ObClockGenerator::getClock();
  MonitorCallbackWrapper wrapper(callback, is_idempotent);
  ret_code_ = ObBGThreadMonitor::get_instance().set(start_ts, warn_ts, wrapper);
}

MonitorGuard::~MonitorGuard()
{
  if (ret_code_ == OB_SUCCESS) {
    ObBGThreadMonitor::get_instance().reset();
  }
  if (NULL != callback_) {
    callback_->destroy();
    BG_DELETE_CALLBACK(callback_);
  }
}
}
}
