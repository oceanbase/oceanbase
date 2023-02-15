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

#ifndef OB_BG_THREAD_MONITOR_H
#define OB_BG_THREAD_MONITOR_H
#include "lib/task/ob_timer.h"              // ObTimerTask
#include "lib/container/ob_fixed_array.h"   // ObFixedArray
#include "lib/allocator/ob_malloc.h"        // ObMalloc
#include "lib/lock/ob_spin_lock.h"          // ObSpinLock
#include "common/ob_i_callback.h"           // ObICallback
#include "share/ob_thread_pool.h"           // ObTheadPool

namespace oceanbase {
namespace share {
using namespace common;

class ObTSIBGMonitorSlotInfo {
public:
  ObTSIBGMonitorSlotInfo() : slot_idx_(-1) {}
  ~ObTSIBGMonitorSlotInfo() { slot_idx_ = -1; }
  void set_slot_idx(const int64_t slot_idx)
  {
    if (-1 == slot_idx_) {
      slot_idx_ = slot_idx;
    }
  }
  int64_t get_slot_idx() const
  {
    return slot_idx_;
  }
  bool need_register() const
  {
    return -1 == slot_idx_;
  }
private:
  int64_t slot_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObTSIBGMonitorSlotInfo);
};

#define BG_NEW_CALLBACK(PTR, CLASS, args...)                                    \
  do {                                                                          \
    int ret = OB_SUCCESS;                                                       \
    int64_t size = static_cast<int64_t>(sizeof(CLASS));                         \
    ObTSIBGMonitorMemory *bg_memory = GET_TSI(ObTSIBGMonitorMemory);            \
    if (OB_UNLIKELY(OB_ISNULL(bg_memory))) {                                    \
      SHARE_LOG(ERROR, "GET_TSI for ObTSIBGMonitorMemory failed", K(ret));      \
    } else if (false == bg_memory->check_has_enough_memory(size)) {             \
      SHARE_LOG(ERROR, "memory overflow, cann't monitor it");                   \
    }else if (OB_FAIL(bg_memory->pin_memory())) {                               \
      SHARE_LOG(ERROR, "ObTSIBGMonitorMemory pin_memory failed", K(ret));       \
    } else {                                                                    \
      PTR = new(bg_memory->get_ptr())CLASS(args);                               \
      PTR->set_class_size(size);                                                \
      bg_memory->add_size(size);                                                \
    }                                                                           \
  } while(0);

#define BG_DELETE_CALLBACK(PTR)                                                 \
  do {                                                                          \
    ObTSIBGMonitorMemory *bg_memory = GET_TSI(ObTSIBGMonitorMemory);            \
    if (OB_UNLIKELY(OB_ISNULL(bg_memory))) {                                    \
      SHARE_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "GET_TSI for ObTSIBGMonitorMemory failed");              \
    } else {                                                                    \
      bg_memory->sub_size(PTR->get_class_size());                               \
    }                                                                           \
  } while(0);

// @brief Used to alloc callback
class ObTSIBGMonitorMemory {
public:
  ObTSIBGMonitorMemory() : ptr_(NULL), pos_(0) {}
  ~ObTSIBGMonitorMemory();
  int pin_memory();
  void unpin_memory();
  char *get_ptr() { return ptr_+pos_; }
  void add_size(const int64_t size) {pos_ +=(size);}
  void sub_size(const int64_t size) {pos_ -=(size);}
  bool check_has_enough_memory(const int64_t size) const
  { return pos_ + size <= MEMORY_SIZE; }
  int64_t get_curr_size() const
  { return pos_; }
  static const int64_t MEMORY_SIZE = 128;
private:
  char *ptr_;
  int64_t pos_;
};

class IBGCallback : public common::ObICallback {
public:
  void set_class_size(const int64_t size)
  { size_ = size; }
  int64_t get_class_size() const
  { return size_; }
  virtual void destroy() = 0;
protected:
  int64_t size_;
};

class MonitorCallbackWrapper {
public:
  MonitorCallbackWrapper();
  MonitorCallbackWrapper(common::ObICallback *callback, bool is_idempotent);
  ~MonitorCallbackWrapper();
  int handle_callback();
  void reset();
private:
  bool is_idempotent_;
  bool has_called_;
  common::ObICallback *callback_;
};

class BGDummyCallback : public IBGCallback {
public:
  BGDummyCallback() : function_name_() {}
  BGDummyCallback(const char *function_name) : function_name_(function_name) {}
  virtual ~BGDummyCallback() {}
  void set_function_name(const common::ObString &function_name)
  { function_name_ = function_name; }
  int callback() final
  {
    int ret = common::OB_SUCCESS;
    SHARE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "BGDummyCallback cost too much time-->", K(function_name_));
    return ret;
  }
  void destroy()
  {
  }
private:
  common::ObString function_name_;
};

struct MonitorEntry {
  MonitorEntry();
  ~MonitorEntry();
  void set(const int64_t start_ts,
           const int64_t warn_ts,
           const MonitorCallbackWrapper &callback);
  void reset();
  bool check_is_cost_too_much_time(const int64_t current_ts) const;
  int callback();
  int64_t start_ts_;
  int64_t warn_ts_;
  int64_t physical_thread_idx_;
  int64_t last_handle_callback_ts_;
  MonitorCallbackWrapper callback_;
  TO_STRING_KV(K(start_ts_), K(warn_ts_), K(physical_thread_idx_));
};

class MonitorEntryStack {
public:
  MonitorEntryStack();
  ~MonitorEntryStack();
  int push(const int64_t start_ts,
           const int64_t warn_ts,
           const MonitorCallbackWrapper &callback);
  void pop();
  void check_and_handle_timeout_task(const int64_t current_ts);
private:
  static const int64_t NEST_LIMIT = 5;
  int64_t curr_idx_;
  MonitorEntry inner_entry_[NEST_LIMIT];

  typedef common::ObSpinLock Lock;
  typedef lib::ObLockGuard<Lock> LockGuard;
  Lock lock_;
};

class ObBGThreadMonitor : public share::ObThreadPool {
public:
  ObBGThreadMonitor();
  ~ObBGThreadMonitor();
  int init();
  void run1() final;
  int start();
  void wait();
  void stop();
  void destroy();
public:
  int set(const int64_t start_ts,
          const int64_t warn_ts,
          const MonitorCallbackWrapper &callback);
  void reset();
  static ObBGThreadMonitor &get_instance();
public:
  const int64_t MONITOR_LIMIT = 500;
  const int64_t CHECK_INTERVAL = 1000*1000;
private:
  void run_loop_();
  int register_slot_idx_(ObTSIBGMonitorSlotInfo *&slot_info);
  int get_slot_idx_(int64_t &slot_idx);
private:
  bool is_inited_;
  int64_t seq_;
  MonitorEntryStack* monitor_entry_stack_;
  common::ObMalloc allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBGThreadMonitor);
};

// Brief: Monitor functions which runs timeout, callers can provide
//        a callback.
//
// Attention: Caller must overwrite destroy function, and do something like
// clean context after MonitorGuard finish.
//
// Usage:
// 1. Caller should provide a callback which inheritance IBGCallback;
//    For example,
//    class PLSMajorityTimeoutCB : IBGCallback {
//    public:
//      explicit PLSMajorityTimeoutCB(ObPartitionLogService *pls) : host_(pls) {}
//      int callback()
//      {
//        int ret = OB_SUCCESS;
//        if (OB_UNLIKELY(!OB_ISNULL(host_))) {
//          ret = OB_NOT_INIT;
//        } else {
//          ret = host_->callback();
//        }
//        return ret;
//      }
//    private:
//      ObPartitionLogService *host_;
//    };
//    Callers should ensure that callback() must be memory-safe;
//    We advise caller use this like PLSMajorityTimeoutCB.
//
// 2. When caller wants to monitor some function, just add code like this:
//    const int64_t warn_ts = 1000*1000;
//    bool is_idempotent = true;
//    PLSMajorityTimeoutCB *cb = NULL;
//    BG_NEW_CALLBACK(cb, PLSMajorityTimeoutCB, this);
//    MonitorGuard(warn_ts, is_idempotent, cb);
//
//    We also provide a dummy callcall names BGDummyCallback, the callback just
//    only print a log, should add code like this:
//
//    function_name is const char*, don't use char[]
//    MonitorGuard guard(warn_ts, function_name);
//
// 3. To simplify the use process, provide macro, users just add code like this:
//    const int64_t warn_ts = 1000*1000;
//    BG_MONITOR_GUARD_DEFAULT(warn_ts);
//
//    Or using a custom callback like this:
//    const bool is_idempotent = false;
//    PLSMajorityTimeoutCB *cb = NULL;
//    // ptr is point to ObPartitionLogService
//    BG_NEW_CALLBACK(cb, PLSMajorityTimeoutCB, ptr);
//    BG_MONITOR_GUARD(warn_ts, is_idempotent, cb);
//
class MonitorGuard {
public:
  explicit MonitorGuard(const int64_t warn_ts,
                        const char *function_name);
  explicit MonitorGuard(const int64_t warn_ts,
                        const bool is_idempotent,
                        IBGCallback *callback);
  ~MonitorGuard();
private:
  int ret_code_;
  IBGCallback *callback_;
private:
  DISALLOW_COPY_AND_ASSIGN(MonitorGuard);
};

#define BG_MONITOR_GUARD_DEFAULT(warn_ts) MonitorGuard __default_guard(warn_ts, __func__)
#define BG_MONITOR_GUARD(warn_ts, is_idempotent, callback) MonitorGuard __guard(warn_ts, is_idempotent, callback)

}
}
#endif
