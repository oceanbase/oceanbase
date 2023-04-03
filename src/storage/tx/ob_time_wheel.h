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

#ifndef OCEANBASE_COMMON_OB_TIME_WHEEL_
#define OCEANBASE_COMMON_OB_TIME_WHEEL_

#include <stdint.h>
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/list/ob_dlist.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "share/ob_define.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace common
{

class ObTimeWheelTask : public ObDLinkBase<ObTimeWheelTask>
{
public:
  static const int64_t INVALID_BUCKET_IDX = -1;
public:
  ObTimeWheelTask() : lock_() { reset(); }
  virtual ~ObTimeWheelTask() { destroy(); }
  void reset();
  void destroy() {}
public:
  void lock() { (void)lock_.lock(); }
  int trylock() const
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!lock_.try_lock())) {
      ret = common::OB_EAGAIN;
    }
    return ret;
  }
  void unlock() { (void)lock_.unlock(); }
  int schedule(const int64_t bucket_idx, const int64_t run_ticket);
  int cancel();
  int64_t get_bucket_idx() const { return bucket_idx_; }
  int64_t get_run_ticket() const { return run_ticket_; }
  void set_scan_ticket(const int64_t ticket) { scan_ticket_ = ticket; }
  int64_t get_scan_ticket() const { return scan_ticket_; }
  bool is_scheduled() const { return is_scheduled_; }
  void runTask();
public:
  virtual void runTimerTask() = 0;
  virtual uint64_t hash() const = 0;
  virtual void begin_run() {}
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
protected:
  int64_t magic_number_;
  int64_t bucket_idx_;
  int64_t run_ticket_;
  int64_t scan_ticket_;
  bool is_scheduled_;
  mutable common::ObByteLock lock_;
};

typedef ObDList<ObTimeWheelTask> TaskList;

class TaskBucket
{
public:
  TaskBucket() : lock_() {}
  ~TaskBucket() {}

  void lock() { (void)lock_.lock(); }
  void unlock() { (void)lock_.unlock(); }
public:
  TaskList list_;
private:
  mutable common::ObByteLock lock_;
} CACHE_ALIGNED;

class TimeWheelBase
    : public share::ObThreadPool
{
public:
  TimeWheelBase() : is_inited_(false), precision_(1),
      start_ticket_(0), scan_ticket_(0) {}
  ~TimeWheelBase() { destroy(); }
  int init(const int64_t precision, const char *name);
public:
  void run1() final;

  int schedule(ObTimeWheelTask *task, const int64_t delay);
  int cancel(ObTimeWheelTask *task);
public:
  static TimeWheelBase *alloc(const char *name);
  static void free(TimeWheelBase *base);
private:
  int schedule_(ObTimeWheelTask *task, const int64_t run_ticket);
  int scan();
private:
  static const int64_t MAX_BUCKET = 10000;
  // scaner max sleep 1000000us
  static const int64_t MAX_SCAN_SLEEP = 1000000;
  static const int64_t MAX_TIMER_NAME_LEN = 16;
private:
  bool is_inited_;
  TaskBucket buckets_[MAX_BUCKET];
  int64_t precision_;
  int64_t start_ticket_;
  int64_t scan_ticket_;
  char tname_[MAX_TIMER_NAME_LEN];
};

class ObTimeWheel
{
public:
  ObTimeWheel() { reset(); }
  ~ObTimeWheel() { destroy(); }
  int init(const int64_t precision, const int64_t real_thread_num, const char *name);
  void reset();
  int start();
  int stop();
  int wait();
  void destroy();
  bool is_running() { return is_running_; }
public:
  int schedule(ObTimeWheelTask *task, const int64_t delay);
  int cancel(ObTimeWheelTask *task);
public:
  static const int64_t MAX_THREAD_NUM = 64;
private:
  static const int64_t MAX_TIMER_NAME_LEN = 16;
private:
  bool is_inited_;
  int64_t precision_;
  bool is_running_;
  int64_t real_thread_num_;
  char tname_[MAX_TIMER_NAME_LEN];
  TimeWheelBase *tw_base_[MAX_THREAD_NUM];
};

} // common
} // oceanbase

#endif
