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

#ifdef PCOUNTER_DEF
    PCOUNTER_DEF(MT, 0)
    PCOUNTER_DEF(MTCTX, 0)
    PCOUNTER_DEF(MTSCANI, 0)
    PCOUNTER_DEF(MTGETI, 0)
    PCOUNTER_DEF(MTGETR, 0)
    PCOUNTER_DEF(PT, 0)
    PCOUNTER_DEF(STORES, 0)
    PCOUNTER_DEF(PMSCANI, 0)
    PCOUNTER_DEF(WLOCK, 0)
    PCOUNTER_DEF(RLOCK, 0)
    PCOUNTER_DEF(RT, 0)
    PCOUNTER_DEF(END, -1)
#endif

#ifndef __PCOUNTER__
#define __PCOUNTER__

#include <pthread.h>
#include <stdarg.h>
#include <cstring>
#include <cstdio>

#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/metrics/ob_counter.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
namespace debug
{
enum
{
#define PCOUNTER_DEF(name, level) PCOUNTER_ ## name,
#include "lib/allocator/ob_pcounter.h"
#undef PCOUNTER_DEF
  PCOUNTER_COUNT = PCOUNTER_END,
};

OB_INLINE int64_t get_us()
{
  struct timeval time_val;
  IGNORE_RETURN gettimeofday(&time_val, NULL);
  return time_val.tv_sec * 1000000 + time_val.tv_usec;
}

OB_INLINE int64_t get_cpu_num()
{
  return sysconf(_SC_NPROCESSORS_ONLN);
}

class PthreadSpinLock
{
public:
  PthreadSpinLock()
  {
    IGNORE_RETURN pthread_spin_init(&lock_, false);
  }
  ~PthreadSpinLock()
  {
    IGNORE_RETURN pthread_spin_destroy(&lock_);
  }
  bool lock()
  {
    return 0 == pthread_spin_lock(&lock_);
  }
  int try_lock()
  {
    return 0 == pthread_spin_trylock(&lock_);
  }
  void unlock()
  {
    IGNORE_RETURN pthread_spin_unlock(&lock_);
  }
private:
  pthread_spinlock_t lock_;
};

struct PCounterDesc
{
  const char *name_;
  int level_;
};
inline const PCounterDesc *get_pcounter_desc(int mod)
{
  const static PCounterDesc desc[] =
  {
#define PCOUNTER_DEF(name, level) {#name, level},
#include "lib/allocator/ob_pcounter.h"
#undef PCOUNTER_DEF
  };
  return desc + mod;
}

class PCounterSet
{
public:
  PCounterSet() { memset(counters_, 0, sizeof(counters_)); }
  ~PCounterSet() {}
public:
  void set(int mod, int64_t val) { counters_[mod].set(val); }
  void add(int mod, int64_t delta) { counters_[mod].inc(delta); }
  int64_t get(int mod) { return counters_[mod].value(); }
private:
  ::oceanbase::common::ObPCCounter counters_[PCOUNTER_COUNT];
};

class Printer
{
public:
  enum { MAX_BUF_SIZE = 4096};
  Printer(): limit_(MAX_BUF_SIZE), pos_(0)
  {
    memset(buf_, 0, MAX_BUF_SIZE);
  }

  ~Printer()
  {
    pos_ = 0;
  }
  void reset()
  {
    pos_ = 0;
    *buf_ = 0;
  }
  char *get_str() { return limit_ > 0 ? buf_ : NULL; }
  char *append(const char *format, ...)
  {
    char *src = NULL;
    int64_t count = 0;
    va_list ap;
    va_start(ap, format);
    if (limit_ > 0 && pos_ < limit_
        && pos_ + (count = vsnprintf(buf_ + pos_, limit_ - pos_, format, ap)) < limit_) {
      src = buf_ + pos_;
      pos_ += count;
    }
    va_end(ap);
    return src;
  }
  char *new_str(const char *format, ...)
  {
    char *src = NULL;
    int64_t count = 0;
    va_list ap;
    va_start(ap, format);
    if (limit_ > 0 && pos_ < limit_
        && pos_ + (count = vsnprintf(buf_ + pos_, limit_ - pos_, format, ap)) + 1 < limit_) {
      src = buf_ + pos_;
      pos_ += count + 1;
    }
    va_end(ap);
    return src;
  }
private:
  char buf_[MAX_BUF_SIZE];
  int64_t limit_;
  int64_t pos_;
};

class PCounterMonitor
{
public:
  typedef PthreadSpinLock Lock;
  PCounterMonitor(PCounterSet &set, int64_t interval):
      pcounter_set_(set),
      report_interval_(interval),
      last_report_time_(0),
      report_seq_(1)
  {
    memset(counters_, 0, sizeof(counters_));
  }
  ~PCounterMonitor() {}
public:
  void report()
  {
    int64_t cur_time = get_us();
    int64_t last_report_time = last_report_time_;
    if (cur_time > last_report_time + report_interval_
        && __sync_bool_compare_and_swap(&last_report_time_, last_report_time, cur_time)
        && lock_.try_lock()) {
      Printer printer;
      int64_t cur_set_idx = report_seq_ & 1;
      int64_t last_set_idx = cur_set_idx ^ 1;
      for (int i = 0; i < PCOUNTER_COUNT; i++) {
        counters_[report_seq_ & 1][i] = pcounter_set_.get(i);
      }
      for (int i = 0; i < PCOUNTER_COUNT; i++) {
        const PCounterDesc *desc = get_pcounter_desc(i);
        counters_[cur_set_idx][i] = pcounter_set_.get(i);
        if (NULL != desc) {
          if (desc->level_ >= 0 && counters_[cur_set_idx][i] != 0) {
            printer.append("%s:%'6ld ", desc->name_,
                           desc->level_ == 1 ? (counters_[cur_set_idx][i] - counters_[last_set_idx][i]) * 1000000 /
                           (cur_time - last_report_time) : counters_[cur_set_idx][i]);
          }
        }
      }
      _OB_LOG(INFO, "PC_REPORT[%3ld] %s", report_seq_, printer.get_str());
      report_seq_++;
      lock_.unlock();
    }
  }
private:
  PCounterSet &pcounter_set_;
  Lock lock_;
  int64_t report_interval_;
  int64_t last_report_time_;
  int64_t report_seq_;
  int64_t counters_[2][PCOUNTER_COUNT];
};

inline PCounterSet &get_pcounter_set() { static PCounterSet pcounter_set; return pcounter_set; }
inline PCounterMonitor &get_pcounter_monitor() { static PCounterMonitor pcounter_monitor(get_pcounter_set(), 1000000); return pcounter_monitor; }

}; // end namespace debug
}; // end namespace common
}; // end namespace oceanbase

#define PC_ADD(mod, x) debug::get_pcounter_set().add(debug::PCOUNTER_ ## mod, x)
#define PC_REPORT() debug::get_pcounter_monitor().report()
#define PC_GET(mod) debug::get_pcounter_set().get(debug::PCOUNTER_ ## mod)

#endif
