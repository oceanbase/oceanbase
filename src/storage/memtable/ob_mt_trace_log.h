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

#ifndef _OB_MT_TRACE_LOG_H_
#define _OB_MT_TRACE_LOG_H_

#include "share/ob_define.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase {
namespace memtable {
class ObMTTraceLog {
public:
  enum { TRACE_LOG_BUF_SIZE = 16L * 1024L };
  ObMTTraceLog() : start_log_ts_(0), last_log_ts_(0), pos_(0)
  {}
  ~ObMTTraceLog()
  {}
  void reset()
  {
    start_log_ts_ = 0;
    last_log_ts_ = 0;
    pos_ = 0;
  }
  int64_t update_ts()
  {
    int64_t cur_ts = ::oceanbase::common::ObTimeUtility::current_time();
    if (start_log_ts_ <= 0) {
      start_log_ts_ = cur_ts;
      last_log_ts_ = cur_ts;
    }
    int64_t ret_ts = cur_ts - last_log_ts_;
    last_log_ts_ = cur_ts;
    return ret_ts;
  }
  void fill(const char* format, ...) __attribute__((format(printf, 2, 3)))
  {
    int tmp_err = 0;
    va_list ap;
    va_start(ap, format);
    if (NULL != format) {
      try_fill_start_ts();
      tmp_err = do_fill(format, ap);
    }
    va_end(ap);
    if (-EOVERFLOW == tmp_err) {
      va_start(ap, format);
      force_flush();
      try_fill_start_ts();
      (void)do_fill(format, ap);
      va_end(ap);
    }
  }
  const char* end_trace_log()
  {
    last_log_ts_ = ::oceanbase::common::ObTimeUtility::current_time();
    fill(" | total_timeu=%ld", last_log_ts_ - start_log_ts_);
    return buf_;
  }
  const char* get_trace_log()
  {
    return buf_;
  }

private:
  void try_fill_start_ts()
  {
    if (0 == pos_) {
      if (start_log_ts_ <= 0) {
        update_ts();
      }
      (void)do_fill("start_ts=%ld ", start_log_ts_);
    }
  }
  void force_flush()
  {
    int64_t cur_ts = ::oceanbase::common::ObTimeUtility::current_time();
    TRANS_LOG(INFO, "MT_TRACE[force_log]", "tlog", get_trace_log(), "timeu", cur_ts - start_log_ts_);
    pos_ = 0;
  }
  int do_fill(const char* format, ...) __attribute__((format(printf, 2, 3)))
  {
    int err = 0;
    va_list ap;
    va_start(ap, format);
    if (NULL != format) {
      err = do_fill(format, ap);
    }
    va_end(ap);
    return err;
  }
  int do_fill(const char* format, va_list ap)
  {
    int err = 0;
    common::ObSpinLockGuard guard(lock_);
    int64_t cnt = 0;
    if (NULL == format) {
      // do nothing
    } else if ((cnt = vsnprintf(buf_ + pos_, sizeof(buf_) - pos_, format, ap)) < 0) {
      // this should not happen
      buf_[pos_] = 0;
    } else if (pos_ + cnt < static_cast<int64_t>(sizeof(buf_))) {
      pos_ += cnt;
    } else {
      buf_[pos_] = 0;
      err = -EOVERFLOW;
    }
    return err;
  }

private:
  common::ObSpinLock lock_;
  int64_t start_log_ts_;
  int64_t last_log_ts_;
  int64_t pos_;
  char buf_[TRACE_LOG_BUF_SIZE];
};
#define MT_FILL_TRACE_LOG(tlog, fmt, ...) \
  (tlog).fill(" | [%s] " fmt " u=%ld", __FUNCTION__, ##__VA_ARGS__, (tlog).update_ts())

};      // end namespace memtable
};      // end namespace oceanbase
#endif  //_OB_MT_TRACE_LOG_H_