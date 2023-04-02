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

#ifndef OB_SYSLOG_RATE_LIMITER_H
#define OB_SYSLOG_RATE_LIMITER_H

#include <stdint.h>
#include <sys/time.h>
#include <algorithm>
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_rate_limiter.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_level.h"
#include "lib/coro/co_var.h"

namespace oceanbase
{
namespace common
{

class ObISyslogRateLimiter : public lib::ObRateLimiter
{
public:
  ObISyslogRateLimiter(const int64_t rate = 0, const char *name = nullptr)
      : lib::ObRateLimiter(rate, name)
  {
  }

  virtual int acquire(const int64_t permits = 1,
                          const int64_t arg0 = 0,
                          const int64_t arg1 = 0) override
  {
    return common::OB_NOT_SUPPORTED;
  }

  virtual int try_acquire(const int64_t permits = 1,
                      const int64_t arg0 = 0,
                      const int64_t arg1 = 0) override
  {
    RLOCAL_INLINE(bool, in_acquire);
    int ret = common::OB_SUCCESS;
    int log_level = static_cast<int>(arg0);
    int errcode = static_cast<int>(arg1);
    if (!in_acquire) {
      if (0 == rate_) {
        ret = common::OB_EAGAIN;
      } else if (OB_LOG_LEVEL_ERROR == log_level
                 || OB_LOG_LEVEL_DBA_WARN == log_level
                 || OB_LOG_LEVEL_DBA_ERROR == log_level) {
        // allow
      } else {
        in_acquire = 1;
        ret = do_acquire(permits, log_level, errcode);
        in_acquire = 0;
      }
    }
    return ret;
  }

  virtual int do_acquire(int64_t permits, int log_level, int errcode) = 0;
};

class ObSimpleRateLimiterImpl
{
public:
  explicit ObSimpleRateLimiterImpl(int64_t &rate);
  int try_acquire(int64_t premits);
private:
  static int64_t current_time();
private:
  int64_t &rate_;
  int64_t permits_;
  int64_t last_ts_;
};

class ObSimpleRateLimiter : public lib::ObRateLimiter
{
public:
  explicit ObSimpleRateLimiter(int64_t rate) : ObRateLimiter(rate), impl_(rate_) {}
  virtual int acquire(const int64_t permits = 1,
                      const int64_t arg0 = 0,
                      const int64_t arg1 = 0) override
  {
    return common::OB_NOT_SUPPORTED;
  }
  bool is_force_allows() const override { return false; }
  void reset_force_allows() override { };

  virtual int try_acquire(const int64_t permits = 1,
                          const int64_t arg0 = 0,
                          const int64_t arg1 = 0) override
  {
    UNUSED(arg0);
    UNUSED(arg1);
    return impl_.try_acquire(permits);
  }
private:
  ObSimpleRateLimiterImpl impl_;
};

class ObSyslogSimpleRateLimiter : public ObISyslogRateLimiter
{
  static constexpr int64_t DEFAULT_RATE = 100;
public:
  explicit ObSyslogSimpleRateLimiter(int64_t rate = DEFAULT_RATE);

  bool is_force_allows() const override { return false; }
  void reset_force_allows() override { };
  int do_acquire(int64_t permits, int log_level, int errcode) override
  {
    UNUSED(log_level);
    UNUSED(errcode);
    return impl_.try_acquire(permits);
  }

private:
  ObSimpleRateLimiterImpl impl_;
};

class ObSyslogSampleRateLimiter : public ObISyslogRateLimiter
{
public:
  ObSyslogSampleRateLimiter(int64_t initial = 1000, int64_t thereafter = 100,
                            int64_t duration=1000L * 1000L/*1s*/);
  bool is_force_allows() const override { return false; }
  void reset_force_allows() override { };
  int do_acquire(int64_t permits, int log_level, int errcode) override;

private:
  int64_t initial_;
  int64_t thereafter_;
  int64_t duration_;
  int64_t reset_ts_;
  int64_t count_;
};

}  // common
}  // oceanbase

#endif /* OB_SYSLOG_RATE_LIMITER_H */
