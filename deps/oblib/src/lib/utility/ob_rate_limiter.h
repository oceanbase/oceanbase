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

#ifndef OB_RATE_LIMITER_H
#define OB_RATE_LIMITER_H

#include <stdint.h>

namespace oceanbase {
namespace lib {

class ObRateLimiter {
public:
  ObRateLimiter();
  virtual ~ObRateLimiter();

  void set_name(const char* name);
  const char* name() const;

  void set_rate(int64_t rate);
  int64_t rate() const;

  virtual bool is_force_allows() const = 0;
  virtual void reset_force_allows() = 0;
  virtual int try_acquire(int64_t permits = 1) = 0;
  virtual int acquire(int64_t permits = 1) = 0;

protected:
  int64_t rate_;
  const char* name_;
};

inline ObRateLimiter::ObRateLimiter() : rate_(0), name_(nullptr)
{}

inline ObRateLimiter::~ObRateLimiter()
{}

inline void ObRateLimiter::set_name(const char* name)
{
  name_ = name;
}

inline const char* ObRateLimiter::name() const
{
  return name_;
}

inline void ObRateLimiter::set_rate(int64_t rate)
{
  rate_ = rate;
}

inline int64_t ObRateLimiter::rate() const
{
  return rate_;
}

}  // namespace lib
}  // namespace oceanbase

#endif /* OB_RATE_LIMITER_H */
