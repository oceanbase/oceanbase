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

#define USING_LOG_PREFIX COMMON

#include "common/ob_timeout_ctx.h"

namespace oceanbase
{
namespace common
{

ObTimeoutCtx::ObTimeoutCtx() : abs_timeout_us_(-1), trx_timeout_us_(-1), next_(NULL)
{
  do_link_self();
}

ObTimeoutCtx::ObTimeoutCtx(bool link_self) : abs_timeout_us_(-1), trx_timeout_us_(-1), next_(NULL)
{
  if (link_self) {
    do_link_self();
  }
}

void ObTimeoutCtx::do_link_self()
{
  next_ = header();
  header() = this;

  if (NULL != next_) {
    abs_timeout_us_ = next_->abs_timeout_us_;
    trx_timeout_us_ = next_->trx_timeout_us_;
  }
}

ObTimeoutCtx::~ObTimeoutCtx()
{
  if (NULL != header()) {
    if (this != header()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "destruct timeout context should be header");
    } else {
      header() = next_;
    }
  }
}

ObTimeoutCtx *&ObTimeoutCtx::header()
{
  RLOCAL(ObTimeoutCtx *, head_ptr);
  return head_ptr;
}

const ObTimeoutCtx &ObTimeoutCtx::get_ctx()
{
  const static bool link_self = false;
  static ObTimeoutCtx def(link_self);
  const ObTimeoutCtx *res = &def;
  if (NULL != header()) {
    res = header();
  }
  return *res;
}

int ObTimeoutCtx::set_timeout(const int64_t timeout_interval_us)
{
  int ret = OB_SUCCESS;
  if (timeout_interval_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid timeout interval", K(ret), K(timeout_interval_us));
  } else {
    abs_timeout_us_ = ObTimeUtility::current_time() + timeout_interval_us;
  }
  return ret;
}

int ObTimeoutCtx::set_abs_timeout(const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  if (abs_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid absolute timeout", K(ret), K(abs_timeout_us));
  } else {
    abs_timeout_us_ = abs_timeout_us;
  }
  return ret;
}

int64_t ObTimeoutCtx::get_timeout(const int64_t def_timeout_interval_us /* = -1 */) const
{
  int64_t timeout = def_timeout_interval_us;
  if (abs_timeout_us_ > 0) {
    timeout = abs_timeout_us_ - ObTimeUtility::current_time();
  }
  return timeout;
}

int64_t ObTimeoutCtx::get_abs_timeout(const int64_t def_abs_timeout_us /* = -1 */) const
{
  int64_t abs_timeout = def_abs_timeout_us;
  if (abs_timeout_us_ > 0) {
    abs_timeout = abs_timeout_us_;
  }
  return abs_timeout;
}

bool ObTimeoutCtx::is_timeouted() const
{
  bool timeouted = false;
  if (abs_timeout_us_ > 0 && abs_timeout_us_ < ObTimeUtility::current_time()) {
    timeouted = true;
  }

  return timeouted;
}

// set time of transaction timeout
int ObTimeoutCtx::set_trx_timeout_us(int64_t trx_timeout_us)
{
  int ret = OB_SUCCESS;
  if (trx_timeout_us <= 0) {
   ret = OB_INVALID_ARGUMENT;
   LOG_WARN("invalid argument", K(ret), K(trx_timeout_us));
  } else {
    trx_timeout_us_ = trx_timeout_us;
  }
  return ret;
}

bool ObTimeoutCtx::is_trx_timeout_set() const
{
  return trx_timeout_us_ > 0;
}

int64_t ObTimeoutCtx::get_trx_timeout_us() const
{
  int64_t timeout = 0;
  if (is_trx_timeout_set()) {
    timeout = trx_timeout_us_;
  }
  return timeout;
}

// Attention!! This function will ignore the result of previous ObTimeouCtx.
void ObTimeoutCtx::reset_timeout_us()
{
  abs_timeout_us_ = -1;
  trx_timeout_us_ = -1;
}

} // end namespace common
} // end namespace oceanbase
