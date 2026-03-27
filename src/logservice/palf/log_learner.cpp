/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "log_learner.h"

namespace oceanbase
{
using namespace common;

namespace palf
{

LogLearner::LogLearner()
    : server_(),
      region_(DEFAULT_REGION_NAME),
      register_time_us_(OB_INVALID_TIMESTAMP),
      keepalive_ts_(OB_INVALID_TIMESTAMP)
{
}

LogLearner::LogLearner(const common::ObAddr &server, const int64_t register_time_us)
    : server_(server),
      region_(DEFAULT_REGION_NAME),
      register_time_us_(register_time_us),
      keepalive_ts_(OB_INVALID_TIMESTAMP)
{
}

LogLearner::LogLearner(const LogLearner &child)
  : server_(child.server_),
    region_(child.region_),
    register_time_us_(child.register_time_us_),
    keepalive_ts_(child.keepalive_ts_)
{
}

LogLearner::LogLearner(const common::ObAddr &server,
                       const common::ObRegion &region,
                       const int64_t register_time_us)
    : server_(server),
      region_(region),
      register_time_us_(register_time_us),
      keepalive_ts_(OB_INVALID_TIMESTAMP)
{
}

LogLearner::~LogLearner()
{
  reset();
}

bool LogLearner::is_valid() const
{
  return server_.is_valid() && !region_.is_empty() && register_time_us_ >= 0;
}

void LogLearner::reset()
{
  server_.reset();
  region_ = DEFAULT_REGION_NAME;
  register_time_us_ = OB_INVALID_TIMESTAMP;
  keepalive_ts_ = OB_INVALID_TIMESTAMP;
}

bool LogLearner::is_timeout(const int64_t timeout_us) const
{
  bool bool_ret = false;
  const int64_t now_us = ObTimeUtility::current_time();
  if (now_us - keepalive_ts_ > timeout_us) {
    bool_ret = true;
  } else {
  }
  return bool_ret;
}

const common::ObAddr &LogLearner::get_server() const
{
  return server_;
}


void LogLearner::update_keepalive_ts()
{
  const int64_t curr_time_us = ObTimeUtility::current_time();
  keepalive_ts_ = (curr_time_us > keepalive_ts_)? curr_time_us: keepalive_ts_;
}

bool LogLearner::operator<(const LogLearner &val) const
{
  return server_ < val.server_;
}

bool LogLearner::operator==(const LogLearner &val) const
{
  return server_ == val.server_;
}

bool LogLearner::operator!=(const LogLearner &val) const
{
  return server_ != val.server_;
}

LogLearner &LogLearner::operator=(const LogLearner &val)
{
  server_ = val.server_;
  region_ = val.region_;
  register_time_us_ = val.register_time_us_;
  keepalive_ts_ = val.keepalive_ts_;
  return *this;
}

OB_SERIALIZE_MEMBER(LogLearner, server_, region_, register_time_us_);
} // namespace palf end
} // namespace oceanbase end
