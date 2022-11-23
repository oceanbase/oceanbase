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

#include "log_learner.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
using namespace common;

namespace palf
{

LogLearner::LogLearner()
    : server_(),
      region_(DEFAULT_REGION_NAME),
      register_ts_ns_(OB_INVALID_TIMESTAMP),
      keepalive_ts_(OB_INVALID_TIMESTAMP)
{
}

LogLearner::LogLearner(const common::ObAddr &server, const int64_t register_ts_ns)
    : server_(server),
      region_(DEFAULT_REGION_NAME),
      register_ts_ns_(register_ts_ns),
      keepalive_ts_(OB_INVALID_TIMESTAMP)
{
}

LogLearner::LogLearner(const LogLearner &child)
  : server_(child.server_),
    region_(child.region_),
    register_ts_ns_(child.register_ts_ns_),
    keepalive_ts_(child.keepalive_ts_)
{
}

LogLearner::LogLearner(const common::ObAddr &server,
                       const common::ObRegion &region,
                       const int64_t register_ts_ns)
    : server_(server),
      region_(region),
      register_ts_ns_(register_ts_ns),
      keepalive_ts_(OB_INVALID_TIMESTAMP)
{
}

LogLearner::~LogLearner()
{
  reset();
}

bool LogLearner::is_valid() const
{
  return server_.is_valid() && !region_.is_empty() && register_ts_ns_ >= 0;
}

void LogLearner::reset()
{
  server_.reset();
  region_ = DEFAULT_REGION_NAME;
  register_ts_ns_ = OB_INVALID_TIMESTAMP;
  keepalive_ts_ = OB_INVALID_TIMESTAMP;
}

bool LogLearner::is_timeout(const int64_t timeout_ns) const
{
  bool bool_ret = false;
  const int64_t now_ns = ObTimeUtility::current_time_ns();
  if (now_ns - keepalive_ts_ > timeout_ns) {
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
  const int64_t curr_ts_ns = ObTimeUtility::current_time_ns();
  keepalive_ts_ = (curr_ts_ns > keepalive_ts_)? curr_ts_ns: keepalive_ts_;
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
  register_ts_ns_ = val.register_ts_ns_;
  keepalive_ts_ = val.keepalive_ts_;
  return *this;
}

OB_SERIALIZE_MEMBER(LogLearner, server_, region_, register_ts_ns_);
} // namespace palf end
} // namespace oceanbase end