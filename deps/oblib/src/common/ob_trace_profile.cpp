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

#include "common/ob_trace_profile.h"

namespace oceanbase {
namespace common {
void ObTraceProfile::TraceEntry::reset()
{
  partition_key_.reset();
  server_.reset();
  flag_ = NULL;
  time_ = OB_INVALID_TIMESTAMP;
}

ObTraceProfile::ObTraceProfile()
{
  reset();
}

int ObTraceProfile::init(const char* module, const int64_t warn_timeout, const bool is_tracing)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(module) || warn_timeout < 0) {
    COMMON_LOG(WARN, "invalid argument", KP(module), K(warn_timeout));
    ret = OB_INVALID_ARGUMENT;
  } else {
    module_name_ = module;
    warn_timeout_ = warn_timeout;
    is_tracing_ = is_tracing;
    is_inited_ = true;
  }
  return ret;
}

void ObTraceProfile::set_sign(const uint64_t sign)
{
  sign_ = sign;
}

void ObTraceProfile::reset()
{
  module_name_ = NULL;
  sign_ = OB_INVALID_ID;
  warn_timeout_ = OB_INVALID_TIMESTAMP;
  idx_ = 0;
  is_tracing_ = false;
  is_inited_ = false;

  for (int64_t i = 0; i < MAX_TRACE_NUM; ++i) {
    entry_[i].reset();
  }
}

int ObTraceProfile::trace(const ObPartitionKey& partition_key, const char* flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!partition_key.is_valid() || OB_ISNULL(flag)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((!is_tracing_) || (idx_ >= MAX_TRACE_NUM)) {
    // do nothing
  } else {
    entry_[idx_].partition_key_ = partition_key;
    entry_[idx_].flag_ = flag;
    entry_[idx_].time_ = ObTimeUtility::fast_current_time();
    ++idx_;
  }
  return ret;
}

int ObTraceProfile::trace(const ObPartitionKey& partition_key, const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!partition_key.is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((!is_tracing_) || (idx_ >= MAX_TRACE_NUM)) {
    // do nothing
  } else {
    entry_[idx_].partition_key_ = partition_key;
    entry_[idx_].server_ = server;
    entry_[idx_].time_ = ObTimeUtility::fast_current_time();
    ++idx_;
  }
  return ret;
}

int ObTraceProfile::trace(const ObPartitionKey& partition_key, const char* flag, const int64_t time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!partition_key.is_valid() || OB_ISNULL(flag)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((!is_tracing_) || (idx_ >= MAX_TRACE_NUM)) {
    // do nothing
  } else {
    entry_[idx_].partition_key_ = partition_key;
    entry_[idx_].flag_ = flag;
    entry_[idx_].time_ = time;
    ++idx_;
  }
  return ret;
}

int ObTraceProfile::trace(const ObPartitionKey& partition_key, const char* flag, const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!partition_key.is_valid() || OB_ISNULL(flag) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((!is_tracing_) || (idx_ >= MAX_TRACE_NUM)) {
    // do nothing
  } else {
    entry_[idx_].partition_key_ = partition_key;
    entry_[idx_].flag_ = flag;
    entry_[idx_].server_ = server;
    entry_[idx_].time_ = ObTimeUtility::fast_current_time();
    ++idx_;
  }
  return ret;
}

void ObTraceProfile::report_trace()
{
  if ((!is_inited_) || (!is_tracing_) || (idx_ <= 0)) {
    // do nothing
  } else if ((entry_[idx_ - 1].time_ - entry_[0].time_) >= warn_timeout_) {
    char buf[MAX_OUTPUT_BUFFER];
    const int64_t buf_len = MAX_OUTPUT_BUFFER;
    int64_t pos = 0;
    databuff_printf(
        buf, buf_len, pos, "%s:sign=%lu:tt=%ld:", module_name_, sign_, entry_[idx_ - 1].time_ - entry_[0].time_);
    bool has_pk = false;
    const int64_t begin_time = entry_[0].time_;
    for (int64_t i = 0; i < idx_; ++i) {
      databuff_printf(buf, buf_len, pos, "[%s:", (entry_[i].flag_ != NULL) ? entry_[i].flag_ : "NULL");
      if (entry_[i].partition_key_.is_valid() && !has_pk) {
        has_pk = true;
        databuff_printf(buf, buf_len, pos, "%s:", to_cstring(entry_[i].partition_key_));
      }
      if (entry_[i].server_.is_valid()) {
        databuff_printf(buf, buf_len, pos, "%s:", to_cstring(entry_[i].server_));
      }
      databuff_printf(buf, buf_len, pos, "%ld]", entry_[i].time_ - begin_time);
    }
    COMMON_LOG(WARN, "trace_info", "info", buf);
  }
}

}  // namespace common
}  // namespace oceanbase
