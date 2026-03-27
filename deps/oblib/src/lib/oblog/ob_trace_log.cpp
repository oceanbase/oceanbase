/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include "lib/oblog/ob_trace_log.h"

namespace oceanbase
{
namespace common
{
const char *const ObTraceLogConfig::LOG_LEVEL_ENV_KEY = "_OB_TRACE_LOG_LEVEL_";
const char *const ObTraceLogConfig::level_strs_[] = {"ERROR", "WARN", "INFO", "EDIAG", "WDIAG", "TRACE", "DEBUG"};
volatile int ObTraceLogConfig::log_level_ = OB_LOG_LEVEL_TRACE;
bool ObTraceLogConfig::got_env_ = false;

int32_t ObTraceLogConfig::set_log_level(const char *log_level_str)
{
  if (NULL != log_level_str) {
    set_log_level(log_level_str, log_level_);
    got_env_ = true;
  }
  return log_level_;
}

int32_t ObTraceLogConfig::set_log_level(const char *log_level_str, volatile int &log_level)
{
  if (NULL != log_level_str) {
    int32_t level_num = sizeof(level_strs_) / sizeof(const char *);
    bool find_level = false;
    for (int32_t idx = 0; !find_level && idx < level_num; ++idx) {
      if (0 == STRCASECMP(level_strs_[idx], log_level_str)) {
        log_level = idx;
        find_level = true;
      }
    }
  }
  return log_level;
}

}
}
