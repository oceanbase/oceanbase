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

#ifndef  OCEANBASE_LIB_OBLOG_OB_TRACE_LOG_
#define  OCEANBASE_LIB_OBLOG_OB_TRACE_LOG_
#include <stdlib.h>           // for getenv
#include "lib/oblog/ob_log.h"  // OB_LOGGER
#include "lib/trace/ob_trace_event.h"

// #if __INCLUDE_LEVEL__ > 0
// #error "ob_trace_log.h should only be included in .cpp files directly"
// #endif

#define OB_TRACE_CONFIG ::oceanbase::common::ObTraceLogConfig

OB_INLINE bool CHECK_TRACE_TIMES(uint64_t times, const uint64_t *cur_trace_id)
{
  struct TraceArray {
    uint64_t v_[3];
  };
  /* {trace_id1, trace_id2, count} */
  RLOCAL(TraceArray, buffer);
  uint64_t *trace_id = (&buffer)->v_;
  bool printable = true;
  if (OB_ISNULL(cur_trace_id)) {
  } else if (cur_trace_id[0] == 0) {
  } else if (cur_trace_id[0] == trace_id[0] &&
             cur_trace_id[1] == trace_id[1]) {
    ++trace_id[2];
    if (trace_id[2] > times) {
      printable = false;
    }
  } else {
    trace_id[0] = cur_trace_id[0];
    trace_id[1] = cur_trace_id[1];
    trace_id[2] = 1;
  }
  return printable;
}

#define NG_TRACE_EXT_TIMES(times, ...)                               \
  if (oceanbase::lib::is_trace_log_enabled() && CHECK_TRACE_TIMES(times, ObCurTraceId::get())) {               \
    NG_TRACE_EXT(__VA_ARGS__);                                       \
  }


#define NG_TRACE_TIMES(times, ...)                                   \
  if (oceanbase::lib::is_trace_log_enabled() && CHECK_TRACE_TIMES(times, ObCurTraceId::get())) {               \
    NG_TRACE(__VA_ARGS__);                                           \
  }

#define NG_TRACE_TIMES_WITH_TRACE_ID(times, tmp_cur_trace_id, ...)   \
  if (oceanbase::lib::is_trace_log_enabled() && CHECK_TRACE_TIMES(times, tmp_cur_trace_id)) {                  \
    NG_TRACE(__VA_ARGS__);                                           \
  }

//Ordinary print log level, judged according to the current system configuration level
#define PRINT_TRACE(log_buffer)                                          \
  if (oceanbase::lib::is_trace_log_enabled() && OB_TRACE_CONFIG::get_log_level() <= OB_LOGGER.get_log_level()) {   \
    if (NULL != log_buffer) {                                            \
      ::oceanbase::common::OB_PRINT("[TRACE]", OB_LOG_LEVEL_DIRECT(TRACE), \
                                    "[normal]", LOG_KVS("TRACE", log_buffer)); \
    }                                                                    \
  }

//For logs that are forced to be printed, you can also judge according to the log level: if it is ERROR level, it will not print, and other levels will be forced to print
#define FORCE_PRINT_TRACE(log_buffer, HEAD)                                         \
  if (oceanbase::lib::is_trace_log_enabled() && OB_LOGGER.get_log_level() != OB_LOG_LEVEL_DBA_ERROR) {              \
    _Pragma("GCC diagnostic push")                                      \
    _Pragma("GCC diagnostic ignored \"-Wtautological-pointer-compare\"")    \
    if (NULL != log_buffer) {                                                       \
      ::oceanbase::common::OB_PRINT("[TRACE] ", OB_LOG_LEVEL_DIRECT(TRACE), HEAD, \
                                    LOG_KVS("TRACE", *log_buffer)); \
    }                                                                               \
    _Pragma("GCC diagnostic pop")                                       \
  }


namespace oceanbase
{
namespace common
{
class ObTraceLogConfig
{
  static const char *const LOG_LEVEL_ENV_KEY;
public:
  static int32_t set_log_level(const char *log_level_str);
  static int32_t set_log_level(const char *log_level_str, volatile int &log_level_);
  inline static int32_t get_log_level()
  {
    if (!got_env_) {
      const char *log_level_str = getenv(LOG_LEVEL_ENV_KEY);
      set_log_level(log_level_str);
      got_env_ = true;
    }
    return log_level_;
  }
private:
  static const char *const level_strs_[];
  static volatile int log_level_;
  static bool got_env_;
};
}
}

#endif //OCEANBASE_LIB_OBLOG_OB_TRACE_LOG_
