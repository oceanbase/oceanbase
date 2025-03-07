/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "oceanbase/ob_plugin_base.h"
#include "oceanbase/ob_plugin_errno.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @addtogroup ObPlugin
 * @{
 */

/**
 * Log Level
 */
enum ObPluginLogLevel
{
  OBP_LOG_LEVEL_TRACE,
  OBP_LOG_LEVEL_INFO,
  OBP_LOG_LEVEL_WARN,
};

/**
 * test if we should output the log with specific level
 * @note you shouldn't call this routine
 * @return
 *   - OBP_SUCCESS enabled
 *   - others not enabled
 */
OBP_PUBLIC_API int obp_log_enabled(int32_t level);

/**
 * logging
 * @note you should use OBP_LOG_xxx instead
 */
OBP_PUBLIC_API void obp_log_format(int32_t level,
                                   const char *filename,
                                   int32_t lineno,
                                   const char *location_string,
                                   int64_t location_string_size,
                                   const char *function,
                                   const char *format, ...) __attribute__((format(printf, 7, 8)));

/**
 * logging macro
 */
#define OBP_LOG(level, fmt, args...)                                              \
  do {                                                                            \
    if (OBP_SUCCESS == obp_log_enabled(level)) {                                  \
      (void)obp_log_format(level,                                                 \
                           __FILE__,                                              \
                           __LINE__,                                              \
                           __FILE__ ":" OBP_STRINGIZE(__LINE__),                  \
                           sizeof(__FILE__ ":" OBP_STRINGIZE(__LINE__)),          \
                           __FUNCTION__,                                          \
                           fmt,                                                   \
                           ##args);                                               \
    }                                                                             \
  } while (0)

#define OBP_LOG_TRACE(fmt, args...)  OBP_LOG(OBP_LOG_LEVEL_TRACE, fmt, ##args)
#define OBP_LOG_INFO(fmt, args...)   OBP_LOG(OBP_LOG_LEVEL_INFO,  fmt, ##args)
#define OBP_LOG_WARN(fmt, args...)   OBP_LOG(OBP_LOG_LEVEL_WARN,  fmt, ##args)

/** @} */

#ifdef __cplusplus
} // extern "C"
#endif
