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

#include "oceanbase/ob_plugin_log.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_defer.h"

namespace oceanbase {
namespace plugin {

static const char *ob_plugin_log_module_name = "[PLUGIN]";

int32_t obp_map_log_level(ObPluginLogLevel level)
{
  switch (level) {
    case OBP_LOG_LEVEL_TRACE: return OB_LOG_LEVEL_TRACE;
    case OBP_LOG_LEVEL_INFO:  return OB_LOG_LEVEL_INFO;
    case OBP_LOG_LEVEL_WARN:  return OB_LOG_LEVEL_WARN;
    default:                  return OB_LOG_LEVEL_TRACE;
  }
}

} // namespace plugin
} // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::plugin;

#ifdef __cplusplus
extern "C" {
#endif

OBP_PUBLIC_API int obp_log_enabled(int32_t level)
{
  return OB_LOGGER.need_to_print(obp_map_log_level(static_cast<ObPluginLogLevel>(level))) ?
      OBP_SUCCESS : OBP_PLUGIN_ERROR;
}

OBP_PUBLIC_API void obp_log_format(int32_t level,
                                   const char *filename,
                                   int32_t lineno,
                                   const char *location_string,
                                   int64_t location_string_size,
                                   const char *function,
                                   const char *format, ...)
{
  if (OB_NOT_NULL(format) && OB_NOT_NULL(location_string)) {
    va_list args;
    va_start(args, format);
    DEFER(va_end(args));
    uint64_t location_hash_val = oceanbase::common::hash::fnv_hash_for_logger(location_string, location_string_size);
    OB_LOGGER.log_message_va(ob_plugin_log_module_name,
                             obp_map_log_level(static_cast<ObPluginLogLevel>(level)),
                             filename,
                             lineno,
                             function,
                             location_hash_val,
                             0,/*errcode*/
                             format,
                             args);
  }
}

#ifdef __cplusplus
} // extern "C"
#endif
