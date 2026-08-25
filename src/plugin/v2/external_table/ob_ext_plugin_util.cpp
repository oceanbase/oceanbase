/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "plugin/v2/external_table/ob_ext_plugin_util.h"

#include "lib/ob_define.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

namespace
{
// Common body for the three output-buffer destroy wrappers: the plugin owns the
// release (static -> no-op, host-alloc -> host->mem.mem_free, own-alloc -> own free).
// OB never frees a plugin output buffer directly.
void destroy_buffer(const ObExtTablePluginApi *api, char *buf, int32_t len,
                    const ObExtTableHostApi *host,
                    void (*slot)(char *, int32_t, const ObExtTableHostApi *))
{
  if (OB_NOT_NULL(api) && OB_NOT_NULL(buf) && OB_NOT_NULL(slot)) {
    slot(buf, len, host);
  }
}
} // namespace

void ob_ext_schema_destroy(const ObExtTablePluginApi *api, char *buf, int32_t len,
                           const ObExtTableHostApi *host)
{
  destroy_buffer(api, buf, len, host, api ? api->schema_destroy : nullptr);
}

void ob_ext_tasks_destroy(const ObExtTablePluginApi *api, char *buf, int32_t len,
                          const ObExtTableHostApi *host)
{
  destroy_buffer(api, buf, len, host, api ? api->tasks_destroy : nullptr);
}

void ob_ext_stats_destroy(const ObExtTablePluginApi *api, char *buf, int32_t len,
                          const ObExtTableHostApi *host)
{
  destroy_buffer(api, buf, len, host, api ? api->stats_destroy : nullptr);
}

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX
