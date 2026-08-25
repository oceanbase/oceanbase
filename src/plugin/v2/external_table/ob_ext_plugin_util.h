/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_plugin_util.h
/// \brief Small helpers for driving the external-table plugin contract:
/// releasing a plugin's output JSON buffers, etc.
///
/// The plugin reports errors ONLY via the int errno return of each vtable
/// function (an OB errno verbatim — no translation layer). The plugin logs a
/// diagnostic with its own source location via `host->log` before returning;
/// OB consumes just the errno. There is no error object crossing the boundary,
/// so there is nothing to read or destroy here.

#ifndef OB_EXT_PLUGIN_UTIL_H
#define OB_EXT_PLUGIN_UTIL_H

#include "plugin/v2/include/ob_external_table_plugin.h"

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

/// Release a plugin's output JSON buffer. The plugin — not OB — owns the release
/// (see "Memory ownership" in ob_external_table_plugin.h): the buffer may be a
/// static constant (no-op), host-allocated, or from the plugin's own allocator, and
/// the plugin decides inside its destroy slot. These wrappers null-check `api`,
/// `buf`, and the slot before calling. Pass the same `host` that was passed to the
/// producing call so a host-allocating plugin can reach `host->mem.mem_free`.
void ob_ext_schema_destroy(const ObExtTablePluginApi *api, char *buf, int32_t len,
                           const ObExtTableHostApi *host);
void ob_ext_tasks_destroy(const ObExtTablePluginApi *api, char *buf, int32_t len,
                          const ObExtTableHostApi *host);
void ob_ext_stats_destroy(const ObExtTablePluginApi *api, char *buf, int32_t len,
                          const ObExtTableHostApi *host);

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_PLUGIN_UTIL_H
