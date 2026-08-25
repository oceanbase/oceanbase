/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_EXT_MALLOC_GUARD_H
#define OB_EXT_MALLOC_GUARD_H

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

// Tenant-local OB malloc label for plugin allocations, named after the plugin
// so each format's memory is separately visible in
// tenant memory stats. `format_name` is the plugin's canonical format string
// (ObExtTablePluginApi::format_name); NULL/empty falls back to "ExtTblPlugin".
// Used BOTH for the explicit attr installed on the ctx's memory pool
// (host_ctx_.default_pool.set_attr(...), carried into every host mem_alloc) AND
// for the malloc-hook label set by ObExtMallocGuard around plugin calls.
inline lib::ObMemAttr get_ext_mem_attr(const char *format_name)
{
  uint64_t tenant_id = OB_SERVER_TENANT_ID;
#ifdef MTL_ID
  tenant_id = MTL_ID();
#endif
  return lib::ObMemAttr(tenant_id, (OB_NOT_NULL(format_name) && *format_name)
                                          ? format_name : "ExtTblPlugin");
}

// RAII guard that sets the current thread's malloc-hook label for its scope.
// Put one at the top of each function that calls into the plugin, so the
// plugin's INTERNAL allocations (arrow buffers, format internals, ...) that go
// through OB's malloc hook are accounted to the tenant under the plugin's name.
// Allocations the plugin makes via the host mem_alloc callback already carry
// the attr explicitly (host_ctx_.default_pool's attr), so this guard is the
// safety net for everything else.
class ObExtMallocGuard final : public lib::ObMallocHookAttrGuard
{
public:
  explicit ObExtMallocGuard(const char *format_name)
      : lib::ObMallocHookAttrGuard(get_ext_mem_attr(format_name))
  {
  }
};

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_MALLOC_GUARD_H
