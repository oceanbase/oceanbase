/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_rc.h"
#include "lib/rc/context.h"

namespace oceanbase
{
namespace lib
{
// Get the context tenant id, the implementation code is in ob, so the weak symbol is used here
// Return 500 tenants by default
uint64_t __attribute__ ((weak)) current_resource_owner_id()
{
  return common::OB_SERVER_TENANT_ID;
}
} // end of namespace lib
} // end of namespace oceanbase
