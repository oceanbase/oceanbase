/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_RC_H_
#define OB_RC_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace lib
{
uint64_t current_tenant_id();
// The current resource_owner_id is tenant_id
uint64_t current_resource_owner_id();
} // end of namespace lib
} // end of namespace oceanbase

#endif // OB_RC_H_
