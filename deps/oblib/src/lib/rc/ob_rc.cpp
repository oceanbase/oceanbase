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

#include "lib/rc/ob_rc.h"
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
