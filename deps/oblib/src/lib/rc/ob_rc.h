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
