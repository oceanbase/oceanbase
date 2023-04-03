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

#ifndef OCEABASE_SHARE_OB_I_TENANT_MEM_LIMIT_GETTER_H_
#define OCEABASE_SHARE_OB_I_TENANT_MEM_LIMIT_GETTER_H_

#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace common
{

class ObITenantMemLimitGetter
{
public:
  virtual bool has_tenant(const uint64_t tenant_id) const = 0;
  virtual int get_all_tenant_id(ObIArray<uint64_t> &key) const = 0;
  virtual int get_tenant_mem_limit(const uint64_t tenant_id,
                                   int64_t &lower_limit,
                                   int64_t &upper_limit) const = 0;
};

} // common
} // oceanbase

#endif // OCEABASE_SHARE_OB_I_TENANT_MEM_LIMIT_MGR_H_
