/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
