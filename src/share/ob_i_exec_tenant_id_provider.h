/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_I_EXEC_TENANT_ID_PROVIDER_H_
#define OCEANBASE_SHARE_OB_I_EXEC_TENANT_ID_PROVIDER_H_

#include <stdint.h>

namespace oceanbase
{
namespace share
{

class ObIExecTenantIdProvider
{
public:
  ~ObIExecTenantIdProvider() {}
  // Return tenant id to execute sql.
  virtual uint64_t get_exec_tenant_id() const = 0;
};

}
}
#endif