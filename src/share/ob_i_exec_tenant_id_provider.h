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