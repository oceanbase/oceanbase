/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_RECOVER_TABLE_UTIL_H
#define OCEANBASE_SHARE_RECOVER_TABLE_UTIL_H
#include "lib/ob_define.h"
namespace oceanbase
{
namespace share
{
class ObRecoverTableUtil final
{
public:
  static int check_compatible(const uint64_t target_tenant_id);
};

}
}

#endif