/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/partition_table/ob_partition_table_operator.h"
namespace oceanbase
{
namespace share
{
using namespace common;
class FakeRsListChangeCb
{
public:
  int submit_update_rslist_task(const bool force_update)
  {
    UNUSED(force_update);
    return OB_SUCCESS;
  }
};
}
}
