/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_I_DISK_REPORT
#define OCEANBASE_OBSERVER_OB_I_DISK_REPORT

#include "share/ob_disk_usage_table_operator.h"

namespace oceanbase
{

namespace observer
{

class ObIDiskReport
{
public:
  ObIDiskReport() {}
  virtual ~ObIDiskReport() {}
  virtual int delete_tenant_usage_stat(const uint64_t tenant_id) = 0;
};

}
}

#endif