/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_MOCK_DISK_USAGE_REPORT_H
#define OCEANBASE_STORAGE_MOCK_DISK_USAGE_REPORT_H

#include "observer/report/ob_i_disk_report.h"

namespace oceanbase
{

namespace storage
{

class MockDiskUsageReport : public observer::ObIDiskReport
{
public:
  MockDiskUsageReport() {}
  virtual ~MockDiskUsageReport() {}
  virtual int delete_tenant_usage_stat(const uint64_t tenant_id)
  {
    UNUSED(tenant_id);
    return OB_SUCCESS;
  }
};

}
}

#endif