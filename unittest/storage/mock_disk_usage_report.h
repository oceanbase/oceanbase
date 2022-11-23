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