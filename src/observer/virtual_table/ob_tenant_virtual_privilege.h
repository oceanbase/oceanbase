/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_PRIVILEGE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_PRIVILEGE_
#include "share/ob_virtual_table_scanner_iterator.h"
namespace oceanbase
{
namespace observer
{
class ObTenantVirtualPrivilege : public ObVirtualTableScannerIterator
{
public:
  ObTenantVirtualPrivilege() {};
  virtual ~ObTenantVirtualPrivilege() { reset(); };
  virtual void reset() override
  {
    ObVirtualTableScannerIterator::reset();
  }
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

private:
  ObTenantVirtualPrivilege(const ObTenantVirtualPrivilege &other) = delete;
  ObTenantVirtualPrivilege &operator=(const ObTenantVirtualPrivilege &other) = delete;
  enum PRIVILEGE_COLUMN
  {
    PRIVILEGE_COL =  common::OB_APP_MIN_COLUMN_ID,
    CONTEXT_COL,
    COMMENT_COL
  };
  int fill_scanner();
};
}
}
#endif 