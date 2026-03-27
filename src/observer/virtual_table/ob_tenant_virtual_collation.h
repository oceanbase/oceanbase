/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_COLLATION_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_COLLATION_
#include "share/ob_virtual_table_scanner_iterator.h"
namespace oceanbase
{
namespace observer
{
class ObTenantVirtualCollation:public common::ObVirtualTableScannerIterator
{
public:
  ObTenantVirtualCollation()
  {
  }
  ~ObTenantVirtualCollation()
  {
    reset();
  }
  virtual void reset() override
  {
    ObVirtualTableScannerIterator::reset();
  }
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  ObTenantVirtualCollation(const ObTenantVirtualCollation &other)=delete;
  ObTenantVirtualCollation &operator=(const ObTenantVirtualCollation &other)=delete;
  enum COLLATION_COLUMN
  {
    COLLATION_TYPE = common::OB_APP_MIN_COLUMN_ID,
    COLLATION,
    CHARSET,
    ID,
    IS_DEFAULT,
    IS_COMPILED,
    SORTLEN
  };
  int fill_scanner();
};
}
}
#endif
