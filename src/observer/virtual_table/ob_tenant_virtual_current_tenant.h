/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_CURRENT_TENANT_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_CURRENT_TENANT_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace common
{
class ObMySQLProxy;
class ObNewRow;
}
namespace observer
{
class ObTenantVirtualCurrentTenant : public common::ObVirtualTableScannerIterator
{
public:
  ObTenantVirtualCurrentTenant();
  virtual ~ObTenantVirtualCurrentTenant();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_sql_proxy(common::ObMySQLProxy *sql_proxy) { sql_proxy_ = sql_proxy; }
private:
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantVirtualCurrentTenant);
};
}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_TENANT_VIRTUAL_CURRENT_TENANT_ */
