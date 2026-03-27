/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_WARNING_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_WARNING_
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObTenantVirtualWarning : public common::ObVirtualTableScannerIterator
{
public:
  ObTenantVirtualWarning();
  virtual ~ObTenantVirtualWarning();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int fill_scanner();
private:
  static const char* const SHOW_WARNING_STR;
  static const char* const SHOW_NOTE_STR;
  static const char* const SHOW_ERROR_STR;
  DISALLOW_COPY_AND_ASSIGN(ObTenantVirtualWarning);
};
}//observer
}//oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_WARNING_ */
