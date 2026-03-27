/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OB_INFORMATION_SCHEMA_ENABLE_ROLES_
#define OCEANBASE_OB_INFORMATION_SCHEMA_ENABLE_ROLES_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObInfoSchemaEnableRolesTable : public common::ObVirtualTableScannerIterator
{
public:
  ObInfoSchemaEnableRolesTable();
  virtual ~ObInfoSchemaEnableRolesTable();

  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

private:
  int prepare_scan();

  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaEnableRolesTable);
};
}
}
#endif /* OCEANBASE_OB_INFORMATION_SCHEMA_ENABLE_ROLES_ */
