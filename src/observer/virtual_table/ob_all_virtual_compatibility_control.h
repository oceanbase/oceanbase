/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_COMPATIBILITY_CONTROL_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_COMPATIBILITY_CONTROL_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObVirtualCompatibilityConflictControl : public common::ObVirtualTableScannerIterator
{
public:
  ObVirtualCompatibilityConflictControl();
  ~ObVirtualCompatibilityConflictControl();
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  enum COLUMN_ID_LIST
  {
    TENAND_ID = common::OB_APP_MIN_COLUMN_ID,
    NAME,
    DESCRIPTION,
    IS_ENABLE,
    ENABLE_VERSIONS
  };
  int fill_scanner();
private:
  DISALLOW_COPY_AND_ASSIGN(ObVirtualCompatibilityConflictControl);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_COMPATIBILITY_CONTROL_ */
