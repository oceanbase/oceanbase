/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DATA_TYPE_CLASS_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DATA_TYPE_CLASS_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllDataTypeClassTable : public common::ObVirtualTableScannerIterator
{
  static const int32_t DATA_TYPE_CLASS_COLUMN_COUNT = 2;
  enum COLUMN_NAME {
    DATA_TYPE_CLASS = 16,
    DATA_TYPE_CLASS_STR,
  };

public:
  ObAllDataTypeClassTable();
  virtual ~ObAllDataTypeClassTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllDataTypeClassTable);
};
} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DATA_TYPE_CLASS_TABLE_
