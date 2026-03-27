/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_FILES_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_FILES_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualFilesTable : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualFilesTable();
  virtual ~ObAllVirtualFilesTable();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualFilesTable);
};
} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_FILES_TABLE_
