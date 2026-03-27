/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "observer/virtual_table/ob_all_virtual_files_table.h"

namespace oceanbase
{
using namespace common;

namespace observer
{

ObAllVirtualFilesTable::ObAllVirtualFilesTable() :
    ObVirtualTableScannerIterator()
{
}

ObAllVirtualFilesTable::~ObAllVirtualFilesTable()
{
}

int ObAllVirtualFilesTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  return OB_ITER_END;
}
} // namespace observer
} // namespace oceanbase
