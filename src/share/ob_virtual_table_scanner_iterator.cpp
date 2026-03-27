/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/ob_virtual_table_scanner_iterator.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

ObVirtualTableScannerIterator::ObVirtualTableScannerIterator()
    : ObVirtualTableIterator(),
      scanner_(),
      scanner_it_(),
      start_to_read_(false)
{
}

ObVirtualTableScannerIterator::~ObVirtualTableScannerIterator()
{
}

void ObVirtualTableScannerIterator::reset()
{
  scanner_it_.reset();
  scanner_.reset();
  start_to_read_ = false;
  ObVirtualTableIterator::reset();
}

}/* ns observer*/
}/* ns oceanbase */
