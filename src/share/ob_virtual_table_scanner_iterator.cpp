/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
