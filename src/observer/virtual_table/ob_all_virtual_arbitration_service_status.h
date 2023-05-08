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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_ARB_SERVICE_STATUS_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_ARB_SERVICE_STATUS_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualArbServiceStatus : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualArbServiceStatus();
  virtual ~ObAllVirtualArbServiceStatus();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  void destroy();
private:
private:
  static const int64_t VARCHAR_32 = 32;
  static const int64_t VARCHAR_128 = 128;
private:
};
}//namespace observer
}//namespace oceanbase
#endif
