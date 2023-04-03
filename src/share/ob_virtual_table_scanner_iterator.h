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

#ifndef OCEANBASE_COMMON_OB_VIRTUAL_TABLE_SCANNER_ITERATOR_
#define OCEANBASE_COMMON_OB_VIRTUAL_TABLE_SCANNER_ITERATOR_

#include "lib/net/ob_addr.h"
#include "share/ob_virtual_table_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace common
{
class ObVirtualTableScannerIterator : public common::ObVirtualTableIterator
{
public:
  ObVirtualTableScannerIterator();
  virtual ~ObVirtualTableScannerIterator();
  virtual void reset();
protected:
  common::ObScanner scanner_;
  common::ObScanner::Iterator scanner_it_;
  bool start_to_read_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObVirtualTableScannerIterator);
};
}
}
#endif /* OCEANBASE_COMMON_OB_VIRTUAL_TABLE_SCANNER_ITERATOR_ */
