/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
