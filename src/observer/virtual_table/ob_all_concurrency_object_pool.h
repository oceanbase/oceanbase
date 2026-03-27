/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_CONCURRENCY_OBJECT_POOL_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_CONCURRENCY_OBJECT_POOL_H_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllConcurrencyObjectPool : public common::ObVirtualTableScannerIterator
{
public:
  ObAllConcurrencyObjectPool() {}
  virtual ~ObAllConcurrencyObjectPool() {}
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllConcurrencyObjectPool);
};
}
}

#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_CONCURRENCY_OBJECT_POOL_H_ */
