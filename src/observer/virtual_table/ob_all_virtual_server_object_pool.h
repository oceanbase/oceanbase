/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_H_
#define OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include <lib/objectpool/ob_server_object_pool.h>

namespace oceanbase
{
namespace observer
{
class ObAllVirtualServerObjectPool: public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualServerObjectPool();
  virtual ~ObAllVirtualServerObjectPool() { reset(); }
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  void set_addr(common::ObAddr &addr) { addr_ = &addr; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualServerObjectPool);
private:
  common::ObServerObjectPoolRegistry::ArenaIterator iter_;
  common::ObAddr *addr_;
};

}
}
#endif /* OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_H_ */
