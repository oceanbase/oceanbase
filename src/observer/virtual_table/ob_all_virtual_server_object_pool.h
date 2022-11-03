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
