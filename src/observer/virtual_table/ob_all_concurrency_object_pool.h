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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_CONCURRENCY_OBJECT_POOL_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_CONCURRENCY_OBJECT_POOL_H_

#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/container/ob_vector.h"
#include "lib/net/ob_addr.h"
#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase {
namespace observer {
class ObAllConcurrencyObjectPool : public common::ObVirtualTableScannerIterator {
public:
  ObAllConcurrencyObjectPool();
  virtual ~ObAllConcurrencyObjectPool();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = &addr;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllConcurrencyObjectPool);
  common::ObAddr* addr_;
  common::ObVector<common::ObObjFreeList*> flls_;
  common::ObObjFreeList* fl_;
  int64_t idx_;
};
}  // namespace observer
}  // namespace oceanbase

#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_CONCURRENCY_OBJECT_POOL_H_ */
