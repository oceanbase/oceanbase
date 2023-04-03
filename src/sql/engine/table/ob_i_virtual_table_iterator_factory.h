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

#ifndef OCEANBASE_SQL_ENGINE_TABLE_OB_I_VIRTUAL_TABLE_ITERATOR_FACTORY_
#define OCEANBASE_SQL_ENGINE_TABLE_OB_I_VIRTUAL_TABLE_ITERATOR_FACTORY_

#include "common/ob_range.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{
class ObVirtualTableIterator;
class ObVTableScanParam;
}
namespace sql
{
class ObExecContext;
class ObCreateVirtualTableParams
{
public:
  ObCreateVirtualTableParams() : table_id_(common::OB_INVALID_ID), key_ranges_() {}
  virtual ~ObCreateVirtualTableParams() {}

  uint64_t table_id_;
  common::ObSEArray<common::ObNewRange, 16> key_ranges_;
  TO_STRING_KV(K_(table_id),
               K_(key_ranges));
};

class ObIVirtualTableIteratorFactory
{
public:
  ObIVirtualTableIteratorFactory() {}
  virtual ~ObIVirtualTableIteratorFactory() {}

  virtual int create_virtual_table_iterator(common::ObVTableScanParam &params,
                                            common::ObVirtualTableIterator *&vt_iter) = 0;
  virtual int revert_virtual_table_iterator(common::ObVirtualTableIterator *vt_iter) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIVirtualTableIteratorFactory);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_TABLE_OB_I_VIRTUAL_TABLE_ITERATOR_FACTORY_ */
