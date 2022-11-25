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

#ifndef OCEANBASE_OBSERVER_OB_SHOW_CREATE_TABLEGROUP_
#define OCEANBASE_OBSERVER_OB_SHOW_CREATE_TABLEGROUP_
#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace observer
{
class ObShowCreateTablegroup : public common::ObVirtualTableScannerIterator
{
public:
  ObShowCreateTablegroup();
  virtual ~ObShowCreateTablegroup();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int calc_show_tablegroup_id(uint64_t &show_tablegroup_id);
  int fill_row_cells(uint64_t show_tablegroup_id, const common::ObString &tablegroup_name);
private:
  DISALLOW_COPY_AND_ASSIGN(ObShowCreateTablegroup);
};
}
}
#endif /* OCEANBASE_OBSERVER_OB_SHOW_CREATE_TABLEGROUP_ */
