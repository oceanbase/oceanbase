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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DATA_TYPE_CLASS_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DATA_TYPE_CLASS_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllDataTypeClassTable : public common::ObVirtualTableScannerIterator
{
  static const int32_t DATA_TYPE_CLASS_COLUMN_COUNT = 2;
  enum COLUMN_NAME {
    DATA_TYPE_CLASS = 16,
    DATA_TYPE_CLASS_STR,
  };

public:
  ObAllDataTypeClassTable();
  virtual ~ObAllDataTypeClassTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllDataTypeClassTable);
};
} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DATA_TYPE_CLASS_TABLE_
