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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DATA_TYPE_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DATA_TYPE_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllDataTypeTable : public common::ObVirtualTableScannerIterator
{
  static const int32_t DATA_TYPE_COLUMN_COUNT = 3;
  enum COLUMN_NAME {
    DATA_TYPE = common::OB_APP_MIN_COLUMN_ID,
    DATA_TYPE_STR,
    DATA_TYPE_CLASS,
  };

public:
  ObAllDataTypeTable();
  virtual ~ObAllDataTypeTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllDataTypeTable);
};
} // namespace observer
} // namespace oceanbase
#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DATA_TYPE_TABLE_
