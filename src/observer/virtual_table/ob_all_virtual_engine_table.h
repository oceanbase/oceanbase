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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_ENGINE_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_ENGINE_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualEngineTable : public common::ObVirtualTableScannerIterator
{
  static const int32_t ENGINE_COLUMN_COUNT = 6;
  enum COLUMN_NAME {
    ENGINE = common::OB_APP_MIN_COLUMN_ID,
    SUPPORT,
    COMMENT,
    TRANSACTIONS,
    XA,
    SAVEPOINTS,
  };
public:
  ObAllVirtualEngineTable();
  virtual ~ObAllVirtualEngineTable();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualEngineTable);
};
} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_ENGINE_TABLE_
