/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
