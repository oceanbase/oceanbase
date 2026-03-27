/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_CREATE_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_CREATE_TABLE_

#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace observer
{
class ObShowCreateTable : public common::ObVirtualTableScannerIterator
{
public:
  ObShowCreateTable();
  virtual ~ObShowCreateTable();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int calc_show_table_id(uint64_t &show_table_id);
  int fill_row_cells_with_retry(const uint64_t show_table_id,
                                const share::schema::ObTableSchema &table_schema);
  int fill_row_cells_inner(const uint64_t show_table_id,
                           const share::schema::ObTableSchema &table_schema,
                           const int64_t table_def_buf_size,
                           char *table_def_buf);
private:
  DISALLOW_COPY_AND_ASSIGN(ObShowCreateTable);
};
}// observer
}// oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_CREATE_TABLE_ */
