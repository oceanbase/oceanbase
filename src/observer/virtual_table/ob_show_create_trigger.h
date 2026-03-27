/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_SHOW_CREATE_TRIGGER_H_
#define OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_SHOW_CREATE_TRIGGER_H_

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
class ObTriggerInfo;
}
}

namespace observer
{
class ObShowCreateTrigger : public common::ObVirtualTableScannerIterator
{
private:
  enum ShowCreateTriggerColumns {
    TRIGGER_ID = 16,
    TRIGGER_NAME,
    SQL_MODE,
    CREATE_TRIGGER,
    CHARACTER_SET_CLIENT,
    COLLATION_CONNECTION,
    COLLATION_DATABASE,
  };

public:
  ObShowCreateTrigger();
  virtual ~ObShowCreateTrigger();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int calc_show_trigger_id(uint64_t &tg_id);
  int fill_row_cells(uint64_t tg_id, const share::schema::ObTriggerInfo &tg_info);
private:
  DISALLOW_COPY_AND_ASSIGN(ObShowCreateTrigger);
};
} // observer
} // oceanbase

#endif /* OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_SHOW_CREATE_TRIGGER_H_ */
