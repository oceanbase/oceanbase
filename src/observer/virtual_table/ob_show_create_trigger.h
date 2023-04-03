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
