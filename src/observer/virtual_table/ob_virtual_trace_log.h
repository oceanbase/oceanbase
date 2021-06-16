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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_VIRTUAL_TRACE_LOG_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_VIRTUAL_TRACE_LOG_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
namespace sql {
class ObSQLSessionInfo;
}
namespace observer {
class ObVirtualTraceLog : public common::ObVirtualTableScannerIterator {
public:
  ObVirtualTraceLog();
  virtual ~ObVirtualTraceLog();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

private:
  DISALLOW_COPY_AND_ASSIGN(ObVirtualTraceLog);
  enum TRACE_COLUMN { TITLE = common::OB_APP_MIN_COLUMN_ID, KEY_VALUE, TIME };
  int fill_scanner();
  int fill_trace_buf();
  int fill_phy_plan_into_trace_buf();
};
}  // end namespace observer
}  // end namespace oceanbase

#endif
