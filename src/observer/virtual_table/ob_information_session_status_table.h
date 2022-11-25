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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_SESSION_STATUS_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_SESSION_STATUS_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
class ObInfoSchemaSessionStatusTable : public common::ObVirtualTableScannerIterator
{
  #define SESSION_STATUS_MAP_BUCKET_NUM 10

  static const int32_t SESSION_STATUS_COLUMN_COUNT = 2;
  enum SESSION_STATUS_COLUMN {
    VARIABLE_NAME = common::OB_APP_MIN_COLUMN_ID,
    VARIABLE_VALUE,
  };

  enum VARIABLE {
    THREADS_CONNECTED = 0,
    UPTIME
  };

  typedef common::hash::ObHashMap<common::ObString, common::ObObj> AllStatus;

public:
  ObInfoSchemaSessionStatusTable();
  virtual ~ObInfoSchemaSessionStatusTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

  inline void set_cur_session(sql::ObSQLSessionInfo *session)
  {
    cur_session_ = session;
  }
  inline void set_global_ctx(const ObGlobalContext *global_ctx)
  {
    global_ctx_ = global_ctx;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaSessionStatusTable);
  int fetch_all_session_status(AllStatus &all_status);
private:
  sql::ObSQLSessionInfo *cur_session_;
  const observer::ObGlobalContext *global_ctx_;
  static const char *const variables_name[];
};
} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_SESSION_STATUS_TABLE_
