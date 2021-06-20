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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_PROCESSLIST_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_PROCESSLIST_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/session/ob_sql_session_mgr.h"
namespace oceanbase {
namespace common {
class ObNewRow;
class ObScanner;
}  // namespace common
namespace sql {
class ObSQLSessionInfo;
}
namespace observer {
class ObShowProcesslist : public common::ObVirtualTableScannerIterator {
public:
  ObShowProcesslist();
  virtual ~ObShowProcesslist();
  inline void set_session_mgr(sql::ObSQLSessionMgr* session_mgr)
  {
    session_mgr_ = session_mgr;
  }
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

private:
  enum SESSION_INFO_COLUMN {
    ID = OB_APP_MIN_COLUMN_ID,
    USER,
    TENANT,
    HOST,
    DB_NAME,
    COMMAND,
    SQL_ID,
    TIME,
    STATE,
    INFO,
    SVR_IP,
    SVR_PORT,
    SQL_PORT,
    PROXY_SESSID,
    MASTER_SESSID,
    USER_CLIENT_IP,
    USER_HOST,
    TRANS_ID,
    THREAD_ID,
    SSL_CIPHER
  };
  class FillScanner {
  public:
    FillScanner() : allocator_(NULL), scanner_(NULL), cur_row_(NULL), my_session_(NULL), output_column_ids_()
    {}
    virtual ~FillScanner()
    {}
    bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo* sess_info);
    int init(ObIAllocator* allocator, common::ObScanner* scanner, sql::ObSQLSessionInfo* session_info,
        common::ObNewRow* cur_row, const ObIArray<uint64_t>& column_ids,
        share::schema::ObSchemaGetterGuard* schema_guard);
    inline void reset();

  public:
    bool has_process_privilege();

  private:
    ObIAllocator* allocator_;
    common::ObScanner* scanner_;
    common::ObNewRow* cur_row_;
    sql::ObSQLSessionInfo* my_session_;
    share::schema::ObSchemaGetterGuard* schema_guard_;
    ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM> output_column_ids_;
    DISALLOW_COPY_AND_ASSIGN(FillScanner);
  };
  sql::ObSQLSessionMgr* session_mgr_;
  FillScanner fill_scanner_;
  DISALLOW_COPY_AND_ASSIGN(ObShowProcesslist);
};
}  // namespace observer
}  // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_PROCESSLIST_ */
