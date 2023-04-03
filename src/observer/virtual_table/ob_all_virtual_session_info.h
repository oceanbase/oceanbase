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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_INFO_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_INFO_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/session/ob_sql_session_mgr.h"
namespace oceanbase
{
namespace common
{
class ObNewRow;
class ObScanner;
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObAllVirtualSessionInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualSessionInfo();
  virtual ~ObAllVirtualSessionInfo();
  inline void set_session_mgr(sql::ObSQLSessionMgr *session_mgr) { session_mgr_ = session_mgr; }
  virtual int inner_get_next_row(common::ObNewRow *&row);
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
    SSL_CIPHER,
    TRACE_ID,
    REF_COUNT,
    BACKTRACE,
    TRANS_STATE
  };
  class FillScanner
  {
  public:
    FillScanner()
        :allocator_(NULL),
        scanner_(NULL),
        cur_row_(NULL),
        my_session_(NULL),
        output_column_ids_()
    {
      trace_id_[0] = '\0';
    }
    virtual ~FillScanner(){}
    int operator()(common::hash::HashMapPair<uint64_t, sql::ObSQLSessionInfo *> &entry);
    int init(ObIAllocator *allocator,
             common::ObScanner *scanner,
             sql::ObSQLSessionInfo * session_info,
             common::ObNewRow *cur_row,
             const ObIArray<uint64_t> &column_ids,
             share::schema::ObSchemaGetterGuard* schema_guard);
    inline void reset();
  private:
      ObIAllocator *allocator_;
      common::ObScanner *scanner_;
      common::ObNewRow *cur_row_;
      sql::ObSQLSessionInfo *my_session_;
      share::schema::ObSchemaGetterGuard* schema_guard_;
      ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM> output_column_ids_;
      char trace_id_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
      DISALLOW_COPY_AND_ASSIGN(FillScanner);
  };
  sql::ObSQLSessionMgr *session_mgr_;
  FillScanner fill_scanner_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSessionInfo);
};
}//observer
}//oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_INFO_ */
