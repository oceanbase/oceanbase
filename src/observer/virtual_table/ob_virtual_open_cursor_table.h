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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_OPEN_CURSOR
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_OPEN_CURSOR
#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/session/ob_sql_session_mgr.h"

/* __all_virtual_open_cursor
 * The original implementation logic of open_cursor is:
 *
 * 1. lock each session and get cur_sql_id
 * 2. get all plans through plan_cache, when the plan's sql_id == cur_sql_id, get the current plan, and get sql_text & last_active_time information
 *
 * the problem with this implementation is that:
 *
 * 1. when get a plan, since the current session is not locked, the risk is high
 * 2. only sql_text & last_active_time information is read in the plan. These two values can be get on the session, and the cost of using the plan is relatively high
 *
 * new open_cursor implementation logic:
 *
 * 1. securely get session information through fore_each_session
 * 2. referring to the show_processlist framework, put the results into the scanner for results, this framework is also conducive to subsequent expansion
 * 3. cur_plan & session_cursor two kinds of information are currently recorded
 */

namespace oceanbase
{
namespace common
{
class ObObj;
class ObNewRow;

namespace hash
{
}
}
namespace share
{
namespace schema
{
class ObTableSchema;
class ObDatabaseSchema;
}
}

namespace sql
{
class ObPlanCacheObject;
class ObPlanCache;
}

namespace observer
{

class ObVirtualOpenCursorTable : public common::ObVirtualTableScannerIterator
{
public:

  ObVirtualOpenCursorTable();
  virtual ~ObVirtualOpenCursorTable();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  void set_session_mgr(sql::ObSQLSessionMgr *sess_mgr) { session_mgr_ = sess_mgr; }
  int set_addr(const common::ObAddr &addr);

private:
  // https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-OPEN_CURSOR.html
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    SADDR,                      // session point addr
    SID,                        // session id
    USER_NAME,                  // cur user name
    ADDRESS,                    // now is null
    HASH_VALUE,                 // now is null
    SQL_ID,                     // sql id
    SQL_TEXT,                   // sql text, only 60
    LAST_SQL_ACTIVE_TIME,       // last sql active time
    SQL_EXEC_ID,                // now is null
    CURSOR_TYPE,                // cursor type, only support OPEN & SESSION CURSOR CACHED now
                                /*
                                 * OPEN PL/SQL
                                 * OPEN
                                 * SESSION CURSOR CACHED
                                 * OPEN-RECURSIVE
                                 * DICTIONARY LOOKUP CURSOR CACHED
                                 * BUNDLE DICTIONARY LOOKUP CACHED
                                 * JAVA NAME TRANSLATION CURSOR CACHED
                                 * REPLICATION TRIGGER CURSOR CACHED
                                 * CONSTRAINTS CURSOR CACHED
                                 * PL/SQL CURSOR CACHED
                                 */
    CHILD_ADDRESS,              // Address of the child cursor
    CON_ID,                     // The ID of the container to which the data pertains, only support 1 now
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
    {}
    virtual ~FillScanner(){}
    bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo* sess_info);
    int fill_session_cursor_cell(sql::ObSQLSessionInfo &sess_info,
                                 const int64_t cursor_id);
    int fill_cur_plan_cell(sql::ObSQLSessionInfo &sess_info);
    int init(ObIAllocator *allocator,
             common::ObScanner *scanner,
             sql::ObSQLSessionInfo * session_info,
             common::ObNewRow *cur_row,
             const ObIArray<uint64_t> &column_ids,
             share::schema::ObSchemaGetterGuard* schema_guard,
             common::ObString &ipstr,
             uint32_t port);
    inline void reset();

  private:
      ObIAllocator *allocator_;
      common::ObScanner *scanner_;
      common::ObNewRow *cur_row_;
      sql::ObSQLSessionInfo *my_session_;
      share::schema::ObSchemaGetterGuard* schema_guard_;
      ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM> output_column_ids_;
      common::ObString ipstr_;
      uint32_t port_;
      DISALLOW_COPY_AND_ASSIGN(FillScanner);
  };

private:
  sql::ObSQLSessionMgr *session_mgr_;
  common::ObString ipstr_;
  uint32_t port_;
  FillScanner fill_scanner_;
  DISALLOW_COPY_AND_ASSIGN(ObVirtualOpenCursorTable);
}; // end ObVirtualOpenCursorTable
} // end observer
} // end oceanbase



#endif
