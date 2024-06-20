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

#define USING_LOG_PREFIX SERVER

#include "ob_virtual_open_cursor_table.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "observer/ob_server.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "sql/plan_cache/ob_ps_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace observer
{

ObVirtualOpenCursorTable::ObVirtualOpenCursorTable()
  : ObVirtualTableScannerIterator(),
  session_mgr_(NULL),
  port_(0)
{
}

ObVirtualOpenCursorTable::~ObVirtualOpenCursorTable()
{
  reset();
}

void ObVirtualOpenCursorTable::reset()
{
  session_mgr_ = NULL;
  ipstr_.reset();
  port_ = 0;
  fill_scanner_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObVirtualOpenCursorTable::set_addr(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObString ipstr = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr.get_port();
  }
  return ret;
}

int ObVirtualOpenCursorTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_mgr_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "sessionMgr is NULL", K(ret));
  } else {
    if (!start_to_read_) {
      if (OB_FAIL(fill_scanner_.init(allocator_,
                                     &scanner_,
                                     session_,
                                     &cur_row_,
                                     output_column_ids_,
                                     schema_guard_,
                                     ipstr_,
                                     port_))) {
        SERVER_LOG(WARN, "init fill_scanner fail", K(ret));
      } else if (OB_FAIL(session_mgr_->for_each_session(fill_scanner_))) {
        SERVER_LOG(WARN, "fill scanner fail", K(ret));
      } else {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

bool ObVirtualOpenCursorTable::FillScanner::operator()(sql::ObSQLSessionMgr::Key key,
                                                       ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == scanner_
                  || NULL == allocator_
                  || NULL == cur_row_
                  || NULL == cur_row_->cells_
                  || NULL == sess_info
                  || NULL == my_session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
               "parameter or data member is NULL",
               K(ret),
               K(scanner_),
               K(allocator_),
               K(cur_row_),
               K(sess_info),
               K(my_session_));
  } else if (OB_UNLIKELY(cur_row_->count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN,
                "cells count is less than output column count",
                K(ret),
                K(cur_row_->count_),
                K(output_column_ids_.count()));
  } else {
    ObServer &server = ObServer::get_instance();
    uint64_t cell_idx = 0;
    char ip_buf[common::OB_IP_STR_BUFF];
    char peer_buf[common::OB_IP_PORT_STR_BUFF];
    char sql_id[common::OB_MAX_SQL_ID_LENGTH + 1];
    //If you are in system tenant, you can see all thread.
    //Otherwise, you can show only the threads at the same Tenant with you.
    //If you have the PROCESS privilege, you can show all threads at your Tenant.
    //Otherwise, you can show only your own threads.
    if (sess_info->is_shadow()) {
      //this session info is logical free, shouldn't be added to scanner
    } else if ((OB_SYS_TENANT_ID == my_session_->get_priv_tenant_id())
        || (sess_info->get_priv_tenant_id() == my_session_->get_priv_tenant_id()
            && my_session_->get_user_id() == sess_info->get_user_id())) {
      ObSQLSessionInfo::LockGuard lock_guard(sess_info->get_thread_data_lock());
      OZ (fill_cur_plan_cell(*sess_info));
      for (sql::ObSQLSessionInfo::CursorCache::CursorMap::iterator iter =
              sess_info->get_cursor_cache().pl_cursor_map_.begin();  //ignore ret
            OB_SUCC(ret) && iter != sess_info->get_cursor_cache().pl_cursor_map_.end();
            ++iter) {
        pl::ObPLCursorInfo *cursor_info = iter->second;
        if (OB_ISNULL(cursor_info)) {
          // do not report error
          // ignore ret
          SERVER_LOG(WARN, "get a NULL cursor when record for v$open_cursor.");
        } else {
          OZ (fill_session_cursor_cell(*sess_info, cursor_info->get_id()));
        }
      }
    }
  }
  return OB_SUCCESS == ret;
}

int ObVirtualOpenCursorTable::FillScanner::fill_session_cursor_cell(ObSQLSessionInfo &sess_info,
                                                                    const int64_t cursor_id)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObCharsetType default_charset = ObCharset::get_default_charset();
  ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
  char sql_id[common::OB_MAX_SQL_ID_LENGTH + 1];
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    const uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TENANT_ID: {
        cur_row_->cells_[i].set_int(sess_info.get_priv_tenant_id());
        break;
      }
      case SVR_IP: {
        cur_row_->cells_[i].set_varchar(ipstr_);
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case SVR_PORT: {
        cur_row_->cells_[i].set_int(port_);
        break;
      }
      case SADDR: {
        ObSqlString addr;
        ObString tmp_saddr;
        addr.append_fmt("%lx", reinterpret_cast<uint64_t>(&sess_info));
        OZ (ob_write_string(*allocator_, addr.string(), tmp_saddr));
        // get last 8 char, for oracle compatiable
        int64_t offset = tmp_saddr.length() > 8 ? tmp_saddr.length() - 8 : 0;
        // if tmp_saddr.length() - offset > 8, offset is 0
        // the length make sure (tmp_saddr.ptr() + offset) do not have out-of-bounds access
        int64_t length = tmp_saddr.length() - offset > 8 ? 8 : tmp_saddr.length() - offset;
        ObString saddr(length, tmp_saddr.ptr() + offset);
        cur_row_->cells_[i].set_varchar(saddr);
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case SID: {
        cur_row_->cells_[i].set_int(sess_info.get_sessid());
        break;
      }
      case USER_NAME: {
        cur_row_->cells_[i].set_varchar(sess_info.get_user_name());
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case ADDRESS: {
        // session cursor not set now
        cur_row_->cells_[i].set_null();
        break;
      }
      case HASH_VALUE: {
        // cur_row_->cells_[i].set_int(sess_info.id);
        cur_row_->cells_[i].set_null();
        break;
      }
      case SQL_ID: {
        if (obmysql::COM_QUERY == sess_info.get_mysql_cmd() ||
            obmysql::COM_STMT_EXECUTE == sess_info.get_mysql_cmd() ||
            obmysql::COM_STMT_PREPARE == sess_info.get_mysql_cmd() ||
            obmysql::COM_STMT_PREXECUTE == sess_info.get_mysql_cmd()) {
          sess_info.get_cur_sql_id(sql_id, OB_MAX_SQL_ID_LENGTH + 1);
        } else {
          sql_id[0] = '\0';
        }
        cur_row_->cells_[i].set_varchar(ObString::make_string(sql_id));
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case SQL_TEXT: {
        ObPsStmtId inner_stmt_id = OB_INVALID_ID;
        if (0 == (cursor_id & (1L << 31))) {
          if (OB_ISNULL(sess_info.get_ps_cache())) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN,"ps : ps cache is null.", K(ret), K(cursor_id));
          } else if (OB_FAIL(sess_info.get_inner_ps_stmt_id(cursor_id, inner_stmt_id))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN,"ps : get inner stmt id fail.", K(ret), K(cursor_id));
          } else {
            ObPsStmtInfoGuard guard;
            ObPsStmtInfo *ps_info = NULL;
            if (OB_FAIL(sess_info.get_ps_cache()->get_stmt_info_guard(inner_stmt_id, guard))) {
              SERVER_LOG(WARN,"get stmt info guard failed", K(ret), K(cursor_id), K(inner_stmt_id));
            } else if (OB_ISNULL(ps_info = guard.get_stmt_info())) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN,"get stmt info is null", K(ret));
            } else {
              ObString sql = ps_info->get_ps_sql();
              int64_t len = 60 > sql.length() ? sql.length() : 60;
              cur_row_->cells_[i].set_varchar(ObString(len, ps_info->get_ps_sql().ptr()));
              cur_row_->cells_[i].set_collation_type(default_collation);
            }
          }
        } else {
          // refcursor can not get sql now
          cur_row_->cells_[i].set_varchar("ref cursor");
          cur_row_->cells_[i].set_collation_type(default_collation);
        }
        break;
      }
      case LAST_SQL_ACTIVE_TIME: {
        // session cursor not set now
        cur_row_->cells_[i].set_null();
        break;
      }
      case SQL_EXEC_ID: {
        cur_row_->cells_[i].set_null();
        break;
      }
      case CURSOR_TYPE: {
        cur_row_->cells_[i].set_varchar("SESSION CURSOR CACHED");
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case CHILD_ADDRESS: {
        cur_row_->cells_[i].set_null();
        break;
      }
      case CON_ID: {
        cur_row_->cells_[i].set_int(1);
        break;
      }
      default: {
        break;
      }
    }
  }
  if (OB_UNLIKELY(OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_->add_row(*cur_row_)))) {
    SERVER_LOG(WARN, "fail to add row", K(ret), K(*cur_row_));
  }
  return ret;
}

int ObVirtualOpenCursorTable::FillScanner::fill_cur_plan_cell(ObSQLSessionInfo &sess_info)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObCharsetType default_charset = ObCharset::get_default_charset();
  ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
  char sql_id[common::OB_MAX_SQL_ID_LENGTH + 1];
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    const uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TENANT_ID: {
        cur_row_->cells_[i].set_int(sess_info.get_priv_tenant_id());
        break;
      }
      case SVR_IP: {
        cur_row_->cells_[i].set_varchar(ipstr_);
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case SVR_PORT: {
        cur_row_->cells_[i].set_int(port_);
        break;
      }
      case SADDR: {
        ObSqlString addr;
        ObString tmp_saddr;
        addr.append_fmt("%lx", reinterpret_cast<uint64_t>(&sess_info));
        OZ (ob_write_string(*allocator_, addr.string(), tmp_saddr));
        // get last 8 char, for oracle compatiable
        int64_t offset = tmp_saddr.length() > 8 ? tmp_saddr.length() - 8 : 0;
        // if tmp_saddr.length() - offset > 8, offset is 0
        // the length make sure (tmp_saddr.ptr() + offset) do not have out-of-bounds access
        int64_t length = tmp_saddr.length() - offset > 8 ? 8 : tmp_saddr.length() - offset;
        ObString saddr(length, tmp_saddr.ptr() + offset);
        cur_row_->cells_[i].set_varchar(saddr);
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case SID: {
        cur_row_->cells_[i].set_int(sess_info.get_sessid());
        break;
      }
      case USER_NAME: {
        cur_row_->cells_[i].set_varchar(sess_info.get_user_name());
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case ADDRESS: {
        // plan not set now
        cur_row_->cells_[i].set_null();
        break;
      }
      case HASH_VALUE: {
        // cur_row_->cells_[i].set_int(sess_info.id);
        cur_row_->cells_[i].set_null();
        break;
      }
      case SQL_ID: {
        if (obmysql::COM_QUERY == sess_info.get_mysql_cmd() ||
            obmysql::COM_STMT_EXECUTE == sess_info.get_mysql_cmd() ||
            obmysql::COM_STMT_PREPARE == sess_info.get_mysql_cmd() ||
            obmysql::COM_STMT_PREXECUTE == sess_info.get_mysql_cmd()) {
          sess_info.get_cur_sql_id(sql_id, OB_MAX_SQL_ID_LENGTH + 1);
        } else {
          sql_id[0] = '\0';
        }
        cur_row_->cells_[i].set_varchar(ObString::make_string(sql_id));
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case SQL_TEXT: {
        ObString sql = sess_info.get_current_query_string();
        int64_t len = 60 > sql.length() ? sql.length() : 60;
        cur_row_->cells_[i].set_varchar(ObString(len, sql.ptr()));
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case LAST_SQL_ACTIVE_TIME: {
        // session cursor not set now
        cur_row_->cells_[i].set_timestamp(sess_info.get_query_start_time());
        break;
      }
      case SQL_EXEC_ID: {
        cur_row_->cells_[i].set_null();
        break;
      }
      case CURSOR_TYPE: {
        cur_row_->cells_[i].set_varchar("OPEN");
        cur_row_->cells_[i].set_collation_type(default_collation);
        break;
      }
      case CHILD_ADDRESS: {
        cur_row_->cells_[i].set_null();
        break;
      }
      case CON_ID: {
        cur_row_->cells_[i].set_int(1);
        break;
      }
      default: {
        break;
      }
    }
  }
  if (OB_UNLIKELY(OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_->add_row(*cur_row_)))) {
    SERVER_LOG(WARN, "fail to add row", K(ret), K(*cur_row_));
  }
  return ret;
}

void ObVirtualOpenCursorTable::FillScanner::reset()
{
  allocator_ = NULL;
  scanner_ = NULL;
  cur_row_ = NULL;
  my_session_ = NULL;
  output_column_ids_.reset();
  ipstr_.reset();
  port_ = 0;
}

int ObVirtualOpenCursorTable::FillScanner::init(ObIAllocator *allocator,
                                         common::ObScanner *scanner,
                                         sql::ObSQLSessionInfo *session_info,
                                         common::ObNewRow *cur_row,
                                         const ObIArray<uint64_t> &column_ids,
                                         share::schema::ObSchemaGetterGuard* schema_guard,
                                         common::ObString &ipstr,
                                         uint32_t port)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == allocator
                  || NULL == scanner
                  || NULL == cur_row
                  || NULL == session_info)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
               "some parameter is NULL", K(ret), K(allocator), K(scanner), K(cur_row), K(session_info));
  } else if (OB_FAIL(output_column_ids_.assign(column_ids))) {
    SQL_ENG_LOG(WARN, "fail to assign output column ids", K(ret), K(column_ids));
  } else {
    allocator_ = allocator;
    scanner_ = scanner;
    cur_row_ = cur_row;
    my_session_ = session_info;
    schema_guard_ = schema_guard;
    ipstr_ = ipstr;
    port_ = port;
  }
  return ret;
}


} // namespace observer
} // namespace oceanbase
