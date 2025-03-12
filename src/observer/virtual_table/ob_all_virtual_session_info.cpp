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

#include "observer/virtual_table/ob_all_virtual_session_info.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace observer
{

ObAllVirtualSessionInfo::ObAllVirtualSessionInfo()
    : ObVirtualTableScannerIterator(),
      session_mgr_(NULL),
      fill_scanner_()
{
}

ObAllVirtualSessionInfo::~ObAllVirtualSessionInfo()
{
  reset();
}

void ObAllVirtualSessionInfo::reset()
{
  session_mgr_ = NULL;
  fill_scanner_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSessionInfo::inner_get_next_row(ObNewRow *&row)
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
                                     table_schema_))) {
        SERVER_LOG(WARN, "init fill_scanner fail", K(ret));
      } else if (OB_FAIL(session_mgr_->for_each_hold_session(fill_scanner_))) {
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

int ObAllVirtualSessionInfo::FillScanner::operator()(
              hash::HashMapPair<uint64_t, ObSQLSessionInfo *> &entry)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *sess_info = entry.second;
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
    if ((OB_SYS_TENANT_ID == my_session_->get_priv_tenant_id())
        || (sess_info->get_priv_tenant_id() == my_session_->get_priv_tenant_id())) {
      ObSQLSessionInfo::LockGuard lock_guard(sess_info->get_thread_data_lock());
      const int64_t col_count = output_column_ids_.count();
      ObCharsetType default_charset = ObCharset::get_default_charset();
      ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
      // for compatibility，the time type of the new version is converted from int to double
      bool type_is_double = true;
      const ObColumnSchemaV2 *tmp_column_schema = NULL;
      if (OB_ISNULL(table_schema_) ||
          OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema("TIME"))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
      } else {
        type_is_double = tmp_column_schema->get_meta_type().is_double();
      }
      int64_t current_time = ::oceanbase::common::ObTimeUtility::current_time();
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch(col_id) {
          case ID: {
            cur_row_->cells_[cell_idx].set_uint64(static_cast<uint64_t>(
                                  sess_info->get_compatibility_sessid()));
            break;
          }
          case USER: {
            if (sess_info->get_is_deserialized()) {
              // this is a tmp solution to avoid core when we execute 'show processlist'
              // before we finally find the reason and resolve the bug. otherwise we cannot
              // use this command in on-line cluster.
              // see
              cur_row_->cells_[cell_idx].set_varchar("");
              cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            } else {
              cur_row_->cells_[cell_idx].set_varchar(sess_info->get_user_name());
              cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            }
            break;
          }
          case TENANT: {
            cur_row_->cells_[cell_idx].set_varchar(sess_info->get_effective_tenant_name());
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            break;
          }
          case HOST: {
            if (OB_FAIL(sess_info->get_peer_addr().ip_port_to_string(peer_buf, common::OB_IP_PORT_STR_BUFF))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "fail to get ip string", K(ret), K(sess_info->get_peer_addr()));
            } else {
              cur_row_->cells_[cell_idx].set_varchar(ObString::make_string(peer_buf));
              cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            }
            break;
          }
          case DB_NAME: {
            if (0 == sess_info->get_database_name().length()) {//session don't select database
              cur_row_->cells_[cell_idx].set_varchar(ObString::make_string("NULL"));
            } else {
              cur_row_->cells_[cell_idx].set_varchar(sess_info->get_database_name());
            }
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            break;
          }
          case COMMAND: {
            cur_row_->cells_[cell_idx].set_varchar(ObString::make_string(sess_info->get_mysql_cmd_str()));
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            break;
          }
          case SQL_ID: {
            sess_info->get_cur_sql_id(sql_id, OB_MAX_SQL_ID_LENGTH + 1);
            cur_row_->cells_[cell_idx].set_varchar(ObString::make_string(sql_id));
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            break;
          }
          case TIME: {
            if (type_is_double) {
              double time_sec = (static_cast<double> (current_time - sess_info->get_cur_state_start_time())) / 1000000;
              cur_row_->cells_[cell_idx].set_double(time_sec);
              cur_row_->cells_[cell_idx].set_scale(6);
            } else {
              int64_t time_sec = (current_time - sess_info->get_cur_state_start_time()) / 1000000;
              cur_row_->cells_[cell_idx].set_int(time_sec);
            }
            break;
          }
          case STATE: {
            cur_row_->cells_[cell_idx].set_varchar(ObString::make_string(sess_info->get_session_state_str()));
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            break;
          }
          case INFO: {
            if (obmysql::COM_QUERY == sess_info->get_mysql_cmd() ||
                obmysql::COM_STMT_EXECUTE == sess_info->get_mysql_cmd() ||
                obmysql::COM_STMT_PREPARE == sess_info->get_mysql_cmd() ||
                obmysql::COM_STMT_PREXECUTE == sess_info->get_mysql_cmd()) {
              cur_row_->cells_[cell_idx].set_varchar(sess_info->get_current_query_string());
              cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            } else {
              cur_row_->cells_[cell_idx].set_null();
            }
            break;
          }
          case SVR_IP: {
            if (!server.get_self().ip_to_string(ip_buf, common::OB_IP_STR_BUFF)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "fail to get ip string", K(ret), K(server.get_self()));
            } else {
              cur_row_->cells_[cell_idx].set_varchar(ObString::make_string(ip_buf));
              cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            }
            break;
          }
          case SVR_PORT: {
            cur_row_->cells_[cell_idx].set_int(server.get_self().get_port());
            break;
          }
          case SQL_PORT: {
            cur_row_->cells_[cell_idx].set_int(GCONF.mysql_port);
            break;
          }
          case PROXY_SESSID: {
            if (ObBasicSessionInfo::VALID_PROXY_SESSID == sess_info->get_proxy_sessid()) {
              cur_row_->cells_[cell_idx].set_null();
            } else {
              cur_row_->cells_[cell_idx].set_uint64(sess_info->get_proxy_sessid());
            }
            break;
          }
          case MASTER_SESSID: {
            if (ObBasicSessionInfo::INVALID_SESSID == sess_info->get_master_sessid()) {
              cur_row_->cells_[cell_idx].set_null();
            } else {
              cur_row_->cells_[cell_idx].set_uint64(sess_info->get_master_sessid());
            }
            break;
          }
          case USER_CLIENT_IP: {
            if (sess_info->get_client_ip().empty()) {
              cur_row_->cells_[cell_idx].set_null();
            } else {
              cur_row_->cells_[cell_idx].set_varchar(sess_info->get_client_ip());
              cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            }
            break;
          }
          case USER_HOST: {
            if (sess_info->get_host_name().empty()) {
              cur_row_->cells_[cell_idx].set_null();
            } else {
              cur_row_->cells_[cell_idx].set_varchar(sess_info->get_host_name());
              cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            }
            break;
          }
          case TRANS_ID: {
            uint64_t tx_id = sess_info->get_tx_id();
            cur_row_->cells_[cell_idx].set_uint64(tx_id);
            break;
          }
          case THREAD_ID: {
            cur_row_->cells_[cell_idx].set_uint64(
                static_cast<uint64_t>(sess_info->get_thread_id()));
            break;
          }
          case SSL_CIPHER: {
            if (sess_info->get_ssl_cipher().empty()) {
              cur_row_->cells_[cell_idx].set_null();
            } else {
              cur_row_->cells_[cell_idx].set_varchar(sess_info->get_ssl_cipher());
              cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            }
            break;
          }
          case TRACE_ID: {
            if (obmysql::COM_QUERY == sess_info->get_mysql_cmd() ||
                obmysql::COM_STMT_EXECUTE == sess_info->get_mysql_cmd() ||
                obmysql::COM_STMT_PREPARE == sess_info->get_mysql_cmd() ||
                obmysql::COM_STMT_PREXECUTE == sess_info->get_mysql_cmd()) {
              int len = sess_info->get_current_trace_id().to_string(trace_id_, sizeof(trace_id_));
              cur_row_->cells_[cell_idx].set_varchar(trace_id_, len);
              cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            } else {
              // when cmd=Sleep, we don't want to display its last query trace id
              // as it is weird, not the meaning for 'processlist'
              cur_row_->cells_[cell_idx].set_null();
            }
            break;
          }
          case REF_COUNT: {
            cur_row_->cells_[cell_idx].set_int(sess_info->get_sess_ref_cnt());
            break;
          }
          case BACKTRACE: {
            cur_row_->cells_[cell_idx].set_varchar(sess_info->get_sess_bt());
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            break;
          }
          case TRANS_STATE: {
            if (sess_info->is_in_transaction()) {
              cur_row_->cells_[cell_idx].set_varchar(
                sess_info->get_tx_desc()->get_tx_state_str());
            } else {
              cur_row_->cells_[cell_idx].set_varchar("");
            }
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
            break;
          }
          case USER_CLIENT_PORT: {
            cur_row_->cells_[cell_idx].set_int(sess_info->get_client_addr_port());
            break;
          }
          case TOTAL_CPU_TIME: {
            if (ObSQLSessionState::QUERY_ACTIVE == sess_info->get_session_state()) {
              // time_sec = current time - sql packet received from easy time
              double time_sec = (static_cast<double> (sess_info->get_retry_active_time() + current_time - sess_info->get_cur_state_start_time())) / 1000000;
              cur_row_->cells_[cell_idx].set_double(time_sec);
            } else {
              double time_sec = (static_cast<double> (sess_info->get_retry_active_time())) / 1000000;
              cur_row_->cells_[cell_idx].set_double(time_sec);
            }
            cur_row_->cells_[cell_idx].set_scale(6);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                       K(i), K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          cell_idx++;
        }
      } // for
      if (OB_UNLIKELY(OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_->add_row(*cur_row_)))) {
        SERVER_LOG(WARN, "fail to add row", K(ret), K(*cur_row_));
      }
    }
  }
  return ret;
}

void ObAllVirtualSessionInfo::FillScanner::reset()
{
  allocator_ = NULL;
  scanner_ = NULL;
  cur_row_ = NULL;
  my_session_ = NULL;
  trace_id_[0] = '\0';
  schema_guard_ = NULL;
  output_column_ids_.reset();
  table_schema_ = NULL;
}

int ObAllVirtualSessionInfo::FillScanner::init(ObIAllocator *allocator,
                                               common::ObScanner *scanner,
                                               sql::ObSQLSessionInfo *session_info,
                                               common::ObNewRow *cur_row,
                                               const ObIArray<uint64_t> &column_ids,
                                               share::schema::ObSchemaGetterGuard* schema_guard,
                                               const share::schema::ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == allocator
                  || NULL == scanner
                  || NULL == cur_row
                  || NULL == session_info
                  || NULL == table_schema)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
               "some parameter is NULL", K(ret), K(allocator), K(scanner), K(cur_row),
                K(session_info), K(table_schema));
  } else if (OB_FAIL(output_column_ids_.assign(column_ids))) {
    SQL_ENG_LOG(WARN, "fail to assign output column ids", K(ret), K(column_ids));
  } else {
    allocator_ = allocator;
    scanner_ = scanner;
    cur_row_ = cur_row;
    my_session_ = session_info;
    schema_guard_ = schema_guard;
    table_schema_ = table_schema;
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
