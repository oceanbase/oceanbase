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
#include "observer/virtual_table/ob_virtual_proxy_server_stat.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace observer
{
ObVirtualProxyServerStat::ObVirtualProxyServerStat()
  : is_inited_(false),
    is_queried_(false),
    table_schema_(NULL),
    sql_proxy_(NULL),
    server_state_(),
    server_idx_(-1),
    server_states_(),
    config_(NULL)
{
}

ObVirtualProxyServerStat::~ObVirtualProxyServerStat()
{
}

int ObVirtualProxyServerStat::init(ObMultiVersionSchemaService &schema_service,
                                   ObMySQLProxy *sql_proxy,
                                   common::ObServerConfig *config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "sql_proxy is NULL", K(ret));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", KR(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schema(OB_SYS_TENANT_ID,
    OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TID, table_schema_))) {
    SERVER_LOG(WARN, "failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table_schema_ is NULL", KP_(table_schema), K(ret));
  } else {
    is_queried_ = false;
    server_state_.reset();
    server_idx_ = -1;
    server_states_.reset();
    sql_proxy_ = sql_proxy;
    config_ = config;
    is_inited_ = true;
  }
  return ret;
}

ObVirtualProxyServerStat::ObServerStateInfo::ObServerStateInfo()
{
  reset();
}

void ObVirtualProxyServerStat::ObServerStateInfo::reset()
{
  svr_ip_buf_[0] = '\0';
  svr_ip_len_ = 0;
  svr_port_ = -1;
  zone_name_buf_[0] = '\0';
  zone_name_len_ = 0;
  status_buf_[0] = '\0';
  status_len_ = 0;
  start_service_time_ = 0;
  stop_time_ = 0;
}

bool ObVirtualProxyServerStat::ObServerStateInfo::is_valid() const
{
  return svr_ip_len_ > 0
    && static_cast<int64_t>(strlen(svr_ip_buf_)) == svr_ip_len_
    && svr_port_ >= 0 // the condition of inner_port=0 in __all_server is exist
    && status_len_ > 0
    && static_cast<int64_t>(strlen(status_buf_)) == status_len_
    && OB_INVALID_TIMESTAMP != start_service_time_
    && OB_INVALID_TIMESTAMP != stop_time_;
}


int ObVirtualProxyServerStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is null" , K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited" , K(ret));
  } else if (!start_to_read_) {
    start_to_read_ = true;
  }

  if (OB_SUCC(ret)) {
    ObArray<Column> columns;
    if (OB_FAIL(get_next_server_state())) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "failed to get server info", K(ret));
      }
    } else if (OB_FAIL(get_full_row(table_schema_, server_state_, columns))) {
      SERVER_LOG(WARN, "failed to get full row", "table_schema", *table_schema_, K(server_state_), K(ret));
    } else if (OB_FAIL(project_row(columns, cur_row_))) {
      SERVER_LOG(WARN, "failed to project row", K(ret));
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObVirtualProxyServerStat::get_full_row(const share::schema::ObTableSchema *table,
                                           const ObServerStateInfo &server_state,
                                           ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited" , K(ret));
  } else if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "table is null", K(ret));
  } else if (!server_state.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid server_state", K(server_state), K(ret));
  } else {
    ADD_COLUMN(set_varchar, table, "svr_ip",
               ObString(server_state.svr_ip_len_, server_state.svr_ip_buf_), columns);
    ADD_COLUMN(set_int, table, "svr_port", server_state.svr_port_, columns);
    ADD_COLUMN(set_varchar, table, "zone",
               ObString(server_state.zone_name_len_, server_state.zone_name_buf_), columns);
    ADD_COLUMN(set_varchar, table, "status",
               ObString(server_state.status_len_, server_state.status_buf_), columns);
    ADD_COLUMN(set_int, table, "start_service_time", server_state.start_service_time_, columns);
    ADD_COLUMN(set_int, table, "stop_time", server_state.stop_time_, columns);
  }
  return ret;
}


int ObVirtualProxyServerStat::get_next_server_state()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited" , K(ret));
  } else if (is_queried_ && (server_idx_ == server_states_.count())) {
    ret = OB_ITER_END;
  } else {
    if (!is_queried_) {
      if (OB_FAIL(get_all_server_state())) {
        SERVER_LOG(WARN, "failed to get all server", K(ret));
      } else {
        server_idx_ = 0;
        is_queried_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (server_idx_ < 0 || server_idx_ >= server_states_.count()) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invaild server_idx_", K_(server_idx), K(ret));
      } else {
        server_state_ = server_states_[server_idx_];
        ++server_idx_;
      }
    }
  }
  return ret;
}

int ObVirtualProxyServerStat::get_all_server_state()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited" , K(ret));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);

    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      ObSqlString sql;
      const static char *SELECT_ALL_SERVER_STATE_INFO_SQL
          = "SELECT svr_ip, inner_port, zone, status, start_service_time, stop_time FROM %s "
            "WHERE inner_port > 0";
      if (OB_FAIL(sql.append_fmt(SELECT_ALL_SERVER_STATE_INFO_SQL, OB_ALL_SERVER_TNAME))) {
        SERVER_LOG(WARN, "failed to append table name", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
      } else {
        ObServerStateInfo server_state ;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          server_state.reset();
          EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", server_state.svr_ip_buf_,
                                     OB_IP_STR_BUFF+1, server_state.svr_ip_len_);
          EXTRACT_INT_FIELD_MYSQL(*result, "inner_port", server_state.svr_port_, int64_t);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, "zone", server_state.zone_name_buf_,
                                     MAX_ZONE_LENGTH+1, server_state.zone_name_len_);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", server_state.status_buf_,
                                     OB_SERVER_STATUS_LENGTH+1, server_state.status_len_);
          EXTRACT_INT_FIELD_MYSQL(*result, "start_service_time", server_state.start_service_time_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "stop_time", server_state.stop_time_, int64_t);
  
          if (OB_SUCC(ret)) {
            if (OB_FAIL(server_states_.push_back(server_state))) {
              SERVER_LOG(WARN, "failed to push back", K(server_state), K(ret));
            }
          }
        }
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to get server state info", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

}//end namespace observer
}//end namespace oceanbase
