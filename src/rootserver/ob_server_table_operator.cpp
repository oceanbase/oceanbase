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

#define USING_LOG_PREFIX RS

#include "ob_server_table_operator.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/config/ob_server_config.h"
#include "share/ob_dml_sql_splicer.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
namespace rootserver {
ObServerTableOperator::ObServerTableOperator() : inited_(false), proxy_(NULL)
{}

ObServerTableOperator::~ObServerTableOperator()
{}

int ObServerTableOperator::init(common::ObISQLClient* proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    proxy_ = proxy;
    inited_ = true;
  }
  return ret;
}

int ObServerTableOperator::get(
    common::ObIArray<ObServerStatus>& server_statuses, const int64_t cluster_id /* = common::OB_INVALID_ID */)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    server_statuses.reset();
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(gmt_modified) AS last_hb_time, "
                                      "id, zone, svr_ip, svr_port, inner_port, status, with_rootserver, "
                                      "block_migrate_in_time, build_version, stop_time, start_service_time, "
                                      "last_offline_time, with_partition "
                                      "FROM %s",
                   OB_ALL_SERVER_TNAME))) {
      LOG_WARN("sql assign_fmt failed", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        ObMySQLResult* result = NULL;
        if (OB_FAIL(proxy_->read(res, cluster_id, OB_SYS_TENANT_ID, sql.ptr()))) {
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else {
          ObServerStatus server_status;
          while (OB_SUCC(ret)) {
            server_status.reset();
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END != ret) {
                LOG_WARN("result next failed", K(ret));
              } else {
                ret = OB_SUCCESS;
                break;
              }
            } else if (OB_FAIL(build_server_status(*result, server_status))) {
              LOG_WARN("build server status failed", K(ret));
            } else if (OB_FAIL(server_statuses.push_back(server_status))) {
              LOG_WARN("build server_status failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObServerTableOperator::remove(const common::ObAddr& server, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char ip_buf[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObTimeoutCtx ctx;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (false == server.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(server), K(ret));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE SVR_IP=\'%s\' AND SVR_PORT=%d",
                 OB_ALL_SERVER_TNAME,
                 ip_buf,
                 server.get_port()))) {
    LOG_WARN("sql string assign_fmt failed", K(ret));
  } else if (!trans.is_started()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans is not start", K(ret));
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else if (!is_zero_row(affected_rows) && !is_single_row(affected_rows)) {
    // duplicated delete task may exists in the queue, so 0 == affected_rows is normal
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected deleted zero or single row", K(affected_rows), K(sql), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (is_single_row(affected_rows)) {
      LOG_INFO("delete server from all_server table succeed", K(server));
    } else if (is_zero_row(affected_rows)) {
      LOG_INFO("server not in all_server table, no need to delete", K(server));
    }
  }
  return ret;
}

int ObServerTableOperator::update(const ObServerStatus& server_status)
{
  int ret = OB_SUCCESS;
  char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char* display_status_str = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server status", K(server_status), K(ret));
  } else if (false == server_status.server_.ip_to_string(svr_ip, sizeof(svr_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", "server", server_status.server_, K(ret));
  } else if (OB_FAIL(ObServerStatus::display_status_str(server_status.get_display_status(), display_status_str))) {
    LOG_WARN("get display status str failed", K(ret), "display_status", server_status.get_display_status());
  } else if (NULL == display_status_str) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null display status string", K(ret));
  } else {
    const int64_t modify_time = server_status.last_hb_time_;
    ObDMLSqlSplicer dml;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(dml.add_pk_column(K(svr_ip))) ||
               OB_FAIL(dml.add_pk_column("svr_port", server_status.server_.get_port())) ||
               OB_FAIL(dml.add_column("id", server_status.id_)) ||
               OB_FAIL(dml.add_column("zone", server_status.zone_.ptr())) ||
               OB_FAIL(dml.add_column("inner_port", server_status.sql_port_)) ||
               OB_FAIL(dml.add_column(OBJ_K(server_status, with_rootserver))) ||
               OB_FAIL(dml.add_column(OBJ_K(server_status, block_migrate_in_time))) ||
               OB_FAIL(dml.add_column(OBJ_K(server_status, start_service_time))) ||
               OB_FAIL(dml.add_column(OBJ_K(server_status, last_offline_time))) ||
               OB_FAIL(dml.add_column("status", display_status_str)) ||
               OB_FAIL(dml.add_column(OBJ_K(server_status, build_version))) ||
               OB_FAIL(dml.add_column(OBJ_K(server_status, stop_time))) ||
               OB_FAIL(dml.add_column(OBJ_K(server_status, with_partition))) ||
               OB_FAIL(dml.add_gmt_modified(modify_time))) {
      LOG_WARN("add column failed", K(ret));
    } else {
      ObDMLExecHelper exec(*proxy_, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert_update(OB_ALL_SERVER_TNAME, dml, affected_rows))) {
        LOG_WARN("exec update failed", K(ret));
      } else if (affected_rows > 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update more than one row", K(affected_rows), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("update server status in all_server table succeed", K(server_status));
  }
  return ret;
}

int ObServerTableOperator::reset_rootserver(const ObAddr& except)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObTimeoutCtx ctx;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!except.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server address", K(ret), K(except));
  } else if (!except.ip_to_string(svr_ip, sizeof(svr_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(except));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET WITH_ROOTSERVER=%d WHERE WITH_ROOTSERVER=%d "
                                    "AND (SVR_IP, SVR_PORT) != ('%s', %d)",
                 OB_ALL_SERVER_TNAME,
                 false,
                 true,
                 svr_ip,
                 except.get_port()))) {
    LOG_WARN("sql string assign_fmt failed", K(ret));
  } else if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else if (!is_zero_row(affected_rows) && !is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect update zero or single row", K(affected_rows), K(sql), K(ret));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("reset existing rootserver to observer succeed");
  }
  return ret;
}

int ObServerTableOperator::update_status(const common::ObAddr& server, const ObServerStatus::DisplayStatus status,
    const int64_t last_hb_time, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char* display_status_str = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || last_hb_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(status), K(last_hb_time), K(ret));
  } else if (OB_FAIL(ObServerStatus::display_status_str(status, display_status_str))) {
    LOG_WARN("get display status string failed", K(ret), K(status));
  } else if (NULL == display_status_str) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL display status string", K(ret));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(server), K(ret));
  } else if (!trans.is_started()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans is not start", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET status = '%s', gmt_modified = usec_to_time(%ld) "
                                      "WHERE svr_ip = '%s' AND svr_port = %d",
                   OB_ALL_SERVER_TNAME,
                   display_status_str,
                   // set min gmt_modified to 1 second to avoid mysql 1292 error.
                   std::max(last_hb_time, 1000000L),
                   ip,
                   server.get_port()))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect update single row", K(affected_rows), K(sql), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("update server status succeed", K(server), K(status), K(last_hb_time));
  }
  return ret;
}

int ObServerTableOperator::update_stop_time(const ObAddr& server, const int64_t stop_time)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stop_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stop time can not smaller than 0", K(stop_time), K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server is invalid", K(server), K(ret));
  } else if (!server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(server), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET stop_time = %ld "
                                      "WHERE svr_ip = '%s' AND svr_port = %d",
                   OB_ALL_SERVER_TNAME,
                   stop_time,
                   ip,
                   server.get_port()))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect updating one row", K(affected_rows), K(sql), K(ret));
    }
  }

  return ret;
}

int ObServerTableOperator::build_server_status(const ObMySQLResult& res, ObServerStatus& server_status) const
{
  int ret = OB_SUCCESS;
  server_status.reset();
  int64_t tmp_real_str_len = 0;
  int64_t last_hb_time = 0;
  char svr_ip[OB_IP_STR_BUFF] = "";
  int64_t svr_port = 0;
  char svr_status[OB_SERVER_STATUS_LENGTH] = "";
  int64_t with_rootserver = 0;
  int64_t with_partition = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(res, "last_hb_time", last_hb_time, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "id", server_status.id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(res, "zone", server_status.zone_.ptr(), MAX_ZONE_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(res, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(res, "svr_port", svr_port, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "inner_port", server_status.sql_port_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(res, "status", svr_status, OB_SERVER_STATUS_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(res, "with_rootserver", with_rootserver, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "block_migrate_in_time", server_status.block_migrate_in_time_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "start_service_time", server_status.start_service_time_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "last_offline_time", server_status.last_offline_time_, int64_t);

    EXTRACT_STRBUF_FIELD_MYSQL(
        res, "build_version", server_status.build_version_, OB_SERVER_VERSION_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(res, "stop_time", server_status.stop_time_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "with_partition", with_partition, int64_t);
    (void)tmp_real_str_len;  // make compiler happy
    server_status.server_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port));
    server_status.with_rootserver_ = static_cast<bool>(with_rootserver);
    server_status.with_partition_ = static_cast<bool>(with_partition);
    ObServerStatus::DisplayStatus display_status = ObServerStatus::OB_DISPLAY_MAX;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObServerStatus::str2display_status(svr_status, display_status))) {
      LOG_WARN("string to display status failed", K(ret), K(svr_status));
    } else if (display_status < 0 || display_status >= ObServerStatus::OB_DISPLAY_MAX) {
      ret = OB_INVALID_SERVER_STATUS;
      LOG_WARN("invalid display status", K(svr_status), K(ret));
    } else {
      LOG_INFO("svr_status", K(svr_status), K(display_status));
      if (ObServerStatus::OB_SERVER_DELETING == display_status) {
        server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_DELETING;
      } else if (ObServerStatus::OB_SERVER_TAKENOVER_BY_RS == display_status) {
        server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_TAKENOVER_BY_RS;
      } else {
        server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
      }

      // set server heartbeat status
      if (ObServerStatus::OB_SERVER_ACTIVE == display_status) {
        const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
        server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
        server_status.last_hb_time_ = now;
        server_status.lease_expire_time_ = now + ObLeaseRequest::SERVICE_LEASE;
      } else if (ObServerStatus::OB_SERVER_TAKENOVER_BY_RS == display_status) {
        server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE;
        server_status.last_hb_time_ = last_hb_time;
        server_status.lease_expire_time_ = 1;
      } else if (ObServerStatus::OB_SERVER_DELETING == display_status) {
        const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
        server_status.last_hb_time_ = last_hb_time;
        server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
        if (now - last_hb_time > GCONF.server_permanent_offline_time) {
          server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE;
        }
      } else {  // ObServerStatus::OB_SERVER_INACTIVE
        server_status.last_hb_time_ = last_hb_time;
        int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
        server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
        if (now - last_hb_time > GCONF.server_permanent_offline_time) {
          server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE;
        }
      }
    }
  }
  return ret;
}

int ObServerTableOperator::update_with_partition(const common::ObAddr& server, bool with_partition)
{
  int ret = OB_SUCCESS;
  char svr_ip[OB_IP_STR_BUFF] = "";
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(with_partition));
  } else if (false == server.ip_to_string(svr_ip, sizeof(svr_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(server));
  } else {
    // Do not update gmt_modified if clear with partition flag, because we read gmt_modified
    // as last_hb_time if server not alive.
    const char* update_modify_time = with_partition ? "" : ", gmt_modified = gmt_modified";
    int64_t affected_rows = 0;
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET with_partition = %d%s WHERE "
                                      "svr_ip = '%s' AND svr_port = %d",
                   OB_ALL_SERVER_TNAME,
                   with_partition,
                   update_modify_time,
                   svr_ip,
                   server.get_port()))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(sql));
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
