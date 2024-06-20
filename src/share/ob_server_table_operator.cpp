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

#include "share/ob_server_table_operator.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/config/ob_server_config.h"
#include "share/ob_dml_sql_splicer.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_heartbeat_service.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace rootserver;

namespace share
{
ObServerInfoInTable::ObServerInfoInTable()
{
  reset();
}
ObServerInfoInTable::~ObServerInfoInTable()
{
}
int ObServerInfoInTable::init(
    const common::ObAddr &server,
    const uint64_t server_id,
    const common::ObZone &zone,
    const int64_t sql_port,
    const bool with_rootserver,
    const ObServerStatus::DisplayStatus status,
    const ObBuildVersion &build_version,
    const int64_t stop_time,
    const int64_t start_service_time,
    const int64_t last_offline_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid()
      || !is_valid_server_id(server_id)
      || zone.is_empty()
      || sql_port <= 0
      || status >= ObServerStatus::OB_DISPLAY_MAX
      || build_version.is_empty()
      || stop_time < 0
      || start_service_time < 0
      || (0 == last_offline_time_ && ObServerStatus::OB_SERVER_INACTIVE == status_)
      || (last_offline_time_ > 0 && ObServerStatus::OB_SERVER_ACTIVE == status_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(server_id), K(zone), K(sql_port),
        K(with_rootserver), K(status), K(build_version), K(stop_time), K(start_service_time),
        K(last_offline_time));
  } else {
    if (OB_FAIL(zone_.assign(zone))) {
      LOG_WARN("fail to assign zone", KR(ret), K(zone));
    } else if (OB_FAIL(build_version_.assign(build_version))) {
      LOG_WARN("fail to assign build_version", KR(ret),  K(build_version));
    } else {
      server_ = server;
      server_id_  = server_id;
      sql_port_ = sql_port;
      with_rootserver_ = with_rootserver;
      status_ = status;
      stop_time_ = stop_time;
      start_service_time_ = start_service_time;
      block_migrate_in_time_ = 0;
      last_offline_time_ = last_offline_time;
    }
  }
  return ret;
}
int ObServerInfoInTable::assign(const ObServerInfoInTable &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_.assign(other.zone_))) {
    LOG_WARN("fail to assign zone", KR(ret), K(other.zone_));
  } else if (OB_FAIL(build_version_.assign(other.build_version_))) {
    LOG_WARN("fail to assign build version", KR(ret),  K(other.build_version_));
  } else {
    server_ = other.server_;
    server_id_  = other.server_id_;
    sql_port_ = other.sql_port_;
    with_rootserver_ = other.with_rootserver_;
    status_ = other.status_;
    stop_time_ = other.stop_time_;
    start_service_time_ = other.start_service_time_;
    block_migrate_in_time_ = other.block_migrate_in_time_;
    last_offline_time_ = other.last_offline_time_;
  }
  return ret;
}
bool ObServerInfoInTable::is_valid() const
{
  return server_.is_valid()
      && is_valid_server_id(server_id_)
      && !zone_.is_empty()
      && sql_port_ > 0
      && status_ < ObServerStatus::OB_DISPLAY_MAX
      && !build_version_.is_empty()
      && stop_time_ >= 0
      && start_service_time_ >= 0
      && ((0 == last_offline_time_ && ObServerStatus::OB_SERVER_INACTIVE != status_)
          || (last_offline_time_ > 0 && ObServerStatus::OB_SERVER_ACTIVE != status_));
}
void ObServerInfoInTable::reset()
{
  server_.reset();
  server_id_ = OB_INVALID_ID;
  zone_.reset();
  sql_port_ = 0;
  with_rootserver_ = false;
  status_ = ObServerStatus::OB_DISPLAY_MAX;
  build_version_.reset();
  stop_time_ = 0;
  start_service_time_ = 0;
  block_migrate_in_time_ = 0;
  last_offline_time_ = 0;
}
int ObServerInfoInTable::build_server_info_in_table(const share::ObServerStatus &server_status)
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable::ObBuildVersion build_version;
  reset();
  if (OB_UNLIKELY(!server_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server status", KR(ret), K(server_status));
  } else if (OB_FAIL(build_version.assign(server_status.build_version_))) {
    LOG_WARN("fail to assign build_version", KR(ret), K(server_status.build_version_));
  } else if(OB_FAIL(init(
      server_status.server_,
      server_status.get_server_id(),
      server_status.zone_,
      server_status.sql_port_,
      server_status.with_rootserver_,
      server_status.get_display_status(),
      build_version,
      server_status.stop_time_,
      server_status.start_service_time_,
      server_status.last_offline_time_))) {
    LOG_WARN("fail to build server_info_in_table", KR(ret), K(server_status), K(build_version));
  }
  return ret;
}
int ObServerInfoInTable::build_server_status(share::ObServerStatus &server_status) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  server_status.reset();
  int64_t build_version_len = build_version_.size();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server info", KR(ret), KPC(this));
  } else if (OB_FAIL(server_status.zone_.assign(zone_))) {
    LOG_WARN("fail to assign zone", KR(ret), K(zone_));
  } else if (build_version_len >= OB_SERVER_VERSION_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("build_version is too long", KR(ret), K(build_version_len));
  } else {
    server_status.server_ = server_;
    server_status.id_ = server_id_;
    server_status.sql_port_ = sql_port_;
    server_status.with_rootserver_ = with_rootserver_;
    strncpy(server_status.build_version_, build_version_.ptr(), build_version_len);
    server_status.stop_time_ = stop_time_;
    server_status.start_service_time_ = start_service_time_;
    server_status.last_offline_time_ = last_offline_time_;
    if (ObServerStatus::OB_SERVER_DELETING == status_) {
      server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_DELETING;
    } else {
      server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
    }
    if (0 == last_offline_time_) {
      server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
    } else if (now - last_offline_time_ >= GCONF.server_permanent_offline_time - GCONF.lease_time) {
      // last_offline_time = last_hb_time + GCONF.lease_time
      server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE;
    } else {
      server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
    }
  }
  return ret;
}
bool ObServerInfoInTable::is_permanent_offline() const
{
  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  bool is_permanent_offline = false;
  if (last_offline_time_ > 0) {
    int64_t last_hb_time = last_offline_time_ - GCONF.lease_time;
    is_permanent_offline = (now - last_hb_time >= GCONF.server_permanent_offline_time);
  }
  return is_permanent_offline;
}
bool ObServerInfoInTable::is_temporary_offline() const
{
  bool is_temporary_offline = false;
  if (last_offline_time_ > 0 && !is_permanent_offline()) {
    is_temporary_offline = true;
  }
  return is_temporary_offline;
}
ObServerTableOperator::ObServerTableOperator()
  : inited_(false),
    proxy_(NULL)
{
}

ObServerTableOperator::~ObServerTableOperator()
{
}

int ObServerTableOperator::init(common::ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    proxy_ = proxy;
    inited_ = true;
  }
  return ret;
}
int ObServerTableOperator::get(common::ObIArray<share::ObServerStatus> &server_statuses)
{
  int ret = OB_SUCCESS;
  ObZone empty_zone;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy_ is null", KR(ret), KP(proxy_));
  } else if (OB_FAIL(get(*proxy_, empty_zone, server_statuses))) {
    LOG_WARN("fail to get", KR(ret), KP(proxy_));
  }
  return ret;
}
int ObServerTableOperator::get(
    common::ObISQLClient &sql_proxy,
    const ObZone &zone,
    common::ObIArray<ObServerStatus> &server_statuses)
{
  int ret = OB_SUCCESS;
  server_statuses.reset();
  ObSqlString sql;
  ObTimeoutCtx ctx;
  if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("SELECT *, time_to_usec(gmt_modified) AS last_hb_time "
      "FROM %s", OB_ALL_SERVER_TNAME))) {
    LOG_WARN("sql assign_fmt failed", KR(ret), K(sql));
  } else if (!zone.is_empty() && OB_FAIL(sql.append_fmt(" WHERE zone = '%s'", zone.str().ptr()))) {
    LOG_WARN("fail to assign zone condition", KR(ret), K(sql));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else {
        ObServerStatus server_status;
        while (OB_SUCC(ret)) {
          server_status.reset();
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("result next failed", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(build_server_status(*result, server_status))) {
            LOG_WARN("build server status failed", KR(ret));
          } else if (OB_FAIL(server_statuses.push_back(server_status))) {
            LOG_WARN("build server_status failed", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObServerTableOperator::remove(const common::ObAddr &server, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char ip_buf[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObTimeoutCtx ctx;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (false == server.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(server), K(ret));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE FROM %s WHERE SVR_IP=\'%s\' AND SVR_PORT=%d",
      OB_ALL_SERVER_TNAME, ip_buf, server.get_port()))) {
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

int ObServerTableOperator::update(const ObServerStatus &server_status)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server status", KR(ret), K(server_status));
  } else if (OB_FAIL(insert_dml_builder(server_status, dml))) {
    LOG_WARN("fail to build insert dml", KR(ret), K(server_status));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
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

int ObServerTableOperator::reset_rootserver(const ObAddr &except)
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
  } else if (OB_FAIL(sql.assign_fmt(
      "UPDATE %s SET WITH_ROOTSERVER=%d WHERE WITH_ROOTSERVER=%d "
      "AND (SVR_IP, SVR_PORT) != ('%s', %d)",
      OB_ALL_SERVER_TNAME, false, true, svr_ip, except.get_port()))) {
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

int ObServerTableOperator::update_status(
    const common::ObAddr &server,
    const ObServerStatus::DisplayStatus status,
    const int64_t last_hb_time,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char *display_status_str = NULL;
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
    } else if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET status = '%s', gmt_modified = usec_to_time(%ld) "
        "WHERE svr_ip = '%s' AND svr_port = %d",
        OB_ALL_SERVER_TNAME, display_status_str,
        // set min gmt_modified to 1 second to avoid mysql 1292 error.
        std::max(last_hb_time, 1000000L), ip, server.get_port()))) {
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

int ObServerTableOperator::update_stop_time(const ObAddr &server,
    const int64_t stop_time)
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
    } else if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET stop_time = %ld "
        "WHERE svr_ip = '%s' AND svr_port = %d",
        OB_ALL_SERVER_TNAME, stop_time, ip, server.get_port()))) {
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

int ObServerTableOperator::build_server_status(
    const ObMySQLResult &res,
    ObServerStatus &server_status)
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
  if (OB_SUCC(ret)) {
    EXTRACT_INT_FIELD_MYSQL(res, "last_hb_time", last_hb_time, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "id", server_status.id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(res, "zone", server_status.zone_.ptr(),
        MAX_ZONE_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(res, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(res, "svr_port", svr_port, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "inner_port", server_status.sql_port_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(res, "status", svr_status,
        OB_SERVER_STATUS_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(res, "with_rootserver", with_rootserver, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "block_migrate_in_time",
        server_status.block_migrate_in_time_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "start_service_time",
        server_status.start_service_time_, int64_t);

    EXTRACT_STRBUF_FIELD_MYSQL(res, "build_version", server_status.build_version_,
        OB_SERVER_VERSION_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(res, "stop_time", server_status.stop_time_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "with_partition", with_partition, int64_t);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(
    res, "last_offline_time", server_status.last_offline_time_, int64_t,
    false/*skip_null_error*/, true/*skip_column_error*/, 0/*invalid_default_value*/);
    (void) tmp_real_str_len; // make compiler happy
    server_status.server_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port));
    server_status.with_rootserver_ = static_cast<bool>(with_rootserver);
    server_status.with_partition_ = static_cast<bool>(with_partition);
    ObServerStatus::DisplayStatus display_status = ObServerStatus::OB_DISPLAY_MAX;
    const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
    if (FAILEDx(ObServerStatus::str2display_status(svr_status, display_status))) {
      LOG_WARN("string to display status failed", K(ret), K(svr_status));
    } else if (display_status < 0 || display_status >= ObServerStatus::OB_DISPLAY_MAX) {
      ret = OB_INVALID_SERVER_STATUS;
      LOG_WARN("invalid display status", K(svr_status), K(ret));
    } else {
      // set server heartbeat status
      if (ObServerStatus::OB_SERVER_ACTIVE == display_status) {
        server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
        server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
        server_status.last_hb_time_ = now;
        server_status.lease_expire_time_ = now + ObLeaseRequest::SERVICE_LEASE;
      } else if (ObServerStatus::OB_SERVER_DELETING == display_status) {
        // Assumption: there is no deleting server while upgrading system to V4.2
        server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_DELETING;
        if (0 == server_status.last_offline_time_) {
          server_status.last_hb_time_ = now;
          server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
          server_status.lease_expire_time_ = now + ObLeaseRequest::SERVICE_LEASE;
        } else {
          server_status.last_hb_time_ = server_status.last_offline_time_ - GCONF.lease_time;
          server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
          if (now - server_status.last_hb_time_ > GCONF.server_permanent_offline_time) {
            server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE;
          }
        }
      } else if (ObServerStatus::OB_SERVER_INACTIVE == display_status) {
        server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
        server_status.last_hb_time_ = last_hb_time;
        server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
        if (now - last_hb_time > GCONF.server_permanent_offline_time) {
          server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE;
        }
        if (0 == server_status.last_offline_time_) {
          server_status.last_offline_time_ = server_status.last_hb_time_ + GCONF.lease_time;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown display status", K(display_status), K(svr_status), K(server_status.server_));
      }
    }
  }
  return ret;
}

int ObServerTableOperator::update_with_partition(
    const common::ObAddr &server,
    bool with_partition)
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
    const char *update_modify_time = with_partition ? "" : ", gmt_modified = gmt_modified";
    int64_t affected_rows = 0;
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET with_partition = %d%s WHERE "
        "svr_ip = '%s' AND svr_port = %d",
        OB_ALL_SERVER_TNAME, with_partition, update_modify_time, svr_ip, server.get_port()))) {
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

int ObServerTableOperator::get_start_service_time(
    const common::ObAddr &server,
    int64_t &start_service_time) const
{
  int ret = OB_SUCCESS;
  char svr_ip[OB_IP_STR_BUFF] = "";
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (false == server.ip_to_string(svr_ip, sizeof(svr_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(server));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("SELECT start_service_time FROM %s WHERE svr_ip = '%s' AND"
               " svr_port = %d", OB_ALL_SERVER_TNAME, svr_ip, server.get_port()))) {
      LOG_WARN("fail to append sql", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        int tmp_ret = OB_SUCCESS;
        ObMySQLResult *result = NULL;
        if (OB_FAIL(proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get sql result", K(sql), K(ret));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get next", KR(ret), K(sql));;
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "start_service_time", start_service_time, int64_t);
        }
        if (OB_SUCC(ret) && (OB_ITER_END != (tmp_ret = result->next()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  return ret;
}
int ObServerTableOperator::get(
  common::ObISQLClient &sql_proxy,
  common::ObIArray<ObServerInfoInTable> &all_servers_info_in_table)
{
  all_servers_info_in_table.reset();
  ObZone empty_zone;
  const bool ONLY_ACTIVE_SERVERS = false;
  return get_servers_info_of_zone(sql_proxy, empty_zone, ONLY_ACTIVE_SERVERS, all_servers_info_in_table);
}
int ObServerTableOperator::get_servers_info_of_zone(
    common::ObISQLClient &sql_proxy,
    const ObZone &zone,
    const bool only_active_servers,
    common::ObIArray<ObServerInfoInTable> &all_servers_info_in_zone)
{
  int ret = OB_SUCCESS;
  ObArray<ObServerStatus> server_statuses;
  all_servers_info_in_zone.reset();
  if (OB_FAIL(get(sql_proxy, zone, server_statuses))) {
    LOG_WARN("fail to get server status", KR(ret));
  } else {
    ObServerInfoInTable server_info;
    ARRAY_FOREACH_X(server_statuses, idx, cnt, OB_SUCC(ret)) {
      server_info.reset();
      if (OB_FAIL(server_info.build_server_info_in_table(server_statuses.at(idx)))) {
        LOG_WARN("fail to build server info in table", KR(ret), K(server_statuses.at(idx)));
      } else if (only_active_servers && !server_info.is_active()) {
        // do nothing
      } else if (OB_FAIL(all_servers_info_in_zone.push_back(server_info))) {
        LOG_WARN("fail to push element into all_servers_info_in_zone", KR(ret), K(server_info));
      }
    }
  }
  return ret;
}

int ObServerTableOperator::get(
    common::ObISQLClient &sql_proxy,
    const common::ObAddr &server,
    ObServerInfoInTable &server_info_in_table)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  char svr_ip[OB_IP_STR_BUFF] = "";
  server_info_in_table.reset();
  if (OB_UNLIKELY(!server.is_valid() || !server.ip_to_string(svr_ip, sizeof(svr_ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("SELECT *, time_to_usec(gmt_modified) AS last_hb_time "
      "FROM %s WHERE svr_ip = '%s' AND svr_port = %d", OB_ALL_SERVER_TNAME, svr_ip, server.get_port()))) {
    LOG_WARN("fail to append sql", KR(ret), K(server));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      int tmp_ret = OB_SUCCESS;
      ObMySQLResult *result = NULL;
      ObServerStatus server_status;
      if (OB_FAIL(sql_proxy.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SERVER_NOT_IN_WHITE_LIST;
        }
        LOG_WARN("fail to get next", KR(ret), K(sql));;
      } else if (OB_FAIL(build_server_status(*result, server_status))) {
        LOG_WARN("fail to build server_status",KR(ret));
      } else if (OB_FAIL(server_info_in_table.build_server_info_in_table(server_status))) {
        LOG_WARN("fail to build server_info_in_table", KR(ret), K(server_status));
      }
      if (OB_SUCC(ret) && (OB_ITER_END != (tmp_ret = result->next()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
      }
    }
  }
  LOG_INFO("get server from table", KR(ret), K(server), K(server_info_in_table));
  return ret;
}
int ObServerTableOperator::insert(
    common::ObISQLClient &sql_proxy,
    const ObServerInfoInTable &server_info_in_table)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObServerStatus server_status;
  if (OB_UNLIKELY(!server_info_in_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_info_in_table));
  } else if (OB_FAIL(server_info_in_table.build_server_status(server_status))) {
    LOG_WARN("fail to build server status", KR(ret), K(server_info_in_table));
  } else if (OB_FAIL(insert_dml_builder(server_status, dml))) {
    LOG_WARN("fail to build insert dml", KR(ret), K(server_info_in_table));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else {
      ObDMLExecHelper exec(sql_proxy, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert(OB_ALL_SERVER_TNAME, dml, affected_rows))) {
        LOG_WARN("fail to exec update", KR(ret), K(server_info_in_table));
      } else if (is_zero_row(affected_rows)) {
        ret = OB_NEED_RETRY;
        LOG_WARN("no affected rows, please retry the operation or "
            "check the table to see if it has been inserted already",
            KR(ret), K(affected_rows), K(server_info_in_table));
      } else if (is_single_row(affected_rows)) {
          LOG_INFO("insert one row into the table successfully", KR(ret), K(affected_rows),
          K(server_info_in_table));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error appears, more than one affected row",
            KR(ret), K(affected_rows), K(server_info_in_table));
      }
    }
  }
  return ret;
}
int ObServerTableOperator::update_status(
    ObMySQLTransaction &trans,
    const common::ObAddr &server,
    const ObServerStatus::DisplayStatus old_status,
    const ObServerStatus::DisplayStatus new_status)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char *old_display_status_str = NULL;
  const char *new_display_status_str = NULL;
  if (OB_UNLIKELY(!server.is_valid() || !server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (OB_FAIL(ObServerStatus::display_status_str(old_status, old_display_status_str))) {
    LOG_WARN("get display status string failed", KR(ret), K(old_status));
  } else if (OB_FAIL(ObServerStatus::display_status_str(new_status, new_display_status_str))) {
    LOG_WARN("get display status string failed", KR(ret), K(new_status));
  } else if (OB_ISNULL(old_display_status_str) || OB_ISNULL(new_display_status_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there exists null display_status_str", KR(ret), KP(old_display_status_str), KP(new_display_status_str));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET status = '%s' WHERE svr_ip = '%s' AND svr_port = %d "
        "AND status = '%s'", OB_ALL_SERVER_TNAME, new_display_status_str, ip, server.get_port(),
            old_display_status_str))) {
      LOG_WARN("fail to assign fmt", KR(ret), K(ip), K(server.get_port()));
    } else if (OB_FAIL(exec_write(trans, sql, false /* is_multi_rows_affected */))) {
      LOG_WARN("fail to update the table", KR(ret), K(sql));
    } else {}
  }
  return ret;
}
int ObServerTableOperator::update_with_rootserver(
      ObMySQLTransaction &trans,
      const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(!server.is_valid() || !server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
      "UPDATE %s SET WITH_ROOTSERVER = (CASE WHEN (SVR_IP, SVR_PORT) = ('%s', %d) THEN 1 ELSE 0 END)",
      OB_ALL_SERVER_TNAME, ip, server.get_port()))) {
      LOG_WARN("fail to assign fmt", KR(ret));
    } else if (OB_FAIL(exec_write(trans, sql, true /* is_multi_rows_affected*/))) {
      LOG_WARN("fail to update the table", KR(ret), K(sql));
    } else {}
  }
  return ret;
}
int ObServerTableOperator::update_build_version(
    ObMySQLTransaction &trans,
      const common::ObAddr &server,
      const ObServerInfoInTable::ObBuildVersion &old_build_version,
      const ObServerInfoInTable::ObBuildVersion &new_build_version)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(new_build_version.is_empty()
      || old_build_version.is_empty()
      || !server.is_valid()
      || !server.ip_to_string(ip, sizeof(ip))
      || old_build_version == new_build_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(new_build_version), K(old_build_version));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET build_version = '%s' "
        "WHERE svr_ip = '%s' AND svr_port = %d AND build_version = '%s'",
        OB_ALL_SERVER_TNAME, new_build_version.ptr(), ip, server.get_port(), old_build_version.ptr()))) {
      LOG_WARN("fail to assign fmt", KR(ret));
    } else if (OB_FAIL(exec_write(trans, sql, false /* is_multi_rows_affected*/))) {
      LOG_WARN("fail to update the table", KR(ret), K(sql));
    } else {}
  }
  return ret;
}

int ObServerTableOperator::update_start_service_time(
    ObMySQLTransaction &trans,
    const common::ObAddr &server,
    const int64_t old_start_service_time,
    const int64_t new_start_service_time)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(new_start_service_time < 0
      || old_start_service_time < 0
      || !server.is_valid()
      || !server.ip_to_string(ip, sizeof(ip))
      || old_start_service_time == new_start_service_time)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(new_start_service_time),
        K(old_start_service_time));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET start_service_time = %ld "
        "WHERE svr_ip = '%s' AND svr_port = %d AND start_service_time = %ld",
        OB_ALL_SERVER_TNAME, new_start_service_time, ip, server.get_port(), old_start_service_time))) {
      LOG_WARN("fail to assign fmt", KR(ret));
    } else if (OB_FAIL(exec_write(trans, sql, false /* is_multi_rows_affected */))) {
      LOG_WARN("fail to update the table", KR(ret), K(sql));
    } else {}
  }
  return ret;
}
int ObServerTableOperator::update_table_for_offline_to_online_server(
      ObMySQLTransaction &trans,
      const bool is_deleting,
      const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(!server.is_valid()
      || !server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET last_offline_time = 0%s"
        " WHERE svr_ip = '%s' AND svr_port = %d AND status != 'ACTIVE'",
        OB_ALL_SERVER_TNAME, (is_deleting ? "" : ", status='ACTIVE'"),
        ip, server.get_port()))) {
      LOG_WARN("fail to assign fmt", KR(ret));
    } else if (OB_FAIL(exec_write(trans, sql, false /* is_multi_rows_affected */))) {
      LOG_WARN("fail to update the table", KR(ret), K(sql));
    } else {}
  }
  return ret;
}
int ObServerTableOperator::update_table_for_online_to_offline_server(
      ObMySQLTransaction &trans,
      const common::ObAddr &server,
      const bool is_deleting,
      int64_t last_offline_time)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(last_offline_time <= 0
      || !server.is_valid()
      || !server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(last_offline_time));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET last_offline_time = %ld, start_service_time = 0%s"
        " WHERE svr_ip = '%s' AND svr_port = %d AND status != 'INACTIVE'",
        OB_ALL_SERVER_TNAME, last_offline_time, (is_deleting ? "" : ", status='INACTIVE'"),
        ip, server.get_port()))) {
      LOG_WARN("fail to assign fmt", KR(ret));
    } else if (OB_FAIL(exec_write(trans, sql, false /* is_multi_rows_affected */))) {
      LOG_WARN("fail to update the table", KR(ret), K(sql));
    } else {}
  }
  return ret;
}
int ObServerTableOperator::update_stop_time(
    ObMySQLTransaction &trans,
    const common::ObAddr &server,
    const int64_t old_stop_time,
    const int64_t new_stop_time)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(!server.is_valid()
      || !server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (old_stop_time == new_stop_time
      || old_stop_time < 0
      || new_stop_time < 0
      || (old_stop_time > 0 && new_stop_time > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(old_stop_time), K(new_stop_time));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET stop_time = %ld "
        "WHERE svr_ip = '%s' AND svr_port = %d AND stop_time = %ld",
        OB_ALL_SERVER_TNAME, new_stop_time, ip, server.get_port(), old_stop_time))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(exec_write(trans, sql, false /* is_multi_rows_affected */))) {
      LOG_WARN("fail to update the table", KR(ret), K(sql));
    } else {}
  }
  return ret;
}
int ObServerTableOperator::insert_dml_builder(
    const ObServerStatus &server_status,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char *display_status_str = NULL;
  dml.reset();
  int64_t build_version_len = strlen(server_status.build_version_);
  if (OB_UNLIKELY(!server_status.is_valid()
      || !server_status.server_.ip_to_string(svr_ip, sizeof(svr_ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_status));
  } else if (OB_FAIL(ObServerStatus::display_status_str(
      server_status.get_display_status(), display_status_str))) {
    LOG_WARN("fail to get display status str", KR(ret),
        "display_status", server_status.get_display_status());
  } else if (OB_ISNULL(display_status_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null display status string", KR(ret), KP(display_status_str));
  } else if (OB_UNLIKELY(build_version_len >= OB_SERVER_VERSION_LENGTH)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("build_version is too long", KR(ret), K(build_version_len));
  } else {
    if (OB_FAIL(dml.add_pk_column(K(svr_ip)))
        || OB_FAIL(dml.add_pk_column("svr_port", server_status.server_.get_port()))
        || OB_FAIL(dml.add_column("id", server_status.get_server_id()))
        || OB_FAIL(dml.add_column("zone", server_status.zone_.ptr()))
        || OB_FAIL(dml.add_column("inner_port", server_status.sql_port_))
        || OB_FAIL(dml.add_column("block_migrate_in_time", server_status.block_migrate_in_time_))
        || OB_FAIL(dml.add_column("with_partition", true))
        || OB_FAIL(dml.add_column("with_rootserver", server_status.with_rootserver_)
        || OB_FAIL(dml.add_column("start_service_time", server_status.start_service_time_))
        || OB_FAIL(dml.add_column("status", display_status_str))
        || OB_FAIL(dml.add_column("build_version", server_status.build_version_))
        || OB_FAIL(dml.add_column("stop_time", server_status.stop_time_)))) {
      LOG_WARN("fail to add column", KR(ret));
    } else {
      if (!ObHeartbeatService::is_service_enabled()) { // the old logic
        if (OB_FAIL(dml.add_gmt_modified(server_status.last_hb_time_))) {
          LOG_WARN("fail to add gmt_modified", KR(ret), K(server_status.last_hb_time_));
        }
      } else { // in version 4.2, we add a new column last_offline_time
        const ObServerStatus::DisplayStatus display_status = server_status.get_display_status();
        const int64_t last_offline_time = server_status.last_offline_time_;
        if ((0 == last_offline_time && ObServerStatus::OB_SERVER_INACTIVE == display_status)
            || (last_offline_time > 0 && ObServerStatus::OB_SERVER_ACTIVE == display_status)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid last_offline_time", KR(ret), K(last_offline_time), K(display_status));
        } else if (OB_FAIL(dml.add_column("last_offline_time", server_status.last_offline_time_))) {
          LOG_WARN("fail to add last_offline_time", KR(ret));
        }
      }
    }
  }
  return ret;
}
int ObServerTableOperator::exec_write(
    ObMySQLTransaction &trans,
    ObSqlString &sql,
    const bool is_multi_rows_affected)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!sql.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sql));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (is_zero_row(affected_rows)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("no affected rows, please retry the operation or "
        "check the table to see if it has been updated already",
        KR(ret), K(affected_rows), K(sql));
  } else if (!is_multi_rows_affected && !is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error appears, more than one affected row",
        KR(ret), K(affected_rows), K(sql));
  } else {}
  LOG_INFO("update __all_server table", KR(ret), K(affected_rows), K(sql));
  return ret;
}
}//end namespace rootserver
}//end namespace oceanbase