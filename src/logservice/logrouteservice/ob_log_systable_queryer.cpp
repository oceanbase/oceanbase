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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_systable_queryer.h"
#include "lib/mysqlclient/ob_isql_client.h"  // ObISQLClient
#include "lib/mysqlclient/ob_mysql_result.h" // ObMySQLResult
#include "lib/string/ob_sql_string.h"      // ObSqlString
#include "share/inner_table/ob_inner_table_schema_constants.h" // OB_***_TNAME
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::share;
namespace oceanbase
{
namespace logservice
{
ObLogSysTableQueryer::ObLogSysTableQueryer() :
    is_inited_(false),
    is_across_cluster_(false),
    cluster_id_(OB_INVALID_CLUSTER_ID),
    sql_proxy_(NULL),
    err_handler_(NULL)
{

}

ObLogSysTableQueryer::~ObLogSysTableQueryer()
{
  destroy();
}

int ObLogSysTableQueryer::init(const int64_t cluster_id,
    const bool is_across_cluster,
    common::ObISQLClient &sql_proxy,
    logfetcher::IObLogErrHandler *err_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogSysTableQueryer has inited", KR(ret));
  } else {
    is_across_cluster_ = is_across_cluster;
    cluster_id_ = cluster_id;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
    err_handler_ = err_handler;
  }

  return ret;
}

void ObLogSysTableQueryer::destroy()
{
  is_inited_ = false;
  is_across_cluster_ = false;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  sql_proxy_ = NULL;
  err_handler_ = NULL;
}

int ObLogSysTableQueryer::get_ls_log_info(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObLSLogInfo &ls_log_info)
{
  int ret = OB_SUCCESS;
  const char *select_fields = "SVR_IP, SVR_PORT, ROLE, BEGIN_LSN, END_LSN";
  const char *log_stat_view_name = OB_GV_OB_LOG_STAT_TNAME;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogSysTableQueryer not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
      || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KT(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_log_info.init(tenant_id, ls_id))) {
    LOG_WARN("fail to init ls palf info", KR(ret), K(tenant_id), K(ls_id));
  } else {
    ObSqlString sql;
    int64_t record_count;

    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT %s FROM %s"
          " WHERE tenant_id = %lu AND ls_id = %lu"
          " ORDER BY tenant_id, ls_id, svr_ip, svr_port ASC",
          select_fields, log_stat_view_name,
          tenant_id, ls_id.id()))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id),
                K(ls_id));
      // Use OB_SYS_TENANT_ID to query the GV$OB_LOG_STAT
      } else if (OB_FAIL(do_query_(OB_SYS_TENANT_ID, sql, result))) {
        LOG_WARN("do_query_ failed", KR(ret), K(cluster_id_), K(tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else if (OB_FAIL(get_records_template_(
              *result.get_result(),
              ls_log_info,
              "ObLSLogInfo",
              record_count))) {
        LOG_WARN("construct log stream palf stat info failed", KR(ret), K(ls_log_info));
      }
    } // SMART_VAR
  }

  return ret;
}

////////////////////////////////////// QueryAllUnitsInfo /////////////////////////////////
int ObLogSysTableQueryer::get_all_units_info(
    const uint64_t tenant_id,
    ObUnitsRecordInfo &units_record_info)
{
  int ret = OB_SUCCESS;
  const char *select_fields = "SVR_IP, SVR_PORT, ZONE, ZONE_TYPE, REGION";

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogSysTableQueryer not init", KR(ret));
  } else if (OB_FAIL(units_record_info.init(cluster_id_))) {
    LOG_WARN("fail to init units_record_info", KR(ret), K(cluster_id_));
  } else {
    ObSqlString sql;
    int64_t record_count;

    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT %s FROM %s"
          " WHERE tenant_id = %lu",
          select_fields, OB_GV_OB_UNITS_TNAME, tenant_id))) {
        LOG_WARN("assign sql string failed", KR(ret), K(cluster_id_), K(tenant_id));
      // Use OB_SYS_TENANT_ID to query the GV$OB_UNITS
      } else if (OB_FAIL(do_query_(OB_SYS_TENANT_ID, sql, result))) {
        LOG_WARN("do_query_ failed", KR(ret), K(cluster_id_), K(tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else if (OB_FAIL(get_records_template_(*result.get_result(), units_record_info,
          "ObUnitsRecordInfo", record_count))) {
        LOG_WARN("construct units record info failed", KR(ret), K(units_record_info));
      }
    }
  }

  return ret;
}


////////////////////////////////////// QueryAllServerInfo /////////////////////////////////
int ObLogSysTableQueryer::get_all_server_info(
    const uint64_t tenant_id,
    ObAllServerInfo &all_server_info)
{
  int ret = OB_SUCCESS;
  const char *select_fields = "id, svr_ip, svr_port, status, zone";

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogSysTableQueryer not init", KR(ret));
  } else if (OB_FAIL(all_server_info.init(cluster_id_))) {
    LOG_WARN("fail to init all_server_info", KR(ret), K(cluster_id_));
  } else {
    ObSqlString sql;
    int64_t record_count;

    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
          "%s FROM %s",
          select_fields, OB_ALL_SERVER_TNAME))) {
        LOG_WARN("assign sql string failed", KR(ret), K(cluster_id_), K(tenant_id));
      } else if (OB_FAIL(do_query_(tenant_id, sql, result))) {
        LOG_WARN("do_query_ failed", KR(ret), K(cluster_id_), K(tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else if (OB_FAIL(get_records_template_(*result.get_result(), all_server_info,
          "ObAllServerInfo", record_count))) {
        LOG_WARN("construct log stream palf stat info failed", KR(ret), K(all_server_info));
      }
    }
  }

  return ret;
}

////////////////////////////////////// QueryAllZoneInfo /////////////////////////////////
int ObLogSysTableQueryer::get_all_zone_info(
    const uint64_t tenant_id,
    ObAllZoneInfo &all_zone_info)
{
  int ret = OB_SUCCESS;
  const char *select_fields = "zone, info";
  const char *region = "region";

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogSysTableQueryer not init", KR(ret));
  } else if (OB_FAIL(all_zone_info.init(cluster_id_))) {
    LOG_WARN("fail to init all_zone_info", KR(ret), K(cluster_id_));
  } else {
    // ObMySQLProxy
    ObSqlString sql;
    int64_t record_count;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
          "%s FROM %s "
          "WHERE name = \'%s\'",
          select_fields, OB_ALL_ZONE_TNAME, region))) {
        LOG_WARN("assign sql string failed", KR(ret), K(cluster_id_), K(tenant_id));
      } else if (OB_FAIL(do_query_(tenant_id, sql, result))) {
        LOG_WARN("do_query_ failed", KR(ret), K(cluster_id_), K(tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else if (OB_FAIL(get_records_template_(*result.get_result(), all_zone_info,
              "ObAllZoneInfo", record_count))) {
        LOG_WARN("get_records_template_ ObAllZoneInfo failed", KR(ret), K(all_zone_info));
      } else {}
    }
  }

  return ret;
}

////////////////////////////////////// QueryAllZoneTypeInfo /////////////////////////////////
int ObLogSysTableQueryer::get_all_zone_type_info(
    const uint64_t tenant_id,
    ObAllZoneTypeInfo &all_zone_type_info)
{
  int ret = OB_SUCCESS;
  const char *select_fields = "zone, info";
  const char *zone_type = "zone_type";

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogSysTableQueryer not init", KR(ret));
  } else if (OB_FAIL(all_zone_type_info.init(cluster_id_))) {
    LOG_WARN("fail to init all_zone_type_info", KR(ret), K(cluster_id_));
  } else {
    ObSqlString sql;
    int64_t record_count;

    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
          "%s FROM %s "
          "WHERE name = \'%s\'",
          select_fields, OB_ALL_ZONE_TNAME, zone_type))) {
        LOG_WARN("assign sql string failed", KR(ret), K(cluster_id_), K(tenant_id));
      } else if (OB_FAIL(do_query_(tenant_id, sql, result))) {
        LOG_WARN("do_query_ failed", KR(ret), K(cluster_id_), K(tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else if (OB_FAIL(get_records_template_(*result.get_result(), all_zone_type_info,
              "ObAllZoneTypeInfo", record_count))) {
        LOG_WARN("get_records_template_ ObAllZoneTypeInfo failed", KR(ret), K(all_zone_type_info));
      } else {}
    }
  }

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_FETCH_LOG_SYS_QUERY_FAILED);
int ObLogSysTableQueryer::do_query_(const uint64_t tenant_id,
    ObSqlString &sql,
    ObISQLClient::ReadResult &result)
{
  int ret = OB_SUCCESS;
  ObTaskId trace_id(*ObCurTraceId::get_trace_id());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogSysTableQueryer not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SqlProxy is NULL", KR(ret));
  } else if (is_across_cluster_) {
    if (OB_FAIL(sql_proxy_->read(result, cluster_id_, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(cluster_id_), K(tenant_id), "sql", sql.ptr());
    }
  } else {
    if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(tenant_id), "sql", sql.ptr());
    }
  }
  if (OB_SUCC(ret) && ERRSIM_FETCH_LOG_SYS_QUERY_FAILED) {
    ret = ERRSIM_FETCH_LOG_SYS_QUERY_FAILED;
    LOG_WARN("errsim do query error", K(ERRSIM_FETCH_LOG_SYS_QUERY_FAILED));
  }
  if (OB_NOT_NULL(err_handler_) && (-ER_CONNECT_FAILED == ret || -ER_ACCESS_DENIED_ERROR == ret
    || OB_SERVER_IS_INIT == ret || OB_TENANT_NOT_EXIST == ret || OB_TENANT_NOT_IN_SERVER == ret)) {
    err_handler_->handle_error(share::SYS_LS, logfetcher::IObLogErrHandler::ErrType::FETCH_LOG, trace_id,
      palf::LSN(palf::LOG_INVALID_LSN_VAL)/*no need to pass lsn*/, ret, "%s");
  }

  return ret;
}

template <typename RecordsType>
int ObLogSysTableQueryer::get_records_template_(common::sqlclient::ObMySQLResult &res,
    RecordsType &records,
    const char *event,
    int64_t &record_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogSysTableQueryer not init", KR(ret));
  } else {
    record_count = 0;

    while (OB_SUCC(ret)) {
      if (OB_FAIL(res.next())) {
        if (OB_ITER_END == ret) {
          // End of iteration
        } else {
          LOG_WARN("get next result failed", KR(ret), K(event));
        }
      } else if (OB_FAIL(parse_record_from_row_(res, records))) {
        LOG_WARN("parse_record_from_row_ failed", KR(ret), K(records));
      } else {
        record_count++;
      }
    } // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

////////////////////////////////////// QueryAllUnitsInfo - parse /////////////////////////////////
int ObLogSysTableQueryer::parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
    ObUnitsRecordInfo &units_record_info)
{
  int ret = OB_SUCCESS;
  ObUnitsRecord units_record;
  ObString ip;
  int64_t port = 0;
  common::ObAddr server;
  ObString zone;
  ObString region;
  ObString zone_type_str;

  (void)GET_COL_IGNORE_NULL(res.get_varchar, "SVR_IP", ip);
  (void)GET_COL_IGNORE_NULL(res.get_int, "SVR_PORT", port);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "ZONE", zone);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "ZONE_TYPE", zone_type_str);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "REGION", region);

  if (false == server.set_ip_addr(ip, static_cast<uint32_t>(port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(ip), K(port));
  } else {
    common::ObZoneType zone_type;
    if (nullptr == zone_type_str.ptr()) {
      // After the ObServer of the primary database crashes and restarts, the Zone Type may become an invalid value.
      // It can be ignored to avoid errors.
    } else {
      zone_type = str_to_zone_type(zone_type_str.ptr());
    }

    if (OB_FAIL(units_record.init(server, zone, zone_type, region))) {
      LOG_ERROR("units_record init failed", KR(ret), K(server), K(zone), K(zone_type), K(region));
    } else if (OB_FAIL(units_record_info.add(units_record))) {
      LOG_WARN("units_record_info add failed", KR(ret), K(units_record));
    } else {
      LOG_TRACE("units_record_info add success", K(units_record));
    }
  }

  return ret;
}

int ObLogSysTableQueryer::parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
    ObLSLogInfo &ls_log_info)
{
  int ret = OB_SUCCESS;
  LogStatRecord log_stat_record;
  ObString ip;
  int64_t port = 0;
  common::ObAddr server;
  ObString role_string;
  ObRole role;
  int64_t begin_lsn_int = 0;
  int64_t end_lsn_int = 0;

  (void)GET_COL_IGNORE_NULL(res.get_varchar, "SVR_IP", ip);
  (void)GET_COL_IGNORE_NULL(res.get_int, "SVR_PORT", port);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "ROLE", role_string);
  (void)GET_COL_IGNORE_NULL(res.get_int, "BEGIN_LSN", begin_lsn_int);
  (void)GET_COL_IGNORE_NULL(res.get_int, "END_LSN", end_lsn_int);

  palf::LSN begin_lsn(static_cast<palf::offset_t>(begin_lsn_int));
  palf::LSN end_lsn(static_cast<palf::offset_t>(end_lsn_int));

  if (false == server.set_ip_addr(ip, static_cast<uint32_t>(port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(ip), K(port));
  } else if (OB_FAIL(common::string_to_role(role_string, role))) {
    LOG_WARN("string_tor_role failed", KR(ret), K(role_string), K(role));
  } else {
    log_stat_record.reset(server, role, begin_lsn, end_lsn);

    if (OB_FAIL(ls_log_info.add(log_stat_record))) {
      LOG_WARN("ls_log_info add failed", KR(ret), K(log_stat_record));
    }
  }

  return ret;
}

int ObLogSysTableQueryer::parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
    ObAllServerInfo &all_server_info)
{
  int ret = OB_SUCCESS;
  AllServerRecord all_server_record;
  uint64_t id = 0;
  ObString ip;
  int64_t port = 0;
  common::ObAddr server;
  ObString status_str;
  ObServerStatus::DisplayStatus status;
  ObString zone;

  (void)GET_COL_IGNORE_NULL(res.get_uint, "id", id);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "svr_ip", ip);
  (void)GET_COL_IGNORE_NULL(res.get_int, "svr_port", port);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "status", status_str);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "zone", zone);

  if (false == server.set_ip_addr(ip, static_cast<uint32_t>(port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(ip), K(port));
  } else if (OB_FAIL(share::ObServerStatus::str2display_status(status_str.ptr(), status))) {
    LOG_ERROR("str2display_status fail", KR(ret), K(status_str), K(status));
  } else if (OB_FAIL(all_server_record.init(id, server, status, zone))) {
    LOG_ERROR("all_server_record init fail", KR(ret), K(id), K(server), K(status), K(zone));
  } else if (OB_FAIL(all_server_info.add(all_server_record))) {
    LOG_WARN("all_server_info add failed", KR(ret), K(all_server_record));
  } else {}

  return ret;
}

int ObLogSysTableQueryer::parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
    ObAllZoneInfo &all_zone_info)
{
  int ret = OB_SUCCESS;
  AllZoneRecord all_zone_record;;
  ObString zone;
  ObString region;

  (void)GET_COL_IGNORE_NULL(res.get_varchar, "zone", zone);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "info", region);

  if (OB_FAIL(all_zone_record.init(zone, region))) {
    LOG_ERROR("all_server_record init fail", KR(ret), K(zone), K(region));
  } else if (OB_FAIL(all_zone_info.add(all_zone_record))) {
    LOG_WARN("all_zone_info add failed", KR(ret), K(all_zone_record));
  } else {}

  return ret;
}

int ObLogSysTableQueryer::parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
    ObAllZoneTypeInfo &all_zone_type_info)
{
  int ret = OB_SUCCESS;
  AllZoneTypeRecord all_zone_type_record;;
  ObString zone;
  ObString zone_type_str;

  (void)GET_COL_IGNORE_NULL(res.get_varchar, "zone", zone);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "info", zone_type_str);

  common::ObZoneType zone_type = str_to_zone_type(zone_type_str.ptr());

  if (OB_FAIL(all_zone_type_record.init(zone, zone_type))) {
    LOG_ERROR("all_server_record init fail", KR(ret), K(zone), K(zone_type));
  } else if (OB_FAIL(all_zone_type_info.add(all_zone_type_record))) {
    LOG_WARN("all_zone_type_info add failed", KR(ret), K(all_zone_type_record));
  } else {
    LOG_DEBUG("query all zone info record", KR(ret), K(all_zone_type_record));
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase
