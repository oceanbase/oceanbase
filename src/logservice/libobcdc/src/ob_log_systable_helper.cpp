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
 *
 * ObCDCSysTableHelper define
 * Class For Query System Table
 */

#define USING_LOG_PREFIX  OBLOG_FETCHER

#include "ob_log_systable_helper.h"

#include "common/ob_role.h"                                     // LEADER

#include "share/inner_table/ob_inner_table_schema_constants.h"  // OB_***_TNAME
#include "share/schema/ob_schema_struct.h"                      // TenantStatus, ObTenantStatus
#include "ob_log_instance.h"                                    // TCTX
#include "ob_log_config.h"                                      // ObLogConfig, TCONF
#include "ob_log_utils.h"
#include "ob_log_svr_blacklist.h"                               // ObLogSvrBlacklist
#include "ob_cdc_tenant_endpoint_provider.h"                    // ObCDCEndpointProvider

#define GET_DATA(type, index, val, val_str) \
    do { \
      if (OB_SUCCESS == ret) {\
        bool is_null_value = false; \
        int64_t idx = (index); \
        if (OB_FAIL(get_##type(idx, (val), is_null_value)) || is_null_value) { \
          LOG_ERROR("get " val_str " fail", KR(ret), K(idx), K(is_null_value)); \
          ret = (OB_SUCCESS == ret ? OB_INVALID_DATA : ret); \
        } \
      } \
    } while (0)

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace libobcdc
{

bool ISQLStrategy::g_is_replica_type_info_valid = true;

////////////////////////////////////// QueryClusterId /////////////////////////////////
int QueryClusterIdStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT DISTINCT VALUE FROM %s WHERE NAME = 'cluster_id'", OB_GV_OB_PARAMETERS_TNAME))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, KCSTRING(sql_buf));
  } else {
    // succ
  }

  return ret;
}

///////////////////////// QueryObserverVersionStrategy /////////////////////////
int QueryObserverVersionStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  const char *query_sql = nullptr;

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(sql_buf), K(mul_statement_buf_len));
  } else if (TCTX.is_tenant_sync_mode()) {
    if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
        "SELECT DISTINCT VALUE FROM %s WHERE NAME = 'min_observer_version'", OB_GV_OB_PARAMETERS_TNAME))) {
      LOG_ERROR("build sql failed", KR(ret), K(pos), "buf_size", mul_statement_buf_len, KCSTRING(sql_buf));
    }
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT DISTINCT VALUE FROM %s WHERE NAME = 'MIN_OBSERVER_VERSION'", OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TNAME))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, KCSTRING(sql_buf));
  } else {
    // succ
  }

  return ret;
}

///////////////////////// QueryTimeZoneInfoVersionStrategy/////////////////////////
int QueryTimeZoneInfoVersionStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  const char *query_sql = NULL;
  const bool tenant_sync_mode = TCTX.is_tenant_sync_mode();

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (TCTX.is_tenant_sync_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not support fetch timezone_version in tenant_sync_mode", KR(ret));
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
      "SELECT VALUE FROM %s WHERE NAME = 'CURRENT_TIMEZONE_VERSION' AND TENANT_ID = %lu", OB_ALL_VIRTUAL_SYS_STAT_TNAME, tenant_id_))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, KCSTRING(sql_buf), K_(tenant_id));
  }

  return ret;
}

///////////////////////// QueryTenantLSInfoStrategy /////////////////////////
int QueryTenantLSInfoStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(sql_buf), K(mul_statement_buf_len));
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT LS_ID FROM %s WHERE LS_ID != 1 AND TENANT_ID = %lu", OB_CDB_OB_LS_TNAME, tenant_id_))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, KCSTRING(sql_buf));
  }

  return ret;
}

///////////////////////// QueryAllServerInfoStrategy /////////////////////////
int QueryAllServerInfoStrategy::build_sql_statement(
    char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(sql_buf), K(mul_statement_buf_len));
  } else if (TCTX.is_tenant_sync_mode()) {
    if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
        "SELECT DISTINCT UNIT.SVR_IP, UNIT.SVR_PORT, LOCATION.SQL_PORT "
        "FROM %s UNIT JOIN %s LOCATION "
        "ON UNIT.SVR_IP = LOCATION.SVR_IP AND UNIT.SVR_PORT = LOCATION.SVR_PORT "
        "WHERE UNIT.STATUS = 'NORMAL'",
        OB_GV_OB_UNITS_TNAME, OB_DBA_OB_LS_LOCATIONS_TNAME))) {
      LOG_ERROR("build_sql_statement failed for query all_server in tenant_sync_mode", KR(ret), K(pos), KCSTRING(sql_buf));
    }
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
      "SELECT SVR_IP, SVR_PORT, SQL_PORT FROM %s WHERE STATUS = 'ACTIVE'", OB_DBA_OB_SERVERS_TNAME))) {
    LOG_ERROR("build_sql_statement failed for query all_server_info", KR(ret), K(pos), KCSTRING(sql_buf));
  }

  return ret;
}

///////////////////////// QueryAllTenantStrategy /////////////////////////
int QueryAllTenantStrategy::build_sql_statement(
    char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(sql_buf), K(mul_statement_buf_len));
  } else if (TCTX.is_tenant_sync_mode()) {
    if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
        "SELECT DISTINCT TENANT_ID, TENANT_NAME FROM %s", OB_DBA_OB_ACCESS_POINT_TNAME))) {
      LOG_ERROR("build_sql_statement failed for query all_tenant_info in tenant_sync_mode", KR(ret), K(pos), KCSTRING(sql_buf));
    }
    // should not filter tenant by status because schema_service may launch sql to all tenant
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
      "SELECT DISTINCT TENANT_ID, TENANT_NAME FROM %s WHERE TENANT_TYPE != 'META'", OB_DBA_OB_TENANTS_TNAME))) {
    LOG_ERROR("build_sql_statement failed for query all_tenant_info", KR(ret), K(pos), KCSTRING(sql_buf));
  }


  return ret;
}

///////////////////////// QueryTenantServerListStrategy /////////////////////////
int QueryTenantServerListStrategy::build_sql_statement(
    char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(sql_buf), K(mul_statement_buf_len));
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
      "SELECT "
      "UNIT.SVR_IP AS SVR_IP, "
      "UNIT.SVR_PORT AS RPC_PORT, "
      "SERVER.SQL_PORT "
      "FROM %s UNIT "
      "JOIN %s SERVER "
      "ON SERVER.SVR_PORT=UNIT.SVR_PORT AND SERVER.SVR_IP=UNIT.SVR_IP "
      "AND SERVER.STATUS='ACTIVE' "
      "AND TENANT_ID = %lu",
      OB_DBA_OB_UNITS_TNAME, OB_DBA_OB_SERVERS_TNAME, tenant_id_))) {
    LOG_ERROR("build_sql_statement failed for query all_server_info", KR(ret), K(pos), K_(tenant_id), KCSTRING(sql_buf));
  }

  return ret;
}

int QueryTenantEndpointStrategy::build_sql_statement(
    char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(sql_buf), K(mul_statement_buf_len));
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
      "SELECT DISTINCT UNIT.SVR_IP, UNIT.SVR_PORT, LOCATION.SQL_PORT "
      "FROM %s UNIT JOIN %s LOCATION "
      "ON UNIT.SVR_IP = LOCATION.SVR_IP AND UNIT.SVR_PORT = LOCATION.SVR_PORT "
      "WHERE UNIT.STATUS = 'NORMAL'",
      OB_GV_OB_UNITS_TNAME, OB_DBA_OB_LS_LOCATIONS_TNAME))) {
    LOG_ERROR("build_sql_statement failed for query tenant_endpoint", KR(ret), K(pos), KCSTRING(sql_buf));
  }

  return ret;
}

///////////////////////// QueryTenantStatusStrategy /////////////////////////
int QueryTenantStatusStrategy::build_sql_statement(
    char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(sql_buf), K(mul_statement_buf_len));
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
      "SELECT IS_DELETE FROM %s WHERE TENANT_ID = %lu ORDER BY SCHEMA_VERSION DESC LIMIT 1",
      OB_ALL_TENANT_HISTORY_TNAME, tenant_id_))) {
    LOG_ERROR("build_sql_statement failed for query all_server_info", KR(ret), K(pos), KCSTRING(sql_buf));
  }

  return ret;
}

IObLogSysTableHelper::BatchSQLQuery::BatchSQLQuery() :
    inited_(false),
    enable_multiple_statement_(false),
    mul_statement_buf_(NULL),
    mul_statement_buf_len_(0),
    pos_(0),
    batch_sql_count_(0)
{
  single_statement_buf_[0] = '\0';
}

IObLogSysTableHelper::BatchSQLQuery::~BatchSQLQuery()
{
  destroy();
}

int IObLogSysTableHelper::BatchSQLQuery::init(const int64_t mul_statement_buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(mul_statement_buf_len <= 0)) {
    LOG_ERROR("invalid arguments", K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(mul_statement_buf_ = static_cast<char *>(ob_malloc(
            mul_statement_buf_len, ObModIds::OB_LOG_BATCH_SQL_QUERY)))) {
    LOG_ERROR("mul_statement_buf_ is null", K(mul_statement_buf_), K(mul_statement_buf_len));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    enable_multiple_statement_ = true;
    mul_statement_buf_len_ = mul_statement_buf_len;

    pos_ = 0;
    batch_sql_count_ = 0;

    inited_ = true;
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::init(ISQLStrategy *strategy)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(strategy)) {
    LOG_ERROR("strategy is null", K(strategy));
    ret = OB_INVALID_ARGUMENT;
  } else {
    enable_multiple_statement_ = false;
    single_statement_buf_[0] = '\0';

    pos_ = 0;
    batch_sql_count_ = 0;

    inited_ = true;

    if (OB_FAIL(do_sql_aggregate(strategy))) {
      LOG_ERROR("do_sql_aggregate fail", KR(ret));
    } else if (OB_FAIL(init_sql())) {
      LOG_ERROR("init_sql fail", KR(ret));
    } else {
      // succ
    }
  }

  return ret;
}

void IObLogSysTableHelper::BatchSQLQuery::destroy()
{
  MySQLQueryBase::destroy();

  inited_ = false;

  enable_multiple_statement_ = false;
  if (NULL != mul_statement_buf_) {
    ob_free(mul_statement_buf_);
    mul_statement_buf_ = NULL;
  }
  mul_statement_buf_len_ = 0;
  single_statement_buf_[0] = '\0';

  pos_ = 0;
  batch_sql_count_ = 0;
}

void IObLogSysTableHelper::BatchSQLQuery::reset()
{
  MySQLQueryBase::destroy();

  pos_ = 0;
  batch_sql_count_ = 0;
}

int IObLogSysTableHelper::BatchSQLQuery::do_sql_aggregate(ISQLStrategy *strategy)
{
  int ret = OB_SUCCESS;
  int64_t sql_len = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(strategy)) {
    LOG_ERROR("strategy is null", K(strategy));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (enable_multiple_statement_) {
      if (OB_FAIL(strategy->build_sql_statement(mul_statement_buf_ + pos_, mul_statement_buf_len_ - pos_, sql_len))) {
        LOG_ERROR("strategy build_sql_statement fail", KR(ret), K(mul_statement_buf_), K(mul_statement_buf_len_),
            K(pos_), K(sql_len));
      }
    } else {
      if (OB_FAIL(strategy->build_sql_statement(single_statement_buf_, sizeof(single_statement_buf_),
              sql_len))) {
        LOG_ERROR("strategy build_sql_statement fail", KR(ret), K(single_statement_buf_),
            "buf_len", sizeof(single_statement_buf_), K(sql_len));
      }
    }

    if (OB_SUCC(ret)) {
      pos_ += sql_len;
      ++batch_sql_count_;
    }
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::init_sql()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else  {
    if (enable_multiple_statement_) {
      if (OB_FAIL(MySQLQueryBase::init(mul_statement_buf_, pos_))) {
        LOG_ERROR("init MySQLQueryBase fail", KR(ret), K(mul_statement_buf_), K(pos_));
      }
    } else {
      if (OB_FAIL(MySQLQueryBase::init(single_statement_buf_, pos_))) {
        LOG_ERROR("init MySQLQueryBase fail", KR(ret), K(single_statement_buf_), K(pos_));
      }
    }
  }

  return ret;
}

template <typename RecordsType>
int IObLogSysTableHelper::BatchSQLQuery::get_records_tpl_(RecordsType &records, const char *event,
    int64_t &record_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(next_result())) {
    // OB_NEED_RETRY indicates that a retry is required
    LOG_ERROR("next_result fail", KR(ret), K(event), "mysql_error_code", get_mysql_err_code(),
        "mysql_err_msg", get_mysql_err_msg());
  } else {
    record_count = 0;

    while (OB_SUCC(ret)) {
      // fetch next row
      if (OB_FAIL(next_row())) {
        if (OB_ITER_END == ret) {
          // End of iteration
        } else {
          // OB_NEED_RETRY indicates that a retry is required
          LOG_ERROR("get next row fail", KR(ret), K(event),
              "mysql_error_code", get_mysql_err_code(), "mysql_err_msg", get_mysql_err_msg());
        }
      }
      // Parsing data from the rows
      else if (OB_FAIL(parse_record_from_row_(records))) {
        LOG_ERROR("parse records from row data fail", KR(ret), K(event));
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

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(ClusterInfo &record)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  const char *column_name = "VALUE";
  // Get  value of column `value`
  // FIXME: it is assumed that show parameters like 'cluster_id' will only return one row, and value must be the value of cluster_id
  if (OB_FAIL(get_column_index(column_name, index))) {
    LOG_ERROR("get_column_index fail", KR(ret), K(column_name), K(index));
  } else {
    GET_DATA(int, index, record.cluster_id_, "VALUE");
  }
  return ret;
}

// TODO
int IObLogSysTableHelper::BatchSQLQuery::get_records(ClusterInfo &record)
{
  int64_t record_count = 0;
  int ret = get_records_tpl_(record, "QueryClusterInfo", record_count);

  if (OB_SUCCESS == ret && 0 == record_count) {
    LOG_ERROR("no cluster id info records, unexcepted error", K(record_count), K(svr_));
    ret = OB_ITEM_NOT_SETTED;
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(ObServerVersionInfoArray &records)
{
  int ret = OB_SUCCESS;
  ObServerVersionInfo record;
  ObString server_version_str;
  uint64_t server_version;
  int64_t index = -1;

  index++;
  GET_DATA(varchar, index, server_version_str, "VALUE");

  if (OB_FAIL(ObClusterVersion::get_version(server_version_str, server_version))) {
    LOG_ERROR("ObClusterVersion get_version fail", KR(ret), K(server_version_str), K(server_version));
  } else {
    record.server_version_ = server_version;

    if (OB_FAIL(records.push_back(record))) {
      LOG_ERROR("push back record fail", KR(ret), K(record), K(records));
    }
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(ObServerVersionInfoArray& records)
{
  int64_t record_count = 0;
  return get_records_tpl_(records, "QueryClusterVersion", record_count);
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(ObServerTZInfoVersionInfo &record)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;

  index++;
  GET_DATA(int, index, record.timezone_info_version_, "VALUE");

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(ObServerTZInfoVersionInfo &record)
{
  int ret = OB_SUCCESS;
  int64_t record_count = 0;

  ret = get_records_tpl_(record, "QueryTimeZoneInfoVersion", record_count);

  if (0 == record_count) {
    record.is_timezone_info_version_exist_ = false;
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(TenantLSIDs &records)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  int64_t ls_id_int = 0;

  index++;
  GET_DATA(int, index, ls_id_int, "LS_ID");
  share::ObLSID ls_id(ls_id_int);

  if (OB_SUCCESS == ret && OB_FAIL(records.push_back(ls_id))) {
    LOG_ERROR("push back record fail", KR(ret), K(ls_id), K(records));
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(TenantLSIDs &records)
{
  int64_t record_count = 0;
  return get_records_tpl_(records, "QueryTenantLSInfo", record_count);
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(
    const ObLogSvrBlacklist &server_blacklist,
    common::ObIArray<common::ObAddr> &records)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  common::ObAddr rpc_addr;
  common::ObAddr sql_addr;
  bool is_in_blacklist = false;
  ObString svr_ip;
  int64_t rpc_port = -1;
  int64_t sql_port = -1;

  index ++;
  GET_DATA(varchar, index, svr_ip, "SVR_IP");
  index ++;
  GET_DATA(int, index, rpc_port, "SVR_PORT");
  index ++;
  GET_DATA(int, index, sql_port, "SQL_PORT");

  if (OB_UNLIKELY(! rpc_addr.set_ip_addr(svr_ip, static_cast<int32_t>(rpc_port)))) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid rpc_addr query from server", KR(ret), K(svr_ip), K(rpc_port), K(sql_port));
  } else if (OB_UNLIKELY(server_blacklist.is_exist(rpc_addr))) {
    LOG_WARN("ignore server in sql_svr_blacklist", K(rpc_addr));
  } else if (OB_UNLIKELY(! sql_addr.set_ip_addr(svr_ip, static_cast<int32_t>(sql_port)))) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid sql_addr query from server", KR(ret), K(svr_ip), K(rpc_port), K(sql_port));
  } else if (OB_FAIL(records.push_back(sql_addr))) {
    LOG_ERROR("push_back sql_addr into sql_server_list failed", KR(ret),
        K(svr_ip), K(rpc_port), K(sql_port), K(records));
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(
    const ObLogSvrBlacklist &server_blacklist,
    common::ObIArray<common::ObAddr >&records)
{
  int64_t record_count = 0;
  return get_records_tpl_(records, "QueryAllServerInfo", record_count);
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(common::ObIArray<TenantInfo> &records)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t index = -1;
  ObString tenant_name;

  index++;
  GET_DATA(uint, index, tenant_id, "TENANT_ID");
  index++;
  GET_DATA(varchar, index, tenant_name, "TENANT_NAME");

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid tenant_id query from server", KR(ret), K(tenant_id));
  } else if (OB_FAIL(records.push_back(TenantInfo(tenant_id, tenant_name)))) {
    LOG_ERROR("push_back tenant_id into tenant_id_list failed", KR(ret), K(tenant_id), K(records));
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(common::ObIArray<TenantInfo> &records)
{
  int64_t record_count = 0;
  return get_records_tpl_(records, "QueryAllTenantInfo", record_count);
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(common::ObIArray<common::ObAddr> &records)
{
  int ret = OB_SUCCESS;
  common::ObAddr sql_addr;
  ObString svr_ip;
  int64_t rpc_port = -1;
  int64_t sql_port = -1;
  int64_t index = -1;

  index++;
  GET_DATA(varchar, index, svr_ip, "SVR_IP");
  index++;
  GET_DATA(int, index, rpc_port, "RPC_PORT");
  index++;
  GET_DATA(int, index, sql_port, "SQL_PORT");

  if (OB_UNLIKELY(! sql_addr.set_ip_addr(svr_ip, static_cast<int32_t>(sql_port)))) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid rpc_addr query from server", KR(ret), K(svr_ip), K(rpc_port), K(sql_port));
  } else if (OB_FAIL(records.push_back(sql_addr))) {
    LOG_ERROR("push_back sql_addr into tenant_server_list failed", KR(ret), K(svr_ip), K(rpc_port), K(sql_port));
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(common::ObIArray<common::ObAddr> &records)
{
  int64_t record_count = 0;
  return get_records_tpl_(records, "QueryTenantServerList", record_count);
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(share::schema::TenantStatus &records)
{
  int ret = OB_SUCCESS;
  int64_t is_delete = -1;
  int64_t index = -1;

  index++;
  GET_DATA(int, index, is_delete, "TENANT_IS_DELETE");

  if (OB_UNLIKELY(1 == is_delete)) {
    records = share::schema::TenantStatus::TENANT_DELETED;
  } else{
    records = share::schema::TenantStatus::TENANT_EXIST;
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(share::schema::TenantStatus &records)
{
  int64_t record_count = 0;
  int ret = get_records_tpl_(records, "QueryTenantStatus", record_count);
  if (OB_UNLIKELY(0 == record_count)) {
    records = share::schema::TenantStatus::TENANT_NOT_CREATE;
  int refresh_svr_list_by_inner(ObIArray<ObAddr> &svr_list);
  }
  return ret;
}

//////////////////////////////////////// ObLogSysTableHelper ////////////////////////////////////////

ObLogSysTableHelper::ObLogSysTableHelper() :
    inited_(false),
    svr_provider_(NULL),
    max_thread_num_(0),
    mysql_conns_(NULL),
    next_svr_idx_array_(NULL),
    thread_counter_(0)
{}

ObLogSysTableHelper::~ObLogSysTableHelper()
{
  destroy();
}

int ObLogSysTableHelper::init(SvrProvider &svr_provider,
    const int64_t access_systable_helper_thread_num,
    const char *mysql_user,
    const char *mysql_password,
    const char *mysql_db)
{
  int ret = OB_SUCCESS;
  ObLogMySQLConnector *conn_array = NULL;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(access_systable_helper_thread_num <= 0)
      || OB_ISNULL(mysql_user)
      || OB_ISNULL(mysql_password)
      || OB_ISNULL(mysql_db)) {
    LOG_ERROR("invalid arguments", K(access_systable_helper_thread_num), K(mysql_user), K(mysql_db));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t max_thread_num = access_systable_helper_thread_num;
    int64_t sql_conn_size = static_cast<int64_t>(sizeof(ObLogMySQLConnector) * max_thread_num);
    int64_t next_svr_idx_array_size = static_cast<int64_t>(sizeof(int64_t) * max_thread_num);
    int64_t buf_size = sql_conn_size + next_svr_idx_array_size;
    void *buf = ob_malloc(buf_size, ObModIds::OB_LOG_MYSQL_CONNECTOR);

    if (OB_ISNULL(buf)) {
      LOG_ERROR("alloc buffer fail", K(buf_size), K(max_thread_num));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      conn_array = static_cast<ObLogMySQLConnector *>(buf);
      next_svr_idx_array_ = reinterpret_cast<int64_t *>(static_cast<char *>(buf) + sql_conn_size);
      int64_t svr_count = svr_provider.get_server_count();

      if (svr_count <= 0) {
        svr_count = 1;
      }

      for (int64_t i = 0; i < max_thread_num; ++i) {
        new(conn_array + i) ObLogMySQLConnector();
        // Ensure that requests from different threads are balanced across servers
        next_svr_idx_array_[i] = i % svr_count;
      }

      (void)snprintf(mysql_user_, sizeof(mysql_user_), "%s", mysql_user);
      (void)snprintf(mysql_password_, sizeof(mysql_password_), "%s", mysql_password);
      (void)snprintf(mysql_db_, sizeof(mysql_db_), "%s", mysql_db);

      svr_provider_ = &svr_provider;
      mysql_conns_ = conn_array;
      max_thread_num_ = max_thread_num;
      thread_counter_ = 0;
      inited_ = true;

      LOG_INFO("init systable helper succ", K(mysql_user_), K(mysql_db_),
          K(access_systable_helper_thread_num));
    }
  }

  return ret;
}

void ObLogSysTableHelper::destroy()
{
  inited_ = false;

  if (NULL != mysql_conns_) {
    for (int64_t idx = 0, cnt = max_thread_num_; (idx < cnt); ++idx) {
      mysql_conns_[idx].~ObLogMySQLConnector();
    }

    ob_free(mysql_conns_);
    mysql_conns_ = NULL;
  }

  svr_provider_ = NULL;
  max_thread_num_ = 0;
  thread_counter_ = 0;
  mysql_conns_ = NULL;
  next_svr_idx_array_ = NULL;    // FIXME：next_svr_idx_array_ shares a piece of memory with mysql_conns_ and does not need to be freed here
  mysql_user_[0] = '\0';
  mysql_password_[0] = '\0';
  mysql_db_[0] = '\0';

  LOG_INFO("destroy systable helper succ");
}

int ObLogSysTableHelper::do_query_and_handle_when_query_error_occurred_(BatchSQLQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY != ret) {
      LOG_ERROR("do_query_ fail", KR(ret));
    } else {
      const int err_code = query.get_mysql_err_code();
      const char *mysql_err_msg = NULL;

      if (0 == err_code) {
        // need retry
      } else {
        if (OB_ISNULL(mysql_err_msg = query.get_mysql_err_msg())) {
          LOG_ERROR("mysql_err_msg is null", K(mysql_err_msg));
          ret = OB_ERR_UNEXPECTED;
        } else {
          const char *ptr = NULL;

          if (ER_BAD_FIELD_ERROR == err_code) {
           // handler if column not exist
            ptr = strstr(mysql_err_msg, "replica_type");

            if (NULL == ptr) {
              LOG_ERROR("ptr is null, unexpected column is not found", K(ptr), K(err_code), K(mysql_err_msg));
              ret = OB_NEED_RETRY;
            } else {
              LOG_DEBUG("column info", K(ptr), K(mysql_err_msg));
              handle_column_not_found_when_query_meta_info_();
            }
          } else {
            LOG_ERROR("do query fail", K(err_code), K(mysql_err_msg));
            ret = OB_NEED_RETRY;
          }
        }
      }
    }
  }

  return ret;
}

void ObLogSysTableHelper::handle_column_not_found_when_query_meta_info_()
{
  // The replica_type field is not available and will not be requested in the future
  ATOMIC_STORE(&ISQLStrategy::g_is_replica_type_info_valid, false);
  LOG_INFO("'replica_type' is not availalbe in meta table. would not request 'replica_type'",
      "g_is_replica_type_info_valid", ISQLStrategy::g_is_replica_type_info_valid);
}

int ObLogSysTableHelper::query_with_multiple_statement(BatchSQLQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init_sql())) {
    LOG_ERROR("query init_sql fail", KR(ret));
  } else if (OB_FAIL(do_query_and_handle_when_query_error_occurred_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("query_with_multiple_statement fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogSysTableHelper::query_cluster_info(ClusterInfo &record)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryClusterIdStrategy query_cluster_id_strategy;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_cluster_id_strategy))) {
    LOG_ERROR("init cluster info query fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_cluster_info fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_cluster_info fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(record))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_cluster_info, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_cluster_info", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_INFO("query_cluster_info", KR(ret), K(record), "query_svr", query.get_server());

  return ret;
}

int ObLogSysTableHelper::query_cluster_min_observer_version(uint64_t &min_observer_version)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryObserverVersionStrategy query_observer_version_strategy;
  ObServerVersionInfoArray records;
  min_observer_version = OB_INVALID_ID;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_observer_version_strategy))) {
    LOG_ERROR("init ObserverVersion query fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_cluster_min_observer_version fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_cluster_min_observer_version fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(records))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_cluster_min_observer_version, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_cluster_min_observer_version", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  if (OB_SUCC(ret)) {
    // Get minimum version of servers in cluster
    for (int64_t idx = 0; OB_SUCC(ret) && idx < records.count(); ++idx) {
      uint64_t server_version = records.at(idx).server_version_;
      if (OB_INVALID_ID == min_observer_version) {
        min_observer_version = server_version;
      } else {
        min_observer_version = std::min(min_observer_version, server_version);
      }
    }

    if (OB_INVALID_ID == min_observer_version) {
      ret = OB_NEED_RETRY;
    }
  }

  LOG_INFO("query_cluster_min_observer_version", KR(ret), K(min_observer_version), K(records), "query_svr", query.get_server());

  return ret;
}

int ObLogSysTableHelper::query_timezone_info_version(const uint64_t tenant_id,
    int64_t &timezone_info_version)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryTimeZoneInfoVersionStrategy query_timezone_info_version_strategy(tenant_id);
  ObServerTZInfoVersionInfo record;
  timezone_info_version = OB_INVALID_TIMESTAMP;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_timezone_info_version_strategy))) {
    LOG_ERROR("init QueryTimeZoneInfoVersionquery fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_timezone_info_version fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_timezone_info_version fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(record))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_timezone_info_version, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_timezone_info_version", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else {
    if (false == record.is_timezone_info_version_exist_) {
      // 1. 1451 version ob cluster __all_zone table does not have timezone_info_version, query is empty
      // 2. 226 versions of the ob cluster have the timezone table split into tenants, if the tenants are not imported, then the query is not available
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      timezone_info_version = record.timezone_info_version_;
    }
  }

  LOG_INFO("query_timezone_info_version", KR(ret), K(timezone_info_version), K(record), "query_svr", query.get_server());

  return ret;
}

int ObLogSysTableHelper::query_tenant_ls_info(const uint64_t tenant_id,
    TenantLSIDs &tenant_ls_ids)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryTenantLSInfoStrategy query_tenant_ls_info_strategy(tenant_id);
  tenant_ls_ids.reset();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_tenant_ls_info_strategy))) {
    LOG_ERROR("init cluster info query fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_tenant_ls_info fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_tenant_ls_info fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(tenant_ls_ids))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_tenant_ls_info, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_tenant_ls_info", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_INFO("query_tenant_ls_info", KR(ret), K(tenant_id), K(tenant_ls_ids), "query_svr", query.get_server());

  return ret;
}

int ObLogSysTableHelper::query_sql_server_list(
    const ObLogSvrBlacklist &server_blacklist,
    common::ObIArray<common::ObAddr> &sql_server_list)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryAllServerInfoStrategy query_all_server_info_strategy;
  sql_server_list.reset();

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("systable_helper not init", KR(ret));
  } else if (OB_FAIL(query.init(&query_all_server_info_strategy))) {
    LOG_ERROR("init all_server_info query fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_all_server_info fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_all_server_info fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(server_blacklist, sql_server_list))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_all_server_info, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_all_server_info", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_DEBUG("query_all_server_info", KR(ret), K(sql_server_list));

  return ret;
}

int ObLogSysTableHelper::query_tenant_info_list(common::ObIArray<TenantInfo> &tenant_info_list)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryAllTenantStrategy query_all_tenant_strategy;
  tenant_info_list.reset();

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("systable_helper not init", KR(ret));
  } else if (OB_FAIL(query.init(&query_all_tenant_strategy))) {
    LOG_ERROR("init all_tenant_info query failed", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_all_tenant_info fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_all_tenant_info fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(tenant_info_list))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_all_tenant_info, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_all_tenant_info", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_DEBUG("query_all_tenant_info", KR(ret), K(tenant_info_list));

  return ret;
}

int ObLogSysTableHelper::query_tenant_id_list(common::ObIArray<uint64_t> &tenant_id_list)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryAllTenantStrategy query_all_tenant_strategy;
  common::ObSEArray<TenantInfo, 16> tenant_info_list;

  tenant_id_list.reset();

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("systable_helper not init", KR(ret));
  } else if (OB_FAIL(query_tenant_info_list(tenant_info_list))) {
    LOG_ERROR("query tenant_id, tenant_name list failed", KR(ret), K(tenant_info_list));
  } else {
    const int64_t tenant_list_size = tenant_info_list.count();
    ARRAY_FOREACH_N(tenant_info_list, i, tenant_list_size) {
      const TenantInfo &tenant_id_name = tenant_info_list.at(i);
      if (OB_FAIL(tenant_id_list.push_back(tenant_id_name.tenant_id))) {
        LOG_ERROR("push tenant_id into tenant_id_list failed", K(tenant_id_name), K(tenant_id_list),
            K(tenant_info_list));
      }
    }
  }

  LOG_DEBUG("query_all_tenant_info", KR(ret), K(tenant_id_list));

  return ret;
}

int ObLogSysTableHelper::query_tenant_sql_server_list(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &tenant_server_list)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryTenantServerListStrategy query_tenant_serverlist_strategy(tenant_id);
  tenant_server_list.reset();

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("systable_helper not init", KR(ret));
  } else if (OB_FAIL(query.init(&query_tenant_serverlist_strategy))) {
    LOG_ERROR("init tenant_server_list query failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_tenant_serverlist fail, need retry", KR(ret), K(tenant_id),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_tenant_serverlist fail", KR(ret), K(tenant_id),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(tenant_server_list))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_tenant_serverlist, need retry", KR(ret), K(tenant_id),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_tenant_serverlist", KR(ret), K(tenant_id),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_DEBUG("query_tenant_serverlist", KR(ret), K(tenant_id), K(tenant_server_list));

  return ret;
}

int ObLogSysTableHelper::query_tenant_status(
    const uint64_t tenant_id,
    share::schema::TenantStatus &tenant_status)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryTenantStatusStrategy query_tenant_status_strategy(tenant_id);
  tenant_status = share::schema::TenantStatus::TENANT_STATUS_INVALID;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("systable_helper not init", KR(ret));
  } else if (OB_FAIL(query.init(&query_tenant_status_strategy))) {
    LOG_ERROR("init tenant_status query failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_tenant_status fail, need retry", KR(ret), K(tenant_id),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_tenant_status fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(tenant_status))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_tenant_status, need retry", KR(ret), K(tenant_id),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_tenant_status", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_DEBUG("query_tenant_status", KR(ret), K(tenant_id), K(tenant_status));

  return ret;
}

int ObLogSysTableHelper::refresh_tenant_endpoint()
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  ObArray<ObAddr> tenant_endpoint_list;
  QueryTenantEndpointStrategy query_tenant_endpoint_strategy;
  ObCDCEndpointProvider *endpoint_provider = static_cast<ObCDCEndpointProvider*>(svr_provider_);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("systable_helper not init", KR(ret));
  } else if (! TCTX.is_tenant_sync_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("refresh_tenant_endpoint not support in non_tenant_sync_mode, please check sync_mode");
  } else if (OB_ISNULL(endpoint_provider)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("endpoint_provider not valid", KR(ret));
  } else if (OB_FAIL(query.init(&query_tenant_endpoint_strategy))) {
    LOG_ERROR("init tenant_endpoint query failed", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_tenant_endpoint fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_tenant_endpoint fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(
      endpoint_provider->get_svr_black_list(),
      tenant_endpoint_list))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_tenant_status, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_tenant_status", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(endpoint_provider->refresh_server_list(tenant_endpoint_list))) {
    LOG_ERROR("refresh_server_list failed", KR(ret), K(tenant_endpoint_list));
  }

  // TODO change to DEBUG level
  LOG_INFO("query_tenant_endpoint_list", KR(ret), K(tenant_endpoint_list));

  return ret;
}

int64_t ObLogSysTableHelper::thread_index_()
{
  static __thread int64_t index = -1;
  return index < 0 ? (index = ATOMIC_FAA(&thread_counter_, 1)) : index;
}

int ObLogSysTableHelper::do_query_(MySQLQueryBase &query)
{
  int ret = OB_SUCCESS;
  int64_t tid = thread_index_();
  static __thread int64_t last_change_server_tstamp = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mysql_conns_) || OB_ISNULL(next_svr_idx_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid mysql_conns_ or next_svr_idx_array_", KR(ret), K(mysql_conns_),
        K(next_svr_idx_array_));
  } else if (OB_ISNULL(svr_provider_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid svr provider or config", KR(ret), K(svr_provider_));
  }
  // Check if the maximum number of threads to access the systable helper is exceeded
  // FIXME: we cache a mysql connector for each access thread.
  // If the number of access threads exceeds the maximum number of threads prepared, an error should be reported.
  else if (OB_UNLIKELY(tid >= max_thread_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("thread index is larger than systable helper's max thread number", KR(ret), K(tid),
        K(max_thread_num_));
  } else {
    bool done = false;
    ObLogMySQLConnector &conn = mysql_conns_[tid];
    // Get the next server index of the corresponding Connector connection
    int64_t &next_svr_idx = next_svr_idx_array_[tid];

    // Check if should force a server switch before querying
    // There is currently a periodic connection reset mechanism to avoid exceptions on one server, e.g. if the query result is wrong
    if (need_change_server_(last_change_server_tstamp, conn, tid, next_svr_idx)) {
      if (conn.is_inited()) {
        conn.destroy();
      }
    }

    if (svr_provider_->get_server_count() <= 0) {
      ret = OB_NEED_RETRY;
      LOG_WARN("no server available to query", KR(ret), K(svr_provider_->get_server_count()));
    } else {
      for (int64_t retry_svr_cnt = 0;
          OB_SUCCESS == ret && ! done && retry_svr_cnt <= svr_provider_->get_server_count();
          ++retry_svr_cnt) {
        // init connection
        if (! conn.is_inited()) {
          // get index of next server
          int64_t svr_idx = get_next_svr_idx_(next_svr_idx, svr_provider_->get_server_count());
          // switch to next server
          if (OB_FAIL(change_to_next_server_(svr_idx, conn))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              // The server list has changed, the required server does not exist, and the current query exits.
              LOG_WARN("server list changed, next_svr_idx does not exist. quit current query",
                  K(svr_idx), K(next_svr_idx), K(retry_svr_cnt),
                  "server_count", svr_provider_->get_server_count());
            } else if (OB_NEED_RETRY == ret) {
              LOG_ERROR("connect to server fail, need switch server", KR(ret), K(svr_idx),
                  K(next_svr_idx), K(retry_svr_cnt),
                  "server_count", svr_provider_->get_server_count());
            } else {
              LOG_ERROR("connect to server fail", KR(ret), K(svr_idx), K(next_svr_idx),
                  K(retry_svr_cnt), "server_count", svr_provider_->get_server_count());
            }
          } else {
            last_change_server_tstamp = ObTimeUtility::current_time();
          }
        }

        // execute query
        if (OB_SUCCESS == ret) {
          if (OB_FAIL(conn.query(query))) {
            if (OB_NEED_RETRY == ret) {
              LOG_WARN("query fail on current server, need retry to change server", KR(ret),
                  "cur_server", query.get_server(), K(next_svr_idx), K(retry_svr_cnt),
                  "server_count", svr_provider_->get_server_count(),
                  "mysql_error_code", query.get_mysql_err_code(),
                  "mysql_error_msg", query.get_mysql_err_msg());
            } else {
              LOG_ERROR("query fail", KR(ret), K(retry_svr_cnt), K(next_svr_idx));
            }
          } else {
            done = true;
          }
        }

        // In case of query failure, reset the connection and retry to the next server next time
        if (OB_SUCCESS != ret) {
          if (conn.is_inited()) {
            conn.destroy();
          }
        }

        // Retry next round
        if (OB_NEED_RETRY == ret) {
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_ENTRY_NOT_EXIST == ret) {
      // The server list has changed
      ret = OB_SUCCESS;
    }

    if (!done) {
      ret = OB_NEED_RETRY;
    }
  }

  return ret;
}

bool ObLogSysTableHelper::need_change_server_(
    const int64_t last_change_server_tstamp,
    const ObLogMySQLConnector &conn,
    const int64_t tid,
    const int64_t next_svr_idx)
{
  bool bool_ret = false;
  static const int64_t PRINT_CONN_SERVER_INTERVAL = 10 * _SEC_;
  const int64_t sql_server_change_interval = TCONF.sql_server_change_interval_sec * _SEC_;
  int64_t cur_time = ObTimeUtility::current_time();

  if (REACH_TIME_INTERVAL(PRINT_CONN_SERVER_INTERVAL)) {
    LOG_INFO("[STAT] [SYSTABLE_HELPER] [QUERY_SQL_SERVER]", K(tid), "svr", conn.get_server(),
        K(next_svr_idx), "last_change_server_tstamp", TS_TO_STR(last_change_server_tstamp));
  }

  if (cur_time - last_change_server_tstamp >= sql_server_change_interval) {
    bool_ret = true;
    LOG_INFO("[STAT] [SYSTABLE_HELPER] [NEED_CHANGE_SQL_SERVER]", K(tid), "svr", conn.get_server(),
        K(next_svr_idx), "last_change_server_tstamp", TS_TO_STR(last_change_server_tstamp));
  }

  return bool_ret;
}

int64_t ObLogSysTableHelper::get_next_svr_idx_(int64_t &next_svr_idx, const int64_t total_svr_count)
{
  // Get index while advancing index value
  int64_t svr_idx = next_svr_idx++;
  if (svr_idx >= total_svr_count) {
    // Corrected values to avoid overflow
    svr_idx = 0;
    next_svr_idx = 1;
  }
  return svr_idx;
}

int ObLogSysTableHelper::change_to_next_server_(const int64_t svr_idx, ObLogMySQLConnector &conn)
{
  int ret = OB_SUCCESS;
  // update connection
  ObAddr svr;
  MySQLConnConfig conn_config;
  int mysql_connect_timeout_sec = TCONF.rs_sql_connect_timeout_sec;
  int mysql_query_timeout_sec = TCONF.rs_sql_query_timeout_sec;
  const bool enable_ssl_client_authentication = (1 == TCONF.ssl_client_authentication);

  if (OB_ISNULL(svr_provider_)) {
    LOG_ERROR("invalid svr provider", K(svr_provider_));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(svr_provider_->get_server(svr_idx, svr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // exit on server exhaustion
    } else {
      LOG_ERROR("get server from svr provider fail", KR(ret), K(svr_idx),
          K(svr_provider_->get_server_count()));
    }
  } else if (OB_FAIL(conn_config.reset(svr, mysql_user_, mysql_password_, mysql_db_,
      mysql_connect_timeout_sec, mysql_query_timeout_sec))) {
    LOG_ERROR("reset mysql config fail", KR(ret), K(svr), K(mysql_user_),
        K(mysql_db_), K(mysql_connect_timeout_sec), K(mysql_query_timeout_sec));
  } else {
    LOG_INFO("connect to next mysql server", "cur_server", conn.get_server(),
        "next_server", svr, "next_svr_idx", svr_idx,
        "server_count", svr_provider_->get_server_count());

    // destroy connection
    if (conn.is_inited()) {
      conn.destroy();
    }

    if (OB_FAIL(conn.init(conn_config, enable_ssl_client_authentication))) {
      if (OB_NEED_RETRY == ret) {
        LOG_WARN("init mysql connector fail, need retry", KR(ret), K(svr), K(svr_idx),
            K(conn_config), K(enable_ssl_client_authentication));
      } else {
        LOG_ERROR("init mysql connector fail", KR(ret), K(svr), K(svr_idx), K(conn_config));
      }
    } else {
      // connection init success
    }
  }
  return ret;
}

int ObLogSysTableHelper::reset_connection()
{
  int ret = OB_SUCCESS;
  int64_t tid = thread_index_();
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", KR(ret));
  } else if (OB_ISNULL(mysql_conns_) || OB_ISNULL(next_svr_idx_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid mysql_conns_ or next_svr_idx_array_", KR(ret), K(mysql_conns_),
        K(next_svr_idx_array_));
  } else if (OB_UNLIKELY(tid >= max_thread_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("thread index is larger than systable helper's max thread number", KR(ret), K(tid),
        K(max_thread_num_));
  } else {
    ObLogMySQLConnector &conn = mysql_conns_[tid];
    const int64_t next_svr_idx = next_svr_idx_array_[tid];

    if (conn.is_inited()) {
      LOG_INFO("reset connection", K(tid), "current_server", conn.get_server(), K(next_svr_idx));
      conn.destroy();
    }
  }

  return ret;
}

}
}
