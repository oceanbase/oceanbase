/**
 * Copyright (c) 2022 OceanBase
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

#include "ob_log_instance.h"                  // TCTX
#include "ob_log_meta_data_queryer.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"   // ObISQLClient
#include "lib/mysqlclient/ob_mysql_result.h"  // ObMySQLResult
#include "lib/string/ob_sql_string.h"         // ObSqlString
#include "share/inner_table/ob_inner_table_schema_constants.h" // OB_***_TNAME

using namespace oceanbase::share;
namespace oceanbase
{
namespace libobcdc
{

// TODO: Currently query ORA_ROWSCN from inner_table, however not support by oracle tenant in obcdc tenant_sync_mode.
// Wait observer support query ORA_ROWSCN by DBA VIEW
// and DBA_OB_DATA_DICTIONARY_IN_LOG VIEW will add column support query ORA_ROWSCN as END_SCN;
// TODO: to compatible with oracle mode in tenant_sync_mode, use REPORT_TIME as end_scn for temporary;
const char* ObLogMetaDataSQLQueryer::QUERY_SQL_FORMAT = "SELECT SNAPSHOT_SCN, %s AS END_SCN, START_LSN, END_LSN "
    "FROM %s WHERE SNAPSHOT_SCN <= %lu ORDER BY SNAPSHOT_SCN DESC %s";

int ObLogMetaDataSQLQueryer::get_data_dict_in_log_info(
    const uint64_t tenant_id,
    const int64_t start_timestamp_ns,
    logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info)
{
  int ret = OB_SUCCESS;
  const static int64_t QUERY_TIMEOUT = 1 * _MIN_;
  ObCDCQueryResult<logfetcher::DataDictionaryInLogInfo> query_result(data_dict_in_log_info);
  const uint64_t query_tenant_id = TCTX.is_tenant_sync_mode() ? OB_SYS_TENANT_ID : tenant_id;

  if (OB_FAIL(query(query_tenant_id, query_result, QUERY_TIMEOUT))) {
    LOG_ERROR("query data dict meta info failed", KR(ret), K(tenant_id), K(query_tenant_id));
  } else if (query_result.is_empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can't find datadict snapshot_scn older than start_timestamp_ns", KR(ret), K(tenant_id), K(start_timestamp_ns));
  } else {
    LOG_INFO("get_data_dict_in_log_info succ", K(tenant_id), K(start_timestamp_ns), K(data_dict_in_log_info));
  }

  return ret;
}

int ObLogMetaDataSQLQueryer::build_sql_statement_(const uint64_t tenant_id, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const bool is_oracle_mode = TCTX.mysql_proxy_.is_oracle_mode();
  const char* limit_expr = is_oracle_mode ? "FETCH FIRST 1 ROWS ONLY" : "LIMIT 1";
  const char* end_scn = is_oracle_mode ? "TIMESTAMP_TO_SCN(REPORT_TIME)" : "ORA_ROWSCN";
  const char* table_name = is_oracle_mode ? share::OB_DBA_OB_DATA_DICTIONARY_IN_LOG_TNAME : share::OB_ALL_DATA_DICTIONARY_IN_LOG_TNAME;

  if (OB_FAIL(sql.assign_fmt(QUERY_SQL_FORMAT, end_scn, table_name, start_timstamp_ns_, limit_expr))) {
    LOG_ERROR("format sql failed", KR(ret), K(tenant_id), K(is_oracle_mode), KCSTRING(limit_expr), KCSTRING(table_name));
  }

  return ret;
}

int ObLogMetaDataSQLQueryer::parse_row_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<logfetcher::DataDictionaryInLogInfo> &result)
{
  int ret = OB_SUCCESS;

  int64_t snapshot_scn_int = 0;
  int64_t end_scn_int = 0;
  int64_t start_lsn_int = 0;
  int64_t end_lsn_int = 0;
  EXTRACT_UINT_FIELD_MYSQL(sql_result, "SNAPSHOT_SCN", snapshot_scn_int, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(sql_result, "END_SCN", end_scn_int, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(sql_result, "START_LSN", start_lsn_int, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(sql_result, "END_LSN", end_lsn_int, int64_t);
  result.get_data().reset(snapshot_scn_int, end_scn_int, palf::LSN(start_lsn_int), palf::LSN(end_lsn_int));

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
