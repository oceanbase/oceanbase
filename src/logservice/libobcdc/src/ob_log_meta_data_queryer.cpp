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
#include "lib/mysqlclient/ob_isql_client.h"   // ObISQLClient
#include "lib/mysqlclient/ob_mysql_result.h"  // ObMySQLResult
#include "lib/string/ob_sql_string.h"         // ObSqlString
#include "share/inner_table/ob_inner_table_schema_constants.h" // OB_***_TNAME

using namespace oceanbase::share;
namespace oceanbase
{
namespace libobcdc
{
ObLogMetaDataSQLQueryer::ObLogMetaDataSQLQueryer() :
    is_inited_(false),
    is_across_cluster_(false),
    cluster_id_(OB_INVALID_CLUSTER_ID),
    sql_proxy_(NULL)
{

}

ObLogMetaDataSQLQueryer::~ObLogMetaDataSQLQueryer()
{
  destroy();
}

int ObLogMetaDataSQLQueryer::init(
    const int64_t cluster_id,
    const bool is_across_cluster,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogMetaDataSQLQueryer has inited", KR(ret));
  } else {
    is_across_cluster_ = is_across_cluster;
    cluster_id_ = cluster_id;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;

    LOG_INFO("ObLogMetaDataSQLQueryer init success", K(cluster_id), K(is_across_cluster));
  }

  return ret;
}

void ObLogMetaDataSQLQueryer::destroy()
{
  is_inited_ = false;
  is_across_cluster_ = false;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  sql_proxy_ = NULL;
}

// Such as:
// select snapshot_scn, start_lsn, end_lsn from __all_virtual_data_dictionary_in_log where tenant_id=1004 and snapshot_scn <= 1669963370971342544 ORDER BY snapshot_scn DESC limit 1;
int ObLogMetaDataSQLQueryer::get_data_dict_in_log_info(
    const uint64_t tenant_id,
    const int64_t start_timstamp_ns,
    int64_t &record_count,
    logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info)
{
  int ret = OB_SUCCESS;
  const bool is_oracle_mode = TCTX.mysql_proxy_.is_oracle_mode();
  const char *select_fields = "SNAPSHOT_SCN, START_LSN, END_LSN";
  const char *limit_expr = is_oracle_mode ? "FETCH FIRST 1 ROWS ONLY" : "LIMIT 1";
  // __all_virtual_data_dictionary_in_log
  const char *data_dictionary_in_log_name = share::OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_TNAME;
  uint64_t query_tenent_id = OB_INVALID_TENANT_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogMetaDataSQLQueryer is not initialized", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_TIMESTAMP == start_timstamp_ns) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KT(tenant_id), K(start_timstamp_ns));
  } else {
    ObSqlString sql;
    record_count = 0;

    if (TCTX.is_tenant_sync_mode()) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT %s FROM %s"
          " WHERE SNAPSHOT_SCN <= %lu"
          " ORDER BY SNAPSHOT_SCN DESC %s",
          select_fields, share::OB_DBA_OB_DATA_DICTIONARY_IN_LOG_TNAME,
          start_timstamp_ns, limit_expr))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id), K(is_oracle_mode));
      } else {
        query_tenent_id = tenant_id;
      }
    } else {
      // OB4.1 doesn't have CDB_OB_DATA_DICTIONARY_IN_LOG VIEW，should only query with virtual table
      if (OB_FAIL(sql.assign_fmt(
          "SELECT %s FROM %s"
          " WHERE TENANT_ID = %lu AND SNAPSHOT_SCN <= %lu"
          " ORDER BY SNAPSHOT_SCN DESC LIMIT 1",
          select_fields, share::OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_TNAME,
          tenant_id, start_timstamp_ns))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id));
      } else {
        // Use OB_SYS_TENANT_ID to query
        query_tenent_id = OB_SYS_TENANT_ID;
      }
    }

    if (OB_SUCC(ret)) {
      SMART_VAR(ObISQLClient::ReadResult, result) {
        if (OB_FAIL(do_query_(OB_SYS_TENANT_ID, sql, result))) {
          LOG_WARN("do_query_ failed", KR(ret), K(cluster_id_), K(tenant_id), "sql", sql.ptr());
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get mysql result failed", KR(ret));
        } else if (OB_FAIL(get_records_template_(
                *result.get_result(),
                data_dict_in_log_info,
                "DataDictionaryInLogInfo",
                record_count))) {
          LOG_WARN("construct data_dict_in_log_info failed", KR(ret), K(data_dict_in_log_info));
        }
      } // SMART_VAR
    }
  }

  return ret;
}

int ObLogMetaDataSQLQueryer::do_query_(
    const uint64_t tenant_id,
    ObSqlString &sql,
    ObISQLClient::ReadResult &result)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogMetaDataSQLQueryer not init", KR(ret));
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

  return ret;
}

template <typename RecordsType>
int ObLogMetaDataSQLQueryer::get_records_template_(
    common::sqlclient::ObMySQLResult &res,
    RecordsType &records,
    const char *event,
    int64_t &record_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogMetaDataSQLQueryer not init", KR(ret));
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

int ObLogMetaDataSQLQueryer::parse_record_from_row_(
    common::sqlclient::ObMySQLResult &res,
    logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_scn_int = 0;
  int64_t start_lsn_int = 0;
  int64_t end_lsn_int = 0;

  (void)GET_COL_IGNORE_NULL(res.get_int, "SNAPSHOT_SCN", snapshot_scn_int);
  (void)GET_COL_IGNORE_NULL(res.get_int, "START_LSN", start_lsn_int);
  (void)GET_COL_IGNORE_NULL(res.get_int, "END_LSN", end_lsn_int);

  data_dict_in_log_info.reset(snapshot_scn_int, palf::LSN(start_lsn_int), palf::LSN(end_lsn_int));

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
