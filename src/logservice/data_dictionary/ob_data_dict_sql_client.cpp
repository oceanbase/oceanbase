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
*
*/


#include "ob_data_dict_sql_client.h"

#include "lib/mysqlclient/ob_mysql_proxy.h"     // ObMySQLProxy
#include "lib/string/ob_sql_string.h"           // ObSqlString
#include "common/ob_smart_var.h"                // SMART_VAR
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "share/ls/ob_ls_operator.h"            // ObLSAttrOperator

#define IF_CLIENT_VALID \
    if (IS_NOT_INIT) { \
      ret = common::OB_NOT_INIT; \
      DDLOG(WARN, "ObDataDictSqlClient not init", KR(ret)); \
    } else if (OB_ISNULL(sql_proxy_)) { \
      ret = oceanbase::common::OB_ERR_UNEXPECTED; \
      DDLOG(WARN, "expect valid sql_proxy for ObDataDictSqlClient", KR(ret)); \
    } else

using namespace oceanbase::common;
using namespace oceanbase::palf;
using namespace oceanbase::share;

namespace oceanbase
{
namespace datadict
{

const char *ObDataDictSqlClient::query_tenant_schema_version_sql_format =
    "SELECT MAX(SCHEMA_VERSION) AS SCHEMA_VERSION FROM %s AS OF SNAPSHOT %lu";
const char *ObDataDictSqlClient::report_data_dict_persist_info_sql_format =
    "REPLACE INTO %s (SNAPSHOT_SCN, START_LSN, END_LSN) VALUES (%lu, %lu, %lu)";

ObDataDictSqlClient::ObDataDictSqlClient()
  : is_inited_(false),
    sql_proxy_(NULL)
{}

int ObDataDictSqlClient::init(ObMySQLProxy *mysql_client)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(mysql_client)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid mysql_client for ObDataDictSqlClient", KR(ret));
  } else {
    sql_proxy_ = mysql_client;
    is_inited_ = true;
  }

  return ret;
}

void ObDataDictSqlClient::destroy()
{
  is_inited_ = false;
  sql_proxy_ = NULL;
}

int ObDataDictSqlClient::get_ls_info(
    const uint64_t tenant_id,
    const share::SCN &snapshot_scn,
    share::ObLSArray &ls_array)
{
  int ret = OB_SUCCESS;

  IF_CLIENT_VALID {
    share::ObLSAttrOperator ls_attr_op(tenant_id, sql_proxy_);
    share::ObLSAttrArray ls_attr_arr;

    if (OB_FAIL(ls_attr_op.load_all_ls_and_snapshot(
        snapshot_scn,
        ls_attr_arr,
        true/*only_existing_ls*/))) {
      DDLOG(WARN, "load_all_ls_and_snapshot failed", KR(ret), K(tenant_id), K(snapshot_scn));
    } else {
      ARRAY_FOREACH(ls_attr_arr, ls_attr_idx) {
        share::ObLSAttr &ls_attr = ls_attr_arr[ls_attr_idx];
        if (ls_attr.is_valid()
            && ! ls_attr.ls_is_creating() // load_all_ls_and_snapshot will filter abort and dropped ls
            && OB_FAIL(ls_array.push_back(ls_attr.get_ls_id()))) {
          DDLOG(WARN, "push_back normal ls into ls_array failed", KR(ret),
              K(tenant_id), K(snapshot_scn), K(ls_attr), K(ls_attr_idx), K(ls_attr_arr));
        }
      }
    }
  }

  return ret;
}

int ObDataDictSqlClient::get_schema_version(
    const uint64_t tenant_id,
    const share::SCN &snapshot_scn,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;

  IF_CLIENT_VALID {
    if (OB_UNLIKELY(! snapshot_scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      DDLOG(WARN, "invalid snapshot_scn to get_schema_version", KR(ret), K(snapshot_scn));
    } else {
      ObSqlString sql;
      int64_t record_count;
      uint64_t gts_ts = snapshot_scn.get_val_for_inner_table_field();

      SMART_VAR(ObISQLClient::ReadResult, result) {
        if (OB_FAIL(sql.assign_fmt(query_tenant_schema_version_sql_format,
            OB_ALL_DDL_OPERATION_TNAME, gts_ts))) {
          DDLOG(WARN, "assign_fmt to sql_string failed", KR(ret),
              K(tenant_id), K(snapshot_scn), K(gts_ts));
        } else if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
          DDLOG(WARN, "read from sql_proxy_ for schema_version failed", KR(ret),
              K(tenant_id), "sql", sql.ptr());
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          DDLOG(WARN, "get sql result failed", KR(ret), "sql", sql.ptr());
        } else if (OB_FAIL(parse_record_from_row_(*result.get_result(), record_count, schema_version))) {
          DDLOG(WARN, "parse_record_from_row_ for schema_version failed", KR(ret),
              K(tenant_id), K(snapshot_scn), "sql", sql.ptr());
        } else {
          DDLOG(INFO, "get_schema_version", K(tenant_id), K(schema_version));
        }
      }
    }
  }

  return ret;
}

int ObDataDictSqlClient::report_data_dict_persist_info(
    const uint64_t tenant_id,
    const share::SCN &snapshot_scn,
    const palf::LSN &start_lsn,
    const palf::LSN &end_lsn)
{
  int ret = OB_SUCCESS;

  IF_CLIENT_VALID {
    if (OB_UNLIKELY(!snapshot_scn.is_valid())
        || OB_UNLIKELY(! start_lsn.is_valid())
        || OB_UNLIKELY(! end_lsn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      DDLOG(WARN, "invalid args used for reporting to DATA_DICT_PERSIST_INFO", KR(ret),
          K(tenant_id), K(snapshot_scn), K(start_lsn), K(end_lsn));
    } else {
      ObSqlString sql;
      int64_t affected_rows = 0;
      uint64_t gts_ts = snapshot_scn.get_val_for_inner_table_field();

      if (OB_FAIL(sql.assign_fmt(report_data_dict_persist_info_sql_format,
          OB_ALL_DATA_DICTIONARY_IN_LOG_TNAME, gts_ts, start_lsn.val_, end_lsn.val_))) {
        DDLOG(WARN, "assign_fmt to sql_string failed", KR(ret),
            K(tenant_id), K(snapshot_scn), K(gts_ts), K(start_lsn), K(end_lsn));
      } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
        DDLOG(WARN, "write to all_data_dictionary_in_log failed", KR(ret),
            K(tenant_id), "sql", sql.ptr());
      } else if (OB_UNLIKELY(affected_rows <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "write affected_rows should not be zero", KR(ret), K(tenant_id), "sql", sql.ptr(), K(snapshot_scn));
      }
    }
  }

  return ret;
}

int ObDataDictSqlClient::parse_record_from_row_(
    common::sqlclient::ObMySQLResult &result,
    int64_t &record_count,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;

  IF_CLIENT_VALID {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(result.next())) {
        if (OB_ITER_END != ret) {
          DDLOG(WARN, "get next result failed", KR(ret), K(record_count));
        }
      } else {
        (void)GET_COL_IGNORE_NULL(result.get_int, "SCHEMA_VERSION", schema_version);

        if (OB_UNLIKELY(0 >= schema_version)) {
          ret = OB_ERR_UNEXPECTED;
          DDLOG(WARN, "invalid schema_version get from __all_ddl_operation", KR(ret), K(schema_version), K(record_count));
        } else {
          record_count++;
        }
      }
    } // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

} // namespace datadict
} // namespace oceanbase
