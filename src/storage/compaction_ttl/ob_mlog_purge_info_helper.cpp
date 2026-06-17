// Copyright (c) 2025 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE
#include "storage/compaction_ttl/ob_mlog_purge_info_helper.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{
int ObMLogPurgeInfoHelper::get_mlog_purge_scn(
  const uint64_t mlog_id,
  const int64_t read_snapshot,
  int64_t &last_purge_scn)
{
  int ret = OB_SUCCESS;
  last_purge_scn = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(MTL_ID());
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObISQLClient *sql_client = GCTX.sql_proxy_;
    common::sqlclient::ObMySQLResult *result = nullptr;
    ObSqlString sql;
    uint64_t tmp_last_purge_scn = 0;
    if (OB_ISNULL(sql_client)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql client is null", K(ret), KP(sql_client));
    } else if (OB_FAIL(sql.assign_fmt("SELECT last_purge_scn FROM %s as of snapshot %ld WHERE tenant_id = 0 and mlog_id = %lu",
        OB_ALL_MLOG_TNAME, read_snapshot, mlog_id))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(sql_client->read(res, exec_tenant_id, sql.ptr()))) {
      if (OB_TABLE_DEFINITION_CHANGED == ret || OB_SNAPSHOT_DISCARDED == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      }
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next", KR(ret));
      } else { // ignore row not exist
        ret = OB_SUCCESS;
      }
    } else {
      EXTRACT_UINT_FIELD_MYSQL(*result, "last_purge_scn", tmp_last_purge_scn, uint64_t);
      if (OB_SUCC(ret)) {
        last_purge_scn = tmp_last_purge_scn;
      } else if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS; // ignore null value
      }
    }
  }
  return ret;
}

int ObMLogPurgeInfoHelper::get_recent_tenant_mlog_purge_scns(
  const int64_t read_snapshot,
  MlogPurgeScnMap &mlog_purge_scn_map)
{
  static constexpr int64_t BATCH_LIMIT = 1024;
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(MTL_ID());
  int64_t last_mlog_id = -1;
  bool scan_done = false;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObISQLClient *sql_client = GCTX.sql_proxy_;
    common::sqlclient::ObMySQLResult *result = nullptr;
    ObSqlString sql;
    if (OB_ISNULL(sql_client)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql client is null", K(ret), KP(sql_client));
    }
    while (OB_SUCC(ret) && !scan_done) {
      if (OB_FAIL(sql.assign_fmt(
              "SELECT mlog_id, last_purge_scn FROM %s as of snapshot %ld "
              "WHERE tenant_id = 0 AND mlog_id > %ld ORDER BY mlog_id ASC "
              "LIMIT %ld",
              OB_ALL_MLOG_TNAME, read_snapshot, last_mlog_id, BATCH_LIMIT))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(sql_client->read(res, exec_tenant_id, sql.ptr()))) {
        if (OB_TABLE_DEFINITION_CHANGED == ret || OB_SNAPSHOT_DISCARDED == ret) {
          ret = OB_SUCCESS;
          // overwrite ret
          scan_done = true;
        } else {
          LOG_WARN("execute sql failed", KR(ret), K(sql));
        }
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else {
        int64_t batch_row_cnt = 0;
        int64_t mlog_id = OB_INVALID_ID;
        uint64_t last_purge_scn = OB_INVALID_SCN_VAL;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to get next", KR(ret));
            } else {
              ret = OB_SUCCESS;
              // overwrite ret
              break;
            }
          } else {
            EXTRACT_INT_FIELD_MYSQL(*result, "mlog_id", mlog_id, int64_t);
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to extract mlog id", KR(ret));
            } else {
              EXTRACT_UINT_FIELD_MYSQL(*result, "last_purge_scn",
                                       last_purge_scn, uint64_t);
              if (OB_ERR_NULL_VALUE == ret) {
                ret = OB_SUCCESS;
                // overwrite ret
              } else if (OB_FAIL(ret)) {
                LOG_WARN("failed to extract last_purge_scn", KR(ret),
                         K(mlog_id));
              } else if (OB_FAIL(mlog_purge_scn_map.set_refactored(
                             mlog_id, last_purge_scn))) {
                LOG_WARN("failed to set mlog purge scn map", KR(ret),
                         K(mlog_id), K(last_purge_scn));
              }
              if (OB_SUCC(ret)) {
                batch_row_cnt++;
                last_mlog_id = mlog_id;
              }
            }
          }
        }
        scan_done = batch_row_cnt < BATCH_LIMIT;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
