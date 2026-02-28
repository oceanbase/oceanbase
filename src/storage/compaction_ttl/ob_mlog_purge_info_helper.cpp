//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "storage/compaction_ttl/ob_mlog_purge_info_helper.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_event_history_table_operator.h"
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

} // namespace storage
} // namespace oceanbase
