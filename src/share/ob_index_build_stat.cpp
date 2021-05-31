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

#include "ob_index_build_stat.h"
#include "share/ob_define.h"
#include "share/inner_table/ob_inner_table_schema.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

int ObIndexBuildStatOperator::get_snapshot_version(const uint64_t data_table_id, const uint64_t index_table_id,
    common::ObMySQLProxy& sql_proxy, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    snapshot_version = 0;
    if (OB_UNLIKELY(OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_table_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(data_table_id), K(index_table_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT snapshot FROM %s "
                                      "WHERE tenant_id = %ld AND data_table_id = %ld AND index_table_id = %ld",
                   OB_ALL_INDEX_BUILD_STAT_TNAME,
                   extract_tenant_id(data_table_id),
                   data_table_id,
                   index_table_id))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "snapshot", snapshot_version, int64_t);
    }
  }
  return ret;
}
