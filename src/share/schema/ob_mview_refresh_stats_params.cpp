/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_mview_refresh_stats_params.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

ObMViewRefreshStatsParams::ObMViewRefreshStatsParams() { reset(); }

ObMViewRefreshStatsParams::ObMViewRefreshStatsParams(
  ObMVRefreshStatsCollectionLevel collection_level, int64_t retention_period)
  : collection_level_(collection_level), retention_period_(retention_period)
{
}

ObMViewRefreshStatsParams::ObMViewRefreshStatsParams(ObIAllocator *allocator) : ObSchema(allocator)
{
  reset();
}

ObMViewRefreshStatsParams::ObMViewRefreshStatsParams(const ObMViewRefreshStatsParams &other)
{
  reset();
  *this = other;
}

ObMViewRefreshStatsParams::~ObMViewRefreshStatsParams() {}

ObMViewRefreshStatsParams &ObMViewRefreshStatsParams::operator=(
  const ObMViewRefreshStatsParams &other)
{
  if (this != &other) {
    reset();
    int &ret = error_ret_;
    collection_level_ = other.collection_level_;
    retention_period_ = other.retention_period_;
  }
  return *this;
}

int ObMViewRefreshStatsParams::assign(const ObMViewRefreshStatsParams &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObMViewRefreshStatsParams::is_valid() const
{
  bool bret = false;
  if (OB_LIKELY(ObSchema::is_valid())) {
    bret = ObMVRefreshStatsCollectionLevel::MAX != collection_level_ &&
           is_retention_period_valid(retention_period_);
  }
  return bret;
}

void ObMViewRefreshStatsParams::reset()
{
  collection_level_ = ObMVRefreshStatsCollectionLevel::MAX;
  retention_period_ = 0;
  ObSchema::reset();
}

int64_t ObMViewRefreshStatsParams::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObMViewRefreshStatsParams));
  return len;
}

OB_SERIALIZE_MEMBER(ObMViewRefreshStatsParams, collection_level_, retention_period_);

int ObMViewRefreshStatsParams::read_stats_params(ObISQLClient &sql_client, uint64_t exec_tenant_id,
                                                 ObSqlString &sql,
                                                 ObMViewRefreshStatsParams &params)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql_client.read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next", KR(ret));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, collection_level, params,
                                       ObMVRefreshStatsCollectionLevel);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, retention_period, params, int64_t);
    }
  }
  return ret;
}

int ObMViewRefreshStatsParams::gen_sys_defaults_dml(uint64_t tenant_id, ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), KPC(this));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_column("collection_level", collection_level_)) ||
        OB_FAIL(dml.add_column("retention_period", retention_period_))) {
      LOG_WARN("add column failed", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefreshStatsParams::set_sys_defaults(ObISQLClient &sql_client, uint64_t tenant_id,
                                                const ObMViewRefreshStatsParams &params)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, set sys defaults is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !params.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(params));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(params.gen_sys_defaults_dml(tenant_id, dml))) {
      LOG_WARN("fail to gen sys defaults dml", KR(ret), K(params));
    } else {
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert_update(OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TNAME, dml,
                                          affected_rows))) {
        LOG_WARN("execute insert update failed", KR(ret));
      } else if (OB_UNLIKELY(!is_zero_row(affected_rows) && !is_single_row(affected_rows) &&
                             !is_double_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be zero, one or two", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObMViewRefreshStatsParams::fetch_sys_defaults(ObISQLClient &sql_client, uint64_t tenant_id,
                                                  ObMViewRefreshStatsParams &params,
                                                  bool for_update)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, fetch sys defaults is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = 0",
                               OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (for_update && OB_FAIL(sql.append(" for update"))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(read_stats_params(sql_client, exec_tenant_id, sql, params))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to read stats params", KR(ret), K(exec_tenant_id), K(sql));
      } else {
        ret = OB_SUCCESS;
        params = get_default();
      }
    }
  }
  return ret;
}

int ObMViewRefreshStatsParams::gen_mview_refresh_stats_params_dml(uint64_t tenant_id,
                                                                  uint64_t mview_id,
                                                                  ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == mview_id || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(mview_id), KPC(this));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("mview_id", mview_id)) ||
        OB_FAIL(dml.add_column("collection_level", collection_level_)) ||
        OB_FAIL(dml.add_column("retention_period", retention_period_))) {
      LOG_WARN("add column failed", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefreshStatsParams::set_mview_refresh_stats_params(
  ObISQLClient &sql_client, uint64_t tenant_id, uint64_t mview_id,
  const ObMViewRefreshStatsParams &params)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, set mview refresh stats params is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == mview_id ||
                         !params.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(mview_id), K(params));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(params.gen_mview_refresh_stats_params_dml(tenant_id, mview_id, dml))) {
      LOG_WARN("fail to gen mview refresh stats params dml", KR(ret), K(params));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(
            exec.exec_insert_update(OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert update failed", KR(ret));
      } else if (OB_UNLIKELY(!is_zero_row(affected_rows) && !is_single_row(affected_rows) &&
                             !is_double_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be zero, one or two", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObMViewRefreshStatsParams::drop_mview_refresh_stats_params(ObISQLClient &sql_client,
                                                               uint64_t tenant_id,
                                                               uint64_t mview_id, bool if_exists)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop mview refresh stats params is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(mview_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("mview_id", mview_id))) {
      LOG_WARN("add column failed", KR(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_delete(OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TNAME, dml, affected_rows))) {
        LOG_WARN("execute update failed", KR(ret));
      } else if (!if_exists && OB_UNLIKELY(!is_single_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObMViewRefreshStatsParams::drop_all_mview_refresh_stats_params(ObISQLClient &sql_client,
                                                                   uint64_t tenant_id,
                                                                   int64_t &affected_rows,
                                                                   int64_t limit)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop all mview refresh stats params is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = %ld",
                               OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TNAME,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (limit > 0 && OB_FAIL(sql.append_fmt(" limit %ld", limit))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObMViewRefreshStatsParams::fetch_mview_refresh_stats_params(ObISQLClient &sql_client,
                                                                uint64_t tenant_id,
                                                                uint64_t mview_id,
                                                                ObMViewRefreshStatsParams &params,
                                                                bool with_sys_defaults)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, fetch mview refresh stats params is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(mview_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    if (with_sys_defaults) {
      if (OB_FAIL(sql.assign_fmt(
            "select collection_level, retention_period from"
            "("
            "  with defvals as"
            "  ("
            "    select"
            "      ifnull(max(collection_level), 1) as collection_level,"
            "      ifnull(max(retention_period), 31) as retention_period"
            "    from %s"
            "    where tenant_id = 0"
            "  )"
            "  select"
            "    ifnull(e.collection_level, d.collection_level) collection_level,"
            "    ifnull(e.retention_period, d.retention_period) retention_period"
            "  from"
            "    (select tenant_id, mview_id, collection_level, retention_period from %s"
            "      right outer join"
            "      (select tenant_id, mview_id from %s where tenant_id = 0 and mview_id = %ld)"
            "      using (tenant_id, mview_id)"
            "    ) e,"
            "    defvals d"
            ")",
            OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TNAME, OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TNAME,
            OB_ALL_MVIEW_TNAME, mview_id))) {
        LOG_WARN("fail to assign sql with sys defaults", KR(ret));
      } else if (OB_FAIL(read_stats_params(sql_client, exec_tenant_id, sql, params))) {
        LOG_WARN("fail to read stats params", KR(ret), K(exec_tenant_id), K(sql));
      }
    } else {
      if (OB_FAIL(sql.assign_fmt("select collection_level, retention_period from %s"
                                 " where tenant_id = 0 and mview_id = %ld",
                                 OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TNAME, mview_id))) {
        LOG_WARN("fail to assign sql without sys defaults", KR(ret));
      } else if (OB_FAIL(read_stats_params(sql_client, exec_tenant_id, sql, params))) {
        LOG_WARN("fail to read stats params", KR(ret), K(exec_tenant_id), K(sql));
      }
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
