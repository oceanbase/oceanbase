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

#define USING_LOG_PREFIX SHARE

#include "share/ob_freeze_info_proxy.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_global_stat_proxy.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace storage;
using namespace share;
using namespace palf;
namespace share
{

OB_SERIALIZE_MEMBER(ObSimpleFrozenStatus, frozen_scn_,
                    schema_version_, data_version_);

int ObFreezeInfoProxy::get_freeze_info(
    ObISQLClient &sql_proxy,
    const SCN &frozen_scn,
    ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (frozen_scn == SCN::min_scn()) {
        if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s ORDER BY frozen_scn DESC LIMIT 1",
            OB_ALL_FREEZE_INFO_TNAME))) {
          LOG_WARN("fail to append sql", KR(ret), K_(tenant_id), K(frozen_scn));
        }
      } else {
        if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE frozen_scn = %lu",
            OB_ALL_FREEZE_INFO_TNAME, frozen_scn.get_val_for_inner_table_field()))) {
          LOG_WARN("fail to append sql", KR(ret), K_(tenant_id), K(frozen_scn));
        }
      }

      int tmp_ret = OB_SUCCESS;
      if (FAILEDx(sql_proxy.read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql), K_(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql), K_(tenant_id));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next", KR(ret), K(sql), K_(tenant_id));
      } else if (OB_FAIL(construct_frozen_status_(*result, frozen_status))) {
        LOG_WARN("fail to construct frozen_status", KR(ret));
      } else if (OB_ITER_END != (tmp_ret = result->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K_(tenant_id));
      } else if (!frozen_status.is_valid()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("fail to find freeze info", KR(ret), K(frozen_scn), K_(tenant_id));
      }
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_all_freeze_info(
    ObISQLClient &sql_proxy,
    ObIArray<ObSimpleFrozenStatus> &frozen_statuses)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE frozen_scn > 1 ORDER BY frozen_scn ASC",
        OB_ALL_FREEZE_INFO_TNAME))) {
      LOG_WARN("fail to append sql", KR(ret), K_(tenant_id));
    } else if (FAILEDx(sql_proxy.read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql), K_(tenant_id));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), K(sql), K_(tenant_id));
    } else {
      while (OB_SUCC(ret)) {
        ObSimpleFrozenStatus frozen_status;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", KR(ret), K_(tenant_id));
          }
        } else if (OB_FAIL(construct_frozen_status_(*result, frozen_status))) {
          LOG_WARN("fail to construct frozen_status", KR(ret));
        } else if (OB_UNLIKELY(!frozen_status.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid frozen status", KR(ret), K_(tenant_id), K(frozen_status), K(sql));
        } else if (OB_FAIL(frozen_statuses.push_back(frozen_status))) {
          LOG_WARN("fail to push back", KR(ret), K(frozen_status), K_(tenant_id));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_freeze_info_larger_or_equal_than(
    ObISQLClient &sql_proxy,
    const SCN &frozen_scn,
    ObIArray<ObSimpleFrozenStatus> &frozen_statuses)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    const uint64_t frozen_scn_val = frozen_scn.get_val_for_inner_table_field();
    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE frozen_scn >= %lu ORDER BY frozen_scn",
        OB_ALL_FREEZE_INFO_TNAME, frozen_scn_val))) {
      LOG_WARN("fail to append sql", KR(ret), K_(tenant_id), K(frozen_scn));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql), K_(tenant_id));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), K(sql), K_(tenant_id));
    } else {
      while (OB_SUCC(ret)) {
        ObSimpleFrozenStatus frozen_status;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", KR(ret), K_(tenant_id));
          }
        } else if (OB_FAIL(construct_frozen_status_(*result, frozen_status))) {
          LOG_WARN("fail to construct frozen_status", KR(ret));
        } else if (OB_UNLIKELY(!frozen_status.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid frozen status", KR(ret), K(frozen_scn), K_(tenant_id), K(frozen_status), K(sql));
        } else if (OB_FAIL(frozen_statuses.push_back(frozen_status))) {
          LOG_WARN("fail to push back", KR(ret), K(frozen_status), K_(tenant_id));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  LOG_INFO("finish load_freeze_info", KR(ret), K_(tenant_id), K(sql));
  return ret;
}

int ObFreezeInfoProxy::get_frozen_scn_larger_or_equal_than(
    ObISQLClient &sql_proxy,
    const SCN &frozen_scn,
    ObIArray<uint64_t> &frozen_scn_vals)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    const uint64_t frozen_scn_val = frozen_scn.get_val_for_inner_table_field();
    if (OB_FAIL(sql.assign_fmt("SELECT frozen_scn FROM %s WHERE frozen_scn >= %lu ORDER BY frozen_scn",
        OB_ALL_FREEZE_INFO_TNAME, frozen_scn_val))) {
      LOG_WARN("fail to append sql", KR(ret), K_(tenant_id), K(frozen_scn));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql), K_(tenant_id));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), K(sql), K_(tenant_id));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", KR(ret), K_(tenant_id));
          }
        } else {
          uint64_t frozen_scn_val = OB_INVALID_SCN_VAL;
          EXTRACT_UINT_FIELD_MYSQL(*result, "frozen_scn", frozen_scn_val, uint64_t);
          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(OB_INVALID_SCN_VAL == frozen_scn_val)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid frozen scn val", KR(ret), K(frozen_scn_val), K_(tenant_id), K(sql));
            } else if (OB_FAIL(frozen_scn_vals.push_back(frozen_scn_val))) {
              LOG_WARN("fail to push back", KR(ret), K(frozen_scn_val), K_(tenant_id));
            }
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  LOG_INFO("finish load frozen scn", KR(ret), K_(tenant_id), K(sql));
  return ret;
}

int ObFreezeInfoProxy::get_max_frozen_scn_smaller_or_equal_than(
    ObISQLClient &sql_proxy,
    const SCN &compaction_scn,
    SCN &max_frozen_scn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(!compaction_scn.is_valid() || (compaction_scn < SCN::base_scn()))) {
    LOG_WARN("invalid argument", KR(ret), K(compaction_scn));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      const uint64_t compaction_scn_val = compaction_scn.get_val_for_inner_table_field();
      if (OB_FAIL(sql.assign_fmt("SELECT MAX(frozen_scn) as value FROM %s WHERE frozen_scn <= %lu",
          OB_ALL_FREEZE_INFO_TNAME, compaction_scn_val))) {
        LOG_WARN("fail to append sql", KR(ret), K_(tenant_id), K(compaction_scn));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql), K_(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql), K_(tenant_id));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("get next result failed", KR(ret), K_(tenant_id), K(sql));
      } else {
        uint64_t max_frozen_scn_val = UINT64_MAX;
        EXTRACT_UINT_FIELD_MYSQL(*result, "value", max_frozen_scn_val, uint64_t);
        if (FAILEDx(max_frozen_scn.convert_for_inner_table_field(max_frozen_scn_val))) {
          LOG_WARN("fail to convert uint64_t to SCN", KR(ret), K(max_frozen_scn_val));
        }
      }
    }
  }
  LOG_INFO("finish to get freeze_info", KR(ret), K_(tenant_id), K(sql));
  return ret;
}

int ObFreezeInfoProxy::set_freeze_info(
    ObISQLClient &sql_proxy,
    const ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  ObDMLExecHelper exec(sql_proxy, tenant_id_);

  if (!frozen_status.is_valid() || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(frozen_status), K_(tenant_id));
  } else if (OB_FAIL(dml.add_uint64_pk_column("frozen_scn", frozen_status.frozen_scn_.get_val_for_inner_table_field()))
            || OB_FAIL(dml.add_column("cluster_version", frozen_status.data_version_))
            || OB_FAIL(dml.add_column("schema_version", frozen_status.schema_version_))) {
    LOG_WARN("fail to add column", KR(ret), K(frozen_status), K_(tenant_id));
  } else if (OB_FAIL(exec.exec_insert(OB_ALL_FREEZE_INFO_TNAME, dml, affected_rows))) {
    LOG_WARN("fail to exec_insert", KR(ret), K_(tenant_id), K(frozen_status));
  } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected affected rows", KR(ret), K(affected_rows), K_(tenant_id), K(frozen_status));
  }
  return ret;
}

int ObFreezeInfoProxy::get_min_major_available_and_larger_info(
    ObISQLClient &sql_proxy,
    const SCN &frozen_scn,
    SCN &min_frozen_scn,
    ObIArray<ObSimpleFrozenStatus> &frozen_statuses)
{
  int ret = OB_SUCCESS;
  frozen_statuses.reset();
  min_frozen_scn = SCN::min_scn();
  if (OB_UNLIKELY(!frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn));
  } else if (OB_FAIL(get_min_major_available_and_larger_info_inner_(sql_proxy,
             frozen_scn, min_frozen_scn, frozen_statuses))) {
    LOG_WARN("fail to get freeze info larger than and min frozen_scn", KR(ret), 
      K_(tenant_id), K(frozen_scn));
  }
  return ret;
}

int ObFreezeInfoProxy::batch_delete(
    ObISQLClient &sql_proxy,
    const SCN &upper_frozen_scn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!upper_frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(upper_frozen_scn));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE frozen_scn <= %lu AND frozen_scn > 1",
             OB_ALL_FREEZE_INFO_TNAME, upper_frozen_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K(upper_frozen_scn));
  } else if (OB_FAIL(sql_proxy.write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql), K_(tenant_id));
  } else {
    LOG_INFO("succ to delete freeze_info", K_(tenant_id), K(upper_frozen_scn), K(affected_rows));
  }
  return ret;
}

int ObFreezeInfoProxy::get_frozen_info_less_than(
    ObISQLClient &sql_proxy,
    const SCN &frozen_scn,
    ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  ObArray<ObSimpleFrozenStatus> frozen_status_arr;
  if (OB_FAIL(get_frozen_info_less_than(sql_proxy, frozen_scn, frozen_status_arr, false))) {
    LOG_WARN("fail to get frozen_info_less_than", KR(ret), K(frozen_scn));
  } else if (frozen_status_arr.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frozen_status_arr should have only one element", KR(ret), K(frozen_status_arr));
  } else {
    frozen_status.assign(frozen_status_arr.at(0));
  }
  return ret;
}

int ObFreezeInfoProxy::get_frozen_info_less_than(
    ObISQLClient &sql_proxy,
    const SCN &frozen_scn,
    ObIArray<ObSimpleFrozenStatus> &frozen_status_arr,
    bool get_all)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      int tmp_ret = OB_SUCCESS;
      const uint64_t frozen_scn_val = frozen_scn.get_val_for_inner_table_field();
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE frozen_scn <= %lu ORDER BY frozen_scn DESC %s",
                                 OB_ALL_FREEZE_INFO_TNAME, frozen_scn_val, (get_all ? "" : "LIMIT 1")))) {
        LOG_WARN("fail to append sql", KR(ret), K_(tenant_id), K(frozen_scn), K(get_all));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql), K_(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql), K_(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          ObSimpleFrozenStatus frozen_status;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next row", KR(ret), K_(tenant_id));
            }
          } else if (OB_FAIL(construct_frozen_status_(*result, frozen_status))) {
            LOG_WARN("fail to construct frozen_status", KR(ret));
          } else if (OB_UNLIKELY(!frozen_status.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid frozen status", KR(ret), K(frozen_scn), K_(tenant_id), K(frozen_status), K(sql));
          } else if (OB_FAIL(frozen_status_arr.push_back(frozen_status))) {
            LOG_WARN("fail to push back", KR(ret), K(frozen_status), K_(tenant_id));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_max_freeze_info(
    ObISQLClient &sql_proxy,
    ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s ORDER BY frozen_scn DESC LIMIT 1",
        OB_ALL_FREEZE_INFO_TNAME))) {
      LOG_WARN("fail to append sql", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql), K_(tenant_id));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), K(sql), K_(tenant_id));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next", KR(ret), K(sql), K_(tenant_id));
    } else if (OB_FAIL(construct_frozen_status_(*result, frozen_status))) {
      LOG_WARN("fail to construct frozen_status", KR(ret), K(sql));
    } else if (OB_ITER_END != (tmp_ret = result->next())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K_(tenant_id), K(sql));
    } else if (!frozen_status.is_valid()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to find frozen status with max frozen_scn", KR(ret), K_(tenant_id), K(frozen_status));
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_min_major_available_and_larger_info_inner_(
    ObISQLClient &sql_proxy,
    const SCN &frozen_scn,
    SCN &min_frozen_scn,
    ObIArray<ObSimpleFrozenStatus> &frozen_statuses)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn));
  } else {
    ObSqlString sql;
    min_frozen_scn.set_max();
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT *  FROM %s", OB_ALL_FREEZE_INFO_TNAME))) {
        LOG_WARN("fail to append sql", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql), K_(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", KR(ret), K(sql), K_(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          ObSimpleFrozenStatus frozen_status;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next row", KR(ret), K_(tenant_id));
            }
          } else if (OB_FAIL(construct_frozen_status_(*result, frozen_status))) {
            LOG_WARN("fail to construct frozen_status", KR(ret));
          } else if (FALSE_IT(min_frozen_scn = (min_frozen_scn < frozen_status.frozen_scn_ ?
                                                min_frozen_scn : frozen_status.frozen_scn_))) {
          } else if (frozen_status.frozen_scn_ > frozen_scn) {
            if (OB_FAIL(frozen_statuses.push_back(frozen_status))) {
              LOG_WARN("fail to push back", KR(ret), K(frozen_status), K_(tenant_id));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObFreezeInfoProxy::construct_frozen_status_(
    sqlclient::ObMySQLResult &result,
    ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  uint64_t frozen_scn_val = OB_INVALID_SCN_VAL;
  EXTRACT_UINT_FIELD_MYSQL(result, "frozen_scn", frozen_scn_val, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "cluster_version", frozen_status.data_version_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "schema_version", frozen_status.schema_version_, int64_t);
  if (FAILEDx(frozen_status.frozen_scn_.convert_for_inner_table_field(frozen_scn_val))) {
    LOG_WARN("fail to convert val to SCN", KR(ret), K(frozen_scn_val));
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////

int ObFreezeInfoProxy::get_freeze_schema_info(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const SCN &frozen_scn,
    TenantIdAndSchemaVersion &schema_version_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || (!frozen_scn.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(frozen_scn));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(tenant_id), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE frozen_scn = %ld",
                                    OB_ALL_FREEZE_INFO_TNAME, frozen_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(frozen_scn));
  }

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to read", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      LOG_WARN("fail to get result", KR(ret), K(tenant_id), K(frozen_scn));
    } else {
      schema_version_info.tenant_id_ = tenant_id;
      EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", schema_version_info.schema_version_, int64_t);
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
