/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_srs_importer.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_basic_session_info.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "sql/engine/ob_exec_context.h"

#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
using namespace share;
using namespace sql;
using namespace common;
using namespace obrpc;
namespace table
{

int ObSRSImporter::exec_op(table::ObModuleDataArg op)
{
  int ret = OB_SUCCESS;
  if (op.op_ == ObModuleDataArg::LOAD_INFO) {
    ret = import_srs_info(op.file_path_);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exec op", K(ret), K(op));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to operate info", K(ret), K(op));
  }
  return ret;
}

int ObSRSImporter::get_srs_cnt(ObCommonSqlProxy *sql_proxy, uint64_t tenant_id, int64_t &srs_cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("select count(*) as srs_cnt from oceanbase.%s", OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else {
    srs_cnt = 0;
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get srs count", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "srs_cnt", srs_cnt, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to srs cnt", KR(ret), K(sql));
        } else {
          LOG_INFO("old srs rows", K(srs_cnt));
        }
      }
    }
  }
  return ret;
}

int ObSRSImporter::import_srs_info(const ObString &file_path)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLTransaction trans;
  int64_t srs_cnt = 0;

  // check old data row_cnt
  if (OB_ISNULL(exec_ctx_.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not null", K(ret), KP(exec_ctx_.get_sql_proxy()));
  } else if (OB_FAIL(get_srs_cnt(exec_ctx_.get_sql_proxy(), tenant_id_, srs_cnt))) {
    LOG_WARN("get srs count failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(sql.assign_fmt("select count(*) as srs_cnt from oceanbase.%s", OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  }
  // truncate table
  if (OB_SUCC(ret)) {
    ObSqlString trucate_sql;
    affected_rows_ = 0;
    if (OB_FAIL(trans.start(exec_ctx_.get_sql_proxy(), tenant_id_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(trucate_sql.assign_fmt("DELETE FROM oceanbase.%s", OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret), K(trucate_sql));
    } else if (OB_FAIL(trans.write(tenant_id_, trucate_sql.ptr(), affected_rows_))) {
      LOG_WARN("failed to exec sql", K(ret), K(trucate_sql), K(tenant_id_));
    } 
  }

  // load new data
  if (OB_SUCC(ret)) {
    affected_rows_ = 0;
    ObSqlString load_sql;
    ObSqlString update_time;
    const char *srs_file = "spatial_reference_systems.data";
    if (OB_FAIL(load_sql.assign_fmt("LOAD DATA INFILE '%.*s/%s' INTO TABLE oceanbase.%s ", file_path.length(), file_path.ptr(), srs_file,
      OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret), K(load_sql));
    } else if (OB_FAIL(update_time.assign_fmt("update oceanbase.%s set gmt_create = NOW(), gmt_modified = NOW()",
      OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TNAME))) {
      LOG_WARN("failed to assign update time sql", K(ret), K(load_sql));
    } else if (OB_FAIL(trans.write(tenant_id_, load_sql.ptr(), affected_rows_))) {
      LOG_WARN("failed to exec sql", K(ret), K(load_sql), K(tenant_id_));
    } else if (OB_FAIL(trans.write(tenant_id_, update_time.ptr(), affected_rows_))) {
      LOG_WARN("failed to exec sql", K(ret), K(load_sql), K(tenant_id_));
    } else if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else {
        LOG_INFO("new srs rows", K(affected_rows_));
      }
    }
  }
  return ret;
}

}  // end namespace table
}  // namespace oceanbase
