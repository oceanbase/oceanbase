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

#include "ob_timezone_importer.h"
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

int ObTimezoneImporter::exec_op(table::ObModuleDataArg op)
{
  int ret = OB_SUCCESS;
  if (op.op_ == ObModuleDataArg::LOAD_INFO) {
    if (OB_FAIL(import_timezone_info(op.file_path_))) {
      LOG_WARN("import timezone info failed", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support check timezone info", K(ret), K(op));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "check timezone is");
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_LOAD_TIME_ZONE_INFO_FAILED);
int ObTimezoneImporter::import_timezone_info(const ObString &file_path)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  char *buf = NULL;
  common::ObMySQLProxy *sql_proxy = NULL;
  ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy = exec_ctx_.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not null", K(ret), KP(sql_proxy));
  } else if (OB_FAIL(trans.start(sql_proxy, tenant_id_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else {
    // 1. truncate tables.
    ObSqlString trunc_sql1;
    ObSqlString trunc_sql2;
    ObSqlString trunc_sql3;
    ObSqlString trunc_sql4;
    if (OB_FAIL(trunc_sql1.assign_fmt("DELETE FROM %s", OB_ALL_TENANT_TIME_ZONE_TNAME))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(trunc_sql2.assign_fmt("DELETE FROM %s", OB_ALL_TENANT_TIME_ZONE_NAME_TNAME))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(trunc_sql3.assign_fmt("DELETE FROM %s", OB_ALL_TENANT_TIME_ZONE_TRANSITION_TNAME))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(trunc_sql4.assign_fmt("DELETE FROM %s", OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TNAME))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id_, trunc_sql1.ptr(), affected_rows))) {
      LOG_WARN("write failed", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id_, trunc_sql2.ptr(), affected_rows))) {
      LOG_WARN("write failed", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id_, trunc_sql3.ptr(), affected_rows))) {
      LOG_WARN("write failed", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id_, trunc_sql4.ptr(), affected_rows))) {
      LOG_WARN("write failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // 2. load data
    ObSqlString load_sql1;
    ObSqlString load_sql2;
    ObSqlString load_sql3;
    ObSqlString load_sql4;
    const char *timezone_file = "timezone.data";
    const char *timezone_name_file = "timezone_name.data";
    const char *timezone_transition_file = "timezone_trans.data";
    const char *timezone_transition_type_file = "timezone_trans_type.data";
    if (OB_FAIL(load_sql1.assign_fmt("LOAD DATA INFILE '%.*s/%s' INTO TABLE %s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'",
                file_path.length(), file_path.ptr(), timezone_file, OB_ALL_TENANT_TIME_ZONE_TNAME))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(load_sql2.assign_fmt("LOAD DATA INFILE '%.*s/%s' INTO TABLE %s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'",
                file_path.length(), file_path.ptr(), timezone_name_file, OB_ALL_TENANT_TIME_ZONE_NAME_TNAME))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(load_sql3.assign_fmt("LOAD DATA INFILE '%.*s/%s' INTO TABLE %s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'",
                file_path.length(), file_path.ptr(), timezone_transition_file, OB_ALL_TENANT_TIME_ZONE_TRANSITION_TNAME))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(load_sql4.assign_fmt("LOAD DATA INFILE '%.*s/%s' INTO TABLE %s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'",
                file_path.length(), file_path.ptr(), timezone_transition_type_file, OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TNAME))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id_, load_sql1.ptr(), affected_rows))) {
      LOG_WARN("write failed", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id_, load_sql2.ptr(), affected_rows))) {
      LOG_WARN("write failed", K(ret));
    } else if (OB_FAIL(EN_LOAD_TIME_ZONE_INFO_FAILED)) {
      LOG_WARN("load time zone info failed due to trace point", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id_, load_sql3.ptr(), affected_rows))) {
      LOG_WARN("write failed", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id_, load_sql4.ptr(), affected_rows))) {
      LOG_WARN("write failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // 3. insert version into __all_sys_stat
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("replace into %s(tenant_id, zone, data_type, name, value, info) "
                  "values(0, '' ,5, 'current_timezone_version', 1, 'current time zone version')",
                  OB_ALL_SYS_STAT_TNAME))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id_, sql.ptr(), affected_rows))) {
      LOG_WARN("write failed", K(ret));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

}  // end namespace table
}  // namespace oceanbase
