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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_zone_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
int ObZoneSqlService::alter_zone(
    const int64_t new_schema_version,
    common::ObISQLClient &sql_client,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    ObSchemaOperation alter_zone_op;
    alter_zone_op.tenant_id_ = OB_SYS_TENANT_ID;
    alter_zone_op.database_id_ = 0;
    alter_zone_op.tablegroup_id_ = 0;
    alter_zone_op.table_id_ = 0;
    alter_zone_op.op_type_ = OB_DDL_ALTER_ZONE;
    alter_zone_op.schema_version_ = new_schema_version;
    alter_zone_op.ddl_stmt_str_ = ddl_stmt_str != NULL ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(alter_zone_op, sql_client))) {
      LOG_WARN("log alter zone ddl operation failed", K(alter_zone_op), K(ret));
    }
  }
  return ret;
}

int ObZoneSqlService::add_zone(
    const int64_t new_schema_version,
    common::ObISQLClient &sql_client,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    ObSchemaOperation add_zone_op;
    add_zone_op.tenant_id_ = OB_SYS_TENANT_ID;
    add_zone_op.database_id_ = 0;
    add_zone_op.tablegroup_id_ = 0;
    add_zone_op.table_id_ = 0;
    add_zone_op.op_type_ = OB_DDL_ADD_ZONE;
    add_zone_op.schema_version_ = new_schema_version;
    add_zone_op.ddl_stmt_str_ = ddl_stmt_str != NULL ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(add_zone_op, sql_client))) {
      LOG_WARN("log add zone ddl operation failed", K(add_zone_op), K(ret));
    }
  }
  return ret;
}

int ObZoneSqlService::delete_zone(
    const int64_t new_schema_version,
    common::ObISQLClient &sql_client,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    ObSchemaOperation delete_zone_op;
    delete_zone_op.tenant_id_ = OB_SYS_TENANT_ID;
    delete_zone_op.database_id_ = 0;
    delete_zone_op.tablegroup_id_ = 0;
    delete_zone_op.table_id_ = 0;
    delete_zone_op.op_type_ = OB_DDL_DELETE_ZONE;
    delete_zone_op.schema_version_ = new_schema_version;
    delete_zone_op.ddl_stmt_str_ = ddl_stmt_str != NULL ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(delete_zone_op, sql_client))) {
      LOG_WARN("log delete zone ddl operation failed", K(delete_zone_op), K(ret));
    }
  }
  return ret;
}

int ObZoneSqlService::start_zone(
    const int64_t new_schema_version,
    common::ObISQLClient &sql_client,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    ObSchemaOperation start_zone_op;
    start_zone_op.tenant_id_ = OB_SYS_TENANT_ID;
    start_zone_op.database_id_ = 0;
    start_zone_op.tablegroup_id_ = 0;
    start_zone_op.table_id_ = 0;
    start_zone_op.op_type_ = OB_DDL_START_ZONE;
    start_zone_op.schema_version_ = new_schema_version;
    start_zone_op.ddl_stmt_str_ = ddl_stmt_str != NULL ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(start_zone_op, sql_client))) {
      LOG_WARN("log start zone ddl operation failed", K(start_zone_op), K(ret));
    }
  }
  return ret;
}

int ObZoneSqlService::stop_zone(
    const int64_t new_schema_version,
    common::ObISQLClient &sql_client,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    ObSchemaOperation stop_zone_op;
    stop_zone_op.tenant_id_ = OB_SYS_TENANT_ID;
    stop_zone_op.database_id_ = 0;
    stop_zone_op.tablegroup_id_ = 0;
    stop_zone_op.table_id_ = 0;
    stop_zone_op.op_type_ = OB_DDL_STOP_ZONE;
    stop_zone_op.schema_version_ = new_schema_version;
    stop_zone_op.ddl_stmt_str_ = ddl_stmt_str != NULL ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(stop_zone_op, sql_client))) {
      LOG_WARN("log stop zone ddl operation failed", K(stop_zone_op), K(ret));
    }
  }
  return ret;
}
} //end of schema
} //end of share
} //end of oceanbase
