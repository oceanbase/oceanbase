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
#include "ob_ccl_rule_sql_service.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
const char *ObCCLRuleSqlService::CCL_RULE_TABLES[2] = {OB_ALL_CCL_RULE_TNAME,
                                                     OB_ALL_CCL_RULE_HISTORY_TNAME};
int ObCCLRuleSqlService::insert_ccl_rule(const ObCCLRuleSchema &ccl_rule_schema,
                                         common::ObISQLClient &sql_client,
                                         const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  uint64_t tenant_id = ccl_rule_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(!ccl_rule_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "ccl_rule is invalid", K(ccl_rule_schema), K(ret));
  }
  for (int64_t i = THE_SYS_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(CCL_RULE_TABLES); ++i) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", CCL_RULE_TABLES[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, ccl_rule_schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
      SQL_COL_APPEND_VALUE(sql, values, 0, "is_deleted", "%d");
      SQL_COL_APPEND_VALUE(sql, values, ccl_rule_schema.get_schema_version(), "schema_version", "%ld");
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                 static_cast<int32_t>(values.length()),
                                 values.ptr()))) {
        LOG_WARN("append sql failed, ", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
      }
    }
    values.reset();
    LOG_INFO("[CCL_RULE][SqlService] insert sql: ", K(sql));
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = ccl_rule_schema.get_tenant_id();
    opt.ccl_rule_id_ = ccl_rule_schema.get_ccl_rule_id();
    opt.op_type_ = OB_DDL_CREATE_CCL_RULE;
    opt.schema_version_ = ccl_rule_schema.get_schema_version();
    opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }

  return ret;
}

int ObCCLRuleSqlService::gen_sql(ObSqlString &sql, ObSqlString &values,
                                        const ObCCLRuleSchema &ccl_rule_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = ccl_rule_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SQL_COL_APPEND_VALUE(sql, values,
                       ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, ccl_rule_schema.get_tenant_id()),
                       "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(
    sql, values, ObSchemaUtils::get_extract_schema_id(exec_tenant_id, ccl_rule_schema.get_ccl_rule_id()),
    "ccl_rule_id", "%lu");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, ccl_rule_schema.get_ccl_rule_name().ptr(),
                                  ccl_rule_schema.get_ccl_rule_name().length(), "ccl_rule_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, ccl_rule_schema.get_affect_user_name().ptr(),
                                  ccl_rule_schema.get_affect_user_name().length(),
                                  "affect_user_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, ccl_rule_schema.get_affect_host().ptr(),
                                  ccl_rule_schema.get_affect_host().length(),
                                  "affect_host");
  SQL_COL_APPEND_VALUE(sql, values, ccl_rule_schema.affect_for_all_databases(),
                       "affect_for_all_databases", "%d");
  SQL_COL_APPEND_VALUE(sql, values, ccl_rule_schema.affect_for_all_tables(),
                       "affect_for_all_tables", "%d");
  if (!ccl_rule_schema.affect_for_all_databases()) {
    SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, ccl_rule_schema.get_affect_database().ptr(),
                                    ccl_rule_schema.get_affect_database().length(), "affect_database");
  }
  if (!ccl_rule_schema.affect_for_all_tables()) {
    SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, ccl_rule_schema.get_affect_table().ptr(),
                                    ccl_rule_schema.get_affect_table().length(), "affect_table");
  }
  SQL_COL_APPEND_VALUE(sql, values, static_cast<int>(ccl_rule_schema.get_affect_dml()), "affect_dml", "%d");
  SQL_COL_APPEND_VALUE(sql, values, ccl_rule_schema.get_affect_scope(),
                       "affect_scope", "%d");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, ccl_rule_schema.get_ccl_keywords().ptr(),
                       ccl_rule_schema.get_ccl_keywords().length(), "ccl_keywords");
  SQL_COL_APPEND_VALUE(sql, values, ccl_rule_schema.get_max_concurrency(), "max_concurrency", "%lu");
  return ret;
}

int ObCCLRuleSqlService::delete_ccl_rule(const ObCCLRuleSchema &ccl_rule_schema,
                                         const int64_t new_schema_version,
                                         common::ObISQLClient &sql_client,
                                         const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  uint64_t tenant_id = ccl_rule_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(!ccl_rule_schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(ccl_rule_schema));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt(
          "DELETE FROM %s WHERE tenant_id = %lu AND ccl_rule_id = %lu",
          CCL_RULE_TABLES[THE_SYS_TABLE_IDX],
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, ccl_rule_schema.get_ccl_rule_id())))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
    }
  }
  for (int64_t i = THE_HISTORY_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(CCL_RULE_TABLES); ++i) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", CCL_RULE_TABLES[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, ccl_rule_schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
      SQL_COL_APPEND_VALUE(sql, values, 1, "is_deleted", "%d");
      SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                 static_cast<int32_t>(values.length()), values.ptr()))) {
        LOG_WARN("append sql failed, ", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
      }
    }
    values.reset();
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = ccl_rule_schema.get_tenant_id();
    opt.ccl_rule_id_ = ccl_rule_schema.get_ccl_rule_id();
    opt.op_type_ = OB_DDL_DROP_CCL_RULE;
    opt.schema_version_ = new_schema_version;
    opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }

  return ret;
}

} // namespace schema
} //end of share
} //end of oceanbase
