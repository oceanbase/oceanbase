/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_ddl_explain_plan.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/resolver/ddl/ob_explain_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/ob_result_set.h"
#include "share/ob_cluster_version.h"
#include "sql/engine/cmd/ob_table_executor.h"
#include "lib/string/ob_sql_string.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
int ObDDLExplainHelper::generate_plan(const ObExplainStmt *explain_stmt,
                                       ObExecContext *exec_ctx,
                                       ObLogValues *values_op)
{
  int ret = OB_SUCCESS;
  ObStmt *ddl_stmt = nullptr;

  uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  if (cluster_version < CLUSTER_VERSION_5_0_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("EXPLAIN DDL requires cluster version >= 5.0.1", K(ret), K(cluster_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "EXPLAIN DDL requires cluster version >= 5.0.1");
  } else if (OB_ISNULL(explain_stmt) || OB_ISNULL(exec_ctx) || OB_ISNULL(values_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer for DDL explain", K(ret), KP(explain_stmt), KP(exec_ctx), KP(values_op));
  } else if (OB_ISNULL(ddl_stmt = explain_stmt->get_explain_ddl_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ddl stmt for DDL explain", K(ret));
  } else if (OB_UNLIKELY(stmt::T_ALTER_TABLE != ddl_stmt->get_stmt_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("EXPLAIN DDL only supports ALTER TABLE", K(ret), "stmt_type", ddl_stmt->get_stmt_type());
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "EXPLAIN DDL only supports ALTER TABLE");
  } else {
    ObSqlString msg;
    ObAlterTableStmt *alter_stmt = static_cast<ObAlterTableStmt *>(ddl_stmt);
    if (!alter_stmt->get_is_explain()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter table stmt is not marked as explain for DDL explain", K(ret));
    } else {
      ObAlterTableExecutor alter_executor;
      share::ObAlterTableDDLPlan ddl_plan;
      if (OB_FAIL(alter_executor.execute_for_explain(*exec_ctx, *alter_stmt, ddl_plan))) {
        LOG_WARN("execute_for_explain failed for EXPLAIN DDL", K(ret));
      } else if (OB_FAIL(ddl_plan.build_explain_message(msg))) {
        LOG_WARN("failed to build message for EXPLAIN DDL", K(ret), K(ddl_plan));
      } else {
        const char *cat = ddl_plan.get_explain_category_str();
        const char *block_dml_str = ddl_plan.get_explain_blocks_dml_str();
        const char *algo = ddl_plan.get_explain_algorithm_str();

        ObObj cells[4];
        cells[0].set_varchar(ObString::make_string(cat));
        cells[0].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        cells[1].set_varchar(ObString::make_string(block_dml_str));
        cells[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        cells[2].set_varchar(ObString::make_string(algo));
        cells[2].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        cells[3].set_varchar(ObString(static_cast<int32_t>(msg.length()), msg.ptr()));
        cells[3].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

        ObNewRow row;
        row.cells_ = cells;
        row.count_ = 4;
        if (OB_FAIL(values_op->add_row(row))) {
          LOG_WARN("failed to add DDL explain result row", K(ret));
        } else {
          LOG_INFO("explain ddl plan generated", K(ddl_plan),
                   "category", cat, "algorithm", algo, "block_dml", block_dml_str,
                   "msg", msg.string());
        }
      }
    }
  }
  return ret;
}

int ObDDLExplainHelper::build_output_exprs(ObLogPlan &log_plan,
                                           ObLogValues *values_op)
{
  int ret = OB_SUCCESS;
  // CATEGORY(varchar)|BLOCK_DML(varchar)|ALGORITHM(varchar)|MESSAGE(varchar)
  for (int64_t i = 0; OB_SUCC(ret) && i < 4; ++i) {
    if (OB_FAIL(log_plan.allocate_output_expr_for_values_op(*values_op))) {
      LOG_WARN("failed to allocate varchar output expr for DDL explain column", K(ret), K(i));
    }
  }
  return ret;
}

int ObDDLExplainHelper::fill_result_set(ObResultSet &result_set)
{
  int ret = OB_SUCCESS;
  // CATEGORY(VARCHAR) | BLOCK_DML(VARCHAR) | ALGORITHM(VARCHAR) | MESSAGE(VARCHAR)
  ObString tname = ObString::make_string("explain_ddl_table");
  ObString type_name = ObString::make_string("varchar");
  struct { const char *name; ObObjType type; } ddl_explain_cols[] = {
    { "CATEGORY",  ObVarcharType },
    { "BLOCK_DML", ObVarcharType },
    { "ALGORITHM", ObVarcharType },
    { "MESSAGE",   ObVarcharType },
  };
  if (OB_FAIL(result_set.reserve_field_columns(4))) {
    LOG_WARN("reserve field columns failed for DDL EXPLAIN", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < 4; ++i) {
    common::ObField ddl_field;
    ddl_field.tname_ = tname;
    ddl_field.org_tname_ = tname;
    ddl_field.cname_ = ObString::make_string(ddl_explain_cols[i].name);
    ddl_field.org_cname_ = ddl_field.cname_;
    ddl_field.type_.set_type(ddl_explain_cols[i].type);
    if (ObVarcharType == ddl_explain_cols[i].type) {
      ddl_field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI;
      ddl_field.type_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      ddl_field.type_.set_collation_level(CS_LEVEL_IMPLICIT);
      ddl_field.type_.set_varchar(type_name);
    }
    if (OB_FAIL(result_set.add_field_column(ddl_field))) {
      LOG_WARN("fail to add DDL EXPLAIN field column", K(ret), K(i));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase