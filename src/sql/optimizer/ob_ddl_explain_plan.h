/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_DDL_EXPLAIN_PLAN_H
#define _OB_DDL_EXPLAIN_PLAN_H

#include "share/ob_alter_table_ddl_plan.h"

namespace oceanbase {
namespace sql {
class ObLogPlan;
class ObExplainStmt;
class ObExecContext;
class ObLogValues;
class ObResultSet;

class ObDDLExplainHelper
{
public:
  // Calls ObRootService::alter_table directly (is_explain_=true, dry-run), then fills values_op with one result row of
  // 4 columns: CATEGORY | BLOCK_DML | ALGORITHM | MESSAGE.
  static int generate_plan(const ObExplainStmt *explain_stmt,
                            ObExecContext *exec_ctx,
                            ObLogValues *values_op);

  // Allocates output exprs for the 4 DDL EXPLAIN result columns on values_op.
  // All columns are VARCHAR.
  static int build_output_exprs(ObLogPlan &log_plan,
                                ObLogValues *values_op);

  static int fill_result_set(ObResultSet &result_set);

  ObDDLExplainHelper() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObDDLExplainHelper);
};

} // namespace sql
} // namespace oceanbase

#endif // _OB_DDL_EXPLAIN_PLAN_H
