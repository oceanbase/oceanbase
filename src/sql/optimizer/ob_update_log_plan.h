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

#ifndef _OB_UPDATE_LOG_PLAN_H
#define _OB_UPDATE_LOG_PLAN_H 1
#include "sql/optimizer/ob_log_plan.h"
#include "sql/resolver/dml/ob_update_stmt.h"

namespace oceanbase {
namespace sql {
class ObLogUpdate;
class ObUpdateLogPlan : public ObLogPlan {
public:
  ObUpdateLogPlan(ObOptimizerContext& ctx, const ObUpdateStmt* update_stmt) : ObLogPlan(ctx, update_stmt)
  {}
  virtual ~ObUpdateLogPlan(){};

  int generate_raw_plan();

  /**
   * GENERATE logical PLAN
   */
  int generate_plan();

private:
  int allocate_update_as_top(ObLogicalOperator*& top);

  int allocate_pdml_update_as_top(ObLogicalOperator*& top);
  int adjust_assignment_exprs(ObTablesAssignments& assigments, ObIArray<ObQueryRefRawExpr*>& subqueries);

  int replace_alias_ref_expr(ObRawExpr*& expr, ObIArray<ObQueryRefRawExpr*>& subqueries);
  int allocate_pdml_update_op(ObLogicalOperator*& top, bool is_index_maintenace, bool is_last_dml_op,
      const common::ObIArray<TableColumns>* table_column, const ObTablesAssignments* one_table_assignment);
  int allocate_pdml_delete_op(ObLogicalOperator*& top, bool is_index_maintenace,
      const common::ObIArray<TableColumns>* table_column, const ObTablesAssignments* one_table_assignment);
  int allocate_pdml_insert_op(ObLogicalOperator*& top, bool is_index_maintenace, bool is_last_dml_op,
      const common::ObIArray<TableColumns>* table_column, const ObTablesAssignments* one_table_assignment);

  DISALLOW_COPY_AND_ASSIGN(ObUpdateLogPlan);

private:
};
}  // namespace sql
}  // namespace oceanbase
#endif
