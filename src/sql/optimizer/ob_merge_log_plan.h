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

#ifndef _OB_MERGE_LOG_PLAN_H
#define _OB_MERGE_LOG_PLAN_H
#include "sql/optimizer/ob_insert_log_plan.h"
#include "sql/resolver/dml/ob_merge_stmt.h"

namespace oceanbase {
namespace sql {
class ObMergeLogPlan : public ObInsertLogPlan {
public:
  ObMergeLogPlan(ObOptimizerContext& ctx, const ObDMLStmt* merge_stmt) : ObInsertLogPlan(ctx, merge_stmt)
  {}
  virtual ~ObMergeLogPlan()
  {}
  int generate_raw_plan();
  virtual int generate_plan();

private:
  int allocate_merge_operator_as_top(ObLogicalOperator*& top);
  int allocate_merge_subquery();
  int get_update_insert_condition_subquery(ObRawExpr* matched_expr, ObRawExpr* null_expr, bool& update_has_subquery,
      bool& insert_has_subquery, ObIArray<ObRawExpr*>& new_subquery_exprs);
  int get_update_insert_target_subquery(ObRawExpr* matched_expr, ObRawExpr* null_expr, bool update_has_subquery,
      bool insert_has_subquery, ObIArray<ObRawExpr*>& new_subquery_exprs);
  int get_delete_condition_subquery(ObRawExpr* matched_expr, ObRawExpr* null_expr, bool update_has_subquery,
      ObIArray<ObRawExpr*>& new_subquery_exprs);
  int generate_merge_conditions_subquery(
      ObRawExpr* matched_expr, ObRawExpr* null_expr, bool is_matched, ObIArray<ObRawExpr*>& condition_exprs);
  DISALLOW_COPY_AND_ASSIGN(ObMergeLogPlan);
};
}  // namespace sql
}  // namespace oceanbase
#endif
