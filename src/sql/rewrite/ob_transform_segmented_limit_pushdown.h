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

#ifndef _OB_TRANSFORM_JOIN_ORDERBY_LIMIT_PUSHDOWN_H
#define _OB_TRANSFORM_JOIN_ORDERBY_LIMIT_PUSHDOWN_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase {

namespace common {
class ObIAllocator;
}

namespace sql {

class ObRawExpr;

class ObTransformSegmentedLimitPushdown : public ObTransformRule {

public:
  struct TransformContext {
    ObSEArray<int64_t, 8> need_rewrite_in_conds_offsets;
    ObSEArray<int64_t, 8> pushdown_conds_offsets;
  };

  explicit ObTransformSegmentedLimitPushdown(ObTransformerCtx *ctx) :
          ObTransformRule(ctx, TransMethod::PRE_ORDER, T_SEGMENTED_LIMIT_PUSHDOWN) {}

  virtual ~ObTransformSegmentedLimitPushdown() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *&stmt, bool &trans_happened) override;
private:
  int check_stmt_validity(ObDMLStmt *stmt, bool &is_valid, TransformContext &transform_ctx);

  int do_transform(ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *&stmt, const TransformContext &transform_ctx, bool &trans_happened);

  int do_one_transform(ObSelectStmt *&select_stmt, ObRawExpr *in_condition, ObIArray<ObRawExpr *> &pushdown_conds, bool last_transform);

  int check_condition(ObSelectStmt *select_stmt, bool &is_valid);

  int check_limit(ObSelectStmt *select_stmt, bool &is_valid);

  int check_order_by(ObSelectStmt *select_stmt, bool &is_valid);

  int is_candidate_in_condition(ObRawExpr *condition_expr, bool &is_candidate_in_condition);

  int extract_conditions_with_offsets(ObSelectStmt *select_stmt, ObIArray<ObRawExpr *> &conds, const ObIArray<int64_t> &offsets);

  int extract_conditions(ObSelectStmt *select_stmt, TransformContext &transform_ctx);

  int get_exec_param_expr(ObSelectStmt *select_stmt, uint64_t ref_table_id, ObExecParamRawExpr *&exec_param_expr);

  int construct_join_condition(ObSelectStmt *select_stmt, uint64_t values_table_id, ObRawExpr *in_condition,
                               ObExecParamRawExpr *&exec_param_expr, ObIArray<ObRawExpr *> &pushdown_conditions);

  int pushdown_limit(ObSelectStmt *upper_stmt, ObSelectStmt *generated_view);

  int create_lateral_table(ObSelectStmt *select_stmt, TableItem *&lateral_table);

  int inlist_to_values_table(ObSelectStmt *select_stmt, ObRawExpr *in_condition, TableItem *&values_table);

  int resolve_values_table_from_inlist(ObRawExpr *in_condition, ObValuesTableDef *&values_table_def);

  int resolve_access_param_values_table(ObOpRawExpr *in_list_expr, ObValuesTableDef *&values_table_def);

  int resolve_access_obj_values_table(ObOpRawExpr *in_list_expr, ObValuesTableDef *&values_table_def);

  int try_merge_column_type(int col_idx, ObValuesTableDef *values_table_def, const ObExprResType &res_type);

  int check_question_mark(ObOpRawExpr *in_list_expr, bool &is_question_mark);

  int find_order_elimination_index(ObSelectStmt *select_stmt, uint64_t table_id, const ObIArray<uint64_t> &order_column_ids,
                                  const ObIArray<uint64_t> &equal_column_ids, ObIArray<uint64_t> &index_column_ids, bool &found_index);

  int get_need_rewrite_in_conditions(ObSelectStmt *select_stmt, const ObIArray<int64_t> &candidate_in_conds_offsets, TransformContext &transform_ctx);

  int estimate_values_table_stats(ObValuesTableDef &table_def, const ParamStore *param_store, ObSQLSessionInfo *session_info);

  int remove_duplicated_elements(ObIArray<uint64_t> &array);
};

} //namespace sql
} //namespace oceanbase
#endif /* _OB_TRANSFORM_JOIN_ORDERBY_LIMIT_PUSHDOWN_H */
