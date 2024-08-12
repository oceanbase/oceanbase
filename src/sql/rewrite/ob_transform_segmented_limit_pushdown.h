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
  explicit ObTransformSegmentedLimitPushdown(ObTransformerCtx *ctx) :
          ObTransformRule(ctx, TransMethod::PRE_ORDER, T_SEGMENTED_LIMIT_PUSHDOWN) {}

  virtual ~ObTransformSegmentedLimitPushdown() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *&stmt, bool &trans_happened) override;
private:
  int check_stmt_validity(ObDMLStmt *stmt, bool &is_valid);

  int do_transform(ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *&stmt, bool &trans_happened);

  int do_one_transform(ObSelectStmt *&select_stmt, ObRawExpr *in_condition, ObIArray<ObRawExpr *> *pushdown_conds, bool last_transform);

  int check_condition(ObSelectStmt *select_stmt, bool &is_valid);

  int check_limit(ObSelectStmt *select_stmt, bool &is_valid);

  int check_order_by(ObSelectStmt *select_stmt, bool &is_valid);

  int is_candidate_in_condition(ObRawExpr *condition_expr, bool &is_candidate_in_condition);

  int extract_conditions(ObSelectStmt *select_stmt, ObIArray<ObRawExpr *> *need_rewrite_in_conds, ObIArray<ObRawExpr *> *pushdown_conds);

  int get_exec_param_expr(ObSelectStmt *select_stmt, uint64_t ref_table_id, ObExecParamRawExpr *&exec_param_expr);

  int construct_join_condition(ObSelectStmt *select_stmt, uint64_t values_table_id, ObRawExpr *in_condition,
                               ObExecParamRawExpr *&exec_param_expr, ObIArray<ObRawExpr *> *pushdown_conditions);

  int pushdown_limit(ObSelectStmt *upper_stmt, ObSelectStmt *generated_view);

  int create_lateral_table(ObSelectStmt *select_stmt, TableItem *&lateral_table);

  int inlist_to_values_table(ObSelectStmt *select_stmt, ObRawExpr *in_condition, TableItem *&values_table);

  int resolve_values_table_from_inlist(ObRawExpr *in_condition, ObValuesTableDef *&values_table_def);

  int gen_values_table_column_items(ObSelectStmt *select_stmt, TableItem *values_table);

  int resolve_access_param_values_table(ObOpRawExpr *in_list_expr, ObValuesTableDef *&values_table_def);

  int resolve_access_obj_values_table(ObOpRawExpr *in_list_expr, ObValuesTableDef *&values_table_def);

  int try_merge_column_type(int col_idx, ObValuesTableDef *values_table_def, const ObExprResType &res_type);

  int check_question_mark(ObOpRawExpr *in_list_expr, bool &is_question_mark);

  int find_order_elimination_index(ObSelectStmt *select_stmt, uint64_t table_id, ObIArray<uint64_t> *order_column_ids,
                                  ObIArray<uint64_t> *equal_column_ids, ObIArray<uint64_t> *index_column_ids, bool &found_index);

  int get_need_rewrite_in_conditions(ObSelectStmt *select_stmt, ObIArray<ObRawExpr *> *candidate_in_conditions,
                                   ObIArray<ObRawExpr *> *need_rewrite_in_conditions, ObIArray<ObRawExpr *> *pushdown_conds);

  int get_equal_conditions(ObSelectStmt *select_stmt, ObIArray<ObRawExpr *> *equal_condition_exprs);

  int add_obj_to_llc_bitmap(const ObObj &obj, char *llc_bitmap, double &num_null);

  int estimate_values_table_stats(ObValuesTableDef &table_def, const ParamStore *param_store, ObSQLSessionInfo *session_info);
};

} //namespace sql
} //namespace oceanbase
#endif /* _OB_TRANSFORM_JOIN_ORDERBY_LIMIT_PUSHDOWN_H */
