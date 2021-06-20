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

#ifndef OB_TRANSFORM_OR_EXPANSION_H_
#define OB_TRANSFORM_OR_EXPANSION_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
namespace oceanbase {
namespace sql {

class ObTransformOrExpansion : public ObTransformRule {
  static const int64_t MAX_STMT_NUM_FOR_OR_EXPANSION;
  static const int64_t MAX_TIMES_FOR_OR_EXPANSION;
  typedef ObBitSet<8> ColumnBitSet;

public:
  ObTransformOrExpansion(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::PRE_ORDER), try_times_(0)
  {}
  virtual ~ObTransformOrExpansion()
  {}
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

protected:
  virtual int adjust_transform_types(uint64_t& transform_types) override;

private:
  int check_stmt_validity(ObDMLStmt& stmt, bool& is_valid);

  int check_select_expr_validity(ObSelectStmt& stmt, bool& is_valid);

  int check_upd_del_stmt_validity(ObDelUpdStmt* stmt, bool& is_valid);

  int has_odd_function(const ObDMLStmt& stmt, bool& has);

  int get_trans_view(ObDMLStmt* stmt, ObDMLStmt*& upper_stmt, ObSelectStmt*& child_stmt);

  int merge_stmt(ObDMLStmt*& upper_stmt, ObSelectStmt* input_stmt, ObSelectStmt* union_stmt);

  int remove_stmt_select_item(ObSelectStmt* select_stmt, int64_t select_item_count);

  int check_condition_on_same_columns(const ObDMLStmt& stmt, const ObRawExpr& expr, bool& using_same_cols);

  int is_contain_join_cond(const ObDMLStmt& stmt, const ObRawExpr* expr, bool& is_contain);

  int is_match_index(const ObDMLStmt& stmt, const ObRawExpr* expr, bool& is_match);

  int is_simple_cond(const ObDMLStmt& stmt, const ObRawExpr* expr, bool& is_simple);

  int is_condition_valid(
      const ObDMLStmt& stmt, const ObRawExpr& expr, int64_t& or_expr_count, bool& is_valid, const bool is_topk);

  int prepare_or_condition(ObSelectStmt* stmt, common::ObIArray<ObRawExpr*>& conds, common::ObIArray<int64_t>& expr_pos,
      common::ObIArray<int64_t>& or_expr_counts, const bool is_topk);
  int transform_or_expansion(common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt* stmt,
      const int64_t transformed_expr_pos, const int64_t or_expr_count, bool can_union_distinct, bool spj_is_unique,
      ObSelectStmt*& transform_stmt);
  int adjust_or_expansion_stmt(const int64_t transformed_expr_pos, const int64_t param_pos, bool can_union_distinct,
      ObSelectStmt*& or_expansion_stmt);
  int create_expr_for_in_expr(const ObRawExpr& transformed_expr, const int64_t param_pos, bool can_union_distinct,
      common::ObIArray<ObRawExpr*>& generated_exprs, common::ObIArray<ObQueryRefRawExpr*>& subqueries);
  int create_expr_for_or_expr(ObRawExpr& transformed_expr, const int64_t param_pos, bool can_union_distinct,
      common::ObIArray<ObRawExpr*>& generated_exprs, common::ObIArray<ObQueryRefRawExpr*>& subqueries);
  int preprocess_or_condition(ObSelectStmt* select_stmt, ObRawExpr* expr, bool& can_union_distinct, bool& spj_is_unique,
      bool& skip_this_cond, const bool is_topk);

  static int extract_columns(const ObRawExpr* expr, const int64_t stmt_level, int64_t& table_id, bool& from_same_table,
      ColumnBitSet& col_bit_set);

  template <typename T>
  static int extract_exprs(const ObIArray<T>& list, ObIArray<ObRawExpr*>& exprs);

  DISALLOW_COPY_AND_ASSIGN(ObTransformOrExpansion);

private:
  int64_t try_times_;
};

template <typename T>
int ObTransformOrExpansion::extract_exprs(const ObIArray<T>& list, ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    if (OB_FAIL(exprs.push_back(list.at(i).expr_))) {
      SQL_LOG(WARN, "failed to push back expr", K(ret));
    }
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OB_TRANSFORM_OR_EXPANSION_H_ */
