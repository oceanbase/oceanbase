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

#ifndef OB_TRANSFORM_WIN_MAGIC_H
#define OB_TRANSFORM_WIN_MAGIC_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
namespace sql {
struct ObStmtMapInfo;
class ObTransformWinMagic : public ObTransformRule {
public:
  ObTransformWinMagic(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}

  virtual ~ObTransformWinMagic()
  {}

  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

protected:
  virtual int adjust_transform_types(uint64_t& transform_types) override;

private:
  int check_subquery_validity(ObDMLStmt* stmt, ObQueryRefRawExpr* query_ref, ObStmtMapInfo& map_info, bool& is_valid);

  int check_aggr_expr_validity(ObSelectStmt& subquery, bool& is_valid);

  int check_lossess_join(
      ObSelectStmt& subquery, ObIArray<FromItem>& from_items, ObIArray<ObRawExpr*>& conditions, bool& is_valid);

  int do_transform(ObDMLStmt* stmt, int64_t query_ref_expr_id, ObStmtMapInfo& map_info, ObDMLStmt*& trans_stmt);

  int compute_push_down_items(ObDMLStmt& stmt, ObStmtMapInfo& map_info, ObSqlBitSet<>& push_down_table_ids,
      ObSqlBitSet<>& push_down_from_ids, ObSqlBitSet<>& push_down_filter_ids);

  int transform_child_stmt(ObDMLStmt& stmt, ObSelectStmt& subquery, ObStmtMapInfo& map_info,
      ObSqlBitSet<>& push_down_table_ids, ObSqlBitSet<>& push_down_from_ids, ObSqlBitSet<>& push_down_filter_ids);

  int transform_upper_stmt(ObDMLStmt& stmt, ObQueryRefRawExpr* query_ref, ObSqlBitSet<>& push_down_table_ids,
      ObSqlBitSet<>& push_down_from_ids, ObSqlBitSet<>& push_down_cond_ids);

  int transform_aggr_to_winfunc(
      ObSelectStmt& subquery, ObDMLStmt& stmt, ObStmtMapInfo& map_info, ObSqlBitSet<>& bit_set);

  int create_window_function(
      ObAggFunRawExpr* agg_expr, ObIArray<ObRawExpr*>& partition_exprs, ObWinFunRawExpr*& win_expr);

  int wrap_case_when_if_necessary(ObDMLStmt* stmt, ObAggFunRawExpr& aggr_expr, ObIArray<ObRawExpr*>& nullable_exprs);

  int is_simple_from_item(ObDMLStmt& stmt, uint64_t table_id, int64_t& from_index);

  int use_given_tables(const ObRawExpr* expr, const ObIArray<uint64_t>& table_ids, bool& is_valid);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OB_TRANSFORM_WIN_MAGIC_H
