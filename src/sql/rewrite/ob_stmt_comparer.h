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

#ifndef OB_STMT_COMPARER_H
#define OB_STMT_COMPARER_H

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase {
namespace sql {

// NOTE remember to de-construct the struct
enum class QueryRelation { LEFT_SUBSET, RIGHT_SUBSET, EQUAL, UNCOMPARABLE };

struct ObStmtMapInfo {

  common::ObSEArray<int64_t, 4> table_map_;
  common::ObSEArray<int64_t, 4> from_map_;
  common::ObSEArray<int64_t, 4> cond_map_;
  common::ObSEArray<int64_t, 4> group_map_;
  common::ObSEArray<int64_t, 4> having_map_;
  common::ObSEArray<int64_t, 4> select_item_map_;
  common::ObSEArray<ObPCParamEqualInfo, 4> equal_param_map_;

  void reset()
  {
    table_map_.reset();
    from_map_.reset();
    cond_map_.reset();
    group_map_.reset();
    having_map_.reset();
    select_item_map_.reset();
    equal_param_map_.reset();
  }
  TO_STRING_KV(K_(table_map), K_(from_map), K_(cond_map), K_(group_map), K_(having_map), K_(select_item_map),
      K_(equal_param_map));
};

// NOTE () remember to de-construct the struct
struct ObStmtCompareContext : ObExprEqualCheckContext {
  ObStmtCompareContext() : ObExprEqualCheckContext(), context_(NULL), table_id_pairs_(), equal_param_info_()
  {
    override_column_compare_ = true;
    override_const_compare_ = true;
    override_query_compare_ = true;
  }
  virtual ~ObStmtCompareContext()
  {}

  // for common expression extraction
  int init(const ObQueryCtx* context);

  // for win_magic rewrite
  int init(const ObDMLStmt* inner, const ObDMLStmt* outer, const common::ObIArray<int64_t>& table_map);

  bool compare_column(const ObColumnRefRawExpr& inner, const ObColumnRefRawExpr& outer) override;

  bool compare_const(const ObConstRawExpr& inner, const ObConstRawExpr& outer) override;

  bool compare_query(const ObQueryRefRawExpr& first, const ObQueryRefRawExpr& second) override;

  int get_calc_expr(const int64_t param_idx, const ObRawExpr*& expr);

  int is_pre_calc_item(const ObConstRawExpr& const_expr, bool& is_calc);

  const ObQueryCtx* context_;
  // first is the table id from the inner stmt
  // second is the table id from the outer stmt
  common::ObSEArray<std::pair<uint64_t, uint64_t>, 4> table_id_pairs_;
  common::ObSEArray<ObPCParamEqualInfo, 4> equal_param_info_;
};

class ObStmtComparer {
public:
  static int compute_stmt_overlap(ObDMLStmt* first, ObDMLStmt* second, ObStmtMapInfo& map_info);

  static int check_stmt_containment(
      ObDMLStmt* first, ObDMLStmt* second, ObStmtMapInfo& map_info, QueryRelation& relation);

  static int compute_conditions_map(ObDMLStmt* first, ObDMLStmt* second, const ObIArray<ObRawExpr*>& first_exprs,
      const ObIArray<ObRawExpr*>& second_exprs, ObStmtMapInfo& map_info, ObIArray<int64_t>& condition_map,
      int64_t& match_count);

  static int compute_from_items_map(ObDMLStmt* first, ObDMLStmt* second, ObStmtMapInfo& map_info, int64_t& match_count);

  static int is_same_from(
      ObDMLStmt* first, const FromItem& first_from, ObDMLStmt* second, const FromItem& second_from, bool& is_same);

  static int is_same_condition(ObRawExpr* left, ObRawExpr* right, ObStmtCompareContext& context, bool& is_same);

  /**
   * @brief compare_basic_table_item
   * consider partition hint
   */
  static int compare_basic_table_item(ObDMLStmt* first, const TableItem* first_table, ObDMLStmt* second,
      const TableItem* second_table, QueryRelation& relation);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OB_STMT_COMPARER_H
