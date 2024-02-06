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

#ifndef OB_TRANSFORM_GROUPBY_PULLUP_H
#define OB_TRANSFORM_GROUPBY_PULLUP_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/optimizer/ob_log_join.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief The ObTransformGroupByReplacement class
 * References:
 *  [1] Including Group-By in Query Optimization
 *  [2] Eager Aggregation and Lazy Aggregation
 */
class ObTransformGroupByPullup : public ObTransformRule
{
public:
  ObTransformGroupByPullup(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_MERGE_HINT)
  {}

  virtual ~ObTransformGroupByPullup() {}
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
protected:
  virtual int adjust_transform_types(uint64_t &transform_types) override;
  virtual int is_expected_plan(ObLogPlan *plan, void *check_ctx, bool is_trans_plan, bool &is_valid) override;
private:
  struct PullupHelper {
    PullupHelper():
      parent_table_(NULL),
      table_id_(OB_INVALID_ID),
      not_null_column_table_id_(OB_INVALID_ID),
      not_null_column_id_(OB_INVALID_ID),
      need_check_null_propagate_(false),
      need_check_having_(false),
      need_merge_(false)
      {}
    virtual ~PullupHelper(){}

    JoinedTable *parent_table_;
    uint64_t table_id_;
    uint64_t not_null_column_table_id_;
    uint64_t not_null_column_id_;
    //如果是在outer join的右侧或者full join的两侧，需要找到一个not_null_column备用；
    bool need_check_null_propagate_;
    //如果是outer join的右表或者是full join的child table
    //不能有having condition
    bool need_check_having_;
    //是否是否有merge hint，强制group by pull up
    //不考虑代价
    bool need_merge_;
    TO_STRING_KV(K_(parent_table),
                 K_(table_id),
                 K_(not_null_column_id),
                 K_(not_null_column_table_id),
                 K_(need_check_null_propagate),
                 K_(need_check_having),
                 K_(need_merge));
  };

  struct ObCostBasedPullupCtx {
    ObCostBasedPullupCtx() {};
    uint64_t view_talbe_id_;
  };

  int check_groupby_validity(const ObSelectStmt &stmt, bool &is_valid);

  int check_collation_validity(const ObDMLStmt &stmt, bool &is_valid);

  /////////////////////    transform group by pull up    //////////////////////////////

  int check_groupby_pullup_validity(ObDMLStmt *stmt,
                                    ObIArray<PullupHelper> &valid_views);

  int check_groupby_pullup_validity(ObDMLStmt *stmt,
                                    TableItem *table,
                                    PullupHelper &helper,
                                    bool contain_inner_table,
                                    ObSqlBitSet<> &ignore_tables,
                                    ObIArray<PullupHelper> &valid_views,
                                    bool &is_valid);

  int check_on_conditions(ObDMLStmt &stmt,
                          ObSqlBitSet<> &ignore_tables);

  int is_valid_group_stmt(ObSelectStmt *sub_stmt,
                          bool &is_valid_group);

  int check_null_propagate(ObDMLStmt *parent_stmt,
                          ObSelectStmt* child_stmt,
                          PullupHelper &helper,
                          bool &is_valid);

  int find_not_null_column(ObDMLStmt &parent_stmt,
                          ObSelectStmt &child_stmt,
                          PullupHelper &helper,
                          ObIArray<ObRawExpr *> &column_exprs,
                          ObRawExpr *&not_null_column);

  int find_not_null_column_with_condition(ObDMLStmt &parent_stmt,
                                          ObSelectStmt &child_stmt,
                                          PullupHelper &helper,
                                          ObIArray<ObRawExpr *> &column_exprs,
                                          ObRawExpr *&not_null_column);
                                          
  int find_null_propagate_column(ObRawExpr *condition,
                                ObIArray<ObRawExpr*> &columns,
                                ObRawExpr *&null_propagate_column,
                                bool &is_valid);

  int do_groupby_pull_up(ObSelectStmt *stmt, PullupHelper &helper);

  int get_trans_view(ObDMLStmt *stmt, ObSelectStmt *&view_stmt);

  int wrap_case_when_if_necessary(ObSelectStmt &child_stmt,
                                  PullupHelper &helper,
                                  ObIArray<ObRawExpr *> &exprs);
  /**
   * @brief wrap_case_when
   * 如果当前视图是left outer join的右表或者right outer join的左表
   * 需要对null rejuect的new column expr包裹一层case when
   * 需要寻找视图的非空列，如果null_reject_columns不为空，
   * 直接拿第一个使用，否则需要在stmt中查找非空列，
   * 也可以是试图内基表的pk，但不能是outer join的补null侧
   */
  int wrap_case_when(ObSelectStmt &child_stmt,
                    ObRawExpr *not_null_column,
                    ObRawExpr *&expr);

  int has_group_by_op(ObLogicalOperator *op,
                      bool &bret);

  int check_group_by_subset(ObRawExpr *expr, const ObIArray<ObRawExpr *> &group_exprs, bool &bret);

  int check_hint_valid(const ObDMLStmt &stmt,
                        const ObSelectStmt &ref_query,
                        bool &is_valid);

  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans) override;

  int check_original_plan_validity(ObLogicalOperator* root,
                                   uint64_t view_table_id,
                                   bool &is_valid);

  int find_operator(ObLogicalOperator* root,
                    ObIArray<ObLogicalOperator*> &parents,
                    uint64_t view_table_id,
                    ObLogicalOperator *&subplan_op);

  int find_base_operator(ObLogicalOperator *&root);

  int extract_columns_in_join_conditions(ObIArray<ObLogicalOperator*> &parent_ops,
                                         uint64_t table_id,
                                         ObIArray<ObRawExpr*> &column_exprs);

  int get_group_by_subset(ObRawExpr *expr,
                          const ObIArray<ObRawExpr *> &group_exprs,
                          ObIArray<ObRawExpr *> &subset_group_exprs);

  int get_group_by_subset(ObIArray<ObRawExpr *> &exprs,
                          const ObIArray<ObRawExpr *> &group_exprs,
                          ObIArray<ObRawExpr *> &subset_group_exprs);

  int calc_group_exprs_ndv(const ObIArray<ObRawExpr*> &group_exprs,
                            ObLogicalOperator *subplan_root,
                            double &group_ndv,
                            double &card);

  int check_all_table_has_statistics(ObLogicalOperator *op, bool &has_stats);

private:
  // help functions
  int64_t get_count_sum_num(const ObIArray<ObRawExpr *> &exprs)
  {
    int64_t num = 0;
    for (int64_t i = 0; i < exprs.count(); ++i) {
      if (OB_ISNULL(exprs.at(i))) {
        // do nothing
      } else if (exprs.at(i)->get_expr_type() == T_FUN_SUM ||
                 exprs.at(i)->get_expr_type() == T_FUN_COUNT) {
        ++num;
      }
    }
    return num;
  }


};

}
}

#endif // OB_TRANSFORM_GROUPBY_REPLACEMENT_H
