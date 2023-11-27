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

#ifndef OB_TRANSFORM_SUBQUERY_COALESCE_H
#define OB_TRANSFORM_SUBQUERY_COALESCE_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/rewrite/ob_stmt_comparer.h"

namespace oceanbase
{
namespace sql
{
class ObUpdateStmt;
class ObTransformSubqueryCoalesce : public ObTransformRule
{
public:
  ObTransformSubqueryCoalesce(ObTransformerCtx *ctx)
   : ObTransformRule(ctx, TransMethod::POST_ORDER, T_COALESCE_SQ),
      allocator_("Coalesce")
  {}
  virtual ~ObTransformSubqueryCoalesce();
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;

protected:
  int adjust_transform_types(uint64_t &transform_types);
private:
  enum TransformFlag {
    DEFAULT,
    EXISTS_NOT_EXISTS,
    ANY_ALL
  };
  struct TransformParam {
    ObRawExpr *exists_expr_;
    ObRawExpr *not_exists_expr_;
    ObRawExpr *any_expr_;
    ObRawExpr *all_expr_;
    ObStmtMapInfo map_info_;
    TransformFlag trans_flag_ = DEFAULT; //default value;

    TO_STRING_KV(K(exists_expr_), 
                 K(not_exists_expr_), 
                 K(any_expr_), 
                 K(all_expr_),
                 K(map_info_),
                 K(trans_flag_));
  };
  typedef ObSEArray<ObSelectStmt*, 4> CoalesceStmts;

  int transform_same_exprs(ObDMLStmt *stmt,
                           ObIArray<ObRawExpr *> &conds,
                           bool &is_happened);

  int transform_diff_exprs(ObDMLStmt *stmt,
                           ObDMLStmt *&trans_stmt,
                           ObIArray<ObPCParamEqualInfo> &rule_based_equal_infos,
                           ObIArray<ObPCParamEqualInfo> &cost_based_equal_infos,
                           bool &rule_based_trans_happened);

  int coalesce_same_exists_exprs(ObDMLStmt *stmt,
                                 const ObItemType type,
                                 common::ObIArray<ObRawExpr *> &filters,
                                 bool &is_happened);

  int coalesce_same_any_all_exprs(ObDMLStmt *stmt,
                                  const ObItemType type,
                                  common::ObIArray<ObRawExpr *> &filters,
                                  bool &is_happened);


  int coalesce_diff_exists_exprs(ObDMLStmt *stmt,
                                 common::ObIArray<ObRawExpr *> &cond_exprs,
                                 common::ObIArray<TransformParam> &trans_params);
  
  int coalesce_diff_any_all_exprs(ObDMLStmt *stmt,
                                  common::ObIArray<ObRawExpr *> &cond_exprs,
                                  common::ObIArray<TransformParam> &trans_params);

  int merge_exists_subqueries(TransformParam &trans_param, ObRawExpr *&new_exist_expr);

  int merge_any_all_subqueries(ObQueryRefRawExpr *any_ref_expr,
                               ObQueryRefRawExpr *all_ref_expr,
                               TransformParam &trans_param,
                               ObRawExpr *&new_any_all_query);

  int classify_conditions(ObIArray<ObRawExpr *> &conditions,
                          ObIArray<ObRawExpr *> &validity_exprs);

  int get_remove_exprs(ObIArray<ObRawExpr *> &ori_exprs,
                       ObIArray<ObRawExpr *> &remain_exprs,
                       ObIArray<ObRawExpr *> &remove_exprs);

  int check_conditions_validity(ObDMLStmt *stmt,
                                common::ObIArray<ObRawExpr *> &conds,
                                common::ObIArray<TransformParam> &trans_params,
                                bool &has_false_conds,
                                bool &hint_force_trans);

  int check_query_ref_validity(ObRawExpr *expr, bool &is_valid);

  int compare_any_all_subqueries(ObDMLStmt *stmt,
                                 TransformParam &param, 
                                 ObIArray<TransformParam> &trans_params,
                                 bool &has_false_conds,
                                 bool &is_used,
                                 bool can_coalesce,
                                 bool &hint_force_trans);
  int get_same_classify_exprs(ObIArray<ObRawExpr *> &validity_exprs,
                              ObIArray<ObRawExpr *> &same_classify_exprs,
                              ObItemType ctype,
                              ObExprInfoFlag flag);

  int merge_exec_params(ObQueryRefRawExpr *source_query_ref,
                        ObQueryRefRawExpr *target_query_ref,
                        ObIArray<ObRawExpr *> &old_params,
                        ObIArray<ObRawExpr *> &new_params);

  int transform_or_exprs(ObDMLStmt *stmt,
                        ObIArray<ObRawExpr *> &conds,
                        bool &is_happened);
  
  int transform_or_expr(ObDMLStmt *stmt,
                        ObRawExpr * &expr,
                        bool &is_happened);

  int check_expr_can_be_coalesce(ObDMLStmt *stmt,
                                ObRawExpr *l_expr,
                                ObRawExpr *r_expr,
                                ObStmtCompareContext &compare_ctx,
                                bool &can_be);

  int check_subquery_validity(ObQueryRefRawExpr *query_ref,
                              ObSelectStmt *subquery,
                              bool &valid);
  
  int coalesce_update_assignment(ObDMLStmt *stmt, bool &trans_happened);

  int get_subquery_assign_exprs(ObIArray<ObRawExpr*> &assign_exprs, 
                                ObIArray<ObSelectStmt*> &subqueries);

  int get_coalesce_infos(ObDMLStmt &parent_stmt,
                         ObIArray<ObSelectStmt*> &subqueries, 
                         ObIArray<StmtCompareHelper*> &coalesce_infos);

  int remove_invalid_coalesce_info(ObIArray<StmtCompareHelper*> &coalesce_infos);

  bool check_subquery_can_coalesce(const ObStmtMapInfo &map_info);

  int coalesce_subquery(StmtCompareHelper &helper, 
                        ObQueryCtx *query_ctx,
                        ObIArray<ObRawExpr*> &select_exprs, 
                        ObIArray<int64_t> &index_map, 
                        ObSelectStmt* &coalesce_query);

  int inner_coalesce_subquery(ObSelectStmt *subquery, 
                              ObQueryCtx *query_ctx,
                              ObStmtMapInfo &map_info,
                              ObIArray<ObRawExpr*> &select_exprs, 
                              ObIArray<int64_t> &index_map, 
                              ObSelectStmt *coalesce_query,
                              const bool is_first_subquery);

  int get_map_table_id(ObSelectStmt *subquery,
                      ObSelectStmt *coalesce_subquery,
                      ObStmtMapInfo& map_info,
                      const uint64_t &subquery_table_id,
                      uint64_t &table_id);

  int adjust_assign_exprs(ObUpdateStmt *upd_stmt,
                          StmtCompareHelper *helper, 
                          ObIArray<ObRawExpr*> &select_exprs, 
                          ObIArray<int64_t> &index_map, 
                          ObSelectStmt *coalesce_query);                                                                                                                                                                                                                                    

  int adjust_alias_assign_exprs(ObRawExpr* &assign_expr,
                                StmtCompareHelper *helper, 
                                ObIArray<ObRawExpr*> &select_exprs, 
                                ObIArray<int64_t> &index_map, 
                                ObQueryRefRawExpr *coalesce_query_expr,
                                ObSelectStmt *coalesce_query,
                                ObIArray<ObRawExpr*> &old_exprs,
                                ObIArray<ObRawExpr*> &new_exprs);

  int adjust_query_assign_exprs(ObRawExpr* &assign_expr,
                                StmtCompareHelper *helper, 
                                ObIArray<ObRawExpr*> &select_exprs, 
                                ObIArray<int64_t> &index_map, 
                                ObQueryRefRawExpr *coalesce_query_expr,
                                ObSelectStmt *coalesce_query,
                                ObIArray<ObRawExpr*> &old_exprs,
                                ObIArray<ObRawExpr*> &new_exprs);

  int inner_adjust_assign_exprs(ObSelectStmt *stmt,
                                const int64_t select_idx,
                                StmtCompareHelper *helper, 
                                ObIArray<ObRawExpr*> &select_exprs, 
                                ObIArray<int64_t> &index_map, 
                                ObQueryRefRawExpr *coalesce_query_expr,
                                ObSelectStmt *coalesce_query,
                                ObRawExpr* &new_expr);

  int get_exec_params(ObDMLStmt *stmt,
                      ObIArray<ObExecParamRawExpr *> &all_params);

  int check_hint_valid(const ObDMLStmt &stmt, 
                       ObSelectStmt &subquery1, 
                       ObSelectStmt &subquery2, 
                       bool &force_trans,
                       bool &force_no_trans) const;

  int check_hint_valid(const ObDMLStmt &stmt, 
                       const ObIArray<ObSelectStmt*> &queries,
                       bool &force_trans,
                       bool &force_no_trans) const;

  int add_coalesce_stmt(ObSelectStmt *subquery1,  ObSelectStmt *subquery2);

  int get_hint_force_set(const ObDMLStmt &stmt, 
                         const ObSelectStmt &subquery,
                         QbNameList &qb_names,
                         bool &hint_force_no_trans);

  int add_coalesce_stmts(const ObIArray<ObSelectStmt*> &stms);

  int sort_coalesce_stmts(Ob2DArray<CoalesceStmts *> &coalesce_stmts);
private:

  ObQueryRefRawExpr * get_exists_query_expr(ObRawExpr *expr);

  ObQueryRefRawExpr * get_any_all_query_expr(ObRawExpr *expr);

  ObRawExpr * get_any_all_left_hand_expr(ObRawExpr *expr);

  int create_and_expr(const ObIArray<ObRawExpr *> &params, ObRawExpr *&ret_expr);

  int make_false(ObIArray<ObRawExpr *> &conds);

private:
  ObArenaAllocator allocator_;
  Ob2DArray<CoalesceStmts *> coalesce_stmts_;
};

}
}



#endif // OB_TRANSFORM_SUBQUERY_COALESCE_H
