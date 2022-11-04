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

#ifndef OB_TRANSFORM_POST_PROCESS_H_
#define OB_TRANSFORM_POST_PROCESS_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObMergeStmt;
class ObTransformPostProcess: public ObTransformRule
{
public:
  ObTransformPostProcess(ObTransformerCtx *ctx)
    :ObTransformRule(ctx, TransMethod::POST_ORDER) {}

  virtual ~ObTransformPostProcess() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:
  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans) override;
  /**
   * @brief transform_for_hierarchical_query
   * 为prior表达式创建copy expr关系，并且为层次查询生成joined table，
   * 最后尝试为connect by的right table合并视图
   */
  int transform_for_hierarchical_query(ObDMLStmt *stmt, bool &trans_happened);
  
  /**
   * @brief pullup_hierarchical_query
   * 如果child stmt是connect by view，尝试合并视图
   */
  int pullup_hierarchical_query(ObDMLStmt *stmt, bool &trans_happened);
  /**
   * @brief transform_prior_exprs
   * 为prior expr创建copy expr关系
   * 如果是新引擎，需要将connect by内的prior表达式copy一份放入connect_by_prior_exprs内
   * 如果是老引擎，需要将映射好的prior表达式放入connect_by_prior_exprs内
   */
  int transform_prior_exprs(ObSelectStmt &stmt, TableItem &left_table, TableItem &right_table);
  int get_prior_exprs(ObIArray<ObRawExpr *> &expr, ObIArray<ObRawExpr *> &prior_exprs);
  int get_prior_exprs(ObRawExpr *expr, ObIArray<ObRawExpr *> &prior_exprs);
  
  /**
   * @brief modify_prior_exprs
   * 在预处理的时候，层次查询把from items压在视图内，并且拷贝了一份
   * 左表作为root节点生成子查询，右表作为子节点生成子查询；
   * 在预处理的时候，层次查询的所有column都引用了右表，现在需要把
   * prior表达式引用的列替换为左表的column，同时为左右表的column维护
   * copy expr的映射关系
   */
  int modify_prior_exprs(ObRawExprFactory &expr_factory,
                         ObSelectStmt &stmt,
                         TableItem &left_table,
                         TableItem &right_table,
                         ObIArray<ObRawExpr *> &prior_exprs,
                         ObIArray<ObRawExpr *> &convert_exprs);

  /**
   * @brief make_connect_by_joined_table
   * 为层次查询创建joined table，把connect by exprs
   * 作为join condition
   */
  int make_connect_by_joined_table(ObSelectStmt &stmt,
                                  TableItem &left_table,
                                  TableItem &right_table);

  int transform_merge_into_subquery(ObDMLStmt *stmt,
                                    bool &trans_happened);

  int create_matched_expr(ObMergeStmt &stmt,
                          ObRawExpr *&matched_flag,
                          ObRawExpr *&not_matched_flag);

  int generate_merge_conditions_subquery(ObRawExpr *matched_expr,
                                         ObIArray<ObRawExpr*> &condition_exprs);

  int get_update_insert_condition_subquery(ObMergeStmt *merge_stmt,
                                           ObRawExpr *matched_expr,
                                           ObRawExpr *not_matched_expr,
                                           bool &update_has_subquery,
                                           bool &insert_has_subquery,
                                           ObIArray<ObRawExpr*> &new_subquery_exprs);

  int get_update_insert_target_subquery(ObMergeStmt *merge_stmt,
                                        ObRawExpr *matched_expr,
                                        ObRawExpr *not_matched_expr,
                                        bool update_has_subquery,
                                        bool insert_has_subquery,
                                        ObIArray<ObRawExpr*> &new_subquery_exprs);


  int get_delete_condition_subquery(ObMergeStmt *merge_stmt,
                                    ObRawExpr *matched_expr,
                                    bool update_has_subquery,
                                    ObIArray<ObRawExpr*> &new_subquery_exprs);

  int transform_onetime_subquery(ObDMLStmt *stmt, bool &trans_happened);

  int extract_onetime_subquery(ObRawExpr *&expr,
                               ObIArray<ObRawExpr *> &onetime_list,
                               bool &is_valid);

  int create_onetime_param(ObDMLStmt *stmt,
                           ObRawExpr *&expr,
                           const ObIArray<ObRawExpr *> &onetime_list,
                           ObIArray<ObRawExpr *> &old_exprs,
                           ObIArray<ObRawExpr *> &new_exprs,
                           bool &is_happened);

  int pullup_exec_exprs(ObDMLStmt *stmt, bool &trans_happened);

  int64_t get_expr_level(const ObRawExpr &expr);

  int extract_exec_exprs(ObDMLStmt *stmt,
                         const int32_t expr_level,
                         ObIArray<ObRawExpr *> &candi_exprs);

  int extract_exec_exprs(ObRawExpr *expr,
                         const int32_t expr_level,
                         ObIArray<ObRawExpr *> &candi_exprs);

  int adjust_exec_param_exprs(ObQueryRefRawExpr *query_ref,
                              ObIArray<ObRawExpr *> &candi_exprs,
                              const int64_t stmt_level);

  int is_non_correlated_exists_for_onetime(ObRawExpr *expr,
                                           bool &is_non_correlated_exists_for_onetime,
                                           int64_t &ref_count);

  DISALLOW_COPY_AND_ASSIGN(ObTransformPostProcess);
};

}
}

#endif /* OB_TRANSFORM_POST_PROCESS_H_ */
