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

#ifndef OCEANBASE_SQL_REWRITE_EXPAND_AGGREGATE_UTILS
#define OCEANBASE_SQL_REWRITE_EXPAND_AGGREGATE_UTILS

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObExpandAggregateUtils
{
public:
  static int expand_aggr_expr(ObDMLStmt *stmt, ObTransformerCtx *ctx, bool &trans_happened);

  static int expand_window_aggr_expr(ObDMLStmt *stmt, ObTransformerCtx *ctx, bool &trans_happened);

private:
  static int extract_candi_aggr(ObDMLStmt *select_stmt,
                                common::ObIArray<ObRawExpr*> &candi_aggr_items,
                                common::ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int extract_candi_window_aggr(ObSelectStmt *select_stmt,
                                       common::ObIArray<ObRawExpr*> &candi_win_items,
                                       common::ObIArray<ObWinFunRawExpr*> &new_win_exprs);

  static int expand_covar_expr(ObTransformerCtx *ctx,
                               ObAggFunRawExpr *aggr_expr,
                               ObRawExpr *&replace_expr,
                               common::ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_corr_expr(ObTransformerCtx *ctx,
                              ObAggFunRawExpr *aggr_expr,
                              ObRawExpr *&replace_expr,
                              common::ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int add_aggr_item(common::ObIArray<ObAggFunRawExpr*> &new_aggr_items,
                           ObAggFunRawExpr *&aggr_expr);

  static int expand_var_expr(ObTransformerCtx *ctx,
                             ObAggFunRawExpr *aggr_expr,
                             ObRawExpr *&replace_expr,
                             ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_regr_expr(ObTransformerCtx *ctx,
                              ObAggFunRawExpr *aggr_expr,
                              ObRawExpr *&replace_expr,
                              ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_regr_slope_expr(ObTransformerCtx *ctx,
                                    ObAggFunRawExpr *aggr_expr,
                                    ObRawExpr *&replace_expr,
                                    ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_regr_intercept_expr(ObTransformerCtx *ctx,
                                        ObAggFunRawExpr *aggr_expr,
                                        ObRawExpr *&replace_expr,
                                        ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_regr_count_expr(ObTransformerCtx *ctx,
                                    ObAggFunRawExpr *aggr_expr,
                                    ObRawExpr *&replace_expr,
                                    ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_regr_r2_expr(ObTransformerCtx *ctx,
                                 ObAggFunRawExpr *aggr_expr,
                                 ObRawExpr *&replace_expr,
                                 ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_regr_avg_expr(ObTransformerCtx *ctx,
                                  ObAggFunRawExpr *aggr_expr,
                                  ObRawExpr *&replace_expr,
                                  ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_regr_s_expr(ObTransformerCtx *ctx,
                                ObAggFunRawExpr *aggr_expr,
                                ObRawExpr *&replace_expr,
                                ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static bool is_valid_aggr_type(const ObItemType aggr_type);

  static bool is_covar_expr_type(const ObItemType aggr_type) {
    return aggr_type == T_FUN_COVAR_POP || aggr_type == T_FUN_COVAR_SAMP;
  }

  static bool is_var_expr_type(const ObItemType aggr_type) {
    return aggr_type == T_FUN_VAR_POP || aggr_type == T_FUN_VAR_SAMP;
  }

  static bool is_regr_expr_type(const ObItemType aggr_type);

  //构造一个特殊的case when expr:
  //   case when param_expr1 is not null and param_expr2 is not null
  //   then then_expr
  //   else NULL
  //   end
  static int build_special_case_when_expr(ObRawExprFactory &expr_factory,
                                          const ObSQLSessionInfo *session,
                                          ObRawExpr *param_expr1,
                                          ObRawExpr *param_expr2,
                                          ObRawExpr *then_expr,
                                          ObRawExpr *&case_when_expr);

  static bool is_keep_aggr_type(const ObItemType aggr_type) {
    return aggr_type == T_FUN_KEEP_AVG || aggr_type == T_FUN_KEEP_STDDEV ||
           aggr_type == T_FUN_KEEP_VARIANCE;
  }

  static int expand_keep_aggr_expr(ObTransformerCtx *ctx,
                                   ObAggFunRawExpr *aggr_expr,
                                   ObRawExpr *&replace_expr,
                                   ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_keep_avg_expr(ObTransformerCtx *ctx,
                                  ObAggFunRawExpr *aggr_expr,
                                  ObRawExpr *&replace_expr,
                                  ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_keep_variance_expr(ObTransformerCtx *ctx,
                                       ObAggFunRawExpr *aggr_expr,
                                       ObRawExpr *&replace_expr,
                                       ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_keep_stddev_expr(ObTransformerCtx *ctx,
                                     ObAggFunRawExpr *aggr_expr,
                                     ObRawExpr *&replace_expr,
                                     ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static bool is_common_aggr_type(const ObItemType aggr_type) {//用于一些普通的aggr展开
    return aggr_type == T_FUN_AVG || aggr_type == T_FUN_STDDEV ||
           aggr_type == T_FUN_VARIANCE || aggr_type == T_FUN_STDDEV_POP ||
           aggr_type == T_FUN_STDDEV_SAMP ||
           aggr_type == T_FUN_APPROX_COUNT_DISTINCT;
  }

  static int expand_common_aggr_expr(ObTransformerCtx *ctx,
                                     ObAggFunRawExpr *aggr_expr,
                                     ObRawExpr *&replace_expr,
                                     ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_avg_expr(ObTransformerCtx *ctx,
                             ObAggFunRawExpr *aggr_expr,
                             ObRawExpr *&replace_expr,
                             ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_oracle_variance_expr(ObTransformerCtx *ctx,
                                         ObAggFunRawExpr *aggr_expr,
                                         ObRawExpr *&replace_expr,
                                         ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_mysql_variance_expr(ObTransformerCtx *ctx,
                                        ObAggFunRawExpr *aggr_expr,
                                        ObRawExpr *&replace_expr,
                                        ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_stddev_expr(ObTransformerCtx *ctx,
                                ObAggFunRawExpr *aggr_expr,
                                ObRawExpr *&replace_expr,
                                ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_stddev_pop_expr(ObTransformerCtx *ctx,
                                    ObAggFunRawExpr *aggr_expr,
                                    ObRawExpr *&replace_expr,
                                    ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_stddev_samp_expr(ObTransformerCtx *ctx,
                                     ObAggFunRawExpr *aggr_expr,
                                     ObRawExpr *&replace_expr,
                                     ObIArray<ObAggFunRawExpr*> &new_aggr_items);

  static int expand_approx_count_distinct_expr(ObTransformerCtx *ctx,
                                               ObAggFunRawExpr *aggr_expr,
                                               ObRawExpr *&replace_expr,
                                               ObIArray<ObAggFunRawExpr *> &new_aggr_items);

  static int add_cast_expr(ObTransformerCtx *ctx,
                           ObRawExpr *expr,
                           const ObExprResType &dst_type,
                           ObRawExpr *&new_expr);

  static int add_win_exprs(ObSelectStmt *select_stmt,
                           ObIArray<ObRawExpr*> &replace_exprs,
                           ObIArray<ObWinFunRawExpr*> &new_win_exprs);

};

} // namespace sql
} //namespace oceanbase
#endif // OCEANBASE_SQL_REWRITE_EXPAND_AGGREGATE_UTILS
