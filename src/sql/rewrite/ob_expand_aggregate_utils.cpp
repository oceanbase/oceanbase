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

#define USING_LOG_PREFIX SQL_REWRITE

#include "sql/rewrite/ob_expand_aggregate_utils.h"
#include "common/ob_common_utility.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "common/ob_common_utility.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

int ObExpandAggregateUtils::expand_aggr_expr(ObDMLStmt *stmt,
                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> candi_aggr_items;
  ObSEArray<ObRawExpr*, 4> replace_exprs;
  ObSEArray<ObAggFunRawExpr*, 4> new_aggr_items;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(extract_candi_aggr(stmt,
                                        candi_aggr_items,
                                        new_aggr_items))) {
    LOG_WARN("failed to extact candi aggr", K(ret));
  } else if (candi_aggr_items.empty()) {
    /*do nothing */
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_aggr_items.count(); ++i) {
      ObRawExpr *replace_expr = NULL;
      ObAggFunRawExpr* aggr_expr = static_cast<ObAggFunRawExpr*>(candi_aggr_items.at(i));
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
      } else if (is_covar_expr_type(aggr_expr->get_expr_type()) &&
                 OB_FAIL(expand_covar_expr(aggr_expr, replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand covar expr", K(ret));
      } else if (aggr_expr->get_expr_type() == T_FUN_CORR &&
                 OB_FAIL(expand_corr_expr(aggr_expr, replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand corr expr", K(ret));
      } else if (is_var_expr_type(aggr_expr->get_expr_type()) &&
                 OB_FAIL(expand_var_expr(aggr_expr, replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand var expr", K(ret));
      } else if (is_regr_expr_type(aggr_expr->get_expr_type()) &&
                 OB_FAIL(expand_regr_expr(aggr_expr, replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand regr expr", K(ret));
      } else if (is_keep_aggr_type(aggr_expr->get_expr_type()) &&
                 OB_FAIL(expand_keep_aggr_expr(aggr_expr, replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand keep aggr expr", K(ret));
      } else if (is_common_aggr_type(aggr_expr->get_expr_type()) &&
                 OB_FAIL(expand_common_aggr_expr(aggr_expr, replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand common aggr expr", K(ret));
      } else if (OB_ISNULL(replace_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(replace_expr), K(aggr_expr->get_expr_type()));
      } else if (OB_FAIL(replace_expr->formalize(session_info_))) {
        LOG_WARN("failed to formalize", K(ret));
      } else if (aggr_expr->get_result_type() != replace_expr->get_result_type() &&
                 OB_FAIL(add_cast_expr(replace_expr, aggr_expr->get_result_type(), replace_expr))) {
        LOG_WARN("failed to add cast expr", K(ret));
      } else if (OB_FAIL(replace_expr->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else if (OB_FAIL(replace_exprs.push_back(replace_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      if (stmt->is_select_stmt() &&
          OB_FAIL(static_cast<ObSelectStmt *>(stmt)->get_aggr_items().assign(new_aggr_items))) {
        LOG_WARN("failed to assign expr", K(ret));
      } else if (!stmt->is_select_stmt() &&
                 OB_FAIL(static_cast<ObDelUpdStmt*>(stmt)->get_returning_aggr_items().assign(
                                                                                 new_aggr_items))) {
        LOG_WARN("failed to assign expr", K(ret));
      } else if (OB_FAIL(stmt->replace_relation_exprs(candi_aggr_items, replace_exprs))) {
        LOG_WARN("failed to replace stmt expr", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObExpandAggregateUtils::expand_window_aggr_expr(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> candi_win_items;
  ObSEArray<ObAggFunRawExpr*, 4> new_aggr_items;
  ObSEArray<ObRawExpr*, 4> replace_exprs;
  ObSEArray<ObWinFunRawExpr*, 4> new_win_exprs;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(stmt));
  } else if (!stmt->is_select_stmt()) {
    /*do nothing*/
  } else if (OB_FAIL(extract_candi_window_aggr(static_cast<ObSelectStmt *>(stmt),
                                               candi_win_items,
                                               new_win_exprs))) {
    LOG_WARN("failed to extract candi window aggr", K(ret));
  } else if (candi_win_items.empty()) {
    /*do nothing */
  } else {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_win_items.count(); ++i) {
      ObRawExpr *replace_expr = NULL;
      ObWinFunRawExpr* win_expr = static_cast<ObWinFunRawExpr*>(candi_win_items.at(i));
      new_aggr_items.reset();
      if (OB_ISNULL(win_expr) || OB_ISNULL(win_expr->get_agg_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(win_expr));
      } else if (is_covar_expr_type(win_expr->get_agg_expr()->get_expr_type()) &&
                 OB_FAIL(expand_covar_expr(win_expr->get_agg_expr(),
                                           replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand covar expr", K(ret));
      } else if (win_expr->get_agg_expr()->get_expr_type() == T_FUN_CORR &&
                 OB_FAIL(expand_corr_expr(win_expr->get_agg_expr(),
                                          replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand corr expr", K(ret));
      } else if (is_var_expr_type(win_expr->get_agg_expr()->get_expr_type()) &&
                 OB_FAIL(expand_var_expr(win_expr->get_agg_expr(),
                                         replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand var expr", K(ret));
      } else if (is_regr_expr_type(win_expr->get_agg_expr()->get_expr_type()) &&
                 OB_FAIL(expand_regr_expr(win_expr->get_agg_expr(),
                                          replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand regr exprs", K(ret));
      } else if (is_keep_aggr_type(win_expr->get_agg_expr()->get_expr_type()) &&
                 OB_FAIL(expand_keep_aggr_expr(win_expr->get_agg_expr(),
                                               replace_expr, new_aggr_items))) {
        LOG_WARN("failed to expand keep aggr exprs", K(ret));
      } else if (is_common_aggr_type(win_expr->get_agg_expr()->get_expr_type()) &&
                 OB_FAIL(expand_common_aggr_expr(win_expr->get_agg_expr(),
                                                 replace_expr, new_aggr_items))) {
        LOG_WARN("failed to common aggr exprs", K(ret));
      } else if (OB_ISNULL(replace_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(replace_expr),
                                         K(win_expr->get_agg_expr()->get_expr_type()));
      } else if (OB_FAIL(replace_expr->formalize(session_info_))) {
        LOG_WARN("failed to formalize", K(ret));
      } else if (win_expr->get_agg_expr()->get_result_type() != replace_expr->get_result_type() &&
                 OB_FAIL(add_cast_expr(replace_expr,
                                       win_expr->get_agg_expr()->get_result_type(),
                                       replace_expr))) {
        LOG_WARN("failed to add cast expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::process_window_complex_agg_expr(expr_factory_,
                                                                         replace_expr->get_expr_type(),
                                                                         win_expr,
                                                                         replace_expr,
                                                                         &new_win_exprs))) {
        LOG_WARN("failed to process window complex agg expr", K(ret));
      } else if (replace_expr->is_aggr_expr() &&
                 OB_FAIL(replace_exprs.push_back(new_win_exprs.at(new_win_exprs.count() - 1)))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (!replace_expr->is_aggr_expr() && OB_FAIL(replace_exprs.push_back(replace_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_win_exprs(select_stmt, replace_exprs, new_win_exprs))) {
        LOG_WARN("failed to win exprs", K(ret));
      } else if (OB_FAIL(select_stmt->replace_relation_exprs(candi_win_items, replace_exprs))) {
        LOG_WARN("failed to replace stmt expr");
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

//前提:(expr1, expr2) 两个expr都不为NULL
//T_FUN_COVAR_POP == node->type_: (SUM(expr1 * expr2) - SUM(expr2) * SUM(expr1) / count(expr1 * expr2)) / count(expr1 * expr2)
//T_FUN_COVAR_SAMP== node->type_: (SUM(expr1 * expr2) - SUM(expr1) * SUM(expr2) / count(expr1 * expr2)) / (count(expr1 * expr2)-1)
int ObExpandAggregateUtils::expand_covar_expr(ObAggFunRawExpr *aggr_expr,
                                              ObRawExpr *&replace_expr,
                                              ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr1 = NULL;
  ObRawExpr *parma_expr2 = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(!is_covar_expr_type(aggr_expr->get_expr_type()) ||
                   aggr_expr->get_real_param_exprs().count() != 2) ||
      OB_ISNULL(parma_expr1 = aggr_expr->get_real_param_exprs().at(0)) ||
      OB_ISNULL(parma_expr2 = aggr_expr->get_real_param_exprs().at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObRawExpr *case_when_expr1 = NULL;
    ObRawExpr *case_when_expr2 = NULL;
    ObRawExpr *multi_expr = NULL;
    ObRawExpr *multi_sum_expr = NULL;
    ObAggFunRawExpr *sum_expr1 = NULL;
    ObAggFunRawExpr *sum_expr2 = NULL;
    ObAggFunRawExpr *sum_product_expr = NULL;
    ObAggFunRawExpr *count_product_expr = NULL;
    ObRawExpr *div_expr = NULL;
    ObRawExpr *minus_expr = NULL;
    ObRawExpr *div_sum_expr = NULL;
    if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                             session_info_,
                                             parma_expr1,
                                             parma_expr2,
                                             parma_expr1,
                                             case_when_expr1))) {
      LOG_WARN("failed to build special case when expr", K(ret));
    } else if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                                    session_info_,
                                                    parma_expr1,
                                                    parma_expr2,
                                                    parma_expr2,
                                                    case_when_expr2))) {
      LOG_WARN("failed to build special case when expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MUL,
                                                                   parma_expr1,
                                                                   parma_expr2,
                                                                   multi_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              case_when_expr1,
                                                              sum_expr1))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_expr1))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              case_when_expr2,
                                                              sum_expr2))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_expr2))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MUL,
                                                                   sum_expr1,
                                                                   sum_expr2,
                                                                   multi_sum_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              multi_expr,
                                                              sum_product_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_product_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_COUNT,
                                                              multi_expr,
                                                              count_product_expr))) {
      LOG_WARN("failed to build count expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, count_product_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_DIV,
                                                                   multi_sum_expr,
                                                                   count_product_expr,
                                                                   div_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MINUS,
                                                                   sum_product_expr,
                                                                   div_expr,
                                                                   minus_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (aggr_expr->get_expr_type() == T_FUN_COVAR_POP) {
      if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                              T_OP_DIV,
                                                              minus_expr,
                                                              count_product_expr,
                                                              div_sum_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else {
        replace_expr = div_sum_expr;
      }
    } else {
      ObRawExpr *null_expr = NULL;
      ObConstRawExpr *one_expr = NULL;
      ObConstRawExpr *zero_expr = NULL;
      ObRawExpr *minus_expr2 = NULL;
      ObRawExpr *ne_expr = NULL;
      ObRawExpr *case_when_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory_, null_expr))) {
        LOG_WARN("failed to build null expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                              ObIntType,
                                                              1,
                                                              one_expr))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                              ObIntType,
                                                              0,
                                                              zero_expr))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                     T_OP_MINUS,
                                                                     count_product_expr,
                                                                     one_expr,
                                                                     minus_expr2))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                     T_OP_NE,
                                                                     minus_expr2,
                                                                     zero_expr,
                                                                     ne_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                              ne_expr,
                                                              minus_expr2,
                                                              null_expr,
                                                              case_when_expr))) {
        LOG_WARN("failed to build case when expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                     T_OP_DIV,
                                                                     minus_expr,
                                                                     case_when_expr,
                                                                     div_sum_expr))) {
        LOG_WARN("failed to common binary op expr", K(ret));
      } else {
        replace_expr = div_sum_expr;
      }
    }
  }
  return ret;
}

//前提:(expr1, expr2) 两个expr都不为NULL
//COVAR_POP(expr1, expr2) / (STDDEV_POP(expr1) * STDDEV_POP(expr2))
int ObExpandAggregateUtils::expand_corr_expr(ObAggFunRawExpr *aggr_expr,
                                             ObRawExpr *&replace_expr,
                                             ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr1 = NULL;
  ObRawExpr *parma_expr2 = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_CORR ||
                  aggr_expr->get_real_param_exprs().count() != 2) ||
      OB_ISNULL(parma_expr1 = aggr_expr->get_real_param_exprs().at(0)) ||
      OB_ISNULL(parma_expr2 = aggr_expr->get_real_param_exprs().at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *covar_pop_expr = NULL;
    ObAggFunRawExpr *stddev_pop_expr1 = NULL;
    ObAggFunRawExpr *stddev_pop_expr2 = NULL;
    ObRawExpr *null_expr = NULL;
    ObRawExpr *case_when_expr1 = NULL;
    ObRawExpr *case_when_expr2 = NULL;
    ObRawExpr *multi_stddev_expr = NULL;
    ObRawExpr *left_multi_expr = NULL;
    ObRawExpr *right_multi_expr = NULL;
    ObRawExpr *div_expr = NULL;
    ObRawExpr *left_div_expr = NULL;
    ObRawExpr *ne_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    ObConstRawExpr *zero_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory_, null_expr))) {
      LOG_WARN("failed to build null expr", K(ret));
    } else if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                                    session_info_,
                                                    parma_expr1,
                                                    parma_expr2,
                                                    parma_expr1,
                                                    case_when_expr1))) {
      LOG_WARN("failed to build special case when expr", K(ret));
    } else if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                                    session_info_,
                                                    parma_expr1,
                                                    parma_expr2,
                                                    parma_expr2,
                                                    case_when_expr2))) {
      LOG_WARN("failed to build special case when expr", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_STDDEV_POP, stddev_pop_expr1))) {
      LOG_WARN("create ObOpRawExpr failed", K(ret));
    } else if (OB_FAIL(stddev_pop_expr1->add_real_param_expr(case_when_expr1))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(expand_stddev_pop_expr(stddev_pop_expr1, left_multi_expr, new_aggr_items))) {
      LOG_WARN("fail to expand stddev pop expr", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_STDDEV_POP, stddev_pop_expr2))) {
      LOG_WARN("create ObOpRawExpr failed", K(ret));
    } else if (OB_FAIL(stddev_pop_expr2->add_real_param_expr(case_when_expr2))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(expand_stddev_pop_expr(stddev_pop_expr2,
                                              right_multi_expr, new_aggr_items))) {
      LOG_WARN("fail to expand stddev pop expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MUL,
                                                                   left_multi_expr,
                                                                   right_multi_expr,
                                                                   multi_stddev_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                            ObIntType,
                                                            0,
                                                            zero_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_NE,
                                                                   multi_stddev_expr,
                                                                   zero_expr,
                                                                   ne_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                            ne_expr,
                                                            multi_stddev_expr,
                                                            null_expr,
                                                            case_when_expr))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_COVAR_POP, covar_pop_expr))) {
      LOG_WARN("create ObOpRawExpr failed", K(ret));
    } else if (OB_FAIL(covar_pop_expr->add_real_param_expr(parma_expr1))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(covar_pop_expr->add_real_param_expr(parma_expr2))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(expand_covar_expr(covar_pop_expr, left_div_expr, new_aggr_items))) {
      LOG_WARN("failed to expand covar expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_DIV,
                                                                   left_div_expr,
                                                                   case_when_expr,
                                                                   div_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else {
      replace_expr = div_expr;
    }
  }
  return ret;
}

int ObExpandAggregateUtils::extract_candi_aggr(ObDMLStmt *stmt,
                                               ObIArray<ObRawExpr*> &candi_aggr_items,
                                               ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (stmt->is_dml_stmt()) {
    ObSEArray<ObAggFunRawExpr*, 4> aggr_items;
    if (stmt->is_select_stmt() &&
        OB_FAIL(append(aggr_items, static_cast<ObSelectStmt *>(stmt)->get_aggr_items()))) {
      LOG_WARN("failed to append aggr items", K(ret));
    } else if (stmt->is_dml_write_stmt() &&
               OB_FAIL(append(aggr_items,
                              static_cast<ObDelUpdStmt*>(stmt)->get_returning_aggr_items()))) {
      LOG_WARN("failed to append aggr items", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < aggr_items.count(); ++i) {
        if (OB_ISNULL(aggr_items.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(aggr_items.at(i)));
        } else if (is_valid_aggr_type(aggr_items.at(i)->get_expr_type())) {
          if (OB_FAIL(candi_aggr_items.push_back(aggr_items.at(i)))) {
            LOG_WARN("failed to push back aggr items", K(ret));
          } else {/*do nothing*/}
        } else if (OB_FAIL(new_aggr_items.push_back(aggr_items.at(i)))) {
          LOG_WARN("failed to push back aggr items", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExpandAggregateUtils::extract_candi_window_aggr(ObSelectStmt *select_stmt,
                                                      ObIArray<ObRawExpr*> &candi_win_items,
                                                      ObIArray<ObWinFunRawExpr*> &new_win_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else {
    ObIArray<ObWinFunRawExpr *> &win_exprs = select_stmt->get_window_func_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < win_exprs.count(); ++i) {
      ObWinFunRawExpr* win_expr = win_exprs.at(i);
      if (OB_ISNULL(win_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(win_expr));
      } else if (win_expr->get_agg_expr() != NULL &&
                 is_valid_aggr_type(win_expr->get_agg_expr()->get_expr_type())) {
        if (OB_FAIL(candi_win_items.push_back(win_expr))) {
          LOG_WARN("failed to push back win expr", K(ret));
        } else {/*do nothing*/}
      } else if (OB_FAIL(new_win_exprs.push_back(win_expr))) {
        LOG_WARN("failed to push back win expr", K(ret));
      }
    }
  }
  return ret;
}

int ObExpandAggregateUtils::add_aggr_item(ObIArray<ObAggFunRawExpr*> &new_aggr_items,
                                          ObAggFunRawExpr *&aggr_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < new_aggr_items.count(); ++i) {
      if (OB_ISNULL(new_aggr_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(new_aggr_items.at(i)));
      } else if (aggr_expr->same_as(*new_aggr_items.at(i))) {
        aggr_expr = new_aggr_items.at(i);
        break;
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && i == new_aggr_items.count()) {
      if (OB_FAIL(new_aggr_items.push_back(aggr_expr))) {
        LOG_WARN("failed to push back aggr expr", K(ret));
      }
    }
  }
  return ret;
}

int ObExpandAggregateUtils::add_win_expr(common::ObIArray<ObWinFunRawExpr*> &new_win_exprs,
                                         ObWinFunRawExpr *&win_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(win_expr));
  } else {
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < new_win_exprs.count(); ++i) {
      if (OB_ISNULL(new_win_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(new_win_exprs.at(i)));
      } else if (win_expr->same_as(*new_win_exprs.at(i))) {
        win_expr = new_win_exprs.at(i);
        break;
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && i == new_win_exprs.count()) {
      if (OB_FAIL(new_win_exprs.push_back(win_expr))) {
        LOG_WARN("failed to push back aggr expr", K(ret));
      }
    }
  }
  return ret;
}

//T_FUN_VAR_POP == node->type_: (SUM(expr*expr) - SUM(expr)* SUM(expr)/ COUNT(expr)) / COUNT(expr)
//T_FUN_VAR_SAMP== node->type_: (SUM(expr*expr) - SUM(expr)* SUM(expr)/ COUNT(expr)) / (COUNT(expr) - 1)
int ObExpandAggregateUtils::expand_var_expr(ObAggFunRawExpr *aggr_expr,
                                            ObRawExpr *&replace_expr,
                                            ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(!is_var_expr_type(aggr_expr->get_expr_type()) ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else if (lib::is_mysql_mode() && aggr_expr->get_expr_type() == T_FUN_VAR_POP) {
  //mysql模式下的VAR_POP() 同 VARIANCE() 实现是一样的
    if (OB_FAIL(expand_mysql_variance_expr(aggr_expr,
                                           replace_expr,
                                           new_aggr_items))) {
      LOG_WARN("failed to expand mysql variance expr", K(ret));
    } else {/*do nothing*/}
  } else {
    ObRawExpr *multi_expr = NULL;
    ObRawExpr *multi_sum_expr = NULL;
    ObAggFunRawExpr *sum_expr = NULL;
    ObRawExpr *cast_sum_expr = NULL;
    ObAggFunRawExpr *sum_product_expr = NULL;
    ObRawExpr *cast_sum_product_expr = NULL;
    ObAggFunRawExpr *count_expr = NULL;
    ObRawExpr *div_expr = NULL;
    ObRawExpr *minus_expr = NULL;
    ObRawExpr *div_minus_expr = NULL;
    //由于目前 mysql模式下的除法实现有点问题，存在部分精度不一致，因此这里暂时先添加cast显示转换为最大精度
    ObExprResType dst_type;
    dst_type.set_number();
    dst_type.set_scale(ObAccuracy::MAX_ACCURACY2[MYSQL_MODE][ObNumberType].get_scale());
    dst_type.set_precision(ObAccuracy::MAX_ACCURACY2[MYSQL_MODE][ObNumberType].get_precision());
    if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                            T_OP_MUL,
                                                            parma_expr,
                                                            parma_expr,
                                                            multi_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              parma_expr,
                                                              sum_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_expr))) {
      LOG_WARN("failed to push back aggr item", K(ret));
    } else if (lib::is_mysql_mode() &&
               OB_FAIL(add_cast_expr(sum_expr, dst_type, cast_sum_expr))) {
      LOG_WARN("failed to add cast expr", K(ret));
    } else if (lib::is_mysql_mode() &&
               OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MUL,
                                                                   cast_sum_expr,
                                                                   cast_sum_expr,
                                                                   multi_sum_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (lib::is_oracle_mode() &&
               OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MUL,
                                                                   sum_expr,
                                                                   sum_expr,
                                                                   multi_sum_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              multi_expr,
                                                              sum_product_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_product_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (lib::is_mysql_mode() &&
               OB_FAIL(add_cast_expr(sum_product_expr, dst_type, cast_sum_product_expr))) {
      LOG_WARN("failed to add cast expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_COUNT,
                                                              parma_expr,
                                                              count_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, count_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_DIV,
                                                                   multi_sum_expr,
                                                                   count_expr,
                                                                   div_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (lib::is_mysql_mode() &&
               OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MINUS,
                                                                   cast_sum_product_expr,
                                                                   div_expr,
                                                                   minus_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (lib::is_oracle_mode() &&
               OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MINUS,
                                                                   sum_product_expr,
                                                                   div_expr,
                                                                   minus_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (aggr_expr->get_expr_type() == T_FUN_VAR_POP) {
      if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                              T_OP_DIV,
                                                              minus_expr,
                                                              count_expr,
                                                              div_minus_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else {
        replace_expr = div_minus_expr;
      }
    } else {
      ObConstRawExpr *one_expr = NULL;
      ObConstRawExpr *zero_expr = NULL;
      ObRawExpr *minus_expr2 = NULL;
      ObRawExpr *ne_expr = NULL;
      ObRawExpr *case_when_expr = NULL;
      ObRawExpr *null_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory_, null_expr))) {
        LOG_WARN("failed to build null expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                              ObIntType,
                                                              1,
                                                              one_expr))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                              ObIntType,
                                                              0,
                                                              zero_expr))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                     T_OP_MINUS,
                                                                     count_expr,
                                                                     one_expr,
                                                                     minus_expr2))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                     T_OP_NE,
                                                                     minus_expr2,
                                                                     zero_expr,
                                                                     ne_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                              ne_expr,
                                                              minus_expr2,
                                                              null_expr,
                                                              case_when_expr))) {
        LOG_WARN("failed to build case when expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                     T_OP_DIV,
                                                                     minus_expr,
                                                                     case_when_expr,
                                                                     div_minus_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else {
        replace_expr = div_minus_expr;
      }
    }
  }
  return ret;
}

int ObExpandAggregateUtils::expand_regr_expr(ObAggFunRawExpr *aggr_expr,
                                             ObRawExpr *&replace_expr,
                                             ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(!is_regr_expr_type(aggr_expr->get_expr_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else if (aggr_expr->get_expr_type() == T_FUN_REGR_SLOPE) {
    if (OB_FAIL(expand_regr_slope_expr(aggr_expr,
                                       replace_expr,
                                       new_aggr_items))) {
      LOG_WARN("failed to expand regr slope expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_REGR_INTERCEPT) {
    if (OB_FAIL(expand_regr_intercept_expr(aggr_expr,
                                           replace_expr,
                                           new_aggr_items))) {
      LOG_WARN("failed to expand regr intercept expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_REGR_COUNT) {
    if (OB_FAIL(expand_regr_count_expr(aggr_expr,
                                       replace_expr,
                                       new_aggr_items))) {
      LOG_WARN("failed to expand regr count expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_REGR_R2) {
    if (OB_FAIL(expand_regr_r2_expr(aggr_expr,
                                    replace_expr,
                                    new_aggr_items))) {
      LOG_WARN("failed to expand regr r2 expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_REGR_AVGX ||
             aggr_expr->get_expr_type() == T_FUN_REGR_AVGY) {
    if (OB_FAIL(expand_regr_avg_expr(aggr_expr,
                                     replace_expr,
                                     new_aggr_items))) {
      LOG_WARN("failed to expand regr avg expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_REGR_SXX ||
             aggr_expr->get_expr_type() == T_FUN_REGR_SYY ||
             aggr_expr->get_expr_type() == T_FUN_REGR_SXY) {
    if (OB_FAIL(expand_regr_s_expr(aggr_expr,
                                   replace_expr,
                                   new_aggr_items))) {
      LOG_WARN("failed to expand regr s expr", K(ret));
    }
  } else {/*do nothing*/}
  return ret;
}

//前提:(expr1, expr2) 两个expr都不为NULL
//REGR_SLOPE(expr1, expr2) = COVAR_POP(expr1, expr2) / VAR_POP(expr2)
int ObExpandAggregateUtils::expand_regr_slope_expr(ObAggFunRawExpr *aggr_expr,
                                                   ObRawExpr *&replace_expr,
                                                   ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr1 = NULL;
  ObRawExpr *parma_expr2 = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_REGR_SLOPE ||
                  aggr_expr->get_real_param_exprs().count() != 2) ||
      OB_ISNULL(parma_expr1 = aggr_expr->get_real_param_exprs().at(0)) ||
      OB_ISNULL(parma_expr2 = aggr_expr->get_real_param_exprs().at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *covar_pop_expr = NULL;
    ObAggFunRawExpr *var_pop_expr = NULL;
    ObConstRawExpr *zero_expr = NULL;
    ObRawExpr *null_expr = NULL;
    ObRawExpr *ne_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    ObRawExpr *left_div_expr = NULL;
    ObRawExpr *right_div_expr = NULL;
    ObRawExpr *div_expr = NULL;
    ObRawExpr *right_div_case_when_expr = NULL;
    if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_COVAR_POP, covar_pop_expr))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(covar_pop_expr->add_real_param_expr(parma_expr1))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(covar_pop_expr->add_real_param_expr(parma_expr2))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else {
      if (OB_FAIL(expand_covar_expr(covar_pop_expr, left_div_expr, new_aggr_items))) {
        LOG_WARN("failed to expand covar expr", K(ret));
      } else if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                                      session_info_,
                                                      parma_expr1,
                                                      parma_expr2,
                                                      parma_expr2,
                                                      case_when_expr))) {
        LOG_WARN("failed to build special case when expr", K(ret));
      } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_VAR_POP, var_pop_expr))) {
        LOG_WARN("failed to create expr", K(ret));
      } else if (OB_FAIL(var_pop_expr->add_real_param_expr(case_when_expr))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else {
        if (OB_FAIL(expand_var_expr(var_pop_expr, right_div_expr, new_aggr_items))) {
          LOG_WARN("failed to expand var expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory_, null_expr))) {
          LOG_WARN("failed to build null expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                                ObIntType,
                                                                0,
                                                                zero_expr))) {
          LOG_WARN("failed to build const int expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                       T_OP_NE,
                                                                       right_div_expr,
                                                                       zero_expr,
                                                                       ne_expr))) {
          LOG_WARN("failed to build common binary op expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                                ne_expr,
                                                                right_div_expr,
                                                                null_expr,
                                                                right_div_case_when_expr))) {
          LOG_WARN("failed to build case when expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                       T_OP_DIV,
                                                                       left_div_expr,
                                                                       right_div_case_when_expr,
                                                                       div_expr))) {
          LOG_WARN("failed to build common binary op expr", K(ret));
        } else {
          replace_expr = div_expr;
        }
      }
    }
  }
  return ret;
}

//前提:(expr1, expr2) 两个expr都不为NULL
//REGR_INTERCEPT(expr1, expr2) = AVG(expr1) - REGR_SLOPE(expr1, expr2) * AVG(expr2)
int ObExpandAggregateUtils::expand_regr_intercept_expr(ObAggFunRawExpr *aggr_expr,
                                                       ObRawExpr *&replace_expr,
                                                       ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr1 = NULL;
  ObRawExpr *parma_expr2 = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_REGR_INTERCEPT||
                  aggr_expr->get_real_param_exprs().count() != 2) ||
      OB_ISNULL(parma_expr1 = aggr_expr->get_real_param_exprs().at(0)) ||
      OB_ISNULL(parma_expr2 = aggr_expr->get_real_param_exprs().at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *regr_slope_expr = NULL;
    ObAggFunRawExpr *sum_expr1 = NULL;
    ObAggFunRawExpr *sum_expr2 = NULL;
    ObAggFunRawExpr *count_expr1 = NULL;
    ObAggFunRawExpr *count_expr2 = NULL;
    ObRawExpr *case_when_expr1 = NULL;
    ObRawExpr *case_when_expr2 = NULL;
    ObRawExpr *minus_expr = NULL;
    ObRawExpr *multi_expr = NULL;
    ObRawExpr *div_expr1 = NULL;
    ObRawExpr *div_expr2 = NULL;
    ObRawExpr *left_multi_expr = NULL;
    if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                             session_info_,
                                             parma_expr1,
                                             parma_expr2,
                                             parma_expr1,
                                             case_when_expr1))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                                    session_info_,
                                                    parma_expr1,
                                                    parma_expr2,
                                                    parma_expr2,
                                                    case_when_expr2))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_REGR_SLOPE, regr_slope_expr))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(regr_slope_expr->add_real_param_expr(parma_expr1))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(regr_slope_expr->add_real_param_expr(parma_expr2))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(expand_regr_slope_expr(regr_slope_expr, left_multi_expr, new_aggr_items))) {
      LOG_WARN("failed to expand regr slope expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              case_when_expr1,
                                                              sum_expr1))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_expr1))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_COUNT,
                                                              case_when_expr1,
                                                              count_expr1))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, count_expr1))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              case_when_expr2,
                                                              sum_expr2))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_expr2))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_COUNT,
                                                              case_when_expr2,
                                                              count_expr2))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, count_expr2))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_DIV,
                                                                   sum_expr1,
                                                                   count_expr1,
                                                                   div_expr1))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_DIV,
                                                                   sum_expr2,
                                                                   count_expr2,
                                                                   div_expr2))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MUL,
                                                                   left_multi_expr,
                                                                   div_expr2,
                                                                   multi_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MINUS,
                                                                   div_expr1,
                                                                   multi_expr,
                                                                   minus_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else {
      replace_expr = minus_expr;
    }
  }
  return ret;
}

//前提:(expr1, expr2) 两个expr都不为NULL
//REGR_COUNT(expr1, expr2) = COUNT(case expr1 is not null and c2 is not null then expr1 else null end);
int ObExpandAggregateUtils::expand_regr_count_expr(ObAggFunRawExpr *aggr_expr,
                                                   ObRawExpr *&replace_expr,
                                                   ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr1 = NULL;
  ObRawExpr *parma_expr2 = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_REGR_COUNT||
                  aggr_expr->get_real_param_exprs().count() != 2) ||
      OB_ISNULL(parma_expr1 = aggr_expr->get_real_param_exprs().at(0)) ||
      OB_ISNULL(parma_expr2 = aggr_expr->get_real_param_exprs().at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *count_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                             session_info_,
                                             parma_expr1,
                                             parma_expr2,
                                             parma_expr1,
                                             case_when_expr))) {
    LOG_WARN("failed to build special case when expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_COUNT,
                                                              case_when_expr,
                                                              count_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, count_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else {
      replace_expr = count_expr;
    }
  }
  return ret;
}

//前提:(expr1, expr2) 两个expr都不为NULL
//REGR_R2(expr1, expr2) = if VAR_POP(expr2)  = 0 ==> NULL
//                        if VAR_POP(expr1)  = 0 and VAR_POP(expr2) != 0 ==> 1
//                        if VAR_POP(expr1)  > 0 and  VAR_POP(expr2) != 0 ==> POWER(CORR(expr1,expr),2)
//   ==> case when VAR_POP(expr2) = 0 then NULL else (case when VAR_POP(expr1)  = 0 then 1 else POWER(CORR(expr1,expr2),2));
int ObExpandAggregateUtils::expand_regr_r2_expr(ObAggFunRawExpr *aggr_expr,
                                                ObRawExpr *&replace_expr,
                                                ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr1 = NULL;
  ObRawExpr *parma_expr2 = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_REGR_R2 ||
                  aggr_expr->get_real_param_exprs().count() != 2) ||
      OB_ISNULL(parma_expr1 = aggr_expr->get_real_param_exprs().at(0)) ||
      OB_ISNULL(parma_expr2 = aggr_expr->get_real_param_exprs().at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *var_pop_expr1 = NULL;
    ObAggFunRawExpr *var_pop_expr2 = NULL;
    ObAggFunRawExpr *corr_expr = NULL;
    ObRawExpr *case_when_expr1 = NULL;
    ObRawExpr *case_when_expr2 = NULL;
    ObConstRawExpr *one_expr = NULL;
    ObConstRawExpr *two_expr = NULL;
    ObConstRawExpr *zero_expr = NULL;
    ObRawExpr *null_expr = NULL;
    ObRawExpr *eq_left_expr1 = NULL;
    ObRawExpr *eq_left_expr2 = NULL;
    ObRawExpr *eq_expr1 = NULL;
    ObRawExpr *eq_expr2 = NULL;
    ObRawExpr *power_param_expr = NULL;
    ObSysFunRawExpr *power_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory_, null_expr))) {
      LOG_WARN("failed to build null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                            ObIntType,
                                                            1,
                                                            one_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                            ObIntType,
                                                            2,
                                                            two_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                            ObIntType,
                                                            0,
                                                            zero_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                                    session_info_,
                                                    parma_expr1,
                                                    parma_expr2,
                                                    parma_expr1,
                                                    case_when_expr1))) {
      LOG_WARN("failed to build special case when expr", K(ret));
    } else if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                                    session_info_,
                                                    parma_expr1,
                                                    parma_expr2,
                                                    parma_expr2,
                                                    case_when_expr2))) {
      LOG_WARN("failed to build special case when expr", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_VAR_POP, var_pop_expr1))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(var_pop_expr1->add_real_param_expr(case_when_expr1))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(expand_var_expr(var_pop_expr1, eq_left_expr1, new_aggr_items))) {
      LOG_WARN("failed to expand var expr", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_VAR_POP, var_pop_expr2))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(var_pop_expr2->add_real_param_expr(case_when_expr2))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(expand_var_expr(var_pop_expr2, eq_left_expr2, new_aggr_items))) {
      LOG_WARN("failed to expand var expr", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_CORR, corr_expr))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(corr_expr->add_real_param_expr(parma_expr1))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(corr_expr->add_real_param_expr(parma_expr2))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(expand_corr_expr(corr_expr, power_param_expr, new_aggr_items))) {
      LOG_WARN("failed to expand corr expr", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_POWER, power_expr))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(power_expr->add_param_expr(power_param_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    } else if (OB_FAIL(power_expr->add_param_expr(two_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_EQ,
                                                                   eq_left_expr1,
                                                                   zero_expr,
                                                                   eq_expr1))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                            eq_expr1,
                                                            one_expr,
                                                            power_expr,
                                                            case_when_expr2))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_EQ,
                                                                   eq_left_expr2,
                                                                   zero_expr,
                                                                   eq_expr2))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                            eq_expr2,
                                                            null_expr,
                                                            case_when_expr2,
                                                            case_when_expr1))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else {
      replace_expr = case_when_expr1;
    }
  }
  return ret;
}

//前提:(expr1, expr2) 两个expr都不为NULL
//REGR_AVGX(expr1, expr2) = AVG(expr2);
//REGR_AVGY(expr1, expr2) = AVG(expr1);
int ObExpandAggregateUtils::expand_regr_avg_expr(ObAggFunRawExpr *aggr_expr,
                                                 ObRawExpr *&replace_expr,
                                                 ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr1 = NULL;
  ObRawExpr *parma_expr2 = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY((aggr_expr->get_expr_type() != T_FUN_REGR_AVGX &&
                   aggr_expr->get_expr_type() != T_FUN_REGR_AVGY) ||
                  aggr_expr->get_real_param_exprs().count() != 2) ||
      OB_ISNULL(parma_expr1 = aggr_expr->get_real_param_exprs().at(0)) ||
      OB_ISNULL(parma_expr2 = aggr_expr->get_real_param_exprs().at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *sum_expr = NULL;
    ObAggFunRawExpr *count_expr = NULL;
    ObRawExpr *div_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    ObRawExpr *then_expr = aggr_expr->get_expr_type() == T_FUN_REGR_AVGX ? parma_expr2 : parma_expr1;
    if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                             session_info_,
                                             parma_expr1,
                                             parma_expr2,
                                             then_expr,
                                             case_when_expr))) {
    LOG_WARN("failed to build special case when expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              case_when_expr,
                                                              sum_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_COUNT,
                                                              case_when_expr,
                                                              count_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, count_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_DIV,
                                                                   sum_expr,
                                                                   count_expr,
                                                                   div_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else {
      replace_expr = div_expr;
    }
  }
  return ret;
}

//前提:(expr1, expr2) 两个expr都不为NULL
//REGR_SXX(expr1, expr2) = REGR_COUNT(expr1, expr2) * VAR_POP(expr2)
//REGR_SYY(expr1, expr2) = REGR_COUNT(expr1, expr2) * VAR_POP(expr1)
//REGR_SXY(expr1, expr2) = REGR_COUNT(expr1, expr2) * COVAR_POP(expr1, expr2)
int ObExpandAggregateUtils::expand_regr_s_expr(ObAggFunRawExpr *aggr_expr,
                                               ObRawExpr *&replace_expr,
                                               ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr1 = NULL;
  ObRawExpr *parma_expr2 = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY((aggr_expr->get_expr_type() != T_FUN_REGR_SXX &&
                   aggr_expr->get_expr_type() != T_FUN_REGR_SYY &&
                   aggr_expr->get_expr_type() != T_FUN_REGR_SXY) ||
                  aggr_expr->get_real_param_exprs().count() != 2) ||
      OB_ISNULL(parma_expr1 = aggr_expr->get_real_param_exprs().at(0)) ||
      OB_ISNULL(parma_expr2 = aggr_expr->get_real_param_exprs().at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *regr_count_expr = NULL;
    ObAggFunRawExpr *var_pop_expr = NULL;
    ObAggFunRawExpr *covar_pop_expr = NULL;
    ObRawExpr *left_multi_expr = NULL;
    ObRawExpr *right_multi_expr = NULL;
    ObRawExpr *multi_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_REGR_COUNT, regr_count_expr))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(regr_count_expr->add_real_param_expr(parma_expr1))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else if (OB_FAIL(regr_count_expr->add_real_param_expr(parma_expr2))) {
      LOG_WARN("fail to add param expr to agg expr", K(ret));
    } else {
      if (OB_FAIL(expand_regr_count_expr(regr_count_expr, left_multi_expr, new_aggr_items))) {
        LOG_WARN("failed to expand regr count expr", K(ret));
      } else if (aggr_expr->get_expr_type() == T_FUN_REGR_SXX ||
                 aggr_expr->get_expr_type() == T_FUN_REGR_SYY) {
        ObRawExpr *then_expr = aggr_expr->get_expr_type() == T_FUN_REGR_SXX ?
                                                                parma_expr2 : parma_expr1;
        if (OB_FAIL(build_special_case_when_expr(expr_factory_,
                                                 session_info_,
                                                 parma_expr1,
                                                 parma_expr2,
                                                 then_expr,
                                                 case_when_expr))) {
          LOG_WARN("failed to build special case when expr", K(ret));
        } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_VAR_POP, var_pop_expr))) {
          LOG_WARN("failed to create expr", K(ret));
        } else if (OB_FAIL(var_pop_expr->add_real_param_expr(case_when_expr))) {
          LOG_WARN("fail to add param expr to agg expr", K(ret));
        } else {
          if (OB_FAIL(expand_var_expr(var_pop_expr, right_multi_expr, new_aggr_items))) {
            LOG_WARN("failed to expand var expr", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                         T_OP_MUL,
                                                                         left_multi_expr,
                                                                         right_multi_expr,
                                                                         multi_expr))) {
            LOG_WARN("failed to build common binary op expr", K(ret));
          } else {
            replace_expr = multi_expr;
          }
        }
      } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_COVAR_POP, covar_pop_expr))) {
        LOG_WARN("failed to create expr", K(ret));
      } else if (OB_FAIL(covar_pop_expr->add_real_param_expr(parma_expr1))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else if (OB_FAIL(covar_pop_expr->add_real_param_expr(parma_expr2))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else {
        if (OB_FAIL(expand_covar_expr(covar_pop_expr, right_multi_expr, new_aggr_items))) {
          LOG_WARN("failed to expand covar expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                       T_OP_MUL,
                                                                       left_multi_expr,
                                                                       right_multi_expr,
                                                                       multi_expr))) {
          LOG_WARN("failed to build common binary op expr", K(ret));
        } else {
          replace_expr = multi_expr;
        }
      }
    }
  }
  return ret;
}

bool ObExpandAggregateUtils::is_valid_aggr_type(const ObItemType aggr_type)
{
  return aggr_type == T_FUN_CORR ||
         aggr_type == T_FUN_COVAR_POP ||
         aggr_type == T_FUN_COVAR_SAMP ||
         aggr_type == T_FUN_VAR_POP ||
         aggr_type == T_FUN_VAR_SAMP ||
         aggr_type == T_FUN_REGR_SLOPE ||
         aggr_type == T_FUN_REGR_INTERCEPT ||
         aggr_type == T_FUN_REGR_COUNT ||
         aggr_type == T_FUN_REGR_R2 ||
         aggr_type == T_FUN_REGR_AVGX ||
         aggr_type == T_FUN_REGR_AVGY ||
         aggr_type == T_FUN_REGR_SXX ||
         aggr_type == T_FUN_REGR_SYY ||
         aggr_type == T_FUN_REGR_SXY ||
         aggr_type == T_FUN_KEEP_AVG ||
         aggr_type == T_FUN_KEEP_STDDEV ||
         aggr_type == T_FUN_KEEP_VARIANCE ||
         aggr_type == T_FUN_AVG ||
         aggr_type == T_FUN_VARIANCE ||
         aggr_type == T_FUN_STDDEV ||
         aggr_type == T_FUN_STDDEV_POP ||
         aggr_type == T_FUN_STDDEV_SAMP ||
         aggr_type == T_FUN_APPROX_COUNT_DISTINCT;
}

bool ObExpandAggregateUtils::is_regr_expr_type(const ObItemType aggr_type)
{
  return aggr_type == T_FUN_REGR_SLOPE ||
         aggr_type == T_FUN_REGR_INTERCEPT ||
         aggr_type == T_FUN_REGR_COUNT ||
         aggr_type == T_FUN_REGR_R2 ||
         aggr_type == T_FUN_REGR_AVGX ||
         aggr_type == T_FUN_REGR_AVGY ||
         aggr_type == T_FUN_REGR_SXX ||
         aggr_type == T_FUN_REGR_SYY ||
         aggr_type == T_FUN_REGR_SXY;
}

int ObExpandAggregateUtils::build_special_case_when_expr(ObRawExprFactory &expr_factory,
                                                         const ObSQLSessionInfo *session,
                                                         ObRawExpr *param_expr1,
                                                         ObRawExpr *param_expr2,
                                                         ObRawExpr *then_expr,
                                                         ObRawExpr *&case_when_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2) || OB_ISNULL(then_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr1), K(param_expr2), K(then_expr), K(ret));
  } else {
    ObRawExpr *null_expr = NULL;
    ObRawExpr *is_not_expr1 = NULL;
    ObRawExpr *is_not_expr2 = NULL;
    ObRawExpr *and_expr = NULL;
    ObRawExpr *cast_null_expr = NULL;
    case_when_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory, null_expr))) {
      LOG_WARN("failed to build null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_expr(expr_factory,
                                                         param_expr1,
                                                         null_expr,
                                                         is_not_expr1))) {
      LOG_WARN("failed to build is not expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_expr(expr_factory,
                                                         param_expr2,
                                                         null_expr,
                                                         is_not_expr2))) {
      LOG_WARN("failed to build is not expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory,
                                                                   T_OP_AND,
                                                                   is_not_expr1,
                                                                   is_not_expr2,
                                                                   and_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(&expr_factory,
                                                                session,
                                                                *null_expr,
                                                                then_expr->get_result_type(),
                                                                cast_null_expr))) {
      LOG_WARN("failed to build cast null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory,
                                                            and_expr,
                                                            then_expr,
                                                            cast_null_expr,
                                                            case_when_expr))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObExpandAggregateUtils::expand_keep_aggr_expr(ObAggFunRawExpr *aggr_expr,
                                                  ObRawExpr *&replace_expr,
                                                  ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(aggr_expr) || OB_UNLIKELY(!is_keep_aggr_type(aggr_expr->get_expr_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else if (aggr_expr->get_expr_type() == T_FUN_KEEP_AVG) {
    if (OB_FAIL(expand_keep_avg_expr(aggr_expr,
                                     replace_expr,
                                     new_aggr_items))) {
      LOG_WARN("failed to expand keep avg expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_KEEP_STDDEV) {
    if (OB_FAIL(expand_keep_stddev_expr(aggr_expr,
                                        replace_expr,
                                        new_aggr_items))) {
      LOG_WARN("failed to expand keep stddev expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_KEEP_VARIANCE) {
    if (OB_FAIL(expand_keep_variance_expr(aggr_expr,
                                          replace_expr,
                                          new_aggr_items))) {
      LOG_WARN("failed to expand keep variance expr", K(ret));
    }
  } else {/*do nothing*/}
  return ret;
}

/*avg(expr) keep(...) <==> sum(expr) keep(...) / count(expr) keep(...)
 */
int ObExpandAggregateUtils::expand_keep_avg_expr(ObAggFunRawExpr *aggr_expr,
                                                 ObRawExpr *&replace_expr,
                                                 ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_KEEP_AVG ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *keep_sum_expr = NULL;
    ObAggFunRawExpr *keep_count_expr = NULL;
    ObRawExpr *div_expr = NULL;
    const ObIArray<OrderItem> &order_items = aggr_expr->get_order_items();
    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                       session_info_,
                                                       T_FUN_KEEP_SUM,
                                                       parma_expr,
                                                       keep_sum_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(append(keep_sum_expr->get_order_items_for_update(), order_items))) {
      LOG_WARN("fail to append order items", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, keep_sum_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_KEEP_COUNT,
                                                              parma_expr,
                                                              keep_count_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(append(keep_count_expr->get_order_items_for_update(), order_items))) {
      LOG_WARN("fail to append order items", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, keep_count_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_DIV,
                                                                   keep_sum_expr,
                                                                   keep_count_expr,
                                                                   div_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else {
      replace_expr = div_expr;
    }
  }
  return ret;
}

//方差(variance):
//D(x) <==> if count(x) = 1 ==> (SUM(x*x) - SUM(x)* SUM(x)/ COUNT(x)) / (1) = 0
//        if count(x) > 1 ==> var_samp(x) ==> (SUM(x*x) - SUM(x)* SUM(x)/ COUNT(x)) / (COUNT(x) - 1)
int ObExpandAggregateUtils::expand_keep_variance_expr(ObAggFunRawExpr *aggr_expr,
                                                      ObRawExpr *&replace_expr,
                                                      ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_KEEP_VARIANCE ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObRawExpr *multi_expr = NULL;
    ObRawExpr *multi_keep_sum_expr = NULL;
    ObAggFunRawExpr *keep_sum_expr = NULL;
    ObAggFunRawExpr *keep_sum_product_expr = NULL;
    ObAggFunRawExpr *keep_count_expr = NULL;
    ObRawExpr *div_expr = NULL;
    ObRawExpr *minus_expr = NULL;
    ObRawExpr *keep_count_minus_expr = NULL;
    ObRawExpr *div_minus_expr = NULL;
    ObConstRawExpr *one_expr = NULL;
    ObRawExpr *eq_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    const ObIArray<OrderItem> &order_items = aggr_expr->get_order_items();
    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                       session_info_,
                                                       T_FUN_KEEP_COUNT,
                                                       parma_expr,
                                                       keep_count_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(append(keep_count_expr->get_order_items_for_update(), order_items))) {
      LOG_WARN("fail to append order items", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, keep_count_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_const_number_expr(expr_factory_, ObNumberType,
                                                              number::ObNumber::get_positive_one(),
                                                              one_expr))) {
      LOG_WARN("failed to build const number expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_EQ,
                                                                   keep_count_expr,
                                                                   one_expr,
                                                                   eq_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MUL,
                                                                   parma_expr,
                                                                   parma_expr,
                                                                   multi_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_KEEP_SUM,
                                                              parma_expr,
                                                              keep_sum_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(append(keep_sum_expr->get_order_items_for_update(), order_items))) {
      LOG_WARN("fail to append order items", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, keep_sum_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MUL,
                                                                   keep_sum_expr,
                                                                   keep_sum_expr,
                                                                   multi_keep_sum_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_KEEP_SUM,
                                                              multi_expr,
                                                              keep_sum_product_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(append(keep_sum_product_expr->get_order_items_for_update(), order_items))) {
      LOG_WARN("fail to append order items", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, keep_sum_product_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_DIV,
                                                                   multi_keep_sum_expr,
                                                                   keep_count_expr,
                                                                   div_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MINUS,
                                                                   keep_count_expr,
                                                                   one_expr,
                                                                   keep_count_minus_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_MINUS,
                                                                   keep_sum_product_expr,
                                                                   div_expr,
                                                                   minus_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                            eq_expr,
                                                            one_expr,
                                                            keep_count_minus_expr,
                                                            case_when_expr))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   T_OP_DIV,
                                                                   minus_expr,
                                                                   case_when_expr,
                                                                   div_minus_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));

    } else {
      replace_expr = div_minus_expr;
    }
  }
  return ret;
}

//stddev(expr) <==> sqrt(variance(expr))
int ObExpandAggregateUtils::expand_keep_stddev_expr(ObAggFunRawExpr *aggr_expr,
                                                    ObRawExpr *&replace_expr,
                                                    ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_KEEP_STDDEV ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObSysFunRawExpr *sqrt_expr = NULL;
    ObRawExpr *sqrt_param_expr = NULL;
    ObAggFunRawExpr *keep_variance_expr = NULL;
    const ObIArray<OrderItem> &order_items = aggr_expr->get_order_items();
    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                       session_info_,
                                                       T_FUN_KEEP_VARIANCE,
                                                       parma_expr,
                                                       keep_variance_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(append(keep_variance_expr->get_order_items_for_update(), order_items))) {
      LOG_WARN("fail to append order items", K(ret));
    } else {
      if (OB_FAIL(expand_keep_variance_expr(keep_variance_expr,
                                            sqrt_param_expr, new_aggr_items))) {
        LOG_WARN("fail to expand keep variance expr", K(ret));
      } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_SQRT, sqrt_expr))) {
        LOG_WARN("failed to crate sqrt expr", K(ret));
      } else if (OB_ISNULL(sqrt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add expr is null", K(ret), K(sqrt_expr));
      } else if (OB_FAIL(sqrt_expr->add_param_expr(sqrt_param_expr))) {
        LOG_WARN("add text expr to add expr failed", K(ret));
      } else {
        ObString func_name = ObString::make_string("sqrt");
        sqrt_expr->set_func_name(func_name);
        replace_expr = sqrt_expr;
      }
    }
  }
  return ret;
}

int ObExpandAggregateUtils::expand_common_aggr_expr(ObAggFunRawExpr *aggr_expr,
                                                    ObRawExpr *&replace_expr,
                                                    ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(aggr_expr) || OB_UNLIKELY(!is_common_aggr_type(aggr_expr->get_expr_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else if (aggr_expr->get_expr_type() == T_FUN_AVG) {
    if (OB_FAIL(expand_avg_expr(aggr_expr,
                                replace_expr,
                                new_aggr_items))) {
      LOG_WARN("failed to expand avg expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_STDDEV) {
    if (OB_FAIL(expand_stddev_expr(aggr_expr,
                                   replace_expr,
                                   new_aggr_items))) {
      LOG_WARN("failed to expand stddev expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_VARIANCE) {
    if (lib::is_oracle_mode() && OB_FAIL(expand_oracle_variance_expr(aggr_expr,
                                                                       replace_expr,
                                                                       new_aggr_items))) {
      LOG_WARN("failed to expand oracle variance expr", K(ret));
    } else if (lib::is_mysql_mode() && OB_FAIL(expand_mysql_variance_expr(aggr_expr,
                                                                            replace_expr,
                                                                            new_aggr_items))) {
      LOG_WARN("failed to expand mysql variance expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_STDDEV_POP) {
    if (OB_FAIL(expand_stddev_pop_expr(aggr_expr,
                                      replace_expr,
                                      new_aggr_items))) {
      LOG_WARN("failed to expand stddev expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_STDDEV_SAMP) {
    if (OB_FAIL(expand_stddev_samp_expr(aggr_expr,
                                        replace_expr,
                                        new_aggr_items))) {
      LOG_WARN("failed to expand stddev expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() == T_FUN_APPROX_COUNT_DISTINCT) {
    if (OB_FAIL(expand_approx_count_distinct_expr(aggr_expr,
                                                  replace_expr,
                                                  new_aggr_items))) {
      LOG_WARN("failed to expand approxy_count_distinct expr", K(ret));
    }
  } else {/*do nothing*/}
  return ret;
}

/*avg(expr) keep(...) <==> sum(expr) keep(...) / count(expr) keep(...)
 */
int ObExpandAggregateUtils::expand_avg_expr(ObAggFunRawExpr *aggr_expr,
                                            ObRawExpr *&replace_expr,
                                            ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_AVG ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *sum_expr = NULL;
    ObAggFunRawExpr *count_expr = NULL;
    ObRawExpr *div_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                       session_info_,
                                                       T_FUN_SUM,
                                                       parma_expr,
                                                       sum_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else {
      sum_expr->set_param_distinct(aggr_expr->is_param_distinct());
      if (OB_FAIL(add_aggr_item(new_aggr_items, sum_expr))) {
        LOG_WARN("failed to push back aggr item");
      } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                                session_info_,
                                                                T_FUN_COUNT,
                                                                parma_expr,
                                                                count_expr))) {
        LOG_WARN("failed to build common aggr expr", K(ret));
      } else {
        count_expr->set_param_distinct(aggr_expr->is_param_distinct());
        if (OB_FAIL(add_aggr_item(new_aggr_items, count_expr))) {
          LOG_WARN("failed to push back aggr item");
        } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                      expand_for_mv_ ? T_OP_DIV : T_OP_AGG_DIV,
                                                                      sum_expr,
                                                                      count_expr,
                                                                      div_expr))) {
          LOG_WARN("failed to build common binary op expr", K(ret));
        } else {
          replace_expr = div_expr;
        }
      }
    }
  }
  return ret;
}

//oracle 的方差(variance)计算方式为:
//D(x) <==> if count(x) = 1 ==> (SUM(x*x) - SUM(x)* SUM(x)/ COUNT(x)) / (1) = 0
//        if count(x) > 1 ==> var_samp(x) ==> (SUM(x*x) - SUM(x)* SUM(x)/ COUNT(x)) / (COUNT(x) - 1)
int ObExpandAggregateUtils::expand_oracle_variance_expr(ObAggFunRawExpr *aggr_expr,
                                                        ObRawExpr *&replace_expr,
                                                        ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_VARIANCE ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObRawExpr *multi_expr = NULL;
    ObRawExpr *multi_sum_expr = NULL;
    ObAggFunRawExpr *sum_expr = NULL;
    ObAggFunRawExpr *sum_product_expr = NULL;
    ObAggFunRawExpr *count_expr = NULL;
    ObRawExpr *div_expr = NULL;
    ObRawExpr *minus_expr = NULL;
    ObRawExpr *count_minus_expr = NULL;
    ObRawExpr *div_minus_expr = NULL;
    ObConstRawExpr *one_expr = NULL; // oracle mode count result type is number
    ObRawExpr *eq_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                       session_info_,
                                                       T_FUN_COUNT,
                                                       parma_expr,
                                                       count_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else {
      count_expr->set_param_distinct(aggr_expr->is_param_distinct());
      if (OB_FAIL(add_aggr_item(new_aggr_items, count_expr))) {
        LOG_WARN("failed to push back aggr item");
      } else if (OB_FAIL(ObRawExprUtils::build_const_number_expr(expr_factory_, ObNumberType,
                                                              number::ObNumber::get_positive_one(),
                                                              one_expr))) {
        LOG_WARN("failed to build const number expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                    T_OP_EQ,
                                                                    count_expr,
                                                                    one_expr,
                                                                    eq_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                    T_OP_MUL,
                                                                    parma_expr,
                                                                    parma_expr,
                                                                    multi_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                                session_info_,
                                                                T_FUN_SUM,
                                                                parma_expr,
                                                                sum_expr))) {
        LOG_WARN("failed to build common aggr expr", K(ret));
      } else {
        sum_expr->set_param_distinct(aggr_expr->is_param_distinct());
        if (OB_FAIL(add_aggr_item(new_aggr_items, sum_expr))) {
          LOG_WARN("failed to push back aggr item");
        } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                      T_OP_MUL,
                                                                      sum_expr,
                                                                      sum_expr,
                                                                      multi_sum_expr))) {
          LOG_WARN("failed to build common binary op expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                                  session_info_,
                                                                  T_FUN_SUM,
                                                                  multi_expr,
                                                                  sum_product_expr))) {
          LOG_WARN("failed to build common aggr expr", K(ret));
        } else {
          sum_product_expr->set_param_distinct(aggr_expr->is_param_distinct());
          if (OB_FAIL(add_aggr_item(new_aggr_items, sum_product_expr))) {
            LOG_WARN("failed to push back aggr item");
          } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                        T_OP_DIV,
                                                                        multi_sum_expr,
                                                                        count_expr,
                                                                        div_expr))) {
              LOG_WARN("failed to build common binary op expr", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                        T_OP_MINUS,
                                                                        count_expr,
                                                                        one_expr,
                                                                        count_minus_expr))) {
            LOG_WARN("failed to build common binary op expr", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                        T_OP_MINUS,
                                                                        sum_product_expr,
                                                                        div_expr,
                                                                        minus_expr))) {
            LOG_WARN("failed to build common binary op expr", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                                  eq_expr,
                                                                  one_expr,
                                                                  count_minus_expr,
                                                                  case_when_expr))) {
          } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                        T_OP_DIV,
                                                                        minus_expr,
                                                                        case_when_expr,
                                                                        div_minus_expr))) {
            LOG_WARN("failed to build common binary op expr", K(ret));
          } else {
            replace_expr = div_minus_expr;
          }
        }
      }
    }
  }
  return ret;
}

//mysql模式的方差计算公式为(variance): avg(expr1*expr1) - avg(expr1)*avg(expr1)
int ObExpandAggregateUtils::expand_mysql_variance_expr(ObAggFunRawExpr *aggr_expr,
                                                       ObRawExpr *&replace_expr,
                                                       ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY((aggr_expr->get_expr_type() != T_FUN_VARIANCE &&
                   aggr_expr->get_expr_type() != T_FUN_VAR_POP) ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObAggFunRawExpr *sum_expr = NULL;
    ObRawExpr *cast_sum_expr = NULL;
    ObAggFunRawExpr *count_expr = NULL;
    ObAggFunRawExpr *sum_product_expr = NULL;
    ObRawExpr *cast_sum_product_expr = NULL;
    ObAggFunRawExpr *count_product_expr = NULL;
    ObRawExpr *minus_expr = NULL;
    ObRawExpr *multi_expr = NULL;
    ObRawExpr *multi_sum_expr = NULL;
    ObRawExpr *multi_count_expr = NULL;
    ObRawExpr *div_expr = NULL;
    ObRawExpr *div_multi_expr = NULL;
    ObRawExpr *cast_minus_expr = NULL;
    //由于目前 mysql模式下的除法实现有点问题，存在部分精度不一致，因此这里暂时先添加cast显示转换规避
    ObExprResType dst_type;
    dst_type.set_number();
    dst_type.set_scale(ObAccuracy::MAX_ACCURACY2[MYSQL_MODE][ObNumberType].get_scale());
    dst_type.set_precision(ObAccuracy::MAX_ACCURACY2[MYSQL_MODE][ObNumberType].get_precision());
    ObExprResType result_type;
    result_type.set_double();
    result_type.set_scale(ObAccuracy(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET).get_scale());
    result_type.set_precision(ObAccuracy(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET).get_precision());
    if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                            expand_for_mv_ ? T_OP_MUL : T_OP_AGG_MUL,
                                                            parma_expr,
                                                            parma_expr,
                                                            multi_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              parma_expr,
                                                              sum_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(add_cast_expr(sum_expr, dst_type, cast_sum_expr))) {
      LOG_WARN("failed to add cast expr");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_SUM,
                                                              multi_expr,
                                                              sum_product_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, sum_product_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(add_cast_expr(sum_product_expr, dst_type, cast_sum_product_expr))) {
      LOG_WARN("failed to add cast expr");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_COUNT,
                                                              parma_expr,
                                                              count_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, count_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                              session_info_,
                                                              T_FUN_COUNT,
                                                              multi_expr,
                                                              count_product_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else if (OB_FAIL(add_aggr_item(new_aggr_items, count_product_expr))) {
      LOG_WARN("failed to push back aggr item");
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                            expand_for_mv_ ? T_OP_MUL : T_OP_AGG_MUL,
                                                            cast_sum_expr,
                                                            cast_sum_expr,
                                                            multi_sum_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   expand_for_mv_ ? T_OP_MUL : T_OP_AGG_MUL,
                                                                   count_expr,
                                                                   count_expr,
                                                                   multi_count_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   expand_for_mv_ ? T_OP_DIV : T_OP_AGG_DIV,
                                                                   multi_sum_expr,
                                                                   multi_count_expr,
                                                                   div_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   expand_for_mv_ ? T_OP_DIV : T_OP_AGG_DIV,
                                                                   cast_sum_product_expr,
                                                                   count_product_expr,
                                                                   div_multi_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                   expand_for_mv_ ? T_OP_MINUS : T_OP_AGG_MINUS,
                                                                   div_multi_expr,
                                                                   div_expr,
                                                                   minus_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(add_cast_expr(minus_expr, result_type, cast_minus_expr))) {
      LOG_WARN("failed to add cast expr");
    } else {
      replace_expr = cast_minus_expr;
    }
  }
  return ret;
}

//stddev(expr) <==> sqrt(variance(expr))
int ObExpandAggregateUtils::expand_stddev_expr(ObAggFunRawExpr *aggr_expr,
                                               ObRawExpr *&replace_expr,
                                               ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_STDDEV ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObSysFunRawExpr *sqrt_expr = NULL;
    ObRawExpr *sqrt_param_expr = NULL;
    ObAggFunRawExpr *variance_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                       session_info_,
                                                       T_FUN_VARIANCE,
                                                       parma_expr,
                                                       variance_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else {
      variance_expr->set_param_distinct(aggr_expr->is_param_distinct());
      if (lib::is_oracle_mode() &&
          OB_FAIL(expand_oracle_variance_expr(variance_expr,
                                              sqrt_param_expr, new_aggr_items))) {
        LOG_WARN("fail to expand oracle variance expr", K(ret));
      } else if (lib::is_mysql_mode() &&
                 OB_FAIL(expand_mysql_variance_expr(variance_expr,
                                                    sqrt_param_expr, new_aggr_items))) {
        LOG_WARN("fail to expand mysql variance expr", K(ret));
      } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_SQRT, sqrt_expr))) {
        LOG_WARN("failed to crate sqrt expr", K(ret));
      } else if (OB_ISNULL(sqrt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add expr is null", K(ret), K(sqrt_expr));
      } else if (OB_FAIL(sqrt_expr->add_param_expr(sqrt_param_expr))) {
        LOG_WARN("add text expr to add expr failed", K(ret));
      } else {
        ObString func_name = ObString::make_string("sqrt");
        sqrt_expr->set_func_name(func_name);
        replace_expr = sqrt_expr;
      }
    }
  }
  return ret;
}

//stddev_pop(expr) <==> sqrt(var_pop(expr))
int ObExpandAggregateUtils::expand_stddev_pop_expr(ObAggFunRawExpr *aggr_expr,
                                                   ObRawExpr *&replace_expr,
                                                   ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_STDDEV_POP ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObSysFunRawExpr *sqrt_expr = NULL;
    ObRawExpr *sqrt_param_expr = NULL;
    ObAggFunRawExpr *var_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                       session_info_,
                                                       T_FUN_VAR_POP,
                                                       parma_expr,
                                                       var_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else {
      if (OB_FAIL(expand_var_expr(var_expr,
                                  sqrt_param_expr, new_aggr_items))) {
        LOG_WARN("fail to expand keep variance expr", K(ret));
      } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_SQRT, sqrt_expr))) {
        LOG_WARN("failed to crate sqrt expr", K(ret));
      } else if (OB_ISNULL(sqrt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add expr is null", K(ret), K(sqrt_expr));
      } else if (OB_FAIL(sqrt_expr->add_param_expr(sqrt_param_expr))) {
        LOG_WARN("add text expr to add expr failed", K(ret));
      } else {
        ObString func_name = ObString::make_string("sqrt");
        sqrt_expr->set_func_name(func_name);
        replace_expr = sqrt_expr;
      }
    }
  }
  return ret;
}

//stddev_samp(expr) <==> sqrt(var_samp(expr))
int ObExpandAggregateUtils::expand_stddev_samp_expr(ObAggFunRawExpr *aggr_expr,
                                                    ObRawExpr *&replace_expr,
                                                    ObIArray<ObAggFunRawExpr*> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parma_expr = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_STDDEV_SAMP ||
                  aggr_expr->get_real_param_exprs().count() != 1) ||
      OB_ISNULL(parma_expr = aggr_expr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr));
  } else {
    ObSysFunRawExpr *sqrt_expr = NULL;
    ObRawExpr *expand_var_expr_inner = NULL;
    ObRawExpr *case_when_expr = NULL;
    ObConstRawExpr *zero_expr = NULL;
    ObAggFunRawExpr *var_expr = NULL;
    ObRawExpr *lt_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory_,
                                                       session_info_,
                                                       T_FUN_VAR_SAMP,
                                                       parma_expr,
                                                       var_expr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    } else {
      if (OB_FAIL(expand_var_expr(var_expr,
                                  expand_var_expr_inner, new_aggr_items))) {
        LOG_WARN("fail to expand keep variance expr", K(ret));
      } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_SQRT, sqrt_expr))) {
        LOG_WARN("failed to crate sqrt expr", K(ret));
      } else if (OB_ISNULL(sqrt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add expr is null", K(ret), K(sqrt_expr));
      } else {
        if (is_oracle_mode()) {
          if (OB_SUCC(ret)) {
            if (OB_FAIL(sqrt_expr->add_param_expr(expand_var_expr_inner))) {
              LOG_WARN("add text expr to add expr failed", K(ret));
            }
          }
        } else {
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                            ObIntType,
                                                            0,
                                                            zero_expr))) {
              LOG_WARN("failed to build const int expr", K(ret));
            } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                                          T_OP_LT,
                                                                          expand_var_expr_inner,
                                                                          zero_expr,
                                                                          lt_expr))) {
              LOG_WARN("failed to build common binary op expr", K(ret));
            } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                                    lt_expr,
                                                                    zero_expr,
                                                                    expand_var_expr_inner,
                                                                    case_when_expr))) {
              LOG_WARN("failed to build the case when expr", K(ret));
            } else if (OB_FAIL(sqrt_expr->add_param_expr(case_when_expr))) {
              LOG_WARN("add text expr to add expr failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObString func_name = ObString::make_string("sqrt");
          sqrt_expr->set_func_name(func_name);
          replace_expr = sqrt_expr;
        }
      }
    }
  }
  return ret;
}

int ObExpandAggregateUtils::expand_approx_count_distinct_expr(ObAggFunRawExpr *aggr_expr,
                                                              ObRawExpr *&replace_expr,
                                                              ObIArray<ObAggFunRawExpr *> &new_aggr_items)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *sys_func_expr = NULL;
  ObAggFunRawExpr *synopsis = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_UNLIKELY(aggr_expr->get_expr_type() != T_FUN_APPROX_COUNT_DISTINCT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(aggr_expr));
  } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS,
                                                         synopsis))) {
    LOG_WARN("failed to create count distinct synopsis", K(ret));
  } else if (OB_ISNULL(synopsis)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("synopsis expr is null", K(ret));
  } else if (OB_FAIL(synopsis->get_real_param_exprs_for_update().assign(
                       aggr_expr->get_real_param_exprs()))) {
    LOG_WARN("failed to assign real param expr for synopsis", K(ret));
  } else if (OB_FAIL(add_aggr_item(new_aggr_items, synopsis))) {
    LOG_WARN("failed to push back synopsis", K(ret));
  } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_ESTIMATE_NDV,
                                                         sys_func_expr))) {
    LOG_WARN("failed to create estimate ndv expr", K(ret));
  } else if (OB_ISNULL(sys_func_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys func expr is null", K(ret), K(sys_func_expr));
  } else if (OB_FAIL(sys_func_expr->add_param_expr(synopsis))) {
    LOG_WARN("failed to add sysnopsis add param", K(ret));
  } else {
    ObString func_name = ObString::make_string("ESTIMATE_NDV");
    sys_func_expr->set_func_name(func_name);
    replace_expr = sys_func_expr;
  }
  return ret;
}

int ObExpandAggregateUtils::add_cast_expr(ObRawExpr *expr,
                                          const ObExprResType &dst_type,
                                          ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *cast_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(expr_factory_,
                                                      expr,
                                                      dst_type,
                                                      cast_expr,
                                                      session_info_))) {
    LOG_WARN("failed to add cast expr", K(ret));
  } else if (OB_ISNULL(cast_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(cast_expr));
  } else if (OB_FAIL(cast_expr->add_flag(IS_OP_OPERAND_IMPLICIT_CAST))) {
    LOG_WARN("failed to add flag", K(ret));
  } else {
    new_expr = cast_expr;
  }
  return ret;
}

int ObExpandAggregateUtils::add_win_exprs(ObSelectStmt *select_stmt,
                                          ObIArray<ObRawExpr*> &replace_exprs,
                                          ObIArray<ObWinFunRawExpr*> &new_win_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else if (replace_exprs.count() > 0 && new_win_exprs.count() > 0) {
    select_stmt->get_window_func_exprs().reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < new_win_exprs.count(); ++i) {
      ObWinFunRawExpr *win_expr = NULL;
      if (OB_ISNULL(new_win_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(new_win_exprs.at(i)));
      } else if (OB_ISNULL(win_expr = select_stmt->get_same_win_func_item(new_win_exprs.at(i)))) {
        if (OB_FAIL(select_stmt->add_window_func_expr(new_win_exprs.at(i)))) {
          LOG_WARN("failed to add window func expr", K(ret));
        }
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < replace_exprs.count(); ++j) {
          if (OB_FAIL(ObRawExprUtils::replace_ref_column(replace_exprs.at(j),
                                                         new_win_exprs.at(i),
                                                         win_expr))) {
            LOG_WARN("failed to replace ref column.", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

