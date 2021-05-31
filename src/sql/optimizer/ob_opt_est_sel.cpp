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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_opt_est_sel.h"
#include <math.h>
#include "common/object/ob_obj_compare.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_basic_session_info.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "share/stat/ob_stat_manager.h"
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/stat/ob_column_stat_cache.h"
#include "share/stat/ob_table_stat.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_join_order.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {
double ObOptEstSel::DEFAULT_COLUMN_DISTINCT_RATIO = EST_DEF_COL_NUM_DISTINCT * 1.0 / OB_EST_DEFAULT_ROW_COUNT;

int ObOptEstSel::calculate_selectivity(const ObEstSelInfo& est_sel_info, const ObIArray<ObRawExpr*>& quals,
    double& selectivity, ObIArray<ObExprSelPair>* all_predicate_sel, ObJoinType join_type, const ObRelIds* left_rel_ids,
    const ObRelIds* right_rel_ids, const double left_row_count, const double right_row_count)
{
  int ret = OB_SUCCESS;
  selectivity = 1.0;
  double expr_selectivity = 1.0;
  ObSEArray<RangeExprs, 3> column_exprs_array;
  double tmp_selectivity = 1.0;
  ObSEArray<ObRawExpr*, 4> join_quals;
  bool is_join_qual = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    bool can_be_extracted = false;
    LOG_TRACE("calc selectivity", "expr", PNAME(const_cast<ObRawExpr*>(quals.at(i))));
    if (OB_ISNULL(quals.at(i))) {  // do nothing
    } else if (OB_FAIL(is_simple_join_condition(*quals.at(i), left_rel_ids, right_rel_ids, is_join_qual, join_quals))) {
      LOG_WARN("failed to check is simple join condition", K(ret));
    } else if (is_join_qual) {
      // calculate single join condition's selectivty, and add it to all_predicate_sel
      if (OB_FAIL(clause_selectivity(est_sel_info,
              quals.at(i),
              tmp_selectivity,
              all_predicate_sel,
              join_type,
              left_rel_ids,
              right_rel_ids,
              left_row_count,
              right_row_count))) {
        LOG_WARN("clause_selectivity error", K(quals.at(i)), K(ret));
      }
    } else if (OB_FAIL(
                   ObOptEstUtils::extract_simple_cond_filters(*quals.at(i), can_be_extracted, column_exprs_array))) {
      LOG_WARN("analysis expr failed", K(ret));
    } else if (can_be_extracted) {
      // calculate single range condition's selectivty, and add it to all_predicate_sel
      if (OB_FAIL(clause_selectivity(est_sel_info,
              quals.at(i),
              tmp_selectivity,
              all_predicate_sel,
              join_type,
              left_rel_ids,
              right_rel_ids,
              left_row_count,
              right_row_count))) {
        LOG_WARN("clause_selectivity error", K(quals.at(i)), K(ret));
      }
    } else if (OB_FAIL(clause_selectivity(est_sel_info,
                   quals.at(i),
                   expr_selectivity,
                   all_predicate_sel,
                   join_type,
                   left_rel_ids,
                   right_rel_ids,
                   left_row_count,
                   right_row_count))) {
      LOG_WARN("clause_selectivity error", K(quals.at(i)), K(ret));
    } else {
      if (fabs(expr_selectivity) < OB_DOUBLE_EPSINON) {
        expr_selectivity = 0;
      } else if (fabs(expr_selectivity - 1) < OB_DOUBLE_EPSINON) {
        expr_selectivity = 1.0;
      } else { /*do nothing*/
      }
      if (expr_selectivity < 0 || expr_selectivity > 1) {
        LOG_WARN(
            "expr selectivity should not less than 0 or bigger than 1", K(expr_selectivity), "expr", *(quals.at(i)));
        expr_selectivity = revise_between_0_1(expr_selectivity);
      }
      selectivity *= expr_selectivity;
      LOG_TRACE("clause selectivity result", K(expr_selectivity), K(selectivity));
    }
  }
  double range_sel = 1.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_array.count(); ++i) {
    if (OB_FAIL(get_range_exprs_sel(est_sel_info, column_exprs_array.at(i), range_sel))) {
      LOG_WARN("Failed to get range_exprs selectivity", K(ret));
    } else {
      selectivity *= range_sel;
    }
  }
  if (OB_SUCC(ret)) {
    double join_selectivity = 1.0;
    if (NULL == left_rel_ids || NULL == right_rel_ids) {
    } else if (1 == join_quals.count()) {
      if (OB_ISNULL(join_quals.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_equal_sel(
                     est_sel_info, *join_quals.at(0), join_selectivity, join_type, left_rel_ids, right_rel_ids))) {
        LOG_WARN("Failed to get equal selectivity", K(ret));
      } else {
        LOG_PRINT_EXPR(DEBUG, "got equal expr selectivity", *join_quals.at(0), K(join_selectivity));
      }
    } else if (join_quals.count() > 1) {
      // multiple join conditions, check if contain composite pk
      if (OB_FAIL(get_equal_sel(est_sel_info,
              join_quals,
              join_selectivity,
              join_type,
              *left_rel_ids,
              *right_rel_ids,
              left_row_count,
              right_row_count))) {
        LOG_WARN("failed to get equal sel");
      } else {
        LOG_TRACE("got equal expr selectivity on multi columns", K(join_selectivity));
      }
    } else { /* do nothing */
    }
    if (OB_SUCC(ret)) {
      selectivity *= join_selectivity;
    }
  }
  return ret;
}

int ObOptEstSel::check_mutex_or(const ObRawExpr* ref_expr, int64_t index, bool& is_mutex)
{
  int ret = OB_SUCCESS;
  bool is_simple = false;
  is_mutex = true;
  const ObRawExpr* column_index = NULL;
  const ObRawExpr* column_tmp = NULL;
  if (OB_ISNULL(ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL expr", K(ret));
  } else if (0 > index || index >= ref_expr->get_param_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index out of range", K(ref_expr->get_param_count()), K(index), K(ret));
  } else if (OB_FAIL(get_simple_eq_op_exprs(ref_expr->get_param_expr(index), column_index, is_simple))) {
    LOG_WARN("failed to get simple expr's column", K(ret));
  } else if (!is_simple || 0 == index) {
    is_mutex = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_mutex && i < index; i++) {
      column_tmp = NULL;
      if (OB_FAIL(get_simple_eq_op_exprs(ref_expr->get_param_expr(i), column_tmp, is_simple))) {
        LOG_WARN("failed to get simple expr's column", K(ret));
      } else if (!is_simple) {
        is_mutex = false;
      } else {
        is_mutex = is_mutex && column_index->same_as(*column_tmp);
      }
    }
  }
  return ret;
}

int ObOptEstSel::get_simple_eq_op_exprs(const ObRawExpr* qual, const ObRawExpr*& column, bool& is_simple)
{
  int ret = OB_SUCCESS;
  column = NULL;
  is_simple = false;
  if (OB_ISNULL(qual)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(qual));
  } else if (T_OP_EQ == qual->get_expr_type() || T_OP_NSEQ == qual->get_expr_type()) {
    // Get exprs like c1 = value
    if (OB_UNLIKELY(2 != qual->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong argument for eq expr", K(qual->get_param_count()), K(ret));
    } else if (T_REF_COLUMN == qual->get_param_expr(0)->get_expr_type() &&
               !qual->get_param_expr(1)->has_flag(CNT_COLUMN)) {
      // in left
      column = qual->get_param_expr(0);
      is_simple = true;
    } else if (T_REF_COLUMN == qual->get_param_expr(1)->get_expr_type() &&
               !qual->get_param_expr(0)->has_flag(CNT_COLUMN)) {
      // in right
      column = qual->get_param_expr(1);
      is_simple = true;
    }
  }
  return ret;
}

int ObOptEstSel::clause_selectivity(const ObEstSelInfo& est_sel_info, const ObRawExpr* qual, double& selectivity,
    ObIArray<ObExprSelPair>* all_predicate_sel, ObJoinType join_type, const ObRelIds* left_rel_ids,
    const ObRelIds* right_rel_ids, const double left_row_count, const double right_row_count)
{
  int ret = OB_SUCCESS;
  selectivity = 1.0;
  if (OB_ISNULL(qual)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument of NULL pointer", K(qual), K(ret));
  } else {
    if (qual->has_flag(CNT_AGG)) {
      if (OB_FAIL(get_agg_sel(est_sel_info, *qual, selectivity, left_row_count, right_row_count))) {
        LOG_WARN("Failed to get agg expr selectivity", K(qual), K(ret));
      } else {
        LOG_PRINT_EXPR(DEBUG, "encounter agg expr in clause_selectivity", qual, K(selectivity));
      }
    } else if (qual->is_const_expr() || qual->has_flag(IS_CALCULABLE_EXPR)) {  // const and calculable expr deal here
      if (OB_FAIL(get_calculable_sel(est_sel_info, *qual, selectivity))) {
        LOG_WARN("Failed to get const expr selectivity", K(qual), K(ret));
      } else {
        LOG_PRINT_EXPR(DEBUG, "encounter const expr in clause_selectivity", qual, K(selectivity));
      }
    } else if (qual->is_column_ref_expr()) {
      if (OB_FAIL(get_column_sel(est_sel_info, *qual, selectivity))) {
        LOG_WARN("Failed to get column selectivity", K(ret), K(qual));
      }
      LOG_PRINT_EXPR(DEBUG, "encounter column expr in clause_selectivity", qual, K(selectivity));
    } else if (T_OP_EQ == qual->get_expr_type() || T_OP_NSEQ == qual->get_expr_type()) {
      if (OB_FAIL(get_equal_sel(est_sel_info, *qual, selectivity, join_type, left_rel_ids, right_rel_ids))) {
        LOG_WARN("Failed to get equal selectivity", K(ret));
      } else {
        LOG_PRINT_EXPR(DEBUG, "got equal expr selectivity", qual, K(selectivity));
      }
    } else if (IS_RANGE_CMP_OP(qual->get_expr_type())) {
      if (OB_FAIL(get_range_cmp_sel(est_sel_info, *qual, selectivity))) {
        LOG_WARN("Failed to get range cmp sel", K(ret));
      } else {
        LOG_PRINT_EXPR(DEBUG, "got range predicate selectivity", qual, K(selectivity));
      }
    } else if (T_OP_AND == qual->get_expr_type()) {
      LOG_TRACE("start to calculate T_OP_AND selectivity in clause_selectivity");
      for (int64_t i = 0; OB_SUCC(ret) && i < qual->get_param_count(); ++i) {
        const ObRawExpr* sub_expr = qual->get_param_expr(i);
        double expr_selec = 1.0;
        if (OB_FAIL(clause_selectivity(est_sel_info,
                sub_expr,
                expr_selec,
                all_predicate_sel,
                join_type,
                left_rel_ids,
                right_rel_ids,
                left_row_count,
                right_row_count))) {
          LOG_WARN("clause_selectivity error", K(sub_expr), K(ret));
        } else {
          selectivity *= expr_selec;
        }
      }
      LOG_TRACE("Get and selectivity", KPNAME(qual), K(selectivity));
    } else if (T_OP_OR == qual->get_expr_type()) {
      LOG_TRACE("start to calculate T_OP_OR selectivity in clause_selectivity");
      double tmp_selec = 1.0;
      selectivity = 0;
      bool is_mutex = false;
      ;
      for (int64_t i = 0; OB_SUCC(ret) && i < qual->get_param_count(); ++i) {
        if (OB_FAIL(clause_selectivity(est_sel_info,
                qual->get_param_expr(i),
                tmp_selec,
                all_predicate_sel,
                join_type,
                left_rel_ids,
                right_rel_ids,
                left_row_count,
                right_row_count))) {
          LOG_WARN("clause_selectivity error", K(qual->get_param_expr(i)), K(ret));
        } else if (OB_FAIL(check_mutex_or(qual, i, is_mutex))) {
          LOG_WARN("failed to check independent", K(ret));
        } else if (is_mutex) {
          selectivity += tmp_selec;
        } else {
          selectivity += tmp_selec - tmp_selec * selectivity;
        }
      }
      LOG_TRACE("Get or selectivity", KPNAME(qual), K(selectivity));
    } else if (T_OP_NOT == qual->get_expr_type()) {
      if (OB_FAIL(get_not_sel(est_sel_info,
              *qual,
              selectivity,
              all_predicate_sel,
              join_type,
              left_rel_ids,
              right_rel_ids,
              left_row_count,
              right_row_count))) {
        LOG_WARN("Failed to get NOT expr selectivity", K(ret));
      } else {
        LOG_TRACE("Get not selectivity", K(selectivity));
      }
    } else if (T_OP_IN == qual->get_expr_type() ||
               T_OP_NOT_IN == qual->get_expr_type()) {  // c1 in(1,2,3),c1 in(c3, 3),1 in (c1, 2)
      if (OB_FAIL(get_in_sel(est_sel_info, *qual, selectivity, join_type, left_rel_ids, right_rel_ids))) {
        LOG_WARN("get_in_sel error", K(qual), K(ret));
      } else {
        LOG_PRINT_EXPR(DEBUG, "Get in selectivity", qual, K(selectivity));
      }
    } else if (T_OP_IS == qual->get_expr_type()            // var is NULL|TRUE|FALSE
               || T_OP_IS_NOT == qual->get_expr_type()) {  // VAR IS NOT NULL|TRUE|FALSE
      const ObRawExpr* left_expr = NULL;
      const ObRawExpr* right_expr = NULL;
      if (OB_ISNULL(left_expr = qual->get_param_expr(0)) || OB_ISNULL(right_expr = qual->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Left and right expr should not be NULL", K(ret), K(left_expr), K(right_expr));
      } else if (OB_FAIL(get_is_or_not_sel(
                     est_sel_info, *left_expr, *right_expr, T_OP_IS == qual->get_expr_type(), selectivity))) {
        LOG_WARN("Failed to get IS expr selectivity", K(ret));
      } else {
        LOG_TRACE("Get is or not selectivity", KPNAME(qual), K(selectivity));
      }
    } else if (T_OP_LIKE == qual->get_expr_type()) {
      if (OB_FAIL(get_like_sel(est_sel_info, *qual, selectivity))) {
        LOG_WARN("Failed to get like selectivity", K(ret));
      } else {
        LOG_TRACE("Get like sel", KPNAME(qual), K(selectivity));
      }
    } else if (T_OP_BTW == qual->get_expr_type() || T_OP_NOT_BTW == qual->get_expr_type()) {
      if (OB_FAIL(get_btw_or_not_sel(est_sel_info, *qual, selectivity))) {
        LOG_WARN("Failed to get between or not between expr selectivity", K(ret));
      } else {
        LOG_TRACE("Get btw or not sel", KPNAME(qual), K(selectivity));
      }
    } else if (T_OP_NE == qual->get_expr_type()) {
      if (OB_FAIL(get_ne_sel(est_sel_info, *qual, selectivity))) {
        LOG_WARN("Failed to get not equal sel", K(ret));
      } else {
        LOG_TRACE("Get not equal selectivity", KPNAME(qual), K(selectivity));
      }
    } else {
      selectivity = DEFAULT_SEL;
      LOG_TRACE("Not deal selectivity calc now", KPNAME(qual), K(selectivity));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(all_predicate_sel)) {
      // We remember each predicate's selectivity in the plan so that we can reorder them
      // in the vector of filters according to their selectivity.
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS !=
                      (tmp_ret = add_var_to_array_no_dup(*all_predicate_sel, ObExprSelPair(qual, selectivity))))) {
        LOG_WARN("failed to add selectivity to plan", K(tmp_ret), K(qual), K(selectivity));
      }
    }
  }
  return ret;
}

int ObOptEstSel::get_calculable_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity)
{
  int ret = OB_SUCCESS;
  const ParamStore* params = est_sel_info.get_params();
  const ObDMLStmt* stmt = est_sel_info.get_stmt();
  if (OB_ISNULL(params) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params is NULL", K(ret), K(params), K(stmt));
  } else if (OB_UNLIKELY(!qual.has_flag(IS_CALCULABLE_EXPR) && !qual.is_const_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("qual is not a const expr", K(ret), K(qual));
  } else if (ObOptEstUtils::is_calculable_expr(qual, params->count())) {
    ObObj value;
    bool is_true = false;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(stmt->get_stmt_type(),
            const_cast<ObSQLSessionInfo*>(est_sel_info.get_session_info()),
            &qual,
            value,
            params,
            const_cast<ObIAllocator&>(est_sel_info.get_allocator())))) {
      LOG_WARN("Failed to get const or calculable expr value", K(ret));
    } else if (OB_FAIL(ObObjEvaluator::is_true(value, is_true))) {
      LOG_WARN("Failed to get bool value", K(ret));
    } else {
      selectivity = is_true ? 1.0 : 0.0;
      LOG_TRACE("calc const selectivity", K(value), K(selectivity), K(ret));
    }
  } else {
    selectivity = DEFAULT_SEL;
  }
  return ret;
}

int ObOptEstSel::get_column_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  if (OB_UNLIKELY(!qual.is_column_ref_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(qual));
  } else if (!ob_is_string_or_lob_type(qual.get_data_type())) {
    bool is_in = false;
    if (OB_FAIL(column_in_current_level_stmt(est_sel_info.get_stmt(), qual, is_in))) {
      LOG_WARN("Failed to check column in cur level stmt", K(ret));
    } else if (is_in) {
      double distinct_sel = EST_DEF_VAR_EQ_SEL;
      double null_sel = EST_DEF_COL_NULL_RATIO;
      if (OB_FAIL(get_var_basic_sel(est_sel_info, qual, &distinct_sel, &null_sel))) {
        LOG_WARN("Failed to calc basic equal sel", K(ret));
      } else {
        selectivity = 1.0 - distinct_sel - null_sel;
      }
    } else {
    }  // do nothing
  } else {
  }  // do nothing
  if (OB_SUCC(ret)) {
    selectivity = revise_between_0_1(selectivity);
  }
  return ret;
}

int ObOptEstSel::get_equal_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity,
    ObJoinType join_type, const ObRelIds* left_rel_ids, const ObRelIds* right_rel_ids)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* left_expr = NULL;
  const ObRawExpr* right_expr = NULL;
  if (OB_UNLIKELY(T_OP_EQ != qual.get_expr_type() && T_OP_NSEQ != qual.get_expr_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid expr type", K(ret), "expr type", qual.get_expr_type());
  } else if (OB_ISNULL(left_expr = qual.get_param_expr(0)) || OB_ISNULL(right_expr = qual.get_param_expr(1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Left expr and right expr can not be NULL", K(left_expr), K(right_expr), K(ret));
  } else if (OB_FAIL(get_equal_sel(est_sel_info,
                 *left_expr,
                 *right_expr,
                 T_OP_NSEQ == qual.get_expr_type(),
                 selectivity,
                 join_type,
                 left_rel_ids,
                 right_rel_ids))) {
    LOG_WARN("Failed to get equal sel", K(ret));
  } else {
  }  // do nothing
  return ret;
}

int ObOptEstSel::get_equal_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& left_expr,
    const ObRawExpr& right_expr, const bool null_safe, double& selectivity, ObJoinType join_type,
    const ObRelIds* left_rel_ids, const ObRelIds* right_rel_ids)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  const ParamStore* params = est_sel_info.get_params();
  const ObDMLStmt* stmt = est_sel_info.get_stmt();
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_ISNULL(params) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(params), K(stmt));
  } else if (T_OP_ROW == left_expr.get_expr_type() && T_OP_ROW == right_expr.get_expr_type()) {
    selectivity = 1.0;
    int64_t num = left_expr.get_param_count() <= right_expr.get_param_count() ? left_expr.get_param_count()
                                                                              : right_expr.get_param_count();
    double single_eq_sel = 1.0;
    const ObRawExpr* l_expr = NULL;
    const ObRawExpr* r_expr = NULL;
    // TODO consider foreign key and union key
    for (int64_t idx = 0; OB_SUCC(ret) && idx < num; ++idx) {
      if (OB_ISNULL(l_expr = left_expr.get_param_expr(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left expr of idx should not be NULL", K(ret), K(idx));
      } else if (OB_ISNULL(r_expr = right_expr.get_param_expr(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("right expr of idx should not be NULL", K(ret), K(idx));
      } else if (OB_FAIL(SMART_CALL(get_equal_sel(est_sel_info,
                     *l_expr,
                     *r_expr,
                     null_safe,
                     single_eq_sel,
                     join_type,
                     left_rel_ids,
                     right_rel_ids)))) {
        LOG_WARN("Failed to get equal sel", K(ret));
      } else {
        selectivity *= single_eq_sel;
      }
    }
  } else if ((left_expr.has_flag(CNT_COLUMN) && !right_expr.has_flag(CNT_COLUMN)) ||
             (!left_expr.has_flag(CNT_COLUMN) && right_expr.has_flag(CNT_COLUMN))) {  // var = const
    // use 1 / ndv
    const ObRawExpr& cnt_col_expr = left_expr.has_flag(CNT_COLUMN) ? left_expr : right_expr;
    const ObRawExpr& calc_expr = left_expr.has_flag(CNT_COLUMN) ? right_expr : left_expr;
    if (OB_FAIL(get_simple_predicate_sel(est_sel_info, cnt_col_expr, &calc_expr, null_safe, selectivity))) {
      LOG_WARN("Failed to get simple predicate selectivity", K(ret));
    } else {
      LOG_TRACE("succeed to get simple predicate sel", K(selectivity), K(ret));
    }
  } else if (left_expr.has_flag(CNT_COLUMN) && right_expr.has_flag(CNT_COLUMN)) {
    if (OB_FAIL(get_cntcol_eq_cntcol_sel(
            est_sel_info, left_expr, right_expr, null_safe, selectivity, join_type, left_rel_ids, right_rel_ids))) {
      LOG_WARN("Failed to get cnt_column equal cnt_column selectivity", K(ret));
    } else {
      LOG_TRACE("succeed to get complex predicate sel", K(selectivity), K(ret));
    }
  } else {
    const ParamStore* params = est_sel_info.get_params();
    if (OB_ISNULL(params)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Params is NULL", K(ret));
    } else if (ObOptEstUtils::is_calculable_expr(left_expr, params->count()) &&
               ObOptEstUtils::is_calculable_expr(right_expr, params->count())) {
      // calculable_expr selectivity calculated in get_calculable_sel, not here.
      // check whether calculable_exprs are equal in cases like:
      //(c1, 1) = (c2, 2), 2 in (2, c1, 10), '1' in (c1, 1)
      //!!!As plan cache use '(c1, 1) = (c2, 2)' and '(c1, 1) = (c1, 1)' the same plan,
      // selectivity just use 0.5 ?
      bool equal = false;
      if (OB_FAIL(ObOptEstUtils::if_expr_value_equal(const_cast<ObOptimizerContext&>(est_sel_info.get_opt_ctx()),
              est_sel_info.get_stmt(),
              left_expr,
              right_expr,
              null_safe,
              equal))) {
        LOG_WARN("Failed to check hvae equal expr", K(ret));
      } else {
        selectivity = equal ? 1.0 : 0.0;
      }
    } else {
      selectivity = DEFAULT_EQ_SEL;
    }
  }
  selectivity = revise_between_0_1(selectivity);
  return ret;
}

int ObOptEstSel::get_is_or_not_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& var_expr,
    const ObRawExpr& value_expr, bool is_op, double& selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  const ParamStore* params = est_sel_info.get_params();
  const ObDMLStmt* stmt = est_sel_info.get_stmt();
  if (OB_ISNULL(params) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(params));
  } else if (var_expr.is_column_ref_expr() && ObOptEstUtils::is_calculable_expr(value_expr, params->count())) {
    bool is_in = false;
    if (OB_FAIL(column_in_current_level_stmt(stmt, var_expr, is_in))) {
      LOG_WARN("Failed to check column whether is in current stmt", K(ret));
    } else if (is_in) {
      ObObj result;
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(stmt->get_stmt_type(),
              const_cast<ObSQLSessionInfo*>(est_sel_info.get_session_info()),
              &value_expr,
              result,
              params,
              const_cast<ObIAllocator&>(est_sel_info.get_allocator())))) {
        LOG_WARN("Failed to calc const or calculable expr", K(ret));
      } else if (result.is_null()) {
        if (OB_FAIL(get_var_basic_sel(est_sel_info, var_expr, NULL, &selectivity))) {
          LOG_WARN("Failed to get var distinct sel", K(ret));
        }
      } else if (result.is_tinyint() && !ob_is_string_or_lob_type(var_expr.get_data_type())) {
        double distinct_sel = 0.0;
        double null_sel = 0.0;
        if (OB_FAIL(get_var_basic_sel(est_sel_info, var_expr, &distinct_sel, &null_sel))) {
          LOG_WARN("Failed to get var distinct sel", K(ret));
        } else {
          if (distinct_sel > (1 - null_sel) / 2.0) {
            // distinct_num < 2. That is distinct_num only 1,(As double and statistics not completely accurate,
            // use (1 - null_sel)/ 2.0 to check)
            // Ihe formula to calc sel of 'c1 is true' is (1 - distinct_sel(var = 0) - null_sel).
            // If distinct_num is 1, the sel would be 0.0.
            // But we don't kown whether distinct value is 0. So gess the selectivity: (1 - null_sel)/2.0
            distinct_sel = (1 - null_sel) / 2.0;  // don't kow the value, just get half.
          }
          selectivity = (result.is_true()) ? (1 - distinct_sel - null_sel) : distinct_sel;
        }
      } else {
      }  // default sel
    } else {
    }  // not in this level, consider as const(default sel)
  } else {
    // TODO func(cnt_column)
  }

  if (!is_op) {
    selectivity = 1.0 - selectivity;
  }
  selectivity = revise_between_0_1(selectivity);
  return ret;
}

int ObOptEstSel::get_ne_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* l_expr = NULL;
  const ObRawExpr* r_expr = NULL;
  selectivity = DEFAULT_SEL;
  if (OB_UNLIKELY(T_OP_NE != qual.get_expr_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("qual expr type should be T_OP_NE", "type", qual.get_expr_type(), K(ret));
  } else if (OB_ISNULL(l_expr = qual.get_param_expr(0)) || OB_ISNULL(r_expr = qual.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NE qual should have two param expr", K(l_expr), K(r_expr), K(ret));
  } else if ((l_expr->has_flag(CNT_COLUMN) && !r_expr->has_flag(CNT_COLUMN)) ||
             (!l_expr->has_flag(CNT_COLUMN) && r_expr->has_flag(CNT_COLUMN))) {
    const ObRawExpr* cnt_col_expr = l_expr->has_flag(CNT_COLUMN) ? l_expr : r_expr;
    const ObRawExpr* const_expr = l_expr->has_flag(CNT_COLUMN) ? r_expr : l_expr;
    ObSEArray<const ObRawExpr*, 2> cur_vars;
    bool only_monotonic_op = true;
    bool null_const = false;
    if (OB_FAIL(ObOptEstUtils::extract_column_exprs_with_op_check(cnt_col_expr, cur_vars, only_monotonic_op))) {
      LOG_WARN("Failed to extract column exprs with op check", K(ret));
    } else if (!only_monotonic_op || cur_vars.count() > 1) {
      selectivity = DEFAULT_SEL;  // cnt_col_expr contain not monotonic op OR has more than 1 var
    } else if (OB_UNLIKELY(1 != cur_vars.count())) {
      selectivity = DEFAULT_SEL;
      LOG_WARN("Some case not considered", K(ret), KPNAME(cnt_col_expr), K(cnt_col_expr));
    } else if (OB_FAIL(ObOptEstUtils::if_expr_value_null(est_sel_info.get_params(),
                   *const_expr,
                   const_cast<ObSQLSessionInfo*>(est_sel_info.get_session_info()),
                   const_cast<ObIAllocator&>(est_sel_info.get_allocator()),
                   null_const))) {
      LOG_WARN("Failed to check whether expr null value", K(ret));
    } else if (null_const) {
      selectivity = 0.0;
    } else if (OB_ISNULL(cur_vars.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Var should not be NULL", K(ret));
    } else {
      double distinct_sel = 1.0;
      double null_sel = 1.0;
      if (OB_FAIL(get_var_basic_sel(est_sel_info, *(cur_vars.at(0)), &distinct_sel, &null_sel))) {
        LOG_WARN("Failed to get var basic sel", K(ret));
      } else if (distinct_sel > ((1 - null_sel) / 2.0)) {
        // The reason doing this is similar as get_is_or_not_sel function.
        // If distinct_num is 1, As formula, selectivity of 'c1 != 1' would be 0.0.
        // But we don't know the distinct value, so just get the half selectivity.
        selectivity = distinct_sel / 2.0;
      } else {
        selectivity = revise_between_0_1(1.0 - distinct_sel - null_sel);
      }
    }
  } else {
  }  // do nothing
  return ret;
}

int ObOptEstSel::get_like_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* left_expr = NULL;
  const ObRawExpr* right_expr = NULL;
  const ObRawExpr* escape_expr = NULL;
  const ParamStore* params = est_sel_info.get_params();
  if (T_OP_LIKE != qual.get_expr_type() || 3 != qual.get_param_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Qual expr should be T_OP_LIKE and have 3 params", K(ret));
  } else if (OB_ISNULL(params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Params in optimizer context should not be NULL", K(ret));
  } else if (OB_ISNULL(left_expr = qual.get_param_expr(0)) || OB_ISNULL(right_expr = qual.get_param_expr(1)) ||
             OB_ISNULL(escape_expr = qual.get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("T_OP_LIKE left expr ,right expr or escape_expr should not be NULL", K(ret));
  } else if (ObOptEstUtils::is_calculable_expr(*escape_expr, params->count())) {
    const ObRawExpr* col_expr = NULL;
    const ObRawExpr* const_expr = NULL;
    if (left_expr->is_column_ref_expr() && ObOptEstUtils::is_calculable_expr(*right_expr, params->count())) {
      col_expr = left_expr;
      const_expr = right_expr;
    } else if (ObOptEstUtils::is_calculable_expr(*left_expr, params->count()) && right_expr->is_column_ref_expr()) {
      col_expr = right_expr;
      const_expr = left_expr;
    } else {
      selectivity = DEFAULT_SEL;
    }
    if (NULL != col_expr) {
      bool is_start_with = false;
      if (static_cast<const ObColumnRefRawExpr*>(col_expr)->is_lob_column()) {
        // there's no statistic for lob type, use default selectivity
        selectivity = DEFAULT_CLOB_LIKE_SEL;
      } else if (OB_FAIL(ObOptEstUtils::if_expr_start_with_patten_sign(params,
                     const_expr,
                     escape_expr,
                     const_cast<ObSQLSessionInfo*>(est_sel_info.get_session_info()),
                     const_cast<ObIAllocator&>(est_sel_info.get_allocator()),
                     is_start_with))) {
        LOG_WARN("failed to check if expr start with percent sign", K(ret));
      } else if (is_start_with) {
        selectivity = DEFAULT_INEQ_SEL;
      } else if (OB_FAIL(get_column_range_sel(
                     est_sel_info, static_cast<const ObColumnRefRawExpr&>(*col_expr), qual, true, selectivity, true))) {
        LOG_WARN("Failed to get column range selectivity", K(ret));
      }
    }
  } else {
    selectivity = DEFAULT_SEL;
  }
  return ret;
}

int ObOptEstSel::get_btw_or_not_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* cmp_expr = NULL;
  const ObRawExpr* l_expr = NULL;
  const ObRawExpr* r_expr = NULL;
  const ParamStore* params = est_sel_info.get_params();
  if ((T_OP_BTW != qual.get_expr_type() && T_OP_NOT_BTW != qual.get_expr_type()) || 3 != qual.get_param_count()) {
    LOG_WARN("Qual expr should be T_OP_BTW or T_OP_OT_BTW and have 3 params", K(ret));
  } else if (OB_ISNULL(params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Params in optimizer context should not be NULL", K(ret));
  } else if (OB_ISNULL(cmp_expr = qual.get_param_expr(0)) || OB_ISNULL(l_expr = qual.get_param_expr(1)) ||
             OB_ISNULL(r_expr = qual.get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cmp_expr,l_expr or r_expr should not be NULL", K(ret));
  } else {
    const ObRawExpr* col_expr = NULL;
    if (cmp_expr->is_column_ref_expr() && ObOptEstUtils::is_calculable_expr(*l_expr, params->count()) &&
        ObOptEstUtils::is_calculable_expr(*r_expr, params->count())) {
      col_expr = cmp_expr;
    } else if (ObOptEstUtils::is_calculable_expr(*cmp_expr, params->count()) && l_expr->is_column_ref_expr() &&
               ObOptEstUtils::is_calculable_expr(*r_expr, params->count())) {
      col_expr = l_expr;
    } else if (ObOptEstUtils::is_calculable_expr(*cmp_expr, params->count()) &&
               ObOptEstUtils::is_calculable_expr(*l_expr, params->count()) && r_expr->is_column_ref_expr()) {
      col_expr = r_expr;
    } else {
      selectivity = DEFAULT_SEL;
    }
    if (NULL != col_expr) {
      if (OB_FAIL(get_column_range_sel(
              est_sel_info, static_cast<const ObColumnRefRawExpr&>(*col_expr), qual, false, selectivity, true))) {
        LOG_WARN("Failed to get column range sel", K(ret));
      }
    }
  }
  return ret;
}

// get basic statistic from :
// 1. est_sel_info
// 2. storage statistic information
// 3. default value
int ObOptEstSel::get_var_basic_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& var, double* distinct_sel_ptr,
    double* null_sel_ptr, double* row_count_ptr /* = NULL */, ObObj* min_obj_ptr /* = NULL */,
    ObObj* max_obj_ptr /* = NULL */, double* origin_row_count_ptr /* = NULL */)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = est_sel_info.get_stmt();
  if (OB_UNLIKELY(!var.is_column_ref_expr()) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument of NULL pointer", K(stmt), K(ret));
  } else {
    const ObColumnRefRawExpr& col_var = static_cast<const ObColumnRefRawExpr&>(var);
    uint64_t tid = col_var.get_table_id();
    const TableItem* table_item = get_table_item_for_statics(*stmt, tid);
    if (OB_ISNULL(table_item)) {  // Can not find table item
      // //considered as DEFAULT_SEL
      // ((distinct_sel != NULL) ? (void)(*distinct_sel = DEFAULT_SEL) : (void) 0);
      // ((null_sel != NULL) ? (void)(*null_sel = DEFAULT_SEL) : (void) 0);
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table_item", K(ret), K(tid));
    } else {

      double distinct_num = 0;
      double null_num = 0;
      double row_count = 0;
      double origin_row_count = 0;
      double burried = 0;
      double ori_burried = 0;
      double tmp_distinct_num = 0;

      bool can_use_statics = table_item->is_basic_table() && !est_sel_info.use_default_stat();
      if (OB_FAIL(get_var_basic_from_est_sel_info(est_sel_info, col_var, distinct_num, row_count, origin_row_count))) {
        LOG_WARN("failed to get var basic from est_sel_info", K(ret));
      } else if (can_use_statics && OB_FAIL(get_var_basic_from_statics(est_sel_info,
                                        col_var,
                                        tmp_distinct_num,
                                        null_num,
                                        row_count == 0 ? row_count : burried,
                                        origin_row_count == 0 ? origin_row_count : ori_burried,
                                        min_obj_ptr,
                                        max_obj_ptr))) {
        LOG_WARN("failed to get var basic from statics", K(ret));
      } else if (distinct_num == 0 && FALSE_IT(distinct_num = tmp_distinct_num)) {
        // never reach
      } else if (tmp_distinct_num == 0 &&
                 OB_FAIL(get_var_basic_default(distinct_num, null_num, row_count, origin_row_count))) {
        LOG_WARN("failed to get var default info", K(ret));
      } else {
        // revise
        if (0 == row_count) {
          row_count = 1;
        }

        if (0 == origin_row_count) {
          origin_row_count = 1;
        }

        if (0 == distinct_num) {
          distinct_num = 1;
        }

        if (null_num > row_count - distinct_num) {
          null_num = row_count - distinct_num > 0 ? row_count - distinct_num : 0;
        }

        double null_sel = revise_between_0_1(null_num / row_count);
        double distinct_sel = revise_between_0_1((1 - null_sel) / distinct_num);
        LOG_TRACE("var basic info", K(row_count), K(null_num), K(distinct_num));

        if (NULL != row_count_ptr) {
          *row_count_ptr = row_count;
        }
        if (NULL != origin_row_count_ptr) {
          *origin_row_count_ptr = origin_row_count;
        }
        if (NULL != distinct_sel_ptr) {
          *distinct_sel_ptr = distinct_sel;
        }
        if (NULL != null_sel_ptr) {
          *null_sel_ptr = null_sel;
        }
      }
    }
  }
  return ret;
}

int ObOptEstSel::get_var_basic_from_est_sel_info(const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& col_var,
    double& distinct_num, double& row_count, double& origin_row_count)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = col_var.get_table_id();
  uint64_t column_id = col_var.get_column_id();
  const ObDMLStmt* stmt = NULL;
  const TableItem* table_item = NULL;

  ObEstAllTableStat& stat = const_cast<ObEstAllTableStat&>(est_sel_info.get_table_stats());

  if (OB_ISNULL(stmt = est_sel_info.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid argument of NULL pointer", K(stmt), K(ret));
  } else if (OB_ISNULL(table_item = get_table_item_for_statics(*stmt, table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table_item", K(ret));
  } else if (OB_SUCCESS != (ret = stat.get_rows(table_id, row_count, origin_row_count))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("cannot found in est_sel_info stat", K(ret));
    } else {
      LOG_WARN("failed to get_rows", K(ret));
    }
  } else if (est_sel_info.get_use_origin_stat()) {
    row_count = origin_row_count;
    ret = stat.get_origin_distinct(table_id, column_id, distinct_num);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("cannot found in est_sel_info stat", K(table_id), K(column_id));
    }
  } else if (OB_FAIL(stat.get_distinct(table_id, column_id, distinct_num))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("cannot found in est_sel_info stat", K(ret));
    } else {
      LOG_WARN("failed to get_distinct", K(ret));
    }
  }
  return ret;
}

int ObOptEstSel::get_var_basic_from_statics(const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& col_var,
    double& distinct_num, double& null_num, double& row_count, double& origin_row_count, ObObj*& min_obj,
    ObObj*& max_obj)
{
  int ret = OB_SUCCESS;

  common::ObColumnStatValueHandle cstat;
  common::ObTableStat tstat;

  ObSqlSchemaGuard* schema_guard = NULL;
  ObStatManager* stat_manager = NULL;
  const ObDMLStmt* stmt = NULL;
  const TableItem* table_item = NULL;
  const share::schema::ObTableSchema* table_schema = NULL;

  int64_t part_id = OB_INVALID_INDEX_INT64;
  uint64_t column_id = col_var.get_column_id();
  uint64_t table_id = col_var.get_table_id();
  bool check_dropped_schema = false;
  if (OB_ISNULL(stmt = est_sel_info.get_stmt()) ||
      OB_ISNULL(stat_manager = const_cast<ObStatManager*>(est_sel_info.get_stat_manager())) ||
      OB_ISNULL(schema_guard = const_cast<ObSqlSchemaGuard*>(est_sel_info.get_sql_schema_guard()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid argument of NULL pointer", K(stmt), K(stat_manager), K(schema_guard), K(ret));
  } else if (OB_ISNULL(table_item = get_table_item_for_statics(*stmt, table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table_item", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_item->ref_id_, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(table_item), K(ret));
  } else if (OB_FAIL(est_sel_info.get_table_stats().get_part_id_by_table_id(table_id, part_id))) {
    LOG_WARN("Failed to get part id from est_sel_info", K(ret));
  } else if (OB_INVALID_INDEX_INT64 == part_id &&
             OB_FAIL(ObTablePartitionKeyIter(*table_schema, check_dropped_schema).next_partition_id_v2(part_id))) {
    LOG_WARN("iter failed", K(ret));
  } else if (OB_FAIL(stat_manager->get_table_stat(
                 ObPartitionKey(table_item->ref_id_, part_id, table_schema->get_partition_cnt()), tstat))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else if (OB_FAIL(stat_manager->get_column_stat(table_item->ref_id_, part_id, column_id, cstat))) {
    LOG_WARN("get column stat error", K(ret), K(table_item->ref_id_), K(part_id));
  } else if (OB_ISNULL(cstat.cache_value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column stat is null");
  } else {
    row_count = static_cast<double>(tstat.get_row_count());
    origin_row_count = row_count;
    distinct_num = static_cast<double>(cstat.cache_value_->get_num_distinct());
    null_num = static_cast<double>(cstat.cache_value_->get_num_null());
    // DEFAULT column statics, use NULL
    if (0 != distinct_num && NULL != min_obj && NULL != max_obj) {
      *min_obj = cstat.cache_value_->get_min_value();
      *max_obj = cstat.cache_value_->get_max_value();
      // get global min/max value
      if (1 == table_schema->get_partition_cnt()) {
        ObIAllocator& alloc = const_cast<ObIAllocator&>(est_sel_info.get_allocator());
        if (OB_FAIL(ob_write_obj(alloc, *min_obj, *min_obj))) {
          LOG_WARN("failed to write obj", K(ret), K(*min_obj));
        } else if (OB_FAIL(ob_write_obj(alloc, *max_obj, *max_obj))) {
          LOG_WARN("failed to write obj", K(ret), K(*max_obj));
        }
      } else if (OB_FAIL(get_global_min_max(est_sel_info, col_var, min_obj, max_obj))) {
        LOG_WARN("failed to get global min max");
      }
    }
  }
  return ret;
}

int ObOptEstSel::get_var_basic_default(
    double& distinct_num, double& null_num, double& row_count, double& origin_row_count)
{
  int ret = OB_SUCCESS;
  if (is_number_euqal_to_zero(row_count)) {
    row_count = static_cast<double>(OB_EST_DEFAULT_ROW_COUNT);
  }

  if (is_number_euqal_to_zero(origin_row_count)) {
    origin_row_count = static_cast<double>(OB_EST_DEFAULT_ROW_COUNT);
  }

  if (is_number_euqal_to_zero(distinct_num)) {
    distinct_num = static_cast<double>(row_count * DEFAULT_COLUMN_DISTINCT_RATIO);
  }
  null_num = static_cast<double>(row_count * EST_DEF_COL_NULL_RATIO);

  // revise
  if (distinct_num > row_count) {
    distinct_num = row_count;
  }
  if (null_num > row_count - distinct_num) {
    null_num = row_count - distinct_num;
  }
  return ret;
}

int ObOptEstSel::calc_sel_for_equal_join_cond(const ObEstSelInfo& est_sel_info, const ObIArray<ObRawExpr*>& conds,
    double& left_selectivity, double& right_selectivity)
{
  int ret = OB_SUCCESS;
  double left_selec = 1.0;
  double right_selec = 1.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
    const ObRawExpr* expr = conds.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(expr), K(i), K(ret));
    } else if (!expr->has_flag(IS_JOIN_COND)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid join condition", K(*expr), K(i), K(ret));
    } else {
      const ObRawExpr* left = expr->get_param_expr(0);
      const ObRawExpr* right = expr->get_param_expr(1);
      double expr_selec_left = 1.0;
      double expr_selec_right = 1.0;
      if (OB_ISNULL(left) || OB_ISNULL(right)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument", K(ret));
        // TODO simple predicate
      } else if (OB_FAIL(get_simple_predicate_sel(est_sel_info, *left, NULL, false, expr_selec_left))) {
        LOG_WARN("failed to calc basic equal selectivity", K(left), K(ret));
      } else if (OB_FAIL(get_simple_predicate_sel(est_sel_info, *right, NULL, false, expr_selec_right))) {
        LOG_WARN("failed to calc basic equal selectivity", K(right), K(ret));
      } else {
        left_selec *= expr_selec_left;
        right_selec *= expr_selec_right;
      }
    }
  }
  if (OB_SUCC(ret)) {
    left_selectivity = left_selec;
    right_selectivity = right_selec;
  }
  return ret;
}

int ObOptEstSel::get_simple_predicate_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& cnt_col_expr,
    const ObRawExpr* calculable_expr, const bool null_safe, double& selectivity)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr*, 2> cur_vars;
  bool only_monotonic_op = true;
  if (OB_FAIL(ObOptEstUtils::extract_column_exprs_with_op_check(&cnt_col_expr, cur_vars, only_monotonic_op))) {
    LOG_WARN("Failed to extract column exprs with op check", K(ret));
  } else if (!only_monotonic_op || cur_vars.count() > 1) {
    selectivity = DEFAULT_EQ_SEL;  // cnt_col_expr contain not monotonic op OR has more than 1 var
  } else if (OB_UNLIKELY(1 != cur_vars.count())) {
    selectivity = DEFAULT_EQ_SEL;
    LOG_WARN("Some case not considered", KPNAME(cnt_col_expr), K(cnt_col_expr));
  } else if (OB_ISNULL(cur_vars.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Column expr is NULL", K(ret));
  } else if (!cur_vars.at(0)->is_column_ref_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The expr should be column ref expr", K(ret));
  } else {
    bool get_distinct_sel = true;
    bool sel_got = false;
    const ObColumnRefRawExpr* col_expr = static_cast<const ObColumnRefRawExpr*>(cur_vars.at(0));
    bool null_value = false;
    if (NULL == calculable_expr) {
      get_distinct_sel = true;
    } else if (OB_FAIL(ObOptEstUtils::if_expr_value_null(est_sel_info.get_params(),
                   *calculable_expr,
                   const_cast<ObSQLSessionInfo*>(est_sel_info.get_session_info()),
                   const_cast<ObIAllocator&>(est_sel_info.get_allocator()),
                   null_value))) {
      LOG_WARN("Failed to check whether expr null value", K(ret));
    } else if (null_value) {
      if (null_safe) {
        get_distinct_sel = false;
      } else {
        selectivity = 0.0;
        sel_got = true;
      }
    } else {
      get_distinct_sel = true;
    }
    if (OB_SUCC(ret)) {
      if (sel_got) {
      } else if (get_distinct_sel) {
        if (OB_FAIL(get_var_basic_sel(est_sel_info, *col_expr, &selectivity, NULL))) {
          LOG_WARN("Failed to get var basic sel", K(ret));
        }
      } else if (OB_FAIL(get_var_basic_sel(est_sel_info, *col_expr, NULL, &selectivity))) {
        LOG_WARN("Failed to get var basic sel", K(ret));
      } else {
      }  // do nothing
    }
  }
  return ret;
}

int ObOptEstSel::get_not_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity,
    ObIArray<ObExprSelPair>* all_predicate_sel, ObJoinType join_type, const ObRelIds* left_rel_ids,
    const ObRelIds* right_rel_ids, const double left_row_count, const double right_row_count)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  const ObRawExpr* child_expr = qual.get_param_expr(0);
  double tmp_selec = 1.0;
  if (OB_FAIL(clause_selectivity(est_sel_info,
          child_expr,
          tmp_selec,
          all_predicate_sel,
          join_type,
          left_rel_ids,
          right_rel_ids,
          left_row_count,
          right_row_count))) {
    LOG_WARN("clause_selectivity error", K(child_expr), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 3> cur_vars;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(const_cast<ObRawExpr*>(child_expr), cur_vars))) {
      LOG_WARN("extract column exprs failed", K(ret));
    } else if (OB_ISNULL(child_expr)) {
      // do nothing
    } else if (1 == cur_vars.count() && T_OP_IS != child_expr->get_expr_type() &&
               T_OP_IS_NOT != child_expr->get_expr_type() &&
               T_OP_NSEQ != child_expr->get_expr_type()) {  // for only one column, consider null_sel
      double null_sel = 1.0;
      if (OB_ISNULL(cur_vars.at(0))) {
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(get_var_basic_sel(est_sel_info, *cur_vars.at(0), NULL, &null_sel))) {
        LOG_WARN("get basic sel failed");
      } else {
        selectivity = 1.0 - null_sel - tmp_selec;
      }
    } else {
      // for other condition, it's is too hard to consider null_sel, so ignore it.
      // t_op_is, t_op_nseq , they are null safe exprs, don't consider null_sel.
      selectivity = 1.0 - tmp_selec;
    }
    selectivity = revise_between_0_1(selectivity);
  }
  return ret;
}

int ObOptEstSel::get_in_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity,
    ObJoinType join_type, const ObRelIds* left_rel_ids, const ObRelIds* right_rel_ids)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  // conditions 1: first_expr-column(CNT_COLUMN) second_expr (!= CNT_COLUMN)
  //           2: first_expr-column(!CNT_COLUMN) second_expr(!= CNT_COLUMN)
  //           3: first_expr-column(CNT_COLUN) second_expr (CNT_COLUMN)
  //           4: first_expr-column(!CNT_COLUMN) second_expr (CNT_COLUMN)

  const ObRawExpr* first_expr = NULL;
  const ObRawExpr* second_expr = NULL;
  if (OB_UNLIKELY(T_OP_IN != qual.get_expr_type() && T_OP_NOT_IN != qual.get_expr_type()) ||
      OB_UNLIKELY(2 != qual.get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Should be in_expr or not_in expr", K(ret), KPNAME(qual), "param_count", qual.get_param_count());
  } else if (OB_ISNULL(first_expr = qual.get_param_expr(0)) || OB_ISNULL(second_expr = qual.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first expr and second expr should not be NULL", K(ret));
  } else if (T_OP_ROW != second_expr->get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in_expr second param should be T_OP_ROW and have param", KPNAME(second_expr), K(ret));
  } else {
    selectivity = 0.0;
    double expr_selectivity = 1.0;
    const ObRawExpr* param_expr = NULL;
    for (int64_t idx = 0; OB_SUCC(ret) && selectivity < 1.0 && idx < second_expr->get_param_count(); ++idx) {
      if (OB_ISNULL(param_expr = second_expr->get_param_expr(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Param expr should not be NULL", K(ret), KPNAME(second_expr));
      } else if (OB_FAIL(get_equal_sel(est_sel_info,
                     *first_expr,
                     *param_expr,
                     false,
                     expr_selectivity,
                     join_type,
                     left_rel_ids,
                     right_rel_ids))) {
        LOG_WARN("Failed to get equal sel", K(ret), KPNAME(first_expr));
      } else {
        selectivity += expr_selectivity;
      }
    }
  }
  selectivity = revise_between_0_1(selectivity);
  if (OB_SUCC(ret) && T_OP_NOT_IN == qual.get_expr_type()) {
    selectivity = 1.0 - selectivity;
    if ((first_expr->has_flag(CNT_COLUMN) && !second_expr->has_flag(CNT_COLUMN))) {
      ObSEArray<ObRawExpr*, 2> cur_vars;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(const_cast<ObRawExpr*>(first_expr), cur_vars))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else if (1 == cur_vars.count()) {  // only one column, consider null_sel
        double null_sel = 0.0;
        double distinct_sel = 0.0;
        if (OB_ISNULL(cur_vars.at(0))) {
          LOG_WARN("expr is null", K(ret));
        } else if (OB_FAIL(get_var_basic_sel(est_sel_info, *cur_vars.at(0), &distinct_sel, &null_sel))) {
          LOG_WARN("get basic sel failed");
        } else if (distinct_sel > ((1.0 - null_sel) / 2.0)) {
          selectivity = distinct_sel / 2.0;
        } else {
          selectivity -= null_sel;
          selectivity = std::max(distinct_sel, selectivity);  // at least one distinct_sel
        }
      } else {
      }  // do nothing
    }
    selectivity = revise_between_0_1(selectivity);
  }
  return ret;
}

int ObOptEstSel::get_range_cmp_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_INEQ_SEL;
  const ObRawExpr* left_expr;
  const ObRawExpr* right_expr;
  if (!IS_RANGE_CMP_OP(qual.get_expr_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The type of expr is not range cmp", K(ret), KPNAME(qual));
  } else if (OB_ISNULL(left_expr = qual.get_param_expr(0)) || OB_ISNULL(right_expr = qual.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Left expr or right expr should not be NULL", K(ret), K(left_expr), K(right_expr));
  } else if ((left_expr->is_column_ref_expr() && right_expr->is_const_expr() && !right_expr->has_flag(IS_EXEC_PARAM)) ||
             (left_expr->is_const_expr() && !left_expr->has_flag(IS_EXEC_PARAM) && right_expr->is_column_ref_expr())) {
    const ObRawExpr* col_expr = left_expr->is_column_ref_expr() ? left_expr : right_expr;
    if (OB_FAIL(get_column_range_sel(
            est_sel_info, static_cast<const ObColumnRefRawExpr&>(*col_expr), qual, false, selectivity, true))) {
      LOG_WARN("Failed to get column range sel", K(*left_expr), K(*right_expr), K(ret));
    }
  } else if (T_OP_ROW == left_expr->get_expr_type() && T_OP_ROW == right_expr->get_expr_type()) {
    // only deal (col1, xx, xx) CMP (const, xx, xx)
    int64_t num = left_expr->get_param_count() <= right_expr->get_param_count() ? left_expr->get_param_count()
                                                                                : right_expr->get_param_count();
    if (num > 0) {
      const ObRawExpr* l_expr = left_expr->get_param_expr(0);
      const ObRawExpr* r_expr = right_expr->get_param_expr(0);
      if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("l_expr or r_expr should not be NULL", K(ret));
      } else if ((l_expr->is_column_ref_expr() && r_expr->is_const_expr()) ||
                 (l_expr->is_const_expr() && r_expr->is_column_ref_expr())) {
        const ObRawExpr* col_expr = (l_expr->is_column_ref_expr()) ? (l_expr) : (r_expr);
        if (OB_FAIL(get_column_range_sel(
                est_sel_info, static_cast<const ObColumnRefRawExpr&>(*col_expr), qual, false, selectivity, true))) {
          LOG_WARN("Failed to get column range sel", K(ret));
        }
      } else {
      }
    }
  } else {
    selectivity = DEFAULT_INEQ_SEL;
  }
  return ret;
}

int ObOptEstSel::get_range_exprs_sel(const ObEstSelInfo& est_sel_info, RangeExprs& range_exprs, double& selectivity)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = est_sel_info.get_stmt();
  const ParamStore* params = est_sel_info.get_params();
  const TableItem* table_item = NULL;
  selectivity = DEFAULT_SEL;  // default
  if (OB_ISNULL(stmt) || OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt should not be NULL", K(ret), K(stmt));
  } else if (NULL == (table_item = stmt->get_table_item_by_id(range_exprs.table_id_))) {  // current_level_stmt
    // not in current level stmt
  } else {  // is_in
    uint64_t tid = range_exprs.table_id_;
    uint64_t cid = range_exprs.column_id_;
    if (TableItem::BASE_TABLE == table_item->type_ || TableItem::ALIAS_TABLE == table_item->type_) {
      const ColumnItem* column_item = stmt->get_column_item_by_id(tid, cid);
      ObSEArray<ColumnItem, 1> range_columns;
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(est_sel_info.get_session_info());
      ObQueryRange query_range;
      ObQueryRangeArray ranges;
      bool all_single_ranges = false;
      ObColumnRefRawExpr* col_expr = NULL;
      if (OB_ISNULL(column_item) || OB_ISNULL(col_expr = column_item->expr_)) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("find column item error in clause_selectivity", K(ret), K(column_item), K(col_expr));
      } else if (OB_FAIL(range_columns.push_back(*column_item))) {
        LOG_WARN("push back column_item failed", K(ret));
      } else if (OB_FAIL(query_range.preliminary_extract_query_range(
                     range_columns, range_exprs.range_exprs_, dtc_params, params))) {
        LOG_WARN("preliminary extract query range failed", K(ret));
      } else if (OB_FAIL(query_range.get_tablet_ranges(const_cast<ObIAllocator&>(est_sel_info.get_allocator()),
                     *params,
                     ranges,
                     all_single_ranges,
                     dtc_params))) {
        LOG_WARN("get table range failed", K(ret));
      } else {
        selectivity = 0.0;
        double range_sel = 1.0;
        for (int64_t idx = 0; idx < ranges.count() && OB_SUCC(ret); ++idx) {
          if (OB_ISNULL(ranges.at(idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("range is null", K(ret));
          } else if (ranges.at(idx)->is_whole_range()) {
            range_sel = DEFAULT_INEQ_SEL;
            selectivity += range_sel;
          } else if (OB_FAIL(get_single_newrange_selectivity(
                         est_sel_info, range_columns, *ranges.at(idx), OB_CURRENT_STAT_EST, false, range_sel))) {
            LOG_WARN("get single range failed", K(*ranges.at(idx)), K(ret));
          } else {
            selectivity += range_sel;
          }
        }

        if (OB_SUCC(ret)) {
          double null_sel = 0.0;
          if (OB_FAIL(get_var_basic_sel(est_sel_info, *col_expr, NULL, &null_sel))) {
            LOG_WARN("Failed to get var basic sel", K(ret));
          } else if (selectivity > (1.0 - null_sel)) {
            // for range filters, selectivity no more than 1.0 - null_sel
            selectivity = 1.0 - null_sel;
          } else {
          }  // do nothing
        }
        LOG_TRACE("Get range exprs sel", K(selectivity), K(ret));
        selectivity = revise_between_0_1(selectivity);
      }
    }
  }
  return ret;
}

int ObOptEstSel::get_column_range_sel(const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& col_expr,
    const ObRawExpr& qual, const bool is_like_sel, double& selectivity, bool no_whole_range)
{
  int ret = OB_SUCCESS;
  bool is_in = false;
  const ObDMLStmt* stmt = est_sel_info.get_stmt();
  const ParamStore* params = est_sel_info.get_params();
  if (OB_ISNULL(stmt) || OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt should not be NULL", K(ret), K(stmt));
  } else if (OB_FAIL(column_in_current_level_stmt(stmt, col_expr, is_in))) {
    LOG_WARN("failed to check if column is in current level stmt", K(col_expr), K(ret));
  } else if (is_in) {
    uint64_t tid = col_expr.get_table_id();
    uint64_t cid = col_expr.get_column_id();
    const TableItem* table_item = stmt->get_table_item_by_id(tid);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find table item for var", K(ret), K(table_item));
    } else if (TableItem::BASE_TABLE == table_item->type_ || TableItem::ALIAS_TABLE == table_item->type_) {
      ObQueryRange query_range;
      ObSEArray<ColumnItem, 1> range_columns;
      ObGetMethodArray get_methods;
      ObQueryRangeArray ranges;
      const ColumnItem* column_item = stmt->get_column_item_by_id(tid, cid);
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(est_sel_info.get_session_info());
      ObIAllocator& allocator = const_cast<ObIAllocator&>(est_sel_info.get_allocator());
      if (OB_ISNULL(column_item)) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("find column item error in clause_selectivity", K(ret));
      } else {
        if (OB_FAIL(range_columns.push_back(*column_item))) {
          LOG_WARN("push_back error in clause_selectivity");
        } else if (OB_FAIL(query_range.preliminary_extract_query_range(range_columns, &qual, dtc_params, params))) {
          LOG_WARN("preliminary_extract_query_range error in clause_selectivity");
        } else if (!query_range.need_deep_copy()) {
          if (OB_FAIL(query_range.get_tablet_ranges(allocator, *params, ranges, get_methods, dtc_params))) {
            LOG_WARN("get tablet ranges failed", K(ret));
          }
        } else if (OB_FAIL(query_range.final_extract_query_range(*params, dtc_params))) {
          LOG_WARN("final_extract_query_range error in clause_selectivity");
        } else if (OB_FAIL(query_range.get_tablet_ranges(ranges, get_methods, dtc_params))) {
          LOG_WARN("final_extract_query_range error in clause_selectivity", K(ret));
        } else { /*do nothing*/
        }

        if (OB_SUCC(ret)) {
          selectivity = 0.0;
          double range_sel = 1.0;
          for (int64_t idx = 0; OB_SUCC(ret) && idx < ranges.count(); ++idx) {
            if (OB_ISNULL(ranges.at(idx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("range error, should not be NULL", K(ret), K(idx));
            } else if (no_whole_range && ranges.at(idx)->is_whole_range()) {
              range_sel = DEFAULT_INEQ_SEL;
              selectivity += range_sel;
            } else if (OB_FAIL(get_single_newrange_selectivity(est_sel_info,
                           range_columns,
                           *ranges.at(idx),
                           OB_CURRENT_STAT_EST,
                           is_like_sel,
                           range_sel))) {
              LOG_WARN("Failed to get single newrange sel", K(ret));
            } else {
              selectivity += range_sel;
            }
          }
          if (OB_SUCC(ret)) {
            double null_sel = 0.0;
            if (OB_FAIL(get_var_basic_sel(est_sel_info, col_expr, NULL, &null_sel))) {
              LOG_WARN("Failed to get var basic sel", K(ret));
            } else if (selectivity > (1.0 - null_sel)) {
              // for range filter, selectivity no more than 1.0 - null_sel
              selectivity = 1.0 - null_sel;
            } else {
            }  // do nothing
          }
          LOG_TRACE("Get column range sel", K(selectivity), K(qual));
          selectivity = revise_between_0_1(selectivity);
        }
      }
    } else {
      selectivity = DEFAULT_SEL;
    }
  } else {
    selectivity = DEFAULT_SEL;
  }
  return ret;
}

int ObOptEstSel::get_cntcol_eq_cntcol_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& left_expr,
    const ObRawExpr& right_expr, bool null_safe, double& selectivity, ObJoinType join_type,
    const ObRelIds* left_rel_ids, const ObRelIds* right_rel_ids)
{
  int ret = OB_SUCCESS;
  const ObRelIds& left_ids = left_expr.get_relation_ids();
  const ObRelIds& right_ids = right_expr.get_relation_ids();
  int64_t left_ids_num = left_ids.num_members();
  int64_t right_ids_num = right_ids.num_members();
  selectivity = DEFAULT_EQ_SEL;
  if (OB_UNLIKELY(0 == left_ids_num || 0 == right_ids_num)) {
    selectivity = EST_DEF_VAR_EQ_SEL;
    LOG_TRACE("Column not in this query, use default var equal const sel",
        KPNAME(left_expr),
        KPNAME(right_expr),
        K(selectivity));
  } else if (1 == left_ids_num && 1 == right_ids_num && left_expr.get_expr_level() == right_expr.get_expr_level() &&
             left_ids == right_ids) {
    selectivity = DEFAULT_EQ_SEL;  // Not join condition
    if (left_expr.is_column_ref_expr() && right_expr.is_column_ref_expr()) {
      if (static_cast<const ObColumnRefRawExpr&>(left_expr).get_column_id() ==
          static_cast<const ObColumnRefRawExpr&>(right_expr).get_column_id()) {  // t1.c1 = / <=> t1.c1
        double null_sel = 0.0;
        if (null_safe) {
          selectivity = 1.0;  // t1.c1 <=> t1.c1, sel is 1.0
        } else if (OB_FAIL(get_var_basic_sel(est_sel_info, left_expr, NULL, &null_sel))) {
          LOG_WARN("Failed to get var basic sel", K(ret));
        } else {
          selectivity = 1.0 - null_sel;  // t1.c1 = t1.c1, 1.0 - null_sel
        }
      }
    }
  } else if (left_expr.is_column_ref_expr() && right_expr.is_column_ref_expr()) {
    // join condition, c1 = | <=> c2
    double left_sel = 0.0;
    double right_sel = 0.0;
    double left_null = 0.0;
    double right_null = 0.0;

    if (OB_FAIL(get_var_basic_sel(est_sel_info, left_expr, &left_sel, &left_null))) {
      LOG_WARN("Failed to get var basic sel", K(ret));
    } else if (OB_FAIL(get_var_basic_sel(est_sel_info, right_expr, &right_sel, &right_null))) {
      LOG_WARN("Failed to get var basic sel", K(ret));
    } else if (IS_SEMI_ANTI_JOIN(join_type)) {
      if (OB_ISNULL(left_rel_ids) || OB_ISNULL(right_rel_ids)) {
        LOG_WARN("pass join type without table item", K(left_rel_ids), K(right_rel_ids));
      } else if (left_ids.overlap(*right_rel_ids) || right_ids.overlap(*left_rel_ids)) {
        std::swap(left_sel, right_sel);
        std::swap(left_null, right_null);
      }
      if (OB_SUCC(ret)) {
        /**
         * # FORMULA
         * ## non NULL safe
         *  a) semi: `(min(nd1, nd2) / nd1) * (1.0 - nullfrac1)`
         * ## NULL safe
         *  a) semi: `(nd2 / nd1) * (1.0 - nullfrac1) + nullfrac2 > 0 && nullsafe ? nullfrac1: 0`
         */
        if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
          selectivity = left_sel / std::max(left_sel, right_sel) * (1 - left_null);
          if (right_null > 0 && null_safe) {
            selectivity += left_null;
          }
        } else {
          selectivity = right_sel / std::max(left_sel, right_sel) * (1 - right_null);
          if (left_null > 0 && null_safe) {
            selectivity += right_null;
          }
        }
      }
    } else {
      selectivity =
          std::min(left_sel / (1 - left_null), right_sel / (1 - right_null)) * (1 - left_null) * (1 - right_null);
      if (null_safe) {
        selectivity += left_null * right_null;
      }
    }
    LOG_TRACE("selectivity of `col_ref1 =|<=> col_ref1`",
        K(left_sel),
        K(left_null),
        K(right_sel),
        K(right_null),
        K(selectivity));
  } else {
    // join condition func(col) | func(col)
    double left_sel = 0.0;
    double right_sel = 0.0;
    if (OB_FAIL(get_simple_predicate_sel(est_sel_info, left_expr, NULL, null_safe, left_sel))) {
      LOG_WARN("Failed to get simple predicate sel", K(ret));
    } else if (OB_FAIL(get_simple_predicate_sel(est_sel_info, right_expr, NULL, null_safe, right_sel))) {
      LOG_WARN("Failed to get simple predicate sel", K(ret));
    } else {
      selectivity = std::min(left_sel, right_sel);
    }
  }
  selectivity = revise_between_0_1(selectivity);
  return ret;
}

int ObOptEstSel::calculate_distinct(const double origin_rows, const ObEstSelInfo& est_sel_info,
    const ObIArray<ObRawExpr*>& exprs, double& rows, const bool use_est_origin_rows /* = false */)
{
  int ret = OB_SUCCESS;
  rows = 1;
  double est_origin_rows = 1.0;
  ObSEArray<ObRawExpr*, 16> column_exprs;
  ObSEArray<ObRawExpr*, 16> filtered_exprs;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(exprs, column_exprs))) {
    LOG_WARN("failed to extract all column", K(ret));
  } else if (OB_FAIL(filter_distinct_by_equal_set(est_sel_info, column_exprs, filtered_exprs))) {
    LOG_WARN("failed eliminate column by equal set", K(ret));
  } else if (use_est_origin_rows && !column_exprs.empty()) {
    if (OB_ISNULL(column_exprs.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpcted null", K(ret));
    } else if (OB_FAIL(get_var_basic_sel(
                   est_sel_info, *column_exprs.at(0), NULL, NULL, NULL, NULL, NULL, &est_origin_rows))) {
      LOG_WARN("failed to get var basic sel", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < filtered_exprs.count(); ++i) {
    ObRawExpr* cur_expr = filtered_exprs.at(i);
    double column_ndv = 0.0;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Error of NULL pointer", K(cur_expr), K(i), K(ret));
    } else if (cur_expr->is_column_ref_expr()) {
      bool is_in = false;
      if (OB_FAIL(column_in_current_level_stmt(est_sel_info.get_stmt(), *cur_expr, is_in))) {
        LOG_PRINT_EXPR(WARN, "failed to check column in current level stmt", cur_expr, K(cur_expr), K(i), K(ret));
      } else if (is_in) {
        if (OB_FAIL(get_var_distinct(est_sel_info, *cur_expr, column_ndv))) {
          LOG_PRINT_EXPR(WARN, "failed to get column_ndv", cur_expr, K(cur_expr), K(ret));
        }
      } else {
        column_ndv = origin_rows * DEFAULT_COLUMN_DISTINCT_RATIO;
      }
    } else {
      column_ndv = origin_rows * DEFAULT_COLUMN_DISTINCT_RATIO;
    }
    if (OB_SUCC(ret)) {
      rows *= column_ndv;
      if (use_est_origin_rows) {
        rows = std::min(rows, est_origin_rows);
      } else {
        rows = std::min(rows, origin_rows);
      }
    }
  }
  LOG_TRACE("CALCULATE DISTINCT", K(origin_rows), K(exprs), K(rows));
  return ret;
}

int ObOptEstSel::filter_distinct_by_equal_set(
    const ObEstSelInfo& est_sel_info, const ObIArray<ObRawExpr*>& column_exprs, ObIArray<ObRawExpr*>& filtered_exprs)
{
  int ret = OB_SUCCESS;
  ObBitSet<> col_added;
  EqualSets equal_sets;
  const ObLogicalOperator* op = est_sel_info.get_logical_operator();
  if (NULL == op) {
    if (OB_FAIL(append(filtered_exprs, column_exprs))) {
      LOG_WARN("failed to append filtered exprs", K(ret));
    }
  } else if (OB_FAIL(op->get_ordering_input_equal_sets(equal_sets))) {
    LOG_WARN("failed to get ordering input equal sets", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < equal_sets.count(); i++) {
      int64_t min_row_idx = OB_INVALID_INDEX_INT64;
      double min_row = 0;
      if (OB_ISNULL(equal_sets.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null equal set", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < equal_sets.at(i)->count(); j++) {
        int64_t idx = OB_INVALID_INDEX_INT64;
        if (ObOptimizerUtil::find_item(column_exprs, equal_sets.at(i)->at(j), &idx)) {
          double distinct = 0;
          if (OB_FAIL(col_added.add_member(idx))) {
            LOG_WARN("failed to add member", K(idx), K(ret));
          } else if (OB_UNLIKELY(idx >= column_exprs.count() || idx < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("index out of range", K(ret), K(idx), K(column_exprs.count()));
          } else if (OB_ISNULL(column_exprs.at(idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get column exprs", K(ret));
          } else if (OB_FAIL(get_var_distinct(est_sel_info, *column_exprs.at(idx), distinct))) {
            LOG_WARN("failed to get var basic sel", K(ret));
          } else if (OB_INVALID_INDEX_INT64 == min_row_idx || distinct < min_row) {
            min_row_idx = idx;
            min_row = distinct;
          }
        }
      }

      if (OB_SUCC(ret) && OB_INVALID_INDEX_INT64 != min_row_idx) {
        if (OB_UNLIKELY(min_row_idx >= column_exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrong number of idx", K(ret));
        } else if (ObOptimizerUtil::find_item(filtered_exprs, column_exprs.at(min_row_idx))) {
        } else if (OB_FAIL(filtered_exprs.push_back(column_exprs.at(min_row_idx)))) {
          LOG_WARN("failed to push back filtered expr", K(ret));
        } else {
          LOG_PRINT_EXPR(DEBUG,
              "add expr in equal set to filtered exprs",
              const_cast<ObRawExpr*>(column_exprs.at(min_row_idx)),
              K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
      if (col_added.has_member(i)) {
        // do nothing
      } else if (OB_FAIL(filtered_exprs.push_back(column_exprs.at(i)))) {
        LOG_WARN("failed to push back filtered expr", K(ret));
      } else {
        LOG_PRINT_EXPR(
            DEBUG, "add expr NOT in equal set to filtered exprs", const_cast<ObRawExpr*>(column_exprs.at(i)), K(ret));
      }
    }
  }
  return ret;
}

double ObOptEstSel::calc_ndv_by_sel(const double distinct_sel, const double null_sel, const double rows)
{
  double distinct = 0;
  if (distinct_sel < OB_DOUBLE_EPSINON) {
    distinct = 1;
  } else {
    distinct = (1 - null_sel) / distinct_sel;
  }

  // clamp
  if (distinct > rows) {
    distinct = rows;
  }

  if (distinct < OB_DOUBLE_EPSINON) {
    distinct = 1;
  }
  return distinct;
}

int ObOptEstSel::get_var_distinct(const ObEstSelInfo& est_sel_info, const ObRawExpr& column_expr, double& distinct)
{
  int ret = OB_SUCCESS;
  double distinct_sel = 0;
  double null_sel = 0;
  double rows = 0;
  if (OB_FAIL(get_var_basic_sel(est_sel_info, column_expr, &distinct_sel, &null_sel, &rows))) {
    LOG_WARN("failed to get var basic sel", K(ret));
  } else {
    distinct = calc_ndv_by_sel(distinct_sel, null_sel, rows);
  }
  return ret;
}

double ObOptEstSel::scale_distinct(double selected_rows, double rows, double ndv)
{
  double new_ndv = ndv;
  if (selected_rows > rows) {
    // enlarge,
    // ref: Haas and Stokes in IBM Research Report RJ 10025
    ndv = (ndv > rows ? rows : ndv);  // revise ndv
    double f1 = ndv * 2 - rows;
    if (f1 > 0) {
      new_ndv = (rows * ndv) / (rows - f1 + f1 * rows / selected_rows);
    }
  } else if (selected_rows < rows) {
    // deduce
    // ref: Join Selectivity by Jonathan Lewis.
    if (ndv > OB_DOUBLE_EPSINON && rows > OB_DOUBLE_EPSINON) {
      new_ndv = ndv * (1 - std::pow(1 - selected_rows / rows, rows / ndv));
    }
    if (new_ndv < 1) {
      new_ndv = 1;
    }
  }
  return new_ndv;
}

int ObOptEstSel::get_single_newrange_selectivity(const ObEstSelInfo& est_sel_info,
    const ObIArray<ColumnItem>& range_columns, const ObNewRange& range, const ObEstimateType est_type,
    const bool is_like_sel, double& selectivity, bool single_value_only)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;

  if (OB_SUCC(ret)) {
    double range_selectivity = 1.0;
    if (range.is_whole_range()) {
      // Whole range
    } else if (range.empty()) {
      range_selectivity = 0;
    } else {
      const ObRowkey& startkey = range.get_start_key();
      const ObRowkey& endkey = range.get_end_key();
      if (startkey.get_obj_cnt() == endkey.get_obj_cnt() && range_columns.count() == startkey.get_obj_cnt()) {
        int64_t column_num = startkey.get_obj_cnt();
        // if prefix of range is single value, process next column;
        // stop at first range column
        bool last_column = false;
        const ObColumnRefRawExpr* col_expr = NULL;
        double column_selectivity = 1.0;
        const ObObj* startobj = NULL;
        const ObObj* endobj = NULL;
        for (int64_t obj_idx = 0; OB_SUCC(ret) && !last_column && obj_idx < column_num; ++obj_idx) {
          startobj = &startkey.get_obj_ptr()[obj_idx];
          endobj = &endkey.get_obj_ptr()[obj_idx];
          if (is_like_sel && startobj->is_string_type() && startobj->get_string().length() > 0 &&
              '\0' == startobj->get_string()[0]) {
            column_selectivity = DEFAULT_INEQ_SEL;
            range_selectivity *= column_selectivity;
          } else if (OB_ISNULL(col_expr = range_columns.at(obj_idx).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Column item's expr is NULL", K(ret));
          } else {
            bool discrete = (col_expr->get_type_class() != ObFloatTC) && (col_expr->get_type_class() != ObDoubleTC);
            if (OB_FAIL(calc_column_range_selectivity(est_sel_info,
                    *col_expr,
                    ObEstRange(startobj, endobj),
                    discrete,
                    range.border_flag_,
                    est_type,
                    last_column,
                    column_selectivity))) {
              LOG_WARN("calc_column_range_selectivity error", K(ret));
            } else if (!(single_value_only && last_column)) {
              range_selectivity *= column_selectivity;
            }
          }
        }
      }
    }
    selectivity = revise_between_0_1(range_selectivity);
  }
  return ret;
}

int ObOptEstSel::calc_column_range_selectivity(const ObEstSelInfo& est_sel_info, const ObRawExpr& column_expr,
    const ObEstRange& range, const bool discrete, const ObBorderFlag border_flag, ObEstimateType est_type,
    bool& last_column, double& selectivity)
{
  int ret = OB_SUCCESS;
  last_column = false;
  selectivity = DEFAULT_SEL;
  const ObObj* startobj = range.startobj_;
  const ObObj* endobj = range.endobj_;

  double distinct_sel = 0;
  double null_sel = 0;
  double rows = 0;
  ObObj maxobj(ObObjType::ObMaxType);
  ObObj minobj(ObObjType::ObMaxType);

  if (OB_ISNULL(startobj) || OB_ISNULL(endobj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument of NULL pointer", K(startobj), K(endobj), K(ret));
  } else if ((startobj->is_min_value() && endobj->is_max_value()) ||
             (startobj->is_max_value() && endobj->is_min_value()) ||
             (startobj->is_max_value() && endobj->is_max_value()) ||
             (startobj->is_min_value() && endobj->is_min_value())) {
    last_column = true;
    selectivity = 1.0;
    LOG_TRACE("[RANGE COL SEL] Col is whole range", K(selectivity), K(last_column));
  } else if (OB_FAIL(get_var_basic_sel(est_sel_info, column_expr, &distinct_sel, &null_sel, &rows, &minobj, &maxobj))) {
    LOG_WARN("Failed to get var basic sel", K(ret));
  } else if (OB_DEFAULT_STAT_EST != est_type && rows > 0 && minobj.is_valid_type() && maxobj.is_valid_type()) {
    if (startobj->is_null() && endobj->is_null()) {
      selectivity = null_sel;
    } else {
      ObObj minscalar;
      ObObj maxscalar;
      ObObj startscalar;
      ObObj endscalar;
      if (OB_FAIL(ObOptEstObjToScalar::convert_objs_to_scalars(
              &minobj, &maxobj, startobj, endobj, &minscalar, &maxscalar, &startscalar, &endscalar))) {
        LOG_WARN("Failed to convert obj to scalar", K(ret));
      } else if (OB_FAIL(do_calc_range_selectivity(ObEstColRangeInfo(minscalar.get_double(),
                                                       maxscalar.get_double(),
                                                       &startscalar,
                                                       &endscalar,
                                                       calc_ndv_by_sel(distinct_sel, null_sel, rows),
                                                       discrete,
                                                       border_flag),
                     last_column,
                     selectivity))) {
        LOG_WARN("do_calc_range_selectivity error", K(ret));
      } else {
        selectivity *= 1 - null_sel;
      }
    }
  } else {
    bool is_half =
        startobj->is_min_value() || endobj->is_max_value() || startobj->is_max_value() || endobj->is_min_value();
    if (share::is_oracle_mode()) {
      is_half = is_half || (!startobj->is_null() && (endobj->is_null()));
    } else {
      is_half = is_half || (startobj->is_null() && !(endobj->is_null()));
    }

    if (is_half) {
      selectivity = OB_DEFAULT_HALF_OPEN_RANGE_SEL;
      last_column = true;
      LOG_TRACE("[RANGE COL SEL] default half open range sel", K(selectivity), K(last_column));
    } else {
      // startobj and endobj cannot be min/max in this branch, no need to defend
      ObObj startscalar;
      ObObj endscalar;
      if (OB_FAIL(ObOptEstObjToScalar::convert_objs_to_scalars(
              NULL, NULL, startobj, endobj, NULL, NULL, &startscalar, &endscalar))) {
        LOG_WARN("Failed to convert objs", K(ret));
      } else {
        LOG_TRACE("range column est", K(*startobj), K(*endobj), K(startscalar), K(endscalar));
        if (startscalar.is_double() && endscalar.is_double()) {
          if (fabs(startscalar.get_double() - endscalar.get_double()) < OB_DOUBLE_EPSINON) {
            selectivity = EST_DEF_VAR_EQ_SEL;
            LOG_TRACE("[RANGE COL SEL] default single value sel", K(selectivity), K(last_column));
          } else {
            selectivity = OB_DEFAULT_CLOSED_RANGE_SEL;
            last_column = true;
            LOG_TRACE("[RANGE COL SEL] default range value sel", K(selectivity), K(last_column));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptEstSel::do_calc_range_selectivity(const ObEstColRangeInfo& col_range_info, bool& scan, double& selectivity)
{
  int ret = OB_SUCCESS;
  scan = true;
  selectivity = DEFAULT_SEL;
  double min = col_range_info.min_;
  double max = col_range_info.max_;
  const ObObj* startobj = col_range_info.startobj_;
  const ObObj* endobj = col_range_info.endobj_;
  double distinct = col_range_info.distinct_;
  if (distinct <= 0) {
    distinct = EST_DEF_COL_NUM_DISTINCT;
  }
  ObBorderFlag border_flag = col_range_info.border_flag_;
  if (OB_ISNULL(startobj) || OB_ISNULL(endobj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument of NULL pointer", K(startobj), K(endobj), K(ret));
  } else if (!(startobj->is_double() || startobj->is_min_value() || startobj->is_max_value()) ||
             !(endobj->is_double() || endobj->is_min_value() || endobj->is_max_value())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid obj type", K(startobj->get_type()), K(endobj->get_type()), K(ret));
  } else {
    selectivity = 0.0;
    double start = 0.0;
    double end = 0.0;
    distinct = std::max(distinct, 1.0);
    scan = true;
    bool half_open = false;
    LOG_TRACE("[DO CALC RANGE] begin calc range expr sel", K(*startobj), K(*endobj), K(min), K(max));
    if (startobj->is_min_value() || startobj->is_max_value()) {
      LOG_TRACE("[DO CALC RANGE] half open range", K(*startobj));
      start = min;
      border_flag.set_inclusive_start();
      half_open = true;
    } else {
      start = startobj->get_double();
    }
    if (endobj->is_min_value() || endobj->is_max_value()) {
      LOG_TRACE("[DO CALC RANGE] half open range", K(*endobj));
      end = max;
      border_flag.set_inclusive_end();
      half_open = true;
    } else {
      end = endobj->get_double();
    }
    if (fabs(start - end) < OB_DOUBLE_EPSINON) {
      selectivity = 1.0 / distinct;  // Single value
      scan = false;
      LOG_TRACE("[DO CALC RANGE] single value");
    } else {
      if (start < min) {
        start = min;
        half_open = true;
        border_flag.set_inclusive_start();
      }
      if (end > max) {
        end = max;
        half_open = true;
        border_flag.set_inclusive_end();
      }
      if (start > end) {  // (start is min_value and end < min) or (end is max and start > max)
        selectivity = 0.0;
      } else if (fabs(max - min) < OB_DOUBLE_EPSINON) {
        selectivity = fabs(end - start) < OB_DOUBLE_EPSINON ? 1.0 : 0.0;
      } else {
        selectivity = (end - start) / (max - min);
        selectivity = revise_range_sel(selectivity,
            distinct,
            col_range_info.discrete_,
            border_flag.inclusive_start(),
            border_flag.inclusive_end());
      }
    }
    if (OB_SUCC(ret) && half_open) {
      selectivity += 1.0 / distinct;
    }
  }
  return ret;
}

double ObOptEstSel::calc_range_exceeds_limit_additional_selectivity(
    double min, double max, double end, double zero_sel_range_ratio)
{
  double sel = 0.0;
  double limit_range = max - min;
  double r = zero_sel_range_ratio * limit_range;
  double max_additional_sel = zero_sel_range_ratio / 2;
  if ((end - max) > r) {
    end = max + r;
  }
  sel = max_additional_sel * (1 - pow(((max + r - end) / r), 2));
  return revise_between_0_1(sel);
}

int ObOptEstSel::column_in_current_level_stmt(const ObDMLStmt* stmt, const ObRawExpr& expr, bool& is_in)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  is_in = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt is NULL", K(stmt), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (stmt->is_select_stmt() && static_cast<const ObSelectStmt*>(stmt)->has_set_op()) {
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt);
    const ObIArray<ObSelectStmt*>& child_query = select_stmt->get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && !is_in && i < child_query.count(); ++i) {
      ret = SMART_CALL(column_in_current_level_stmt(child_query.at(i), expr, is_in));
    }
  } else {
    if (expr.is_column_ref_expr()) {
      const ObColumnRefRawExpr& b_expr = static_cast<const ObColumnRefRawExpr&>(expr);
      const TableItem* table_item = stmt->get_table_item_by_id(b_expr.get_table_id());
      if (NULL != table_item) {
        is_in = true;
      }
    }
  }
  return ret;
}

const TableItem* ObOptEstSel::get_table_item_for_statics(const ObDMLStmt& stmt, uint64_t table_id)
{
  const TableItem* table_item = NULL;
  if (stmt.is_select_stmt() && static_cast<const ObSelectStmt&>(stmt).has_set_op()) {
    const ObSelectStmt& select_stmt = static_cast<const ObSelectStmt&>(stmt);
    const ObIArray<ObSelectStmt*>& child_query = select_stmt.get_set_query();
    for (int64_t i = 0; NULL == table_item && i < child_query.count(); ++i) {
      if (NULL != child_query.at(i)) {
        table_item = get_table_item_for_statics(*child_query.at(i), table_id);
      }
    }
  } else {
    table_item = stmt.get_table_item_in_all_namespace(table_id);
  }
  return table_item;
}

double ObOptEstSel::calc_single_value_exceeds_limit_selectivity(
    double min, double max, double value, double base_sel, double zero_sel_range_ratio)
{
  double sel = 0.0;
  double limit_range = max - min;
  double r = zero_sel_range_ratio * limit_range;
  if (value < min) {
    sel = (value - (min - r)) / r * base_sel;
  } else {
    sel = ((max + r) - value) / r * base_sel;
  }
  return revise_between_0_1(sel);
}

int ObOptEstSel::add_range_sel(ObRawExpr* qual, double selectivity, ObIArray<RangeSel>& range_sels)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(qual)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument of NULL pointer", K(ret), K(qual));
  } else if (qual->has_flag(IS_RANGE_COND)) {
    ObOpRawExpr* expr = static_cast<ObOpRawExpr*>(qual);
    int64_t expr_idx = -1;
    if (T_OP_LT == expr->get_expr_type() || T_OP_LE == expr->get_expr_type()) {
      if (OB_ISNULL(expr->get_param_expr(0)) || OB_ISNULL(expr->get_param_expr(1))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument of NULL pointer", K(ret), K(expr->get_param_expr(0)), K(expr->get_param_expr(1)));
      } else if (expr->get_param_expr(0)->has_flag(IS_CONST)) {
        if (OB_FAIL(find_range_sel(expr->get_param_expr(1), range_sels, expr_idx))) {
          LOG_WARN("failed to find range sel", K(ret), K(*expr->get_param_expr(1)));
        } else if (-1 == expr_idx) {
          ret = range_sels.push_back(RangeSel(expr->get_param_expr(1), false, true, 0, selectivity));
        } else if (expr_idx < 0 || expr_idx >= range_sels.count()) {
          ret = OB_ARRAY_OUT_OF_RANGE;
          LOG_WARN("index out of array range", K(ret), K(expr_idx), K(*expr->get_param_expr(1)), K(range_sels));
        } else {
          ret = range_sels.at(expr_idx).add_sel(false, true, 0, selectivity);
        }
      } else if (expr->get_param_expr(1)->has_flag(IS_CONST)) {
        if (OB_FAIL(find_range_sel(expr->get_param_expr(0), range_sels, expr_idx))) {
          LOG_WARN("failed to find range sel", K(ret), K(*expr->get_param_expr(1)));
        } else if (-1 == expr_idx) {
          ret = range_sels.push_back(RangeSel(expr->get_param_expr(0), true, false, selectivity, 0));
        } else if (expr_idx < 0 || expr_idx >= range_sels.count()) {
          ret = OB_ARRAY_OUT_OF_RANGE;
          LOG_WARN("index out of array range", K(ret), K(expr_idx), K(*expr->get_param_expr(0)), K(range_sels));
        } else {
          ret = range_sels.at(expr_idx).add_sel(true, false, selectivity, 0);
        }
      } else {
      }
    } else if (T_OP_GT == expr->get_expr_type() || T_OP_GE == expr->get_expr_type()) {
      if (OB_ISNULL(expr->get_param_expr(0)) || OB_ISNULL(expr->get_param_expr(1))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument of NULL pointer", K(ret), K(expr->get_param_expr(0)), K(expr->get_param_expr(1)));
      } else if (expr->get_param_expr(0)->has_flag(IS_CONST)) {
        if (OB_FAIL(find_range_sel(expr->get_param_expr(1), range_sels, expr_idx))) {
          LOG_WARN("failed to find range sel", K(ret), K(*expr->get_param_expr(1)));
        } else if (-1 == expr_idx) {
          ret = range_sels.push_back(RangeSel(expr->get_param_expr(1), true, false, selectivity, 0));
        } else if (expr_idx < 0 || expr_idx >= range_sels.count()) {
          ret = OB_ARRAY_OUT_OF_RANGE;
          LOG_WARN("index out of array range", K(ret), K(expr_idx), K(*expr->get_param_expr(1)), K(range_sels));
        } else {
          ret = range_sels.at(expr_idx).add_sel(true, false, selectivity, 0);
        }
      } else if (expr->get_param_expr(1)->has_flag(IS_CONST)) {
        if (OB_FAIL(find_range_sel(expr->get_param_expr(0), range_sels, expr_idx))) {
          LOG_WARN("failed to find range sel", K(ret), K(*expr->get_param_expr(1)));
        } else if (-1 == expr_idx) {
          ret = range_sels.push_back(RangeSel(expr->get_param_expr(0), false, true, 0, selectivity));
        } else if (expr_idx < 0 || expr_idx >= range_sels.count()) {
          ret = OB_ARRAY_OUT_OF_RANGE;
          LOG_WARN("index out of array range", K(ret), K(expr_idx), K(*expr->get_param_expr(0)), K(range_sels));
        } else {
          ret = range_sels.at(expr_idx).add_sel(false, true, 0, selectivity);
        }
      } else {
      }
    } else {
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptEstSel::find_range_sel(ObRawExpr* expr, ObIArray<RangeSel>& range_sels, int64_t& range_idx)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < range_sels.count(); ++i) {
    if (expr == range_sels.at(i).var_) {
      idx = i;
      found = true;
    } else { /*do nothing, continue*/
    }
  }
  range_idx = idx;
  return ret;
}

double ObOptEstSel::revise_range_sel(
    double selectivity, double distinct, bool discrete, bool include_start, bool include_end)
{
  if (distinct < 0) {
    distinct = EST_DEF_COL_NUM_DISTINCT;
  } else if (is_number_euqal_to_zero(distinct)) {
    distinct = 1;
  }
  if (discrete) {
    if (!include_start && !include_end) {
      selectivity -= 1.0 / distinct;
    } else if (include_start && include_end) {
      selectivity += 1.0 / distinct;
    } else {
    }  // do nothing
  } else if (include_start && include_end) {
    selectivity += 2.0 / distinct;
  } else if (include_start || include_end) {
    selectivity += 1.0 / distinct;
  } else {
  }  // do noting
  return revise_between_0_1(selectivity);
}

int ObEstColumnStat::assign(const ObEstColumnStat& other)
{
  int ret = OB_SUCCESS;

  column_id_ = other.column_id_;
  ndv_ = other.ndv_;
  origin_ndv_ = other.origin_ndv_;

  return ret;
}

int ObEstColumnStat::init(const uint64_t column_id, const double rows, const int64_t ndv, const bool is_single_pkey)
{
  int ret = OB_SUCCESS;
  column_id_ = column_id;
  if (is_single_pkey) {
    ndv_ = rows;
    LOG_TRACE("column is primary key", K(rows), K(ndv_));
  } else if (0 == ndv) {
    ndv_ = std::min(rows, 100.0);
    LOG_TRACE("column use default ndv", K(rows), K(ndv_));
  } else {
    ndv_ = static_cast<double>(ndv);
    LOG_TRACE("column use statics", K(rows), K(ndv_));
  }
  origin_ndv_ = ndv_;
  return ret;
}

int ObEstColumnStat::update_column_static_with_rows(double old_rows, double new_rows)
{
  int ret = OB_SUCCESS;
  ndv_ = ObOptEstSel::scale_distinct(new_rows, old_rows, ndv_);
  if (new_rows > old_rows) {
    LOG_TRACE("enlarge ndv", K(old_rows), K(new_rows), "new ndv", ndv_);
  }
  return ret;
}

int ObEstTableStat::assign(const ObEstTableStat& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  ref_id_ = other.ref_id_;
  part_id_ = other.part_id_;
  rel_id_ = other.rel_id_;
  ;
  rows_ = other.rows_;
  origin_rows_ = other.origin_rows_;

  if (OB_FAIL(all_used_parts_.assign(other.all_used_parts_))) {
    LOG_WARN("failed to assign all used parts", K(ret));
  } else if (OB_FAIL(all_cstat_.assign(other.all_cstat_))) {
    LOG_WARN("failed to assign all csata", K(ret));
  } else if (OB_FAIL(pk_ids_.assign(other.pk_ids_))) {
    LOG_WARN("failed to assign pk ids", K(ret));
  }

  return ret;
}

int ObEstTableStat::init(const uint64_t table_id, const ObPartitionKey& pkey, const int32_t rel_id, const double rows,
    ObStatManager& stat_manager, ObSqlSchemaGuard& schema_guard, ObIArray<int64_t>& all_used_part_id,
    const bool use_default_stat)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  uint64_t column_id = OB_INVALID_ID;
  ObSEArray<ObColumnStatValueHandle, 16> col_stats;
  ObSEArray<uint64_t, 8> column_ids;
  char llc_bitmap[ObColumnStat::NUM_LLC_BUCKET];
  all_cstat_.reset();
  all_used_parts_.reset();

  // init common member variable
  table_id_ = table_id;
  ref_id_ = pkey.get_table_id();
  part_id_ = pkey.get_partition_id();
  rel_id_ = rel_id;
  rows_ = rows;
  // revise rows
  if (rows_ <= 0) {
    rows_ = 1;
  }
  origin_rows_ = rows_;
  if (OB_FAIL(append(all_used_parts_, all_used_part_id))) {
    LOG_WARN("failed to append all used partition ids", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(ref_id_, table_schema))) {
    LOG_WARN("failed to get_table_schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null schema get", K(ret));
  } else if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  } else if (OB_FAIL(all_cstat_.prepare_allocate(column_ids.count()))) {
    LOG_WARN("failed to init all stat array", K(ret));
  } else if (use_default_stat) {
    // do nothing
  } else if (OB_FAIL(stat_manager.get_batch_stat(ref_id_, all_used_parts_, column_ids, col_stats))) {
    LOG_WARN("failed to get batch stats", K(ret));
  }
  // init pkey ids
  const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
      LOG_WARN("failed to get column id", K(ret));
    } else if (column_id < OB_END_RESERVED_COLUMN_ID_NUM) {
      if (table_schema->is_new_no_pk_table()) {
        // part key may be hidden key for heap table
        pk_ids_.reset();
        break;
      } else {
        // do nothing
      }
    } else if (OB_FAIL(pk_ids_.push_back(column_id))) {
      LOG_WARN("failed to push back column id", K(ret));
    }
  }
  // init column ndv
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    uint64_t column_id = column_ids.at(i);
    int64_t global_ndv = 0;
    bool is_single_pkey = (1 == pk_ids_.count() && pk_ids_.at(0) == column_id);
    // get global llc bitmap
    if (!is_single_pkey && !use_default_stat) {
      for (int64_t j = 0; OB_SUCC(ret) && j < all_used_part_id.count(); ++j) {
        ObColumnStatValueHandle cstat;
        const char* part_bitmap = NULL;
        if (OB_FAIL(stat_manager.get_column_stat(ref_id_, all_used_part_id.at(j), column_id, cstat))) {
          LOG_WARN("failed to get column stat", K(ret));
        } else if (OB_ISNULL(cstat.cache_value_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_ISNULL(part_bitmap = cstat.cache_value_->get_llc_bitmap())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("llc bitmap is null", K(ret));
        } else {
          for (int64_t k = 0; k < ObColumnStat::NUM_LLC_BUCKET; ++k) {
            if (0 == j || static_cast<uint8_t>(part_bitmap[k]) > static_cast<uint8_t>(llc_bitmap[k])) {
              llc_bitmap[k] = part_bitmap[k];
            }
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(compute_number_distinct(llc_bitmap, global_ndv))) {
        LOG_WARN("failed to compute number distinct", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(all_cstat_.at(i).init(column_id, rows_, global_ndv, is_single_pkey))) {
        LOG_WARN("failed to init column stat from cache", K(ret));
      }
    }
  }
  return ret;
}

int ObEstTableStat::update_stat(const double rows, const bool can_reduce, const bool can_enlarge)
{
  int ret = OB_SUCCESS;
  if (rows < rows_ && can_reduce) {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_cstat_.count(); i++) {
      if (OB_FAIL(all_cstat_.at(i).update_column_static_with_rows(rows_, rows))) {
        LOG_WARN("failed update column ndv", K(ret));
      }
    }
  }
  if (rows > rows_ && can_enlarge) {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_cstat_.count(); i++) {
      if (OB_FAIL(all_cstat_.at(i).update_column_static_with_rows(rows_, rows))) {
        LOG_WARN("failed update column ndv", K(ret));
      }
    }
    origin_rows_ = rows;
  }
  /**
   * subplan scan
   *     |                        result row count for subplan scan shown in the left may
   *  group by A.a,B.b            larger than base table. For this situation, do not enlarge
   *     |                        ndv, only update row count.
   *    / \
   *   A   B
   */
  if (OB_SUCC(ret) && can_reduce) {
    rows_ = rows;
  }
  return ret;
}

int ObEstTableStat::get_column(const uint64_t cid, ObEstColumnStat*& cstat)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  cstat = NULL;
  for (int64_t i = 0; !is_found && i < all_cstat_.count(); i++) {
    if (all_cstat_.at(i).get_column_id() == cid) {
      is_found = true;
      cstat = &all_cstat_.at(i);
    }
  }
  if (OB_UNLIKELY(!is_found)) {
    ret = OB_ITER_END;
    LOG_TRACE("failed to find column static", K(ret), K(cid), K(*this));
  }
  return ret;
}

int ObEstTableStat::compute_number_distinct(const char* llc_bitmap, int64_t& num_distinct)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(llc_bitmap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    double sum_of_pmax = 0;
    double alpha = select_alpha_value(ObColumnStat::NUM_LLC_BUCKET);
    int64_t empty_bucket_num = 0;
    for (int64_t i = 0; i < ObColumnStat::NUM_LLC_BUCKET; ++i) {
      sum_of_pmax += (1 / pow(2, (llc_bitmap[i])));
      if (llc_bitmap[i] == 0) {
        ++empty_bucket_num;
      }
    }
    double estimate_ndv = (alpha * ObColumnStat::NUM_LLC_BUCKET * ObColumnStat::NUM_LLC_BUCKET) / sum_of_pmax;
    num_distinct = static_cast<int64_t>(estimate_ndv);
    // check if estimate result too tiny or large.
    if (estimate_ndv <= 5 * ObColumnStat::NUM_LLC_BUCKET / 2) {
      if (0 != empty_bucket_num) {
        // use linear count
        num_distinct = static_cast<int64_t>(
            ObColumnStat::NUM_LLC_BUCKET * log(ObColumnStat::NUM_LLC_BUCKET / double(empty_bucket_num)));
      }
    }
    if (estimate_ndv > (static_cast<double>(ObColumnStat::LARGE_NDV_NUMBER) / 30)) {
      num_distinct = static_cast<int64_t>((0 - pow(2, 32)) * log(1 - estimate_ndv / ObColumnStat::LARGE_NDV_NUMBER));
    }
  }
  return ret;
}

double ObEstTableStat::select_alpha_value(const int64_t num_bucket)
{
  double ret = 0.0;
  switch (num_bucket) {
    case 16:
      ret = 0.673;
      break;
    case 32:
      ret = 0.697;
      break;
    case 64:
      ret = 0.709;
      break;
    default:
      ret = 0.7213 / (1 + 1.079 / double(num_bucket));
      break;
  }
  return ret;
}

int ObEstAllTableStat::assign(const ObEstAllTableStat& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(all_tstat_.assign(other.all_tstat_))) {
    LOG_WARN("failed to assign other", K(ret));
  }
  return ret;
}

int ObEstAllTableStat::add_table(const uint64_t table_id, const ObPartitionKey& pkey, const int32_t rel_id,
    const double rows, ObStatManager* stat_manager, ObSqlSchemaGuard* schema_guard, ObIArray<int64_t>& all_used_part_id,
    const bool use_default_stat)
{
  int ret = OB_SUCCESS;
  ObEstTableStat empty_tstat;
  int64_t size = all_tstat_.count();
  if (OB_ISNULL(stat_manager) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL param", K(stat_manager), K(schema_guard));
  } else if (OB_FAIL(all_tstat_.push_back(empty_tstat))) {
    LOG_WARN("failed to pushdown empty tstat", K(ret));
  } else if (OB_FAIL(all_tstat_.at(size).init(
                 table_id, pkey, rel_id, rows, *stat_manager, *schema_guard, all_used_part_id, use_default_stat))) {
    LOG_WARN("failed to init new tstat", K(ret));
  } else {
    LOG_TRACE("add table success", "table statics", all_tstat_.at(size));
  }
  return ret;
}

int ObEstAllTableStat::update_stat(const ObRelIds& rel_ids, const double rows, const EqualSets& equal_sets,
    const bool can_reduce, const bool can_enlarge)
{
  int ret = OB_SUCCESS;
  double clamped_rows = rows;
  if (clamped_rows < 1) {
    LOG_TRACE("rows less than 1, clamp to 1", K(rows));
    clamped_rows = 1;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_tstat_.count(); i++) {
    if (rel_ids.has_member(all_tstat_.at(i).get_rel_id())) {
      if (OB_FAIL(all_tstat_.at(i).update_stat(clamped_rows, can_reduce, can_enlarge))) {
        LOG_WARN("failed to update table stat", K(ret));
      }
    }
  }
  LOG_TRACE("show all table status after update", K(all_tstat_));
  if (can_reduce) {
    for (int64_t i = 0; OB_SUCC(ret) && i < equal_sets.count(); ++i) {
      const EqualSet* cur_set = equal_sets.at(i);
      int64_t min_idx = OB_INVALID_INDEX_INT64;
      double min_ndv = 0;
      ObSEArray<ObEstColumnStat*, 4> col_stats;
      if (OB_ISNULL(cur_set)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null equal set", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < cur_set->count(); ++j) {
        const ObRawExpr* cur_expr = cur_set->at(j);
        if (OB_ISNULL(cur_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!cur_expr->is_column_ref_expr()) {
          // do nothing
        } else {
          ObEstColumnStat* col_stat;
          const ObColumnRefRawExpr* col_expr = static_cast<const ObColumnRefRawExpr*>(cur_expr);
          if (OB_FAIL(get_cstat_by_table_id(col_expr->get_table_id(), col_expr->get_column_id(), col_stat))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              LOG_TRACE("column status not find in all table status", K(*cur_expr));
            } else {
              LOG_WARN("failed to get cstat by table id", K(ret));
            }
          } else if (OB_ISNULL(col_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cstat is NULL", K(ret));
          } else if (OB_FAIL(col_stats.push_back(col_stat))) {
            LOG_WARN("failed to push back column status", K(ret));
          } else if (OB_INVALID_INDEX_INT64 == min_idx || col_stat->get_ndv() < min_ndv) {
            min_idx = j;
            min_ndv = col_stat->get_ndv();
          }
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < col_stats.count(); ++j) {
        if (OB_ISNULL(col_stats.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column status is null", K(ret));
        } else {
          col_stats.at(j)->set_ndv(min_ndv);
        }
      }
    }
    LOG_TRACE("show all table status after update using equal set", K(all_tstat_));
  }
  return ret;
}

int ObEstAllTableStat::merge(const ObEstAllTableStat& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_tstat_, other.all_tstat_))) {
    LOG_WARN("failed to append all_tstat_", K(ret));
  }
  return ret;
}

// \return error_code, may return OB_ITER_END.
int ObEstAllTableStat::get_distinct(const uint64_t table_id, const uint64_t column_id, double& distinct_num)
{
  int ret = OB_SUCCESS;
  ObEstColumnStat* cstat = NULL;
  if (OB_FAIL(get_cstat_by_table_id(table_id, column_id, cstat))) {
    if (ret == OB_ITER_END) {
      LOG_TRACE("can not find cstat by table id");
    } else {
      LOG_WARN("failed to find cstat by table id", K(ret));
    }
  } else if (OB_ISNULL(cstat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get NULL cstat", K(ret));
  } else {
    distinct_num = cstat->get_ndv();
  }
  return ret;
}

// \return error_code, may return OB_ITER_END.
int ObEstAllTableStat::get_origin_distinct(const uint64_t table_id, const uint64_t column_id, double& distinct_num)
{
  int ret = OB_SUCCESS;
  ObEstColumnStat* cstat = NULL;
  if (OB_FAIL(get_cstat_by_table_id(table_id, column_id, cstat))) {
    if (ret == OB_ITER_END) {
      LOG_TRACE("can not find cstat by table id");
    } else {
      LOG_WARN("failed to find cstat by table id", K(ret));
    }
  } else if (OB_ISNULL(cstat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get NULL cstat", K(ret));
  } else {
    distinct_num = cstat->get_origin_ndv();
  }
  return ret;
}

// \return error_code, may return OB_ITER_END.
int ObEstAllTableStat::get_rows(const uint64_t table_id, double& rows, double& origin_rows)
{
  int ret = OB_SUCCESS;
  ObEstTableStat* tstat = NULL;
  if (OB_FAIL(get_tstat_by_table_id(table_id, tstat))) {
    // ret must be OB_ITER_END
    LOG_TRACE("failed to find tstat by table id", K(ret));
  } else if (OB_ISNULL(tstat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL tstat", K(ret));
  } else {
    rows = tstat->get_rows();
    origin_rows = tstat->get_origin_rows();
  }
  return ret;
}

int ObEstAllTableStat::get_part_id_by_table_id(const uint64_t& table_id, int64_t& part_id) const
{
  int ret = OB_SUCCESS;
  ObEstTableStat* tstat;
  part_id = OB_INVALID_INDEX_INT64;
  if (OB_FAIL(const_cast<ObEstAllTableStat*>(this)->get_tstat_by_table_id(table_id, tstat))) {
    ret = OB_SUCCESS;
    LOG_TRACE("failed to find tstat", K(ret));
  } else if (OB_ISNULL(tstat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL tstat", K(ret));
  } else {
    part_id = tstat->get_part_id();
  }
  return ret;
}

int ObEstAllTableStat::get_pkey_ids_by_table_id(const uint64_t table_id, ObIArray<uint64_t>& pkey_ids)
{
  int ret = OB_SUCCESS;
  ObEstTableStat* tstat;
  if (OB_FAIL(get_tstat_by_table_id(table_id, tstat))) {
    ret = OB_SUCCESS;
    LOG_TRACE("failed to find tstat", K(ret));
  } else if (OB_ISNULL(tstat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL tstat", K(ret));
  } else if (OB_FAIL(pkey_ids.assign(tstat->get_pkey_ids()))) {
    LOG_WARN("failed to assign column ids", K(ret));
  }
  return ret;
}

// \return error_code, only return OB_SUCCESS or OB_ITER_END.
int ObEstAllTableStat::get_tstat_by_table_id(const uint64_t table_id, ObEstTableStat*& tstat)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  for (int64_t i = 0; !is_found && i < all_tstat_.count(); i++) {
    if (all_tstat_.at(i).get_table_id() == table_id) {
      is_found = true;
      tstat = &all_tstat_.at(i);
    }
  }
  if (OB_UNLIKELY(!is_found)) {
    ret = OB_ITER_END;
    LOG_TRACE("failed to find table", K(ret), K(table_id), K(all_tstat_));
  }
  return ret;
}

// \return error_code, may return OB_ITER_END.
int ObEstAllTableStat::get_cstat_by_table_id(const uint64_t table_id, const uint64_t column_id, ObEstColumnStat*& cstat)
{
  int ret = OB_SUCCESS;
  ObEstTableStat* tstat = NULL;
  if (OB_FAIL(get_tstat_by_table_id(table_id, tstat))) {
    LOG_TRACE("failed to get tstat", K(ret));
  } else if (OB_ISNULL(tstat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL tstat", K(ret));
  } else if (OB_FAIL(tstat->get_column(column_id, cstat))) {
    LOG_TRACE("failed to get cstat", K(ret));
  }
  return ret;
}

void ObEstSelInfo::reset_table_stats()
{
  get_table_stats().get_all_tstat().reset();
}

int ObEstSelInfo::append_table_stats(const ObEstSelInfo* other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input est sel info is null", K(ret), K(other));
  } else if (OB_FAIL(get_table_stats().merge(other->get_table_stats()))) {
    LOG_WARN("failed to merge table stats", K(ret));
  }
  return ret;
}

int ObEstSelInfo::update_table_stats(const ObLogicalOperator* child)
{
  int ret = OB_SUCCESS;
  bool can_reduce = true;
  bool can_enlarge = log_op_def::instance_of_log_table_scan(child->get_type());
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else if (OB_FAIL(get_table_stats().update_stat(child->get_table_set(),
                 child->get_card(),
                 child->get_ordering_output_equal_sets(),
                 can_reduce,
                 can_enlarge))) {
    LOG_WARN("failed to update table stats", K(ret));
  } else {
    LOG_TRACE("in update_table_stats", "rows", child->get_card(), K(get_op_name(child->get_type())));
  }

  return ret;
}

int ObEstSelInfo::update_table_stats(const ObJoinOrder& join_order)
{
  int ret = OB_SUCCESS;
  bool can_reduce_enlarge = PathType::ACCESS == join_order.get_type();
  LOG_TRACE("in update_table_stats", "rows", join_order.get_output_rows(), K(join_order.get_type()));
  if (OB_FAIL(get_table_stats().update_stat(join_order.get_tables(),
          join_order.get_output_rows(),
          join_order.get_ordering_output_equal_sets(),
          can_reduce_enlarge,
          can_reduce_enlarge))) {
    LOG_WARN("failed to update table stats", K(ret));
  }
  return ret;
}

int ObEstSelInfo::add_table_for_table_stat(
    const uint64_t table_id, const ObPartitionKey& pkey, const double rows, ObIArray<int64_t>& all_used_part_id)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = get_stmt();
  int32_t rel_id = OB_INVALID_INDEX;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL stmt", K(ret));
  } else {
    rel_id = stmt->get_table_bit_index(table_id);
    if (OB_FAIL(get_table_stats().add_table(table_id,
            pkey,
            rel_id,
            rows,
            const_cast<ObStatManager*>(get_stat_manager()),
            const_cast<ObSqlSchemaGuard*>(get_sql_schema_guard()),
            all_used_part_id,
            use_default_stat()))) {
      LOG_WARN("failed to add table to table stats", K(ret));
    }
    LOG_TRACE("add table to estimate info", K(table_id), K(pkey.get_table_id()), K(rel_id), K(rows));
  }
  return ret;
}

int ObEstSelInfo::rename_statics(
    const uint64_t subquery_id, const ObEstSelInfo& child_est_sel_info, const double& child_rows)
{
  int ret = OB_SUCCESS;
  ObEstTableStat empty_tstat;
  ObSEArray<ObRawExpr*, 1> exprs;
  int32_t rel_id = OB_INVALID_INDEX;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(child_est_sel_info.get_stmt()) ||
      OB_UNLIKELY(!child_est_sel_info.get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmts are invalid", K(ret), K(get_stmt()), K(child_est_sel_info.get_stmt()));
  } else if (OB_UNLIKELY(OB_INVALID_INDEX == (rel_id = get_stmt()->get_table_bit_index(subquery_id)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rel index", K(ret), K(subquery_id));
  } else {
    const ObSelectStmt* child_stmt = static_cast<const ObSelectStmt*>(child_est_sel_info.get_stmt());
    const ObIArray<ColumnItem>& column_item_arr = get_stmt()->get_column_items();
    ObEstTableStat* tstat = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_item_arr.count(); i++) {
      const ColumnItem& column_item = column_item_arr.at(i);
      if (OB_ISNULL(column_item.expr_)) {
        LOG_WARN("NULL column expr", K(ret));
      } else if (!column_item.expr_->is_explicited_reference()) {
        // do nothing
      } else if (column_item.table_id_ == subquery_id) {
        if (NULL == tstat) {
          if (get_table_stats().get_all_tstat().push_back(empty_tstat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to push back empty tstat", K(ret));
          } else {
            // init tstat
            tstat = &get_table_stats().get_all_tstat().at(get_table_stats().get_all_tstat().count() - 1);
            tstat->set_table_id(subquery_id);
            tstat->set_ref_id(OB_INVALID_ID);
            tstat->set_part_id(OB_INVALID_INDEX_INT64);
            tstat->set_rel_id(rel_id);
            tstat->set_rows(child_rows);
            tstat->set_origin_rows(child_rows);
            tstat->get_pkey_ids().reset();
            tstat->get_all_used_parts().reset();
          }
        }

        if (OB_SUCC(ret)) {
          ObEstColumnStat empty_cstat;
          exprs.reset();
          ObRawExpr* expr = NULL;
          double ndv = 1;
          int64_t idx = column_item.column_id_ - OB_APP_MIN_COLUMN_ID;
          if (OB_UNLIKELY(idx < 0 || idx >= child_stmt->get_select_item_size())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("INVALID column id",
                K(ret),
                "column_id",
                column_item.column_id_,
                K(OB_APP_MIN_COLUMN_ID),
                K(child_stmt->get_select_item_size()),
                "child_select_items",
                child_stmt->get_select_items(),
                "column_expr",
                PNAME(*column_item.expr_));
          } else if (OB_FAIL(tstat->get_all_cstat().push_back(empty_cstat))) {
            LOG_WARN("Failed to push back empty cstat", K(ret));
          } else if (OB_ISNULL(expr = child_stmt->get_select_item(idx).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL expr", K(ret));
          } else if (expr->is_set_op_expr()) {
            ObSelectStmt* set_child_stmt = NULL;
            const int64_t set_epxr_idx = static_cast<ObSetOpRawExpr*>(expr)->get_idx();
            if (OB_ISNULL(set_child_stmt = child_stmt->get_set_query(0))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected child stmt", K(ret), K(*child_stmt));
            } else if (OB_UNLIKELY(set_epxr_idx < 0 || set_epxr_idx >= set_child_stmt->get_select_item_size())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected set child query", K(ret), K(*set_child_stmt), K(set_epxr_idx));
            } else {
              expr = set_child_stmt->get_select_item(set_epxr_idx).expr_;
            }
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL expr", K(ret));
          } else if (OB_FAIL(exprs.push_back(expr))) {
            LOG_WARN("Failed to push back column expr", K(ret));
          } else if (OB_FAIL(ObOptEstSel::calculate_distinct(child_rows, child_est_sel_info, exprs, ndv))) {
            LOG_WARN("Failed to calculate_distinct", K(ret));
          } else {
            ObEstColumnStat& cstat = tstat->get_all_cstat().at(tstat->get_all_cstat().count() - 1);

            cstat.set_column_id(column_item.column_id_);
            cstat.set_ndv(ndv);
            cstat.set_origin_ndv(ndv);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed to rename statistics", K(child_est_sel_info), K(*this), K(ret));
    }
  }
  return ret;
}

/**
 * check if an expr is a simple join condition
 */
int ObOptEstSel::is_simple_join_condition(ObRawExpr& qual, const ObRelIds* left_rel_ids, const ObRelIds* right_rel_ids,
    bool& is_valid, ObIArray<ObRawExpr*>& join_conditions)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (NULL == left_rel_ids || NULL == right_rel_ids) {
  } else if (T_OP_EQ == qual.get_expr_type() || T_OP_NSEQ == qual.get_expr_type()) {
    ObRawExpr* expr0 = qual.get_param_expr(0);
    ObRawExpr* expr1 = qual.get_param_expr(1);
    if (OB_ISNULL(expr0) && OB_ISNULL(expr1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr in equal expr is null", K(ret), K(expr0), K(expr1));
    } else if ((expr0->is_column_ref_expr() && expr1->is_column_ref_expr()) ||
               (T_OP_ROW == expr0->get_expr_type() && T_OP_ROW == expr1->get_expr_type())) {
      if ((left_rel_ids->is_superset(expr0->get_relation_ids()) &&
              right_rel_ids->is_superset(expr1->get_relation_ids())) ||
          (left_rel_ids->is_superset(expr1->get_relation_ids()) &&
              right_rel_ids->is_superset(expr0->get_relation_ids()))) {
        if (OB_FAIL(join_conditions.push_back(&qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {
          is_valid = true;
        }
      } else { /* do nothing */
      }
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObOptEstSel::get_equal_sel(const ObEstSelInfo& est_sel_info, ObIArray<ObRawExpr*>& quals, double& selectivity,
    ObJoinType join_type, const ObRelIds& left_rel_ids, const ObRelIds& right_rel_ids, const double left_rows,
    const double right_rows)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> left_exprs;
  ObSEArray<ObRawExpr*, 4> right_exprs;
  ObSEArray<bool, 4> null_safes;
  bool is_valid;
  if (OB_FAIL(is_valid_multi_join(quals, is_valid))) {
    LOG_WARN("failed to check is valid multi join", K(ret));
  } else if (!is_valid) {
    for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
      ObRawExpr* cur_expr = quals.at(i);
      double tmp_sel = 1.0;
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_equal_sel(est_sel_info, *cur_expr, tmp_sel, join_type, &left_rel_ids, &right_rel_ids))) {
        LOG_WARN("Failed to get equal selectivity", K(ret));
      } else {
        selectivity *= tmp_sel;
        LOG_PRINT_EXPR(DEBUG, "got equal expr selectivity", *cur_expr, K(tmp_sel));
      }
    }
  } else if (OB_FAIL(extract_join_exprs(quals, left_rel_ids, right_rel_ids, left_exprs, right_exprs, null_safes))) {
    LOG_WARN("failed to extract join exprs", K(ret));
  } else if (OB_FAIL(get_cntcols_eq_cntcols_sel(
                 est_sel_info, left_exprs, right_exprs, null_safes, selectivity, join_type, left_rows, right_rows))) {
    LOG_WARN("Failed to get equal sel", K(ret));
  } else { /* do nothing */
  }
  return ret;
}

int ObOptEstSel::extract_join_exprs(ObIArray<ObRawExpr*>& quals, const ObRelIds& left_rel_ids,
    const ObRelIds& right_rel_ids, ObIArray<ObRawExpr*>& left_exprs, ObIArray<ObRawExpr*>& right_exprs,
    ObIArray<bool>& null_safes)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr0 = NULL;
  ObRawExpr* expr1 = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObRawExpr* cur_expr = quals.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_UNLIKELY(T_OP_EQ != cur_expr->get_expr_type() && T_OP_NSEQ != cur_expr->get_expr_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid expr type", K(ret), "expr type", cur_expr->get_expr_type());
    } else if (OB_ISNULL(expr0 = cur_expr->get_param_expr(0)) || OB_ISNULL(expr1 = cur_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (expr0->is_column_ref_expr() && expr1->is_column_ref_expr()) {
      if (left_rel_ids.is_superset(expr0->get_relation_ids()) && right_rel_ids.is_superset(expr1->get_relation_ids())) {
        GROUP_PUSH_BACK(left_exprs, expr0, right_exprs, expr1, null_safes, T_OP_NSEQ == cur_expr->get_expr_type());
      } else if (left_rel_ids.is_superset(expr1->get_relation_ids()) &&
                 right_rel_ids.is_superset(expr0->get_relation_ids())) {
        GROUP_PUSH_BACK(left_exprs, expr1, right_exprs, expr0, null_safes, T_OP_NSEQ == cur_expr->get_expr_type());
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", K(ret), K(expr0), K(expr1));
      }
    } else if (T_OP_ROW == expr0->get_expr_type() && T_OP_ROW == expr1->get_expr_type()) {
      if (OB_UNLIKELY(expr0->get_param_count() != expr1->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(
            "param count for row should be same", K(ret), K(expr0->get_param_count()), K(expr1->get_param_count()));
      }
      ObRawExpr* subexpr0 = NULL;
      ObRawExpr* subexpr1 = NULL;
      for (int64_t j = 0; OB_SUCC(ret) && j < expr0->get_param_count(); ++j) {
        if (OB_ISNULL(subexpr0 = expr0->get_param_expr(j)) || OB_ISNULL(subexpr1 = expr1->get_param_expr(j)) ||
            !subexpr0->is_column_ref_expr() || !subexpr1->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected expr", K(ret), K(subexpr0), K(subexpr1));
        } else if (left_rel_ids.is_superset(subexpr0->get_relation_ids()) &&
                   right_rel_ids.is_superset(subexpr1->get_relation_ids())) {
          GROUP_PUSH_BACK(
              left_exprs, subexpr0, right_exprs, subexpr1, null_safes, T_OP_NSEQ == cur_expr->get_expr_type());
        } else if (left_rel_ids.is_superset(subexpr1->get_relation_ids()) &&
                   right_rel_ids.is_superset(subexpr0->get_relation_ids())) {
          GROUP_PUSH_BACK(
              left_exprs, subexpr1, right_exprs, subexpr0, null_safes, T_OP_NSEQ == cur_expr->get_expr_type());
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected expr", K(ret), K(expr0), K(expr1));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all expr should be column ref or row", K(ret), K(expr0), K(expr1));
    }
  }
  return ret;
}

int ObOptEstSel::get_cntcols_eq_cntcols_sel(const ObEstSelInfo& est_sel_info, const ObIArray<ObRawExpr*>& left_exprs,
    const ObIArray<ObRawExpr*>& right_exprs, const ObIArray<bool>& null_safes, double& selectivity,
    ObJoinType join_type, const double left_rows, const double right_rows)
{
  int ret = OB_SUCCESS;
  UNUSED(left_rows);
  UNUSED(right_rows);
  ObSEArray<double, 4> left_null_sels;
  ObSEArray<double, 4> right_null_sels;
  ObSEArray<double, 4> left_distinct_sels;
  ObSEArray<double, 4> right_distinct_sels;
  selectivity = DEFAULT_EQ_SEL;
  double left_ndv = 1.0;
  double right_ndv = 1.0;
  double left_null = 1.0;
  double right_null = 1.0;
  double left_sel = 1.0;
  double right_sel = 1.0;
  double left_origin_rows = 1.0;
  double right_origin_rows = 1.0;
  double left_filter_rows = 1.0;
  double right_filter_rows = 1.0;
  bool left_contain_pkey = false;
  bool right_contain_pkey = false;
  bool left_is_union_pkey = false;
  bool right_is_union_pkey = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < left_exprs.count(); ++i) {
    if (OB_ISNULL(left_exprs.at(i)) || OB_ISNULL(right_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(get_var_basic_sel(est_sel_info,
                   *left_exprs.at(i),
                   &left_sel,
                   &left_null,
                   &left_filter_rows,
                   NULL,
                   NULL,
                   &left_origin_rows))) {
      LOG_WARN("Failed to get var basic sel", K(ret));
    } else if (OB_FAIL(get_var_basic_sel(est_sel_info,
                   *right_exprs.at(i),
                   &right_sel,
                   &right_null,
                   &right_filter_rows,
                   NULL,
                   NULL,
                   &right_origin_rows))) {
      LOG_WARN("Failed to get var basic sel", K(ret));
    } else if (OB_FAIL(left_null_sels.push_back(left_null))) {
      LOG_WARN("failed to push back double", K(ret));
    } else if (OB_FAIL(right_null_sels.push_back(right_null))) {
      LOG_WARN("failed to push back double", K(ret));
    } else if (OB_FAIL(left_distinct_sels.push_back(left_sel))) {
      LOG_WARN("failed to push back double", K(ret));
    } else if (OB_FAIL(right_distinct_sels.push_back(right_sel))) {
      LOG_WARN("failed to push back double", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(is_columns_contain_pkey(est_sel_info, left_exprs, left_contain_pkey, left_is_union_pkey))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  } else if (OB_FAIL(is_columns_contain_pkey(est_sel_info, right_exprs, right_contain_pkey, right_is_union_pkey))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  } else if (OB_FAIL(calculate_distinct(left_filter_rows, est_sel_info, left_exprs, left_ndv))) {
    LOG_WARN("Failed to calculate distinct", K(ret));
  } else if (OB_FAIL(calculate_distinct(right_filter_rows, est_sel_info, right_exprs, right_ndv))) {
    LOG_WARN("Failed to calculate distinct", K(ret));
  } else if (IS_SEMI_ANTI_JOIN(join_type)) {
    if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
      selectivity = std::min(left_ndv, right_ndv) / left_ndv;
      for (int64_t i = 0; i < left_null_sels.count(); ++i) {
        selectivity *= (1.0 - left_null_sels.at(i));
      }
      for (int64_t i = 0; i < null_safes.count(); ++i) {
        if (null_safes.at(i) && right_null_sels.at(i) > 0) {
          double factor = 1.0;
          for (int64_t j = 0; j < null_safes.count(); ++j) {
            if (i == j) {
              factor *= left_null_sels.at(j);
            } else {
              factor *= left_distinct_sels.at(j) / std::max(left_distinct_sels.at(j), right_distinct_sels.at(j)) *
                        (1.0 - left_null_sels.at(j));
            }
          }
          selectivity += factor;
        } else { /* do nothing */
        }
      }
    } else {
      selectivity = std::min(left_ndv, right_ndv) / right_ndv;
      for (int64_t i = 0; i < right_null_sels.count(); ++i) {
        selectivity *= (1.0 - right_null_sels.at(i));
      }
      for (int64_t i = 0; i < null_safes.count(); ++i) {
        if (null_safes.at(i) && left_null_sels.at(i) > 0) {
          double factor = 1.0;
          for (int64_t j = 0; j < null_safes.count(); ++j) {
            if (i == j) {
              factor *= right_null_sels.at(j);
            } else {
              factor *= right_distinct_sels.at(j) / std::max(left_distinct_sels.at(j), right_distinct_sels.at(j)) *
                        (1.0 - right_null_sels.at(j));
            }
          }
          selectivity += factor;
        } else { /* do nothing */
        }
      }
    }
  } else {
    if (left_contain_pkey == right_contain_pkey) {
    } else if (left_contain_pkey && left_is_union_pkey) {
      right_ndv = std::min(right_ndv, left_origin_rows);
    } else if (right_contain_pkey && right_is_union_pkey) {
      left_ndv = std::min(left_ndv, right_origin_rows);
    } else {
      // do nothing
    }
    selectivity = 1.0 / std::max(left_ndv, right_ndv);
    for (int64_t i = 0; i < left_null_sels.count(); ++i) {
      selectivity *= (1.0 - left_null_sels.at(i)) * (1.0 - right_null_sels.at(i));
    }
    for (int64_t i = 0; i < null_safes.count(); ++i) {
      if (null_safes.at(i)) {
        double factor = 1.0;
        for (int64_t j = 0; j < null_safes.count(); ++j) {
          if (i == j) {
            factor *= left_null_sels.at(j) * right_null_sels.at(j);
          } else {
            factor *= std::min(left_distinct_sels.at(j) / (1.0 - left_null_sels.at(j)),
                          right_distinct_sels.at(j) / (1.0 - right_null_sels.at(j))) *
                      (1.0 - left_null_sels.at(j)) * (1.0 - right_null_sels.at(j));
          }
        }
        selectivity += factor;
      } else { /* do nothing */
      }
    }
  }
  LOG_TRACE("selectivity of `col_ref1 =|<=> col_ref1 and col_ref2 =|<=> col_ref2`", K(selectivity));

  selectivity = revise_between_0_1(selectivity);
  return ret;
}

int ObOptEstSel::get_agg_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity,
    const double origen_rows,   // row count before group by
    const double grouped_rows)  // row count after group by
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  const ObRawExpr* aggr_expr = NULL;
  const ObRawExpr* const_expr1 = NULL;
  const ObRawExpr* const_expr2 = NULL;
  selectivity = DEFAULT_AGG_RANGE;
  ObItemType type = qual.get_expr_type();
  // for aggregate function in having clause, only support
  // =  <=>  !=  >  >=  <  <=  [not] btw  [not] in
  if (-1.0 == origen_rows || -1.0 == grouped_rows) {
    // e.g. select * from t7 group by c1 having count(*) > (select c1 from t8 limit 1);
  } else if ((type >= T_OP_EQ && type <= T_OP_NE) || T_OP_IN == type || T_OP_NOT_IN == type || T_OP_BTW == type ||
             T_OP_NOT_BTW == type) {
    if (OB_FAIL(is_valid_agg_qual(qual, is_valid, aggr_expr, const_expr1, const_expr2))) {
      LOG_WARN("failed to check is valid agg qual", K(ret));
    } else if (!is_valid) {
      /* use default selectivity */
    } else if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_FUN_MAX == aggr_expr->get_expr_type() || T_FUN_MIN == aggr_expr->get_expr_type() ||
               T_FUN_COUNT == aggr_expr->get_expr_type()) {
      if (T_OP_EQ == type || T_OP_NSEQ == type) {
        selectivity = DEFAULT_AGG_EQ;
      } else if (T_OP_NE == type || IS_RANGE_CMP_OP(type)) {
        selectivity = DEFAULT_AGG_RANGE;
      } else if (T_OP_BTW == type) {
        // agg(col) btw const1 and const2  <=> agg(col) > const1 AND agg(col) < const2
        selectivity = DEFAULT_AGG_RANGE * DEFAULT_AGG_RANGE;
      } else if (T_OP_NOT_BTW == type) {
        // agg(col) not btw const1 and const2  <=> agg(col) < const1 OR agg(col) > const2
        selectivity = DEFAULT_AGG_RANGE;
        selectivity += DEFAULT_AGG_RANGE - DEFAULT_AGG_RANGE * selectivity;
      } else if (T_OP_IN == type) {
        int64_t N;
        if (OB_ISNULL(const_expr1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null");
        } else if (FALSE_IT(N = const_expr1->get_param_count())) {
        } else if (N < 6) {
          selectivity = DEFAULT_AGG_EQ * N;
        } else {
          selectivity = DEFAULT_AGG_EQ * 5 + (DEFAULT_AGG_EQ - 0.0005 * (N - 6)) * (N - 5);
        }
      } else if (T_OP_NOT_IN == type) {
        // agg(col) not in (const1, const2, ...) <=> agg(col) != const1 and agg(col) != const2 and ...
        if (OB_ISNULL(const_expr1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null");
        } else {
          selectivity = std::pow(DEFAULT_AGG_RANGE, const_expr1->get_param_count());
        }
      } else { /* use default selectivity */
      }
    } else if (T_FUN_SUM == aggr_expr->get_expr_type() || T_FUN_AVG == aggr_expr->get_expr_type()) {
      LOG_TRACE("show group by origen rows and grouped rows", K(origen_rows), K(grouped_rows));
      double rows_per_group = grouped_rows == 0.0 ? origen_rows : origen_rows / grouped_rows;
      if (OB_FAIL(get_agg_sel_with_minmax(
              est_sel_info, *aggr_expr, const_expr1, const_expr2, type, selectivity, rows_per_group))) {
        LOG_WARN("failed to get agg sel with minmax", K(ret));
      }
    } else { /* not max/min/count/sum/avg, use default selectivity */
    }
  } else { /* use default selectivity */
  }
  selectivity = revise_between_0_1(selectivity);
  return ret;
}

int ObOptEstSel::get_agg_sel_with_minmax(const ObEstSelInfo& est_sel_info, const ObRawExpr& aggr_expr,
    const ObRawExpr* const_expr1, const ObRawExpr* const_expr2, const ObItemType type, double& selectivity,
    const double rows_per_group)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_AGG_RANGE;
  const ParamStore* params = est_sel_info.get_params();
  const ObDMLStmt* stmt = est_sel_info.get_stmt();
  ObSQLSessionInfo* session_info = const_cast<ObSQLSessionInfo*>(est_sel_info.get_session_info());
  ObIAllocator& alloc = const_cast<ObIAllocator&>(est_sel_info.get_allocator());
  ObObj result1;
  ObObj result2;
  double distinct_sel = 1.0;
  ObObj maxobj(ObObjType::ObMaxType);
  ObObj minobj(ObObjType::ObMaxType);
  if (OB_ISNULL(aggr_expr.get_param_expr(0)) || OB_ISNULL(params) || OB_ISNULL(stmt) || OB_ISNULL(const_expr1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr.get_param_expr(0)), K(params), K(stmt), K(const_expr1));
  } else if (!aggr_expr.get_param_expr(0)->is_column_ref_expr()) {
    // only handle predicate with format `aggr(column)`
  } else if (OB_FAIL(get_var_basic_sel(
                 est_sel_info, *aggr_expr.get_param_expr(0), &distinct_sel, NULL, NULL, &minobj, &maxobj))) {
    LOG_WARN("failed to get var basic sel", K(ret));
  } else if (minobj.is_invalid_type() || maxobj.is_invalid_type()) {

  } else if (T_OP_IN == type || T_OP_NOT_IN == type) {
    if (OB_UNLIKELY(T_OP_ROW != const_expr1->get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should be row", K(ret), K(*const_expr1));
    } else {
      // if row contain more than 5 params, only calculate selectivity for first 5 params and
      // enlarge result.
      int64_t N = const_expr1->get_param_count() > 5 ? 5 : const_expr1->get_param_count();
      selectivity = T_OP_IN == type ? 0.0 : 1.0;
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        double tmp_sel = T_OP_IN == type ? DEFAULT_AGG_EQ : DEFAULT_AGG_RANGE;
        const ObRawExpr* sub_expr = NULL;
        ObObj tmp_result;
        if (OB_ISNULL(sub_expr = const_expr1->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!ObOptEstUtils::is_calculable_expr(*sub_expr, params->count())) {
        } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(
                       stmt->get_stmt_type(), session_info, sub_expr, tmp_result, params, alloc))) {
          LOG_WARN("Failed to calc const or calculable expr", K(ret));
        } else {
          tmp_sel = get_agg_eq_sel(maxobj,
              minobj,
              tmp_result,
              distinct_sel,
              rows_per_group,
              T_OP_IN == type,
              T_FUN_SUM == aggr_expr.get_expr_type());
        }
        if (T_OP_IN == type) {
          selectivity += tmp_sel;
        } else {
          selectivity *= tmp_sel;
        }
      }
      if (OB_SUCC(ret)) {
        selectivity *= static_cast<double>(const_expr1->get_param_count()) / static_cast<double>(N);
      }
    }
  } else if (!ObOptEstUtils::is_calculable_expr(*const_expr1, params->count())) {
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(
                 stmt->get_stmt_type(), session_info, const_expr1, result1, params, alloc))) {
    LOG_WARN("Failed to calc const or calculable expr", K(ret));
  } else if (T_OP_EQ == type || T_OP_NSEQ == type) {
    selectivity = get_agg_eq_sel(
        maxobj, minobj, result1, distinct_sel, rows_per_group, true, T_FUN_SUM == aggr_expr.get_expr_type());
  } else if (T_OP_NE == type) {
    selectivity = get_agg_eq_sel(
        maxobj, minobj, result1, distinct_sel, rows_per_group, false, T_FUN_SUM == aggr_expr.get_expr_type());
  } else if (IS_RANGE_CMP_OP(type)) {
    selectivity =
        get_agg_range_sel(maxobj, minobj, result1, rows_per_group, type, T_FUN_SUM == aggr_expr.get_expr_type());
  } else if (T_OP_BTW == type || T_OP_NOT_BTW == type) {
    if (OB_ISNULL(const_expr2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!ObOptEstUtils::is_calculable_expr(*const_expr2, params->count())) {
    } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(
                   stmt->get_stmt_type(), session_info, const_expr2, result2, params, alloc))) {
      LOG_WARN("Failed to calc const or calculable expr", K(ret));
    } else {
      selectivity = get_agg_btw_sel(
          maxobj, minobj, result1, result2, rows_per_group, type, T_FUN_SUM == aggr_expr.get_expr_type());
    }
  } else { /* do nothing */
  }
  return ret;
}

// for sum/avg(col) =/<=>/!= const
double ObOptEstSel::get_agg_eq_sel(const ObObj& maxobj, const ObObj& minobj, const ObObj& constobj,
    const double distinct_sel, const double rows_per_group, const bool is_eq, const bool is_sum)
{
  int ret = OB_SUCCESS;
  double sel_ret = DEFAULT_AGG_EQ;
  if (constobj.is_null()) {
    sel_ret = 0.0;
  } else if (minobj.is_integer_type()) {
    double const_val;
    // if convert failed, use default selectivity
    if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj, const_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else {
      int64_t min_val = minobj.get_int();
      int64_t max_val = maxobj.get_int();
      LOG_TRACE("get values for eq sel", K(max_val), K(min_val), K(const_val));
      if (is_sum) {
        min_val *= rows_per_group;
        max_val *= rows_per_group;
      }
      int64_t length = max_val - min_val + 1;
      if (is_eq) {
        sel_ret = 1.0 / length;
        if (const_val < min_val) {
          sel_ret -= sel_ret * (min_val - const_val) / length;
        } else if (const_val > max_val) {
          sel_ret -= sel_ret * (const_val - max_val) / length;
        } else {
        }
      } else {
        sel_ret = 1.0 - 1.0 / length;
      }
    }
  } else {
    // for non int type, we think ndv won't change obviously after sum/avg, just use origin ndv.
    sel_ret = is_eq ? distinct_sel : 1.0 - distinct_sel;
  }
  sel_ret = revise_between_0_1(sel_ret);
  return sel_ret;
}

// for sum/avg(col) >/>=/</<= const
double ObOptEstSel::get_agg_range_sel(const ObObj& maxobj, const ObObj& minobj, const ObObj& constobj,
    const double rows_per_group, const ObItemType type, const bool is_sum)
{
  int ret = OB_SUCCESS;
  double sel_ret = DEFAULT_AGG_RANGE;
  if (constobj.is_null()) {
    sel_ret = 0.0;
  } else {
    double min_val;
    double max_val;
    double const_val;
    // if convert failed, use default selectivity
    if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&minobj, min_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&maxobj, max_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj, const_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else {
      LOG_TRACE("get values for range sel", K(max_val), K(min_val), K(const_val));
      if (is_sum) {
        min_val *= rows_per_group;
        max_val *= rows_per_group;
      }
      double length = max_val - min_val + 1.0;
      if (T_OP_GE == type || T_OP_GT == type) {
        if (T_OP_GT == type) {
          // c1 > 1 <=> c1 >= 2, for non int type result is not precise.
          const_val += 1.0;
        }
        if (const_val <= min_val) {
          sel_ret = 1.0;
        } else if (const_val <= max_val) {
          sel_ret = (max_val - const_val + 1.0) / length;
        } else {
          sel_ret = 1.0 / length;
          sel_ret -= sel_ret * (const_val - max_val) / length;
        }
      } else if (T_OP_LE == type || T_OP_LT == type) {
        if (T_OP_LT == type) {
          // c1 < 1 <=> c1 <= 0, for non int type result is not precise.
          const_val -= 1.0;
        }
        if (const_val >= max_val) {
          sel_ret = 1.0;
        } else if (const_val >= min_val) {
          sel_ret = (const_val - min_val + 1.0) / length;
        } else {
          sel_ret = 1.0 / length;
          sel_ret -= sel_ret * (min_val - const_val) / length;
        }
      } else { /* do nothing */
      }
    }
  }
  sel_ret = revise_between_0_1(sel_ret);
  return sel_ret;
}

// for sum/avg(col) [not] between const1 and const2
double ObOptEstSel::get_agg_btw_sel(const ObObj& maxobj, const ObObj& minobj, const ObObj& constobj1,
    const ObObj& constobj2, const double rows_per_group, const ObItemType type, const bool is_sum)
{
  int ret = OB_SUCCESS;
  double sel_ret = DEFAULT_AGG_RANGE;
  if (constobj1.is_null() || constobj2.is_null()) {
    sel_ret = 0.0;
  } else {
    double min_val;
    double max_val;
    double const_val1;
    double const_val2;
    // if convert failed, use default selectivity
    if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&minobj, min_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&maxobj, max_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj1, const_val1))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj2, const_val2))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else {
      LOG_TRACE("get values for between sel", K(max_val), K(min_val), K(const_val1), K(const_val2));
      if (is_sum) {
        min_val *= rows_per_group;
        max_val *= rows_per_group;
      }
      double length = max_val - min_val + 1.0;
      if (T_OP_BTW == type) {
        if (const_val1 > const_val2) {
          sel_ret = 0.0;
        } else {
          double tmp_min = std::max(const_val1, min_val);
          double tmp_max = std::min(const_val2, max_val);
          sel_ret = (tmp_max - tmp_min + 1.0) / length;
        }
      } else if (T_OP_NOT_BTW == type) {
        if (const_val1 > const_val2) {
          sel_ret = 1.0;
        } else {
          double tmp_min = std::max(const_val1, min_val);
          double tmp_max = std::min(const_val2, max_val);
          sel_ret = 1 - (tmp_max - tmp_min + 1.0) / length;
        }
      } else { /* do nothing */
      }
    }
  }
  sel_ret = revise_between_0_1(sel_ret);
  return sel_ret;
}

int ObOptEstSel::is_valid_agg_qual(const ObRawExpr& qual, bool& is_valid, const ObRawExpr*& aggr_expr,
    const ObRawExpr*& const_expr1, const ObRawExpr*& const_expr2)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObRawExpr* expr0 = NULL;
  const ObRawExpr* expr1 = NULL;
  const ObRawExpr* expr2 = NULL;
  if (T_OP_BTW == qual.get_expr_type() || T_OP_NOT_BTW == qual.get_expr_type()) {
    if (OB_ISNULL(expr0 = qual.get_param_expr(0)) || OB_ISNULL(expr1 = qual.get_param_expr(1)) ||
        OB_ISNULL(expr2 = qual.get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (expr0->has_flag(IS_AGG) && (expr1->is_const_expr() || expr1->has_flag(IS_CALCULABLE_EXPR)) &&
               (expr2->is_const_expr() || expr2->has_flag(IS_CALCULABLE_EXPR))) {
      is_valid = true;
      aggr_expr = expr0;
      const_expr1 = expr1;
      const_expr2 = expr2;
    } else { /* do nothing */
    }
  } else {
    if (OB_ISNULL(expr0 = qual.get_param_expr(0)) || OB_ISNULL(expr1 = qual.get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_OP_IN == qual.get_expr_type() || T_OP_NOT_IN == qual.get_expr_type()) {
      if (qual.has_flag(CNT_SUB_QUERY)) {
        // do nothing
      } else if (expr0->has_flag(IS_AGG) && T_OP_ROW == expr1->get_expr_type()) {
        is_valid = true;
        aggr_expr = expr0;
        const_expr1 = expr1;
      } else if (expr1->has_flag(IS_AGG) && T_OP_ROW == expr0->get_expr_type()) {
        is_valid = true;
        aggr_expr = expr1;
        const_expr1 = expr0;
      } else { /* do nothing */
      }
    } else if (expr0->has_flag(IS_AGG) && (expr1->is_const_expr() || expr1->has_flag(IS_CALCULABLE_EXPR))) {
      is_valid = true;
      aggr_expr = expr0;
      const_expr1 = expr1;
    } else if ((expr0->is_const_expr() || expr0->has_flag(IS_CALCULABLE_EXPR)) && expr1->has_flag(IS_AGG)) {
      is_valid = true;
      aggr_expr = expr1;
      const_expr1 = expr0;
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObOptEstSel::get_global_min_max(
    const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& column_expr, ObObj*& minobj, ObObj*& maxobj)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnStatValueHandle, 8> cstats;
  ObColumnStatValueHandle cstat;
  ObStatManager* stat_manager = NULL;
  uint64_t column_id = column_expr.get_column_id();
  uint64_t table_id = column_expr.get_table_id();
  ObEstTableStat* tstat = NULL;

  if (OB_ISNULL(minobj) || OB_ISNULL(maxobj) ||
      OB_ISNULL(stat_manager = const_cast<ObStatManager*>(est_sel_info.get_stat_manager()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(ret), K(minobj), K(maxobj), K(stat_manager));
  } else if (OB_SUCCESS !=
             const_cast<ObEstSelInfo&>(est_sel_info).get_table_stats().get_tstat_by_table_id(table_id, tstat)) {
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(tstat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    const ObIArray<uint64_t>& all_used_parts = tstat->get_all_used_parts();
    int64_t ref_id = tstat->get_ref_id();
    if (all_used_parts.count() < 2) {
    } else if (OB_UNLIKELY(OB_INVALID_ID == ref_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref_id for basic table should not be OB_INVALID_ID", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < all_used_parts.count(); ++i) {
        uint64_t part_id = all_used_parts.at(i);
        if (OB_FAIL(stat_manager->get_column_stat(ref_id, part_id, column_id, cstat))) {
          LOG_WARN("get column stat error", K(ret), K(ref_id), K(part_id));
        } else if (OB_FAIL(cstats.push_back(cstat))) {
          LOG_WARN("failed to push back ColumnStatValueHandle", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < cstats.count(); ++i) {
        if (OB_ISNULL(cstats.at(i).cache_value_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column stat is null");
        } else if (0 != cstats.at(i).cache_value_->get_num_distinct()) {
          const ObObj& tmp_min = cstats.at(i).cache_value_->get_min_value();
          const ObObj& tmp_max = cstats.at(i).cache_value_->get_max_value();
          if (tmp_min < *minobj) {
            *minobj = tmp_min;
          }
          if (tmp_max > *maxobj) {
            *maxobj = tmp_max;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObIAllocator& alloc = const_cast<ObIAllocator&>(est_sel_info.get_allocator());
      if (OB_FAIL(ob_write_obj(alloc, *minobj, *minobj))) {
        LOG_WARN("failed to write obj", K(ret), K(*minobj));
      } else if (OB_FAIL(ob_write_obj(alloc, *maxobj, *maxobj))) {
        LOG_WARN("failed to write obj", K(ret), K(*maxobj));
      }
    }
  }
  return ret;
}

int ObOptEstSel::is_column_pkey(const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& col_expr, bool& is_pkey)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> dummy_col_ids;
  bool dummy_is_union_pkey;
  if (OB_FAIL(dummy_col_ids.push_back(col_expr.get_column_id()))) {
    LOG_WARN("failed to push back column id", K(ret));
  } else if (OB_FAIL(is_columns_contain_pkey(
                 est_sel_info, dummy_col_ids, col_expr.get_table_id(), is_pkey, dummy_is_union_pkey))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  }
  return ret;
}

int ObOptEstSel::is_columns_contain_pkey(
    const ObEstSelInfo& est_sel_info, const ObIArray<ObRawExpr*>& col_exprs, bool& is_pkey, bool& is_union_pkey)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> col_ids;
  uint64_t table_id;
  if (OB_FAIL(extract_column_ids(col_exprs, col_ids, table_id))) {
    LOG_WARN("failed to extract column ids", K(ret));
  } else if (OB_FAIL(is_columns_contain_pkey(est_sel_info, col_ids, table_id, is_pkey, is_union_pkey))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  }
  return ret;
}

int ObOptEstSel::is_columns_contain_pkey(const ObEstSelInfo& est_sel_info, const ObIArray<uint64_t>& col_ids,
    const uint64_t table_id, bool& is_pkey, bool& is_union_pkey)
{
  int ret = OB_SUCCESS;
  is_pkey = true;
  is_union_pkey = false;
  ObSEArray<uint64_t, 4> pkey_ids;
  if (OB_FAIL(const_cast<ObEstSelInfo&>(est_sel_info).get_table_stats().get_pkey_ids_by_table_id(table_id, pkey_ids))) {
    LOG_WARN("failed to get pkey ids by table id", K(ret));
  } else if (pkey_ids.empty()) {
    is_pkey = false;
  } else {
    for (int64_t i = 0; is_pkey && i < pkey_ids.count(); ++i) {
      bool find = false;
      for (int64_t j = 0; !find && j < col_ids.count(); ++j) {
        if (pkey_ids.at(i) == col_ids.at(j)) {
          find = true;
        }
      }
      is_pkey = find;
    }
    is_union_pkey = pkey_ids.count() > 1;
  }
  return ret;
}

int ObOptEstSel::extract_column_ids(
    const ObIArray<ObRawExpr*>& col_exprs, ObIArray<uint64_t>& col_ids, uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* cur_col = NULL;
  table_id = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_exprs.count(); ++i) {
    ObRawExpr* cur_expr = col_exprs.at(i);
    if (OB_ISNULL(cur_expr) || OB_UNLIKELY(!cur_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret));
    } else if (FALSE_IT(cur_col = static_cast<ObColumnRefRawExpr*>(cur_expr))) {
    } else if (OB_INVALID_INDEX == table_id) {
      table_id = cur_col->get_table_id();
    } else if (OB_UNLIKELY(table_id != cur_col->get_table_id())) {
      LOG_WARN("columns not belong to same table", K(ret), K(*cur_col), K(table_id));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(col_ids.push_back(cur_col->get_column_id()))) {
      LOG_WARN("failed to push back column id", K(ret));
    }
  }
  return ret;
}

/**
 *  check if multi column join only related to two tables
 */
int ObOptEstSel::is_valid_multi_join(ObIArray<ObRawExpr*>& quals, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_UNLIKELY(quals.count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("quals should have more than 1 exprs", K(ret));
  } else if (OB_ISNULL(quals.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObRelIds& rel_ids = quals.at(0)->get_relation_ids();
    is_valid = rel_ids.num_members() == 2;
    for (int64_t i = 1; OB_SUCC(ret) && is_valid && i < quals.count(); ++i) {
      ObRawExpr* cur_expr = quals.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!rel_ids.equal(cur_expr->get_relation_ids())) {
        is_valid = false;
      }
    }
  }
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase
