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
#include "sql/optimizer/ob_access_path_estimation.h"
#include "sql/optimizer/ob_sel_estimator.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{
inline double revise_ndv(double ndv) { return ndv < 1.0 ? 1.0 : ndv; }

void SimpleRange::set_whole_range()
{
  start_.set_min_value();
  end_.set_max_value();
  inclusive_start_ = false;
  inclusive_end_ = false;
}

void SimpleRange::set_false_range()
{
  start_.set_max_value();
  end_.set_min_value();
  inclusive_start_ = false;
  inclusive_end_ = false;
}

int SimpleRange::compare_with_end(const SimpleRange &r, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  if (end_.is_max_value()) {
    if (!r.end_.is_max_value()) {
      cmp = 1;
    }
  } else if (r.end_.is_max_value()) {
    cmp = -1;
  } else if (OB_FAIL(end_.compare(r.end_, cmp))) {
    LOG_WARN("failed to compare", K(ret));
  } else {
    if (0 == cmp) {
      if (inclusive_end_ && !r.inclusive_end_) {
        cmp = 1;
      } else if (!inclusive_end_ && r.inclusive_end_) {
        cmp = -1;
      }
    }
  }
  return ret;
}

int SimpleRange::compare_with_start(const SimpleRange &r, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  if (start_.is_min_value()) {
    if (!r.start_.is_min_value()) {
      cmp = -1;
    }
  } else if (r.start_.is_min_value()) {
    cmp = 1;
  } else if (OB_FAIL(start_.compare(r.start_, cmp))) {
    LOG_WARN("failed to compare", K(ret));
  } else {
    if (0 == cmp) {
      if (inclusive_start_ && !r.inclusive_start_) {
        cmp = -1;
      } else if (!inclusive_start_ && r.inclusive_start_) {
        cmp = 1;
      }
    }
  }
  return ret;
}

bool SimpleRange::intersect(const SimpleRange &r)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  int cmp_start = 0;
  int cmp_end = 0;
  if (!start_.can_compare(r.start_) || !end_.can_compare(r.end_)) {
    // do nothing
  } else if (OB_FAIL(compare_with_start(r, cmp_start))) {
    LOG_WARN("failed to compare start", K(ret));
  } else if (OB_FAIL(compare_with_end(r, cmp_end))) {
    LOG_WARN("failed to compare end", K(ret));
  } else {
    bret = true;
    if (cmp_start == -1) {
      start_ = r.start_;
      inclusive_start_ = r.inclusive_start_;
    }
    if (cmp_end == 1) {
      end_ = r.end_;
      inclusive_end_ = r.inclusive_end_;
    }
  }
  return bret;
}

void SimpleRange::set_bound(ObItemType item_type, ObObj bound)
{
  if (bound.is_null()) {
    if (T_OP_IS == item_type || T_OP_NSEQ == item_type) {
      start_.set_null();
      end_.set_null();
      inclusive_start_ = true;
      inclusive_end_ = true;
    } else if (T_OP_IS_NOT == item_type) {
      set_whole_range();
    } else {
      set_false_range();
    }
  } else if (T_OP_LE == item_type) {
    end_ = bound;
    inclusive_end_ = true;
  } else if (T_OP_LT == item_type) {
    end_ = bound;
    inclusive_end_ = false;
  } else if (T_OP_GE == item_type) {
    start_ = bound;
    inclusive_start_ = true;
  } else if (T_OP_GT == item_type) {
    start_ = bound;
    inclusive_start_ = false;
  } else if (T_OP_EQ == item_type || T_OP_NSEQ == item_type) {
    start_ = bound;
    end_ = bound;
    inclusive_start_ = true;
    inclusive_end_ = true;
  }
}

void SimpleRange::set_bound(ObItemType item_type, double bound)
{
  ObObj obj;
  obj.set_double(bound);
  set_bound(item_type, obj);
}

bool SimpleRange::is_valid_range()
{
  bool bret = false;
  int cmp = 0;
  if (!start_.can_compare(end_)) {
    bret = false;
  } else if (start_.is_null() && end_.is_null()) {
    bret = true;
  } else if (start_.is_max_value() || end_.is_min_value() ||
             start_.is_null() || end_.is_null()) {
    bret = false;
  } else if (start_.is_min_value() || end_.is_max_value()) {
    bret = true;
  } else if (OB_SUCCESS != start_.compare(end_, cmp)) {
    bret = false;
  } else {
    if (-1 == cmp) {
      bret = true;
    } else if (1 == cmp) {
      bret = false;
    } else if (0 == cmp) {
      if (inclusive_start_ && inclusive_end_) {
        bret = true;
      } else {
        bret = false;
      }
    }
  }
  return bret;
}

bool SimpleRange::is_superset(const SimpleRange &r) const
{
  bool bret = false;
  int ret = OB_SUCCESS;
  int cmp_start = 0;
  int cmp_end = 0;
  if (!start_.can_compare(r.start_) || !end_.can_compare(r.end_)) {
    // do nothing
  } else if (OB_FAIL(compare_with_start(r, cmp_start))) {
    LOG_WARN("failed to compare start", K(ret));
  } else if (OB_FAIL(compare_with_end(r, cmp_end))) {
    LOG_WARN("failed to compare end", K(ret));
  } else {
    bret = cmp_start <= 0 && cmp_end >= 0;
  }
  return bret;
}

void SimpleRange::multiply_double(double coff)
{
  if (start_.is_double()) {
    start_.set_double(start_.get_double() * coff);
  }
  if (end_.is_double()) {
    end_.set_double(end_.get_double() * coff);
  }
  if (coff < 0) {
    std::swap(start_, end_);
    std::swap(inclusive_start_, inclusive_end_);
    if (start_.is_min_value()) {
      start_.set_max_value();
    } else if (start_.is_max_value()) {
      start_.set_min_value();
    }
    if (end_.is_min_value()) {
      end_.set_max_value();
    } else if (end_.is_max_value()) {
      end_.set_min_value();
    }
  }
}

int ObSelEstimator::append_estimators(ObIArray<ObSelEstimator *> &sel_estimators, ObSelEstimator *new_estimator)
{
  int ret = OB_SUCCESS;
  bool find_same_class = false;
  if (OB_ISNULL(new_estimator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("estimator is null", K(new_estimator));
  } else if (new_estimator->is_independent()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !find_same_class && i < sel_estimators.count(); i ++) {
      if (OB_ISNULL(sel_estimators.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("estimator is null", K(ret), K(sel_estimators));
      } else if (OB_FAIL(sel_estimators.at(i)->merge(*new_estimator, find_same_class))) {
        LOG_WARN("failed to merge same class", K(ret), KPC(sel_estimators.at(i)), KPC(new_estimator));
      }
    }
  }
  if (OB_SUCC(ret) && !find_same_class) {
    if (OB_FAIL(sel_estimators.push_back(new_estimator))) {
      LOG_WARN("failed to push back", K(ret), K(sel_estimators));
    }
  }
  return ret;
}

void ObSelEstimator::extract_default_eigen_expr(const ObRawExpr *expr)
{
  bool is_valid = true;
  if (OB_NOT_NULL(expr) && !expr->is_const_expr()) {
    eigen_expr_ = expr;
  } else {
    eigen_expr_ = NULL;
  }
  while (is_valid && OB_NOT_NULL(eigen_expr_) &&
         OB_SUCCESS == ObOptSelectivity::remove_ignorable_func_for_est_sel(eigen_expr_) &&
         eigen_expr_->get_param_count() > 0 &&
         (IS_BOOL_OP(eigen_expr_->get_expr_type()) ||
          T_OP_ADD == eigen_expr_->get_expr_type() ||
          T_OP_MINUS == eigen_expr_->get_expr_type() ||
          T_OP_ROW == eigen_expr_->get_expr_type())) {
    is_valid = true;
    const ObRawExpr *variable_expr = NULL;
    for (int64_t i = 0; is_valid && i < eigen_expr_->get_param_count(); i ++) {
      if (NULL == eigen_expr_->get_param_expr(i)) {
        is_valid = false;
      } else if (eigen_expr_->get_param_expr(i)->is_const_expr()) {
        // do nothing
      } else if (NULL != variable_expr) {
        is_valid = false;
      } else {
        variable_expr = eigen_expr_->get_param_expr(i);
      }
    }
    if (is_valid) {
      eigen_expr_ = variable_expr;
    }
  }
  return;
}

int ObDefaultSelEstimator::get_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  double &selectivity,
                                  ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  const ObRawExpr &qual = *expr_;
  double tmp_sel = 1.0;
  int64_t idx = 0;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", KPC(this));
  } else if (qual.is_spatial_expr()) {
    selectivity = DEFAULT_SPATIAL_SEL;
  } else if (ObOptimizerUtil::find_item(all_predicate_sel, ObExprSelPair(&qual, 0), &idx)) {
    selectivity = all_predicate_sel.at(idx).sel_;
  } else {
    selectivity = DEFAULT_SEL;
  }
  return ret;
}

int ObConstSelEstimator::get_const_sel(const OptSelectivityCtx &ctx,
                                       const ObRawExpr &qual,
                                       double &selectivity)
{
  int ret = OB_SUCCESS;
  const ParamStore *params = ctx.get_params();
  const ObDMLStmt *stmt = ctx.get_stmt();
  if (OB_ISNULL(params) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(params), K(stmt));
  } else if (ObOptEstUtils::is_calculable_expr(qual, params->count())) {
    ObObj const_value;
    bool got_result = false;
    bool is_true = false;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                          &qual,
                                                          const_value,
                                                          got_result,
                                                          ctx.get_allocator()))) {
      LOG_WARN("failed to calc const or calculable expr", K(ret));
    } else if (!got_result) {
      selectivity = DEFAULT_SEL;
    } else if (OB_FAIL(ObObjEvaluator::is_true(const_value, is_true))) {
      LOG_WARN("failed to check is const value true", K(ret));
    } else {
      selectivity = is_true ? 1.0 : 0.0;
    }
  } else {
    selectivity = DEFAULT_SEL;
  }
  return ret;
}

int ObColumnSelEstimator::get_column_sel(const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const ObRawExpr &qual,
                                         double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  double distinct_sel = 0.0;
  double null_sel = 0.0;
  if (!ob_is_string_or_lob_type(qual.get_data_type())) {
    if (OB_FAIL(ObOptSelectivity::check_column_in_current_level_stmt(ctx.get_stmt(), qual))) {
      LOG_WARN("Failed to check column in cur level stmt", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, qual, &distinct_sel, &null_sel))) {
      LOG_WARN("Failed to calc basic equal sel", K(ret));
    } else {
      selectivity = 1.0 - distinct_sel - null_sel;
    }
  }
  return ret;
}

int ObInSelEstimator::get_in_sel(const OptTableMetas &table_metas,
                                 const OptSelectivityCtx &ctx,
                                 const ObRawExpr &qual,
                                 double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = 0.0;
  double tmp_selectivity = 1.0;
  double distinct_sel = 1.0;
  double null_sel = 0.0;
  const ObRawExpr *left_expr = NULL;
  const ObRawExpr *right_expr = NULL;
  const ObRawExpr *param_expr = NULL;
  bool contain_null = false;
  if (OB_UNLIKELY(2 != qual.get_param_count()) ||
      OB_ISNULL(left_expr = qual.get_param_expr(0)) ||
      OB_ISNULL(right_expr = qual.get_param_expr(1)) ||
      T_OP_ROW != right_expr->get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect expr", K(ret), K(qual), K(left_expr), K(right_expr));
  } else if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(left_expr))) {
    LOG_WARN("failed to remove ignorable function", K(ret));
  } else if (OB_LIKELY(left_expr->is_column_ref_expr() && !right_expr->has_flag(CNT_COLUMN))) {
    ObOptColumnStatHandle handler;
    ObObj expr_value;
    const ObColumnRefRawExpr *col = static_cast<const ObColumnRefRawExpr *>(left_expr);
    hash::ObHashSet<ObObj> obj_set;
    ObHistEqualSelHelper helper;
    if (OB_FAIL(obj_set.create(hash::cal_next_prime(right_expr->get_param_count()),
                               "OptSelHashSet", "OptSelHashSet"))) {
      LOG_WARN("failed to create hash set", K(ret), K(right_expr->get_param_count()));
    } else if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, *left_expr, &distinct_sel, &null_sel))) {
      LOG_WARN("failed to get column basic selectivity", K(ret));
    } else if (OB_FAIL(helper.init(table_metas, ctx, *col))) {
      LOG_WARN("failed to get histogram by column", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_expr->get_param_count(); ++i) {
      // bool can_use_hist = false;
      bool get_value = false;
      if (OB_ISNULL(param_expr = right_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::get_compare_value(ctx, col, param_expr, expr_value, get_value))) {
        // cast may failed due to invalid type or value out of range.
        // Then use ndv instead of histogram
        get_value = false;
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        if (helper.is_valid() && get_value) {
          double null_sel = 0;
          helper.set_compare_value(expr_value);
          if (OB_HASH_EXIST == obj_set.exist_refactored(expr_value)) {
            // duplicate value, do nothing
          } else if (OB_FAIL(obj_set.set_refactored(expr_value))) {
            LOG_WARN("failed to set refactorcd", K(ret), K(expr_value));
          } else if (OB_FAIL(helper.get_sel(ctx, tmp_selectivity))) {
            LOG_WARN("failed to get equal density", K(ret));
          } else {
            selectivity += tmp_selectivity * (1 - null_sel);
          }
        } else if (!get_value) {
          // invalid value, for example c1 in (exec_param). Do not check obj exists.
          if (param_expr->get_result_type().is_null()) {
            contain_null = true;
          } else {
            selectivity += distinct_sel;
          }
        } else if (OB_HASH_EXIST == obj_set.exist_refactored(expr_value)) {
          // do nothing
        } else if (OB_FAIL(obj_set.set_refactored(expr_value))) {
          LOG_WARN("failed to set refactorcd", K(ret), K(expr_value));
        } else if (expr_value.is_null()) {
          contain_null = true;
        } else {
          selectivity += distinct_sel;
        }
      }
    }
    if (obj_set.created()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = obj_set.destroy())) {
        LOG_WARN("failed to destroy hash set", K(tmp_ret), K(ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < right_expr->get_param_count(); ++i) {
      if (OB_ISNULL(param_expr = right_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (OB_FAIL(ObEqualSelEstimator::get_equal_sel(table_metas, ctx, *left_expr, *param_expr,
                                                            false, tmp_selectivity))) {
        LOG_WARN("Failed to get equal sel", K(ret), KPC(left_expr));
      } else {
        selectivity += tmp_selectivity;
      }
    }
  }

  selectivity = ObOptSelectivity::revise_between_0_1(selectivity);
  if (OB_SUCC(ret) && T_OP_NOT_IN == qual.get_expr_type()) {
    selectivity = 1.0 - selectivity;
    if (contain_null) {
      selectivity = 0.0;
    } else if (left_expr->has_flag(CNT_COLUMN) && !right_expr->has_flag(CNT_COLUMN)) {
      ObSEArray<ObRawExpr*, 2> cur_vars;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(left_expr, cur_vars))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (1 == cur_vars.count()) { // only one column, consider null_sel
        if (OB_ISNULL(cur_vars.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret));
        } else if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, *cur_vars.at(0),
                                                                  &distinct_sel, &null_sel))) {
          LOG_WARN("failed to get column basic sel", K(ret));
        } else if (distinct_sel > ((1.0 - null_sel) / 2.0)) {
          // ndv < 2
          // TODO: @yibo 这个refine过程不太理解
          selectivity = distinct_sel / 2.0;
        } else {
          selectivity -= null_sel;
          selectivity = std::max(distinct_sel, selectivity); // at least one distinct_sel
        }
      } else { }//do nothing
    }
  }
  return ret;
}

int ObIsSelEstimator::get_sel(const OptTableMetas &table_metas,
                              const OptSelectivityCtx &ctx,
                              double &selectivity,
                              ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  if (can_calc_sel_) {
    if (OB_ISNULL(expr_) || OB_ISNULL(left_expr_) || OB_UNLIKELY(!left_expr_->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpect error", K(ret), KPC(expr_), KPC(left_expr_));
    } else if (OB_LIKELY(right_const_obj_.is_null())) {
      if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, *left_expr_, NULL, &selectivity))) {
        LOG_WARN("Failed to get var distinct sel", K(ret));
      }
    } else if (right_const_obj_.is_tinyint() &&
               !ob_is_string_or_lob_type(left_expr_->get_data_type())) {
      double distinct_sel = 0.0;
      double null_sel = 0.0;
      if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, *left_expr_, &distinct_sel, &null_sel))) {
        LOG_WARN("Failed to get var distinct sel", K(ret));
      } else {
        //distinct_num < 2. That is distinct_num only 1,(As double and statistics not completely accurate,
        //use (1 - null_sel)/ 2.0 to check)
        if (distinct_sel > (1 - null_sel) / 2.0) {
          //Ihe formula to calc sel of 'c1 is true' is (1 - distinct_sel(var = 0) - null_sel).
          //If distinct_num is 1, the sel would be 0.0.
          //But we don't kown whether distinct value is 0. So gess the selectivity: (1 - null_sel)/2.0
          distinct_sel = (1- null_sel) / 2.0;//don't kow the value, just get half.
        }
        selectivity = (right_const_obj_.is_true()) ? (1 - distinct_sel - null_sel) : distinct_sel;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), KPC(left_expr_), K(right_const_obj_));
    }
  }

  if (OB_SUCC(ret) && T_OP_IS_NOT == expr_->get_expr_type()) {
    selectivity = 1.0 - selectivity;
  }
  return ret;
}

int ObCmpSelEstimator::get_sel(const OptTableMetas &table_metas,
                               const OptSelectivityCtx &ctx,
                               double &selectivity,
                               ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_INEQ_SEL;
  if (can_calc_sel_) {
    if (OB_ISNULL(expr_) || OB_ISNULL(col_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret), KPC(col_expr_), KPC(expr_));
    } else if (OB_FAIL(ObOptSelectivity::get_column_range_sel(table_metas, ctx, *col_expr_, *expr_, true, selectivity))) {
      LOG_WARN("Failed to get column range sel", KPC(expr_), KPC(col_expr_), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObBtwSelEstimator::get_sel(const OptTableMetas &table_metas,
                               const OptSelectivityCtx &ctx,
                               double &selectivity,
                               ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  if (can_calc_sel_) {
    if (OB_ISNULL(expr_) || OB_ISNULL(col_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret), KPC(col_expr_), KPC(expr_));
    } else if (OB_FAIL(ObOptSelectivity::get_column_range_sel(table_metas, ctx, *col_expr_, *expr_, true, selectivity))) {
      LOG_WARN("failed to get column range sel", K(ret), KPC(expr_), KPC(col_expr_));
    }
  }
  return ret;
}

int ObEqualSelEstimator::get_sel(const OptTableMetas &table_metas,
                                 const OptSelectivityCtx &ctx,
                                 double &selectivity,
                                 ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  const ObRawExpr &qual = *expr_;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", KPC(this));
  } else {
    const ObRawExpr *left_expr = qual.get_param_expr(0);
    const ObRawExpr *right_expr = qual.get_param_expr(1);
    if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret), K(qual), K(left_expr), K(right_expr));
    } else if (T_OP_NE == qual.get_expr_type()) {
      if (OB_FAIL(get_ne_sel(table_metas, ctx, *left_expr, *right_expr, selectivity))) {
        LOG_WARN("failed to get equal sel", K(ret));
      }
    } else if (T_OP_EQ == qual.get_expr_type() ||
               T_OP_NSEQ == qual.get_expr_type()) {
      if (OB_FAIL(get_equal_sel(table_metas, ctx, *left_expr, *right_expr,
                                T_OP_NSEQ == qual.get_expr_type(), selectivity))) {
        LOG_WARN("failed to get equal sel", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr", KPC(this));
    }
  }
  return ret;
}

int ObEqualSelEstimator::get_ne_sel(const OptTableMetas &table_metas,
                                    const OptSelectivityCtx &ctx,
                                    const ObRawExpr &l_expr,
                                    const ObRawExpr &r_expr,
                                    double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  if (T_OP_ROW == l_expr.get_expr_type() && T_OP_ROW == r_expr.get_expr_type()) {
    // (var1, var2) != (var3, var4) => var1 != var3 or var2 != var4
    selectivity = 0;
    double tmp_selectivity = 1.0;
    const ObRawExpr *l_param = NULL;
    const ObRawExpr *r_param = NULL;
    const ObRawExpr *l_row = &l_expr;
    const ObRawExpr *r_row = &r_expr;
    if (l_expr.get_param_count() == 1 && OB_NOT_NULL(l_expr.get_param_expr(0)) &&
        T_OP_ROW == l_expr.get_param_expr(0)->get_expr_type()) {
      l_row = l_expr.get_param_expr(0);
    }
    if (r_expr.get_param_count() == 1 && OB_NOT_NULL(r_expr.get_param_expr(0)) &&
        T_OP_ROW == r_expr.get_param_expr(0)->get_expr_type()) {
      r_row = r_expr.get_param_expr(0);
    }
    if (OB_UNLIKELY(l_row->get_param_count() != r_row->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", KPC(l_row), KPC(r_row), K(ret));
    } else {
      int64_t num = l_row->get_param_count();
      ObSEArray<double, 4> selectivities;
      for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
        if (OB_ISNULL(l_param = l_row->get_param_expr(i)) ||
            OB_ISNULL(r_param = r_row->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null expr", K(ret), K(l_row), K(r_row), K(i));
        } else if (OB_FAIL(SMART_CALL(get_ne_sel(table_metas, ctx, *l_param,
                                                 *r_param, tmp_selectivity)))) {
          LOG_WARN("failed to get equal selectivity", K(ret));
        } else if (OB_FAIL(selectivities.push_back(1 - tmp_selectivity))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      selectivity = 1 - ctx.get_correlation_model().combine_filters_selectivity(selectivities);
    }
  } else if (l_expr.has_flag(CNT_COLUMN) && r_expr.has_flag(CNT_COLUMN)) {
    if (OB_FAIL(get_cntcol_op_cntcol_sel(table_metas, ctx, l_expr, r_expr, T_OP_NE, selectivity))) {
      LOG_WARN("failed to get cntcol op cntcol sel", K(ret));
    }
  } else if ((l_expr.has_flag(CNT_COLUMN) && !r_expr.has_flag(CNT_COLUMN)) ||
             (!l_expr.has_flag(CNT_COLUMN) && r_expr.has_flag(CNT_COLUMN))) {
    const ObRawExpr *cnt_col_expr = l_expr.has_flag(CNT_COLUMN) ? &l_expr : &r_expr;
    const ObRawExpr *const_expr = l_expr.has_flag(CNT_COLUMN) ? &r_expr : &l_expr;
    ObSEArray<const ObColumnRefRawExpr *, 2> column_exprs;
    bool only_monotonic_op = true;
    bool null_const = false;
    double ndv = 1.0;
    double nns = 0;
    ObHistEqualSelHelper helper;
    bool can_use_hist = false;
    if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(cnt_col_expr))) {
      LOG_WARN("failed to remove ignorable function", K(ret));
    } else if (cnt_col_expr->is_column_ref_expr()) {
      // column != const
      const ObColumnRefRawExpr *col = static_cast<const ObColumnRefRawExpr*>(cnt_col_expr);
      if (OB_FAIL(helper.init(table_metas, ctx, *col))) {
        LOG_WARN("failed to get histogram by column", K(ret));
      } else if (!helper.is_valid()) {
        // do nothing
      } else if (OB_FAIL(helper.set_compare_value(ctx, const_expr, can_use_hist))) {
        // cast may failed due to invalid type or value out of range.
        // Then use ndv instead of histogram
        can_use_hist = false;
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      if (can_use_hist) {
        if (OB_FAIL(helper.get_sel(ctx, selectivity))) {
          LOG_WARN("Failed to get equal density", K(ret));
        } else if (OB_FAIL(ObOptSelectivity::get_column_ndv_and_nns(table_metas, ctx, *cnt_col_expr, NULL, &nns))) {
          LOG_WARN("failed to get column ndv and nns", K(ret));
        } else {
          selectivity = (1.0 - selectivity) * nns;
        }
      } else if (OB_FAIL(ObOptEstUtils::extract_column_exprs_with_op_check(cnt_col_expr,
                                                                           column_exprs,
                                                                           only_monotonic_op))) {
        LOG_WARN("failed to extract column exprs with op check", K(ret));
      } else if (!only_monotonic_op || column_exprs.count() > 1) {
        selectivity = DEFAULT_SEL; //cnt_col_expr contain not monotonic op OR has more than 1 var
      } else if (OB_UNLIKELY(1 != column_exprs.count()) || OB_ISNULL(column_exprs.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected contain column expr", K(ret), K(*cnt_col_expr));
      } else if (OB_FAIL(ObOptEstUtils::if_expr_value_null(ctx.get_params(),
                                                          *const_expr,
                                                          ctx.get_opt_ctx().get_exec_ctx(),
                                                          ctx.get_allocator(),
                                                          null_const))) {
        LOG_WARN("Failed to check whether expr null value", K(ret));
      } else if (null_const) {
        selectivity = 0.0;
      } else if (OB_FAIL(ObOptSelectivity::get_column_ndv_and_nns(table_metas, ctx, *column_exprs.at(0), &ndv, &nns))) {
        LOG_WARN("failed to get column ndv and nns", K(ret));
      } else if (ndv < 2.0) {
        //The reason doing this is similar as get_is_sel function.
        //If distinct_num is 1, As formula, selectivity of 'c1 != 1' would be 0.0.
        //But we don't know the distinct value, so just get the half selectivity.
        selectivity = nns / ndv / 2.0;
      } else {
        selectivity = nns * (1.0 - 1 / ndv);
      }
    }
  } else { }//do nothing
  return ret;
}

int ObEqualSelEstimator::get_equal_sel(const OptTableMetas &table_metas,
                                       const OptSelectivityCtx &ctx,
                                       const ObRawExpr &qual,
                                       double &selectivity)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *left_expr = qual.get_param_expr(0);
  const ObRawExpr *right_expr = qual.get_param_expr(1);
  if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(qual), K(left_expr), K(right_expr));
  } else if (OB_FAIL(get_equal_sel(table_metas, ctx, *left_expr, *right_expr,
                                   T_OP_NSEQ == qual.get_expr_type(), selectivity))) {
    LOG_WARN("failed to get equal sel", K(ret));
  }
  return ret;
}

int ObEqualSelEstimator::get_equal_sel(const OptTableMetas &table_metas,
                                       const OptSelectivityCtx &ctx,
                                       const ObRawExpr &left_expr,
                                       const ObRawExpr &right_expr,
                                       const bool null_safe,
                                       double &selectivity)
{
  int ret = OB_SUCCESS;
  if (T_OP_ROW == left_expr.get_expr_type() && T_OP_ROW == right_expr.get_expr_type()) {
    // normally row equal row will unnest as `var = var and var = var ...`
    selectivity = 1.0;
    double tmp_selectivity = 1.0;
    const ObRawExpr *l_expr = NULL;
    const ObRawExpr *r_expr = NULL;
    const ObRawExpr *l_row = &left_expr;
    const ObRawExpr *r_row = &right_expr;
    // (c1, c2) in ((const1, const2)) may transform to (c1, c2) = ((const1, const2))
    if (left_expr.get_param_count() == 1 && OB_NOT_NULL(left_expr.get_param_expr(0)) &&
        T_OP_ROW == left_expr.get_param_expr(0)->get_expr_type()) {
      l_row = left_expr.get_param_expr(0);
    }
    if (right_expr.get_param_count() == 1 && OB_NOT_NULL(right_expr.get_param_expr(0)) &&
        T_OP_ROW == right_expr.get_param_expr(0)->get_expr_type()) {
      r_row = right_expr.get_param_expr(0);
    }
    if (OB_UNLIKELY(l_row->get_param_count() != r_row->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", KPC(l_row), KPC(l_row), K(ret));
    } else {
      int64_t num = l_row->get_param_count();
      ObSEArray<double, 4> selectivities;
      for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
        if (OB_ISNULL(l_expr = l_row->get_param_expr(i)) ||
            OB_ISNULL(r_expr = r_row->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null expr", K(ret), K(l_expr), K(r_expr), K(i));
        } else if (OB_FAIL(SMART_CALL(get_equal_sel(table_metas, ctx, *l_expr,
                                                    *r_expr, null_safe, tmp_selectivity)))) {
          LOG_WARN("failed to get equal selectivity", K(ret));
        } else if (OB_FAIL(selectivities.push_back(tmp_selectivity))) {
          LOG_WARN("failed to push back");
        }
      }
      selectivity = ctx.get_correlation_model().combine_filters_selectivity(selectivities);
    }
  } else if ((left_expr.has_flag(CNT_COLUMN) && !right_expr.has_flag(CNT_COLUMN)) ||
             (!left_expr.has_flag(CNT_COLUMN) && right_expr.has_flag(CNT_COLUMN))) {
    // column = const
    const ObRawExpr *cnt_col_expr = left_expr.has_flag(CNT_COLUMN) ? &left_expr : &right_expr;
    const ObRawExpr &calc_expr = left_expr.has_flag(CNT_COLUMN) ? right_expr : left_expr;
    ObOptColumnStatHandle handler;
    ObObj expr_value;
    bool can_use_hist = false;
    ObHistEqualSelHelper helper;
    if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(cnt_col_expr))) {
      LOG_WARN("failed to remove ignorable function", K(ret));
    } else if (cnt_col_expr->is_column_ref_expr()) {
      const ObColumnRefRawExpr* col = static_cast<const ObColumnRefRawExpr*>(cnt_col_expr);
      if (OB_FAIL(helper.init(table_metas, ctx, *col))) {
        LOG_WARN("failed to get histogram by column", K(ret));
      } else if (!helper.is_valid()) {
        // do nothing
      } else if (OB_FAIL(helper.set_compare_value(ctx, &calc_expr, can_use_hist))) {
        // cast may failed due to invalid type or value out of range.
        // Then use ndv instead of histogram
        can_use_hist = false;
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      if (can_use_hist) {
        double nns = 0;
        double hist_scale = 0;
        if (OB_FAIL(helper.get_sel(ctx, selectivity))) {
          LOG_WARN("Failed to get equal density", K(ret));
        } else if (OB_FAIL(ObOptSelectivity::get_column_ndv_and_nns(table_metas, ctx, *cnt_col_expr, NULL, &nns))) {
          LOG_WARN("failed to get column ndv and nns", K(ret));
        } else {
          selectivity *= nns;
        }
      } else if (OB_FAIL(get_simple_equal_sel(table_metas, ctx, *cnt_col_expr,
                                              &calc_expr, null_safe, selectivity))) {
        LOG_WARN("failed to get simple equal selectivity", K(ret));
      }
      LOG_TRACE("succeed to get equal predicate sel", K(can_use_hist), K(selectivity));
    }
  } else if (left_expr.has_flag(CNT_COLUMN) && right_expr.has_flag(CNT_COLUMN)) {
    if (OB_FAIL(get_cntcol_op_cntcol_sel(table_metas, ctx, left_expr, right_expr,
                                         null_safe ? T_OP_NSEQ : T_OP_EQ, selectivity))) {
      LOG_WARN("failed to get contain column equal contain column selectivity", K(ret));
    } else {
      LOG_TRACE("succeed to get contain column equal contain column sel", K(selectivity), K(ret));
    }
  } else {
    // CONST_PARAM = CONST_PARAM
    const ParamStore *params = ctx.get_params();
    if (OB_ISNULL(params)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Params is NULL", K(ret));
    } else if (ObOptEstUtils::is_calculable_expr(left_expr, params->count()) &&
               ObOptEstUtils::is_calculable_expr(right_expr, params->count())) {
      // 1 in (c1, 2, 3) will reach this branch
      bool equal = false;
      if (OB_FAIL(ObOptEstUtils::if_expr_value_equal(const_cast<ObOptimizerContext &>(ctx.get_opt_ctx()),
                                                     ctx.get_stmt(),
                                                     left_expr, right_expr, null_safe, equal))) {
        LOG_WARN("Failed to check hvae equal expr", K(ret));
      } else {
        selectivity = equal ? 1.0 : 0.0;
      }
    } else {
      selectivity = DEFAULT_EQ_SEL;
    }
  }
  return ret;
}

int ObEqualSelEstimator::get_simple_equal_sel(const OptTableMetas &table_metas,
                                              const OptSelectivityCtx &ctx,
                                              const ObRawExpr &cnt_col_expr,
                                              const ObRawExpr *calculable_expr,
                                              const bool null_safe,
                                              double &selectivity)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObColumnRefRawExpr*, 2> column_exprs;
  bool only_monotonic_op = true;
  const ObColumnRefRawExpr *column_expr = NULL;
  double distinct_sel = 1.0;
  double null_sel = 1.0;
  bool is_null_value = false;
  if (OB_FAIL(ObOptEstUtils::extract_column_exprs_with_op_check(&cnt_col_expr,
                                                                column_exprs,
                                                                only_monotonic_op))) {
    LOG_WARN("failed to extract column exprs with op check", K(ret));
  } else if (!only_monotonic_op || column_exprs.count() > 1) {
    // cnt_col_expr contain not monotonic op OR has more than 1 column
    ObSEArray<ObRawExpr *, 1> exprs;
    ObRawExpr *expr = const_cast<ObRawExpr*>(&cnt_col_expr);
    double ndv = 1.0;
    bool refine_ndv_by_current_rows = (ctx.get_current_rows() >= 0);
    if (OB_FAIL(exprs.push_back(expr))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(table_metas, ctx, exprs, ctx.get_current_rows(),
                                                            ndv, refine_ndv_by_current_rows))) {
      LOG_WARN("Failed to calculate distinct", K(ret));
    } else {
      selectivity = (ndv > 1.0) ? 1 / ndv : DEFAULT_EQ_SEL;
    }
  } else if (OB_UNLIKELY(1 != column_exprs.count()) ||
             OB_ISNULL(column_expr = column_exprs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect column expr", K(column_exprs), K(cnt_col_expr), K(column_expr));
  } else if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, *column_expr,
                                                            &distinct_sel, &null_sel))) {
    LOG_WARN("failed to get column basic selelectivity", K(ret));
  } else if (NULL == calculable_expr) {
    selectivity = distinct_sel;
  } else if (OB_FAIL(ObOptEstUtils::if_expr_value_null(ctx.get_params(),
                                                       *calculable_expr,
                                                       ctx.get_opt_ctx().get_exec_ctx(),
                                                       ctx.get_allocator(),
                                                       is_null_value))) {
    LOG_WARN("failed to check if expr value null", K(ret));
  } else if (!is_null_value) {
    selectivity = distinct_sel;
  } else if (null_safe) {
    selectivity = null_sel;
  } else {
    selectivity = 0.0;
  }
  return ret;
}

/**
 * For the equal predicate 'a = b', we calculate the NDV of (a, b),
 * and use the maximum number of tuples that might satisfy the equality as the result of predicate filtering.
 * Therefore, the selectivity should be 'min(ndv(a), ndv(b)) / ndv(a, b)'.
 * In the case of a join, the left and right sides of the equality are always independent,
 * so the selectivity can be simplified as '1 / max(ndv(a), ndv(b))'.
*/
int ObEqualSelEstimator::get_cntcol_op_cntcol_sel(const OptTableMetas &table_metas,
                                                  const OptSelectivityCtx &ctx,
                                                  const ObRawExpr &input_left_expr,
                                                  const ObRawExpr &input_right_expr,
                                                  ObItemType op_type,
                                                  double &selectivity)
{
  int ret = OB_SUCCESS;
  double left_ndv = 1.0;
  double right_ndv = 1.0;
  double left_nns = 0.0;
  double right_nns = 0.0;
  selectivity = DEFAULT_EQ_SEL;
  const ObRawExpr* left_expr = &input_left_expr;
  const ObRawExpr* right_expr = &input_right_expr;
  double left_base_ndv = -1.0;
  double right_base_ndv = -1.0;
  if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(left_expr)) ||
      OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(right_expr))) {
    LOG_WARN("failed to remove ignorable function", K(ret));
  } else {
    bool calc_with_hist = false;
    if (OB_FAIL(try_calc_equal_sel_with_hist(table_metas,
                                             ctx,
                                             left_expr,
                                             right_expr,
                                             op_type,
                                             selectivity,
                                             calc_with_hist))) {
      LOG_WARN("failed to calc equal sel with hist", K(ret));
    } else if (calc_with_hist) {
      // do nothing
    } else if (OB_FAIL(ObOptSelectivity::calc_expr_basic_info(table_metas,
                                                              ctx,
                                                              left_expr,
                                                              &left_ndv,
                                                              &left_nns,
                                                              IS_ANTI_JOIN(ctx.get_join_type()) ?
                                                              &left_base_ndv :
                                                              NULL))) {
      LOG_WARN("Failed to calculate distinct", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::calc_expr_basic_info(table_metas,
                                                              ctx,
                                                              right_expr,
                                                              &right_ndv,
                                                              &right_nns,
                                                              IS_ANTI_JOIN(ctx.get_join_type()) ?
                                                              &right_base_ndv :
                                                              NULL))) {
      LOG_WARN("Failed to calculate distinct", K(ret));
    } else if (left_expr->get_relation_ids() == right_expr->get_relation_ids()) {
      selectivity = ObOptSelectivity::calc_equal_filter_sel(ctx, left_expr->same_as(*right_expr), op_type,
                                                            left_ndv, right_ndv,
                                                            left_nns, right_nns);
    } else {
      if (OB_NOT_NULL(ctx.get_left_rel_ids()) && OB_NOT_NULL(ctx.get_right_rel_ids()) &&
          (left_expr->get_relation_ids().overlap(*ctx.get_right_rel_ids()) ||
           right_expr->get_relation_ids().overlap(*ctx.get_left_rel_ids()))) {
        std::swap(left_ndv, right_ndv);
        std::swap(left_base_ndv, right_base_ndv);
        std::swap(left_nns, right_nns);
      }
      selectivity = ObOptSelectivity::calc_equal_join_sel(ctx, op_type,
                                                          left_ndv, right_ndv,
                                                          left_nns, right_nns,
                                                          left_base_ndv, right_base_ndv);
    }
  }
  return ret;
}

int ObEqualSelEstimator::try_calc_equal_sel_with_hist(const OptTableMetas &table_metas,
                                                      const OptSelectivityCtx &ctx,
                                                      const ObRawExpr *left_expr,
                                                      const ObRawExpr *right_expr,
                                                      ObItemType op_type,
                                                      double &selectivity,
                                                      bool &calc_with_hist)
{
  int ret = OB_SUCCESS;
  ObOptColumnStatHandle left_handler;
  ObOptColumnStatHandle right_handler;
  const ObOptColumnStat *left_col_stat = NULL;
  const ObOptColumnStat *right_col_stat = NULL;
  obj_cmp_func cmp_func = NULL;
  calc_with_hist = false;
  const ObColumnRefRawExpr *left_col = static_cast<const ObColumnRefRawExpr *>(left_expr);
  const ObColumnRefRawExpr *right_col = static_cast<const ObColumnRefRawExpr *>(right_expr);
  if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", KPC(left_expr), KPC(right_expr));
  } else if (!left_expr->is_column_ref_expr() || !right_expr->is_column_ref_expr()) {
    // do nothing
  } else if (left_expr->get_relation_ids() == right_expr->get_relation_ids()) {
    // do nothing
  } else if (!ObObjCmpFuncs::can_cmp_without_cast(left_col->get_result_type(),
                                                  right_col->get_result_type(),
                                                  CO_EQ, cmp_func))  {
    // do nothing
  } else if (OB_FAIL(ObOptSelectivity::get_histogram_by_column(table_metas, ctx, left_col->get_table_id(),
                                                               left_col->get_column_id(), left_handler))) {
    LOG_WARN("failed to get histogram by column", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::get_histogram_by_column(table_metas, ctx, right_col->get_table_id(),
                                                               right_col->get_column_id(), right_handler))) {
    LOG_WARN("failed to get histogram by column", K(ret));
  } else if (left_handler.stat_ != NULL && right_handler.stat_ != NULL &&
             left_handler.stat_->get_histogram().is_frequency() &&
             right_handler.stat_->get_histogram().is_frequency()) {
    calc_with_hist = true;
    left_col_stat = left_handler.stat_;
    right_col_stat = right_handler.stat_;
    if (OB_NOT_NULL(ctx.get_left_rel_ids()) && OB_NOT_NULL(ctx.get_right_rel_ids()) &&
        (left_expr->get_relation_ids().overlap(*ctx.get_right_rel_ids()) ||
         right_expr->get_relation_ids().overlap(*ctx.get_left_rel_ids()))) {
      std::swap(left_col_stat, right_col_stat);
      std::swap(left_expr, right_expr);
    }
  }
  if (OB_SUCC(ret) && calc_with_hist) {
    double total_rows = 0;
    double left_rows = left_col_stat->get_histogram().get_sample_size();
    double right_rows = right_col_stat->get_histogram().get_sample_size();
    double left_null_sel = 0;
    double right_null_sel = 0;
    if (OB_FAIL(ObOptSelectivity::get_join_pred_rows(left_col_stat->get_histogram(),
                                                     right_col_stat->get_histogram(),
                                                     ctx.get_join_type(), total_rows))) {
      LOG_WARN("failed to get join pred rows", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, *left_expr, NULL, &left_null_sel))) {
      LOG_WARN("Failed to calc basic equal sel", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, *right_expr, NULL, &right_null_sel))) {
      LOG_WARN("Failed to calc basic equal sel", K(ret));
    } else if (IS_SEMI_ANTI_JOIN(ctx.get_join_type())) {
      if (IS_RIGHT_SEMI_ANTI_JOIN(ctx.get_join_type())) {
        std::swap(left_rows, right_rows);
        std::swap(left_null_sel, right_null_sel);
        std::swap(left_col_stat, right_col_stat);
      }
      double left_nns = 1 - left_null_sel;
      double right_nns = 1 - right_null_sel;
      if (T_OP_NSEQ == op_type) {
        selectivity = left_nns * total_rows / left_rows +
                     (right_null_sel > OB_DOUBLE_EPSINON ? left_null_sel : 0);
      } else if (T_OP_EQ == op_type) {
        selectivity = left_nns * total_rows / left_rows;
      } else if (T_OP_NE == op_type) {
        if (right_col_stat->get_histogram().get_bucket_size() > 1) {
          selectivity = left_nns;
        } else {
          selectivity = left_nns * (1 - total_rows / left_rows);
        }
      }
      if (IS_ANTI_JOIN(ctx.get_join_type())) {
        selectivity = std::min(selectivity, 1 - DEFAULT_ANTI_JOIN_SEL);
      }
    } else {
      double left_nns = 1 - left_null_sel;
      double right_nns = 1 - right_null_sel;
      if (T_OP_NSEQ == op_type) {
        selectivity = left_nns * right_nns * total_rows / left_rows / right_rows + left_null_sel * right_null_sel;
      } else if (T_OP_EQ == op_type) {
        selectivity = left_nns * right_nns * total_rows / left_rows / right_rows;
      } else if (T_OP_NE == op_type) {
        selectivity = left_nns * right_nns * (1 - total_rows / left_rows / right_rows);
      }
    }
    selectivity = ObOptSelectivity::revise_between_0_1(selectivity);
  }
  return ret;
}

int ObAggSelEstimator::get_sel(const OptTableMetas &table_metas,
                               const OptSelectivityCtx &ctx,
                               double &selectivity,
                               ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  const ObRawExpr &qual = *expr_;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", KPC(this));
  } else if (OB_FAIL(get_agg_sel(table_metas, ctx, qual, selectivity))) {
    LOG_WARN("failed to get agg expr selectivity", K(ret), K(qual));
  }
  return ret;
}

int ObAggSelEstimator::get_agg_sel(const OptTableMetas &table_metas,
                                   const OptSelectivityCtx &ctx,
                                   const ObRawExpr &qual,
                                   double &selectivity)
{
  int ret = OB_SUCCESS;
  const double origin_rows = ctx.get_row_count_1(); // rows before group by
  const double grouped_rows = ctx.get_row_count_2();// rows after group by
  bool is_valid = false;
  const ObRawExpr *aggr_expr = NULL;
  const ObRawExpr *const_expr1 = NULL;
  const ObRawExpr *const_expr2 = NULL;
  selectivity = DEFAULT_AGG_RANGE;
  ObItemType type = qual.get_expr_type();
  // for aggregate function in having clause, only support
  // =  <=>  !=  >  >=  <  <=  [not] btw  [not] in
  if (-1.0 == origin_rows || -1.0 == grouped_rows) {
    // 不是在group by层计算的having filter，使用默认选择率
    // e.g. select * from t7 group by c1 having count(*) > (select c1 from t8 limit 1);
    //      该sql中having filter需要在subplan filter中计算
  } else if ((type >= T_OP_EQ && type <= T_OP_NE) ||
             T_OP_IN == type || T_OP_NOT_IN == type ||
             T_OP_BTW == type || T_OP_NOT_BTW == type) {
    if (OB_FAIL(is_valid_agg_qual(qual, is_valid, aggr_expr, const_expr1, const_expr2))) {
      LOG_WARN("failed to check is valid agg qual", K(ret));
    } else if (!is_valid) {
      /* use default selectivity */
    } else if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_FUN_MAX == aggr_expr->get_expr_type() ||
               T_FUN_MIN == aggr_expr->get_expr_type() ||
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
        // 计算方式参考OR
        selectivity = DEFAULT_AGG_RANGE + DEFAULT_AGG_RANGE;
      } else if (T_OP_IN == type) {
        /**
         *  oracle 对 max/min/count(col) in (const1, const2, const3, ...)的选择率估计
         *  当const的数量小于等于5时，每增加一个const值，选择率增加 DEFAULT_AGG_EQ(0.01)
         *  当const的数量大于5时，每增加一个const值，选择率增加
         *      DEFAULT_AGG_EQ - 0.001 * (const_num - 5)
         *  # 这里的选择率增加量采用线性下降其实并不是很精确，oracle的选择率增加量可能采用了了指数下降，
         *    在测试过程中测试了1-30列递增的情况，线性下降和指数下降区别不大。
         */
        int64_t N;
        if(OB_ISNULL(const_expr1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null");
        } else if (FALSE_IT(N = const_expr1->get_param_count())) {
        } else if (N < 6) {
          selectivity = DEFAULT_AGG_EQ * N;
        } else {
          N = std::min(N, 15L);
          selectivity = DEFAULT_AGG_EQ * 5 + (DEFAULT_AGG_EQ - 0.0005 * (N - 4)) * (N - 5);
        }
      } else if (T_OP_NOT_IN == type) {
        // agg(col) not in (const1, const2, ...) <=> agg(col) != const1 and agg(col) != const2 and ...
        if(OB_ISNULL(const_expr1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null");
        } else {
          selectivity = std::pow(DEFAULT_AGG_RANGE, const_expr1->get_param_count());
        }
      } else { /* use default selectivity */ }
    } else if (T_FUN_SUM == aggr_expr->get_expr_type() || T_FUN_AVG == aggr_expr->get_expr_type()) {
      LOG_TRACE("show group by origen rows and grouped rows", K(origin_rows), K(grouped_rows));
      double rows_per_group = grouped_rows == 0.0 ? origin_rows : origin_rows / grouped_rows;
      if (OB_FAIL(get_agg_sel_with_minmax(table_metas, ctx, *aggr_expr, const_expr1,
                                          const_expr2, type, selectivity, rows_per_group))) {
        LOG_WARN("failed to get agg sel with minmax", K(ret));
      }
    } else { /* not max/min/count/sum/avg, use default selectivity */ }
  } else { /* use default selectivity */ }
  return ret;
}

int ObAggSelEstimator::get_agg_sel_with_minmax(const OptTableMetas &table_metas,
                                               const OptSelectivityCtx &ctx,
                                               const ObRawExpr &aggr_expr,
                                               const ObRawExpr *const_expr1,
                                               const ObRawExpr *const_expr2,
                                               const ObItemType type,
                                               double &selectivity,
                                               const double rows_per_group)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_AGG_RANGE;
  const ParamStore *params = ctx.get_params();
  const ObDMLStmt *stmt = ctx.get_stmt();
  ObExecContext *exec_ctx = ctx.get_opt_ctx().get_exec_ctx();
  ObIAllocator &alloc = ctx.get_allocator();
  ObObj result1;
  ObObj result2;
  bool got_result;
  double distinct_sel = 1.0;
  ObObj maxobj;
  ObObj minobj;
  maxobj.set_max_value();
  minobj.set_min_value();
  if (OB_ISNULL(aggr_expr.get_param_expr(0)) || OB_ISNULL(params) ||
      OB_ISNULL(stmt) || OB_ISNULL(const_expr1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr.get_param_expr(0)),
                                    K(params), K(stmt), K(const_expr1));
  } else if (!aggr_expr.get_param_expr(0)->is_column_ref_expr()) {
    // 只处理sum(column)的形式，sum(column + 1)/sum(column1 + column2)都是用默认选择率
  } else if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, *aggr_expr.get_param_expr(0),
                                                            &distinct_sel, NULL))) {
    LOG_WARN("failed to get column basic sel", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::get_column_min_max(table_metas, ctx, *aggr_expr.get_param_expr(0),
                                                          minobj, maxobj))) {
    LOG_WARN("failed to get column min max", K(ret));
  } else if (minobj.is_min_value() || maxobj.is_max_value()) {
    // do nothing
  } else if (T_OP_IN == type || T_OP_NOT_IN == type) {
    if (OB_UNLIKELY(T_OP_ROW != const_expr1->get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should be row", K(ret), K(*const_expr1));
    } else {
      // 如果row超过5列，则计算5列上的选择率，再按比例放大
      int64_t N = const_expr1->get_param_count() > 5 ? 5 :const_expr1->get_param_count();
      selectivity = T_OP_IN == type ? 0.0 : 1.0;
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        double tmp_sel = T_OP_IN == type ? DEFAULT_AGG_EQ : DEFAULT_AGG_RANGE;
        const ObRawExpr *sub_expr = NULL;
        ObObj tmp_result;
        if (OB_ISNULL(sub_expr = const_expr1->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!ObOptEstUtils::is_calculable_expr(*sub_expr, params->count())) {
        } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                                     sub_expr,
                                                                     tmp_result,
                                                                     got_result,
                                                                     alloc))) {
          LOG_WARN("failed to calc const or calculable expr", K(ret));
        } else if (!got_result) {
          // do nothing
        } else {
          tmp_sel = get_agg_eq_sel(maxobj, minobj, tmp_result, distinct_sel, rows_per_group,
                                   T_OP_IN == type, T_FUN_SUM == aggr_expr.get_expr_type());
        }
        if (T_OP_IN == type) {
          selectivity += tmp_sel;
        } else {
          selectivity *= tmp_sel;
        }
      }
      if (OB_SUCC(ret)) {
        selectivity *= static_cast<double>(const_expr1->get_param_count())
              / static_cast<double>(N);
      }
    }
  } else if (!ObOptEstUtils::is_calculable_expr(*const_expr1, params->count())) {
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                               const_expr1,
                                                               result1,
                                                               got_result,
                                                               alloc))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  } else if (!got_result) {
    // do nothing
  } else if (T_OP_EQ == type || T_OP_NSEQ == type) {
    selectivity = get_agg_eq_sel(maxobj, minobj, result1, distinct_sel, rows_per_group,
                                 true, T_FUN_SUM == aggr_expr.get_expr_type());
  } else if (T_OP_NE == type) {
    selectivity = get_agg_eq_sel(maxobj, minobj, result1, distinct_sel, rows_per_group,
                                 false, T_FUN_SUM == aggr_expr.get_expr_type());
  } else if (IS_RANGE_CMP_OP(type)) {
    selectivity = get_agg_range_sel(maxobj, minobj, result1, rows_per_group,
                                    type, T_FUN_SUM == aggr_expr.get_expr_type());
  } else if (T_OP_BTW == type || T_OP_NOT_BTW == type) {
    if (OB_ISNULL(const_expr2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!ObOptEstUtils::is_calculable_expr(*const_expr2, params->count())) {
    } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                                 const_expr2,
                                                                 result2,
                                                                 got_result,
                                                                 alloc))) {
      LOG_WARN("Failed to calc const or calculable expr", K(ret));
    } else if (!got_result) {
      // do nothing
    } else {
      selectivity = get_agg_btw_sel(maxobj, minobj, result1, result2, rows_per_group,
                                    type, T_FUN_SUM == aggr_expr.get_expr_type());
    }
  } else { /* do nothing */ }
  return ret;
}

// 计算sum/avg(col) =/<=>/!= const的选择率
double ObAggSelEstimator::get_agg_eq_sel(const ObObj &maxobj,
                                         const ObObj &minobj,
                                         const ObObj &constobj,
                                         const double distinct_sel,
                                         const double rows_per_group,
                                         const bool is_eq,
                                         const bool is_sum)
{
  int ret = OB_SUCCESS;
  double sel_ret = DEFAULT_AGG_EQ;
  if (constobj.is_null()) {
    // sum/avg(col)的结果中不会存在null，即使是null safe equal选择率依然为0
    sel_ret = 0.0;
  } else if (minobj.is_integer_type() ||
             (minobj.is_number() && minobj.get_meta().get_obj_meta().get_scale() == 0) ||
             (minobj.is_unumber() && minobj.get_meta().get_obj_meta().get_scale() == 0)) {
    double const_val;
    double min_val;
    double max_val;
    // 如果转化的时候出错，就使用默认的选择率
    if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj, const_val)) ||
        OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&minobj, min_val)) ||
        OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&maxobj, max_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else {
      LOG_TRACE("get values for agg eq sel", K(max_val), K(min_val), K(const_val));
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
        } else {}
      } else {
        sel_ret = 1.0 - 1.0 / length;
      }
    }
  } else {
    // 对于非整数的类型，认为sum/avg(col)后 ndv 不会发生显著变化，直接使用该列原有的ndv计算
    sel_ret = is_eq ? distinct_sel : 1.0 - distinct_sel;
  }
  sel_ret = ObOptSelectivity::revise_between_0_1(sel_ret);
  return sel_ret;
}

// 计算sum/avg(col) >/>=/</<= const的选择率
double ObAggSelEstimator::get_agg_range_sel(const ObObj &maxobj,
                                            const ObObj &minobj,
                                            const ObObj &constobj,
                                            const double rows_per_group,
                                            const ObItemType type,
                                            const bool is_sum)
{
  int ret = OB_SUCCESS;
  double sel_ret = DEFAULT_AGG_RANGE;
  if (constobj.is_null()) {
    // sum/avg(col)的结果中不会存在null，因此选择率为0
    sel_ret = 0.0;
  } else {
    double min_val;
    double max_val;
    double const_val;
    // 如果转化的时候出错，就使用默认的选择率
    if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&minobj, min_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&maxobj, max_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj, const_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else {
      LOG_TRACE("get values for agg range sel", K(max_val), K(min_val), K(const_val));
      if (is_sum) {
        min_val *= rows_per_group;
        max_val *= rows_per_group;
      }
      double length = max_val - min_val + 1.0;
      if (T_OP_GE == type || T_OP_GT == type) {
        if (T_OP_GT == type) {
          // c1 > 1 <=> c1 >= 2, 对非int类型的列并不精确
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
          // c1 < 1 <=> c1 <= 0, 对非int类型的列并不精确
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
      } else { /* do nothing */ }
    }
  }
  sel_ret = ObOptSelectivity::revise_between_0_1(sel_ret);
  return sel_ret;
}

// 计算sum/avg(col) [not] between const1 and const2的选择率
double ObAggSelEstimator::get_agg_btw_sel(const ObObj &maxobj,
                                          const ObObj &minobj,
                                          const ObObj &constobj1,
                                          const ObObj &constobj2,
                                          const double rows_per_group,
                                          const ObItemType type,
                                          const bool is_sum)
{
  int ret = OB_SUCCESS;
  double sel_ret = DEFAULT_AGG_RANGE;
  if (constobj1.is_null() || constobj2.is_null()) {
    sel_ret= 0.0;
  } else {
    double min_val;
    double max_val;
    double const_val1;
    double const_val2;
    // 如果转化的时候出错，就使用默认的选择率
    if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&minobj, min_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&maxobj, max_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj1, const_val1))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj2, const_val2))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else {
      LOG_TRACE("get values for agg between sel", K(max_val), K(min_val), K(const_val1), K(const_val2));
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
      } else if (T_OP_NOT_BTW == type){
        if (const_val1 > const_val2) {
          sel_ret = 1.0;
        } else {
          double tmp_min = std::max(const_val1, min_val);
          double tmp_max = std::min(const_val2, max_val);
          sel_ret = 1 - (tmp_max - tmp_min + 1.0) / length;
        }
      } else { /* do nothing */ }
    }
  }
  sel_ret = ObOptSelectivity::revise_between_0_1(sel_ret);
  return sel_ret;
}

int ObAggSelEstimator::is_valid_agg_qual(const ObRawExpr &qual,
                                         bool &is_valid,
                                         const ObRawExpr *&aggr_expr,
                                         const ObRawExpr *&const_expr1,
                                         const ObRawExpr *&const_expr2)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObRawExpr *expr0 = NULL;
  const ObRawExpr *expr1 = NULL;
  const ObRawExpr *expr2 = NULL;
  if (T_OP_BTW == qual.get_expr_type() || T_OP_NOT_BTW == qual.get_expr_type()) {
    if (OB_ISNULL(expr0 = qual.get_param_expr(0)) ||
        OB_ISNULL(expr1 = qual.get_param_expr(1)) ||
        OB_ISNULL(expr2 = qual.get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (expr0->has_flag(IS_AGG) &&
               expr1->is_const_expr() &&
               expr2->is_const_expr()) {
      is_valid = true;
      aggr_expr = expr0;
      const_expr1 = expr1;
      const_expr2 = expr2;
    } else { /* do nothing */ }
  } else {
    if (OB_ISNULL(expr0 = qual.get_param_expr(0)) || OB_ISNULL(expr1 = qual.get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_OP_IN == qual.get_expr_type() || T_OP_NOT_IN == qual.get_expr_type()) {
      if (!qual.has_flag(CNT_SUB_QUERY) &&
          expr0->has_flag(IS_AGG) &&
          T_OP_ROW == expr1->get_expr_type()) {
        is_valid = true;
        aggr_expr = expr0;
        const_expr1 = expr1;
      } else { /* do nothing */ }
    } else if (expr0->has_flag(IS_AGG) &&
               expr1->is_const_expr()) {
      is_valid = true;
      aggr_expr = expr0;
      const_expr1 = expr1;
    } else if (expr0->is_const_expr() &&
               expr1->has_flag(IS_AGG)) {
      is_valid = true;
      aggr_expr = expr1;
      const_expr1 = expr0;
    } else { /* do nothing */ }
  }
  return ret;
}

int ObLikeSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                         const OptSelectivityCtx &ctx,
                                         const ObRawExpr &expr,
                                         ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  estimator = NULL;
  ObLikeSelEstimator *like_estimator = NULL;
  if (T_OP_LIKE != expr.get_expr_type()) {
    // do nothing
  } else if (OB_FAIL(factory.create_estimator_inner(like_estimator))) {
    LOG_WARN("failed to create estimator ", K(ret));
  } else  {
    like_estimator->expr_ = &expr;
    estimator = like_estimator;
    const ParamStore *params = ctx.get_params();
    if (3 != expr.get_param_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("like expr should have 3 param", K(ret), K(expr));
    } else if (OB_ISNULL(params) ||
               OB_ISNULL(like_estimator->variable_ = expr.get_param_expr(0)) ||
               OB_ISNULL(like_estimator->pattern_ = expr.get_param_expr(1)) ||
               OB_ISNULL(like_estimator->escape_ = expr.get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null params", K(ret), K(params), K(expr));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(like_estimator->variable_,
                                                                       like_estimator->variable_))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (like_estimator->variable_->is_column_ref_expr() &&
               like_estimator->pattern_->is_static_const_expr() &&
               like_estimator->escape_->is_static_const_expr()) {
      bool is_start_with = false;
      like_estimator->eigen_expr_ = like_estimator->variable_;
      if (OB_FAIL(ObOptEstUtils::if_expr_start_with_patten_sign(params, like_estimator->pattern_,
                                                                like_estimator->escape_,
                                                                ctx.get_opt_ctx().get_exec_ctx(),
                                                                ctx.get_allocator(),
                                                                is_start_with,
                                                                like_estimator->match_all_str_))) {
        LOG_WARN("failed to check if expr start with percent sign", K(ret));
      } else if (like_estimator->match_all_str_) {
        like_estimator->can_calc_sel_by_prefix_ = true;
      } else if (is_lob_storage(like_estimator->variable_->get_data_type())) {
        // do nothing
      } else if (!is_start_with) {
        like_estimator->can_calc_sel_by_prefix_ = true;
      }
    }
  }
  return ret;
}

int ObLikeSelEstimator::can_calc_like_sel(const OptSelectivityCtx &ctx, const ObRawExpr &expr, bool &can_calc_sel)
{
  int ret = OB_SUCCESS;
  can_calc_sel = false;
  if (T_OP_LIKE == expr.get_expr_type()) {
    const ParamStore *params = ctx.get_params();
    const ObRawExpr *variable = NULL;
    const ObRawExpr *pattern = NULL;
    const ObRawExpr *escape = NULL;
    if (3 != expr.get_param_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("like expr should have 3 param", K(ret), K(expr));
    } else if (OB_ISNULL(params) ||
               OB_ISNULL(variable = expr.get_param_expr(0)) ||
               OB_ISNULL(pattern = expr.get_param_expr(1)) ||
               OB_ISNULL(escape = expr.get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null params", K(ret), K(params), K(expr));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(variable,
                                                                       variable))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (variable->is_column_ref_expr() &&
               pattern->is_static_const_expr() &&
               escape->is_static_const_expr()) {
      bool is_start_with = false;
      bool match_all_str = false;
      if (OB_FAIL(ObOptEstUtils::if_expr_start_with_patten_sign(params, pattern, escape,
                                                                ctx.get_opt_ctx().get_exec_ctx(),
                                                                ctx.get_allocator(),
                                                                is_start_with,
                                                                match_all_str))) {
        LOG_WARN("failed to check if expr start with percent sign", K(ret));
      } else if (match_all_str) {
        can_calc_sel = true;
      } else if (is_lob_storage(variable->get_data_type())) {
        // do nothing
      } else if (!is_start_with) {
        can_calc_sel = true;
      }
    }
  }
  return ret;
}

int ObLikeSelEstimator::get_sel(const OptTableMetas &table_metas,
                                const OptSelectivityCtx &ctx,
                                double &selectivity,
                                ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  const ObRawExpr &qual = *expr_;
  bool can_calc_sel = false;
  if (OB_ISNULL(expr_) || OB_ISNULL(variable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", KPC(this));
  } else if (match_all_str_ && can_calc_sel_by_prefix_) {
    double nns = 0.0;
    if (OB_FAIL(ObOptSelectivity::get_column_ndv_and_nns(table_metas, ctx, *variable_, NULL, &nns))) {
      LOG_WARN("failed to get nns");
    } else {
      selectivity = nns;
    }
  } else if (can_calc_sel_by_prefix_) {
    if (OB_UNLIKELY(!variable_->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr", KPC(variable_));
    } else if (OB_FAIL(ObOptSelectivity::get_column_range_sel(table_metas, ctx,
                                                              static_cast<const ObColumnRefRawExpr&>(*variable_),
                                                              qual, false, selectivity))) {
      LOG_WARN("Failed to get column range selectivity", K(ret));
    }
  } else if (is_lob_storage(variable_->get_data_type())) {
    // no statistics for lob type, use default selectivity
    selectivity = DEFAULT_LIKE_SEL;
  } else if (!ctx.check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                           COMPAT_VERSION_4_3_3)) {
    selectivity = DEFAULT_INEQ_SEL;
  } else if (OB_FAIL(calculate_like_sel_by_substr(table_metas,
                                                  ctx,
                                                  selectivity))) {
    LOG_WARN("failed to calculate like sel", K(ret));
  }
  return ret;
}

int ObLikeSelEstimator::get_wildcard_length(const OptSelectivityCtx &ctx, double &wildcard_length)
{
  int ret = OB_SUCCESS;
  ObObj pattern_value;
  bool got_result = false;
  wildcard_length = 1.0; // default guess value
  if (OB_ISNULL(pattern_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!pattern_->is_static_const_expr()) {
    ObString percent_str = ObCharsetUtils::get_const_str(pattern_->get_collation_type(), '%');
    wildcard_length = percent_str.length();
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                               pattern_,
                                                               pattern_value,
                                                               got_result,
                                                               ctx.get_allocator()))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  } else if (!got_result || !pattern_value.is_string_type() || pattern_value.is_null()) {
    // do nothing
  } else {
    const ObString &expr_str = pattern_value.get_string();
    ObStringScanner scanner(expr_str, pattern_->get_collation_type());
    ObString percent_str = ObCharsetUtils::get_const_str(pattern_->get_collation_type(), '%');
    ObString underline_str = ObCharsetUtils::get_const_str(pattern_->get_collation_type(), '_');
    ObString encoding;
    int32_t wc = 0;
    wildcard_length = 0.0;
    while (OB_SUCC(ret)
           && scanner.next_character(encoding, wc, ret)) {
      if (0 == percent_str.compare(encoding)) {
        wildcard_length += percent_str.length();
      }
      if (0 == underline_str.compare(encoding)) {
        wildcard_length += underline_str.length();
      }
    }
    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;
      wildcard_length = percent_str.length();
    }
  }
  return ret;
}

/**
 * try estimate the like selectivity by substr
 * e.g.
 * `c1 like '%abc'` <=> `substr(c1, -3) = 'abc'`
 * Assumption:
 * 1. All strings in the variable and pattern have the same length.
 * 2. The positions of non-wildcard characters are fixed.
 * 3. If the pattern is not a constant, then it contains exactly one wildcard.
 * 4. The pattern will not be null
*/
int ObLikeSelEstimator::calculate_like_sel_by_substr(const OptTableMetas &table_metas,
                                                     const OptSelectivityCtx &ctx,
                                                     double &selectivity)
{
  int ret = OB_SUCCESS;
  // default strategy, not reliable
  double variable_len = 0;
  double pattern_len = 0;
  double substr_ndv = 1.0;
  double pattern_ndv = 1.0;
  double substr_nns = 1.0;
  double pattern_nns = 1.0; // assume that the pattern is not null
  double wildcard_length = 1.0;
  selectivity = DEFAULT_LIKE_SEL;
  if (OB_ISNULL(variable_) || OB_ISNULL(pattern_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!variable_->get_result_type().is_string_type() ||
             !pattern_->get_result_type().is_string_type() ||
             !variable_->is_column_ref_expr()) {
    // The length is not reliable, use default selectivity
  } else if (OB_FAIL(get_wildcard_length(ctx, wildcard_length))) {
    LOG_WARN("failed to get wildcard count", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_expr_avg_len(table_metas, ctx, pattern_, pattern_len))) {
    LOG_WARN("failed to calc expr len", KPC(pattern_), K(ret));
  } else if (pattern_len <= ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH + wildcard_length) {
    // do nothing
  } else if (FALSE_IT(pattern_len -= ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH)) {
  } else if (OB_FAIL(ObOptSelectivity::calculate_substrb_info(
      table_metas, ctx, variable_, pattern_len - wildcard_length, ctx.get_current_rows(), substr_ndv, substr_nns))) {
    LOG_WARN("failed to calculate substrb ndv", KPC_(variable));
  } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(table_metas, ctx, *pattern_, ctx.get_current_rows(), pattern_ndv))) {
    LOG_WARN("failed to calcualte distinct", KPC_(pattern));
  } else {
    if (NULL == ctx.get_left_rel_ids() || NULL == ctx.get_right_rel_ids()) {
      selectivity = ObOptSelectivity::calc_equal_filter_sel(ctx, false, T_OP_EQ,
                                                            substr_ndv, pattern_ndv,
                                                            substr_nns, pattern_nns);
    } else {
      double left_ndv = substr_ndv;
      double right_ndv = pattern_ndv;
      double left_nns = substr_nns;
      double right_nns = pattern_nns;
      if (variable_->get_relation_ids().overlap(*ctx.get_right_rel_ids()) ||
          pattern_->get_relation_ids().overlap(*ctx.get_left_rel_ids())) {
        std::swap(left_ndv, right_ndv);
        std::swap(left_nns, right_nns);
      }
      selectivity = ObOptSelectivity::calc_equal_join_sel(ctx, T_OP_EQ, left_ndv, right_ndv, left_nns, right_nns);
    }
    LOG_TRACE("succeed to calculate like selectivity by substr",
        K(selectivity), K(substr_ndv), K(substr_nns), K(pattern_ndv), K(pattern_nns), K(wildcard_length));
  }
  return ret;
}

int ObBoolOpSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                           const OptSelectivityCtx &ctx,
                                           const ObRawExpr &expr,
                                           ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  estimator = NULL;
  ObBoolOpSelEstimator *bool_estimator = NULL;
  if (T_OP_NOT != expr.get_expr_type() &&
      T_OP_AND != expr.get_expr_type() &&
      T_OP_OR != expr.get_expr_type() &&
      T_FUN_SYS_LNNVL != expr.get_expr_type() &&
      T_OP_BOOL != expr.get_expr_type()) {
    // do nothing
  } else if (OB_FAIL(factory.create_estimator_inner(bool_estimator))) {
    LOG_WARN("failed to create estimator ", K(ret));
  } else {
    bool_estimator->expr_ = &expr;
    estimator = bool_estimator;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
      const ObRawExpr *child_expr = expr.get_param_expr(i);
      ObSelEstimator *child_estimator = NULL;
      if (OB_ISNULL(child_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(factory.create_estimator(ctx, child_expr, child_estimator)))) {
        LOG_WARN("failed to create estimator", KPC(child_expr));
      } else {
        if (T_OP_AND == expr.get_expr_type()) {
          if (OB_FAIL(append_estimators(bool_estimator->child_estimators_, child_estimator))) {
            LOG_WARN("failed to append estimators", K(ret));
          }
        } else {
          if (OB_FAIL(bool_estimator->child_estimators_.push_back(child_estimator))) {
            LOG_WARN("failed to push back estimators", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      const ObRawExpr *eigen_expr = NULL;
      bool inited = false;
      for (int64_t i = 0; i < bool_estimator->child_estimators_.count(); i ++) {
        ObSelEstimator *child_estimator = bool_estimator->child_estimators_.at(i);
        if (ObSelEstType::CONST == child_estimator->get_type()) {
          // do nothing
        } else if (!inited) {
          eigen_expr = child_estimator->get_eigen_expr();
          inited = true;
        } else if (eigen_expr != child_estimator->get_eigen_expr()) {
          eigen_expr = NULL;
          break;
        }
      }
      bool_estimator->eigen_expr_ = eigen_expr;
    }
  }
  return ret;
}

bool ObBoolOpSelEstimator::is_complex_filter_qual()
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < child_estimators_.count(); ++i) {
    ObSelEstimator *estimator = child_estimators_.at(i);
    bret |= OB_NOT_NULL(estimator) ? estimator->is_complex_filter_qual() : false;
  }
  return bret;
}

bool ObBoolOpSelEstimator::is_complex_join_qual()
{
  bool bret = false;
  if (OB_NOT_NULL(expr_) && T_OP_AND == expr_->get_expr_type()) {
    bret = child_estimators_.count() > 1;
  }
  for (int64_t i = 0; !bret && i < child_estimators_.count(); ++i) {
    ObSelEstimator *estimator = child_estimators_.at(i);
    bret |= OB_NOT_NULL(estimator) ? estimator->is_complex_join_qual() : false;
  }
  return bret;
}

int ObBoolOpSelEstimator::get_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  double &selectivity,
                                  ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  const ObRawExpr &qual = *expr_;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", KPC(this), K(ctx));
  } else if (T_OP_NOT == qual.get_expr_type() ||
             T_FUN_SYS_LNNVL == qual.get_expr_type() ||
             T_OP_BOOL == qual.get_expr_type()) {
    ObSEArray<ObRawExpr*, 3> cur_vars;
    const ObRawExpr *child_expr = NULL;
    ObSelEstimator *estimator = NULL;
    double tmp_selectivity = 0.0;
    if (OB_UNLIKELY(child_estimators_.count() != 1) ||
        OB_ISNULL(child_expr = qual.get_param_expr(0)) ||
        OB_ISNULL(estimator = child_estimators_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param", KPC(this));
    } else if (OB_FAIL(estimator->get_sel(table_metas, ctx, tmp_selectivity, all_predicate_sel))) {
      LOG_WARN("failed to get sel", KPC(estimator), K(ret));
    } else if (T_FUN_SYS_LNNVL == qual.get_expr_type()) {
      selectivity = 1.0 - tmp_selectivity;
    } else if (T_OP_BOOL == qual.get_expr_type()) {
      selectivity = tmp_selectivity;
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(child_expr, cur_vars))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (1 == cur_vars.count() &&
               T_OP_IS != child_expr->get_expr_type() &&
               T_OP_IS_NOT != child_expr->get_expr_type() &&
               T_OP_NSEQ != child_expr->get_expr_type()) { // for only one column, consider null_sel
      double null_sel = 1.0;
      if (OB_ISNULL(cur_vars.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::get_column_basic_sel(table_metas, ctx, *cur_vars.at(0), NULL, &null_sel))) {
        LOG_WARN("failed to get column basic sel", K(ret));
      } else {
        // not op.
        // if can calculate null_sel, sel = 1.0 - null_sel - op_sel
        selectivity = ObOptSelectivity::revise_between_0_1(1.0 - null_sel - tmp_selectivity);
      }
    } else {
      // for other condition, it's is too hard to consider null_sel, so ignore it.
      // t_op_is, t_op_nseq , they are null safe exprs, don't consider null_sel.
      selectivity = 1.0 - tmp_selectivity;
    }
  } else if (T_OP_OR == qual.get_expr_type()) {
    double tmp_selectivity = 1.0;
    ObSEArray<double, 4> selectivities;
    bool is_mutex = (NULL != eigen_expr_);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_estimators_.count(); ++i) {
      ObSelEstimator *estimator = NULL;
      if (OB_ISNULL(estimator = child_estimators_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected param", KPC(this));
      } else if (OB_FAIL(estimator->get_sel(table_metas, ctx, tmp_selectivity, all_predicate_sel))) {
        LOG_WARN("failed to get sel", KPC(estimator), K(ret));
      } else if (OB_FAIL(selectivities.push_back(tmp_selectivity))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (is_mutex) {
        selectivity = 0.0;
        for (int64_t i = 0; i < selectivities.count(); i ++) {
          selectivity += selectivities.at(i);
        }
        selectivity = ObOptSelectivity::revise_between_0_1(selectivity);
      } else {
        // sel(p1 or p2 or p3) = sel(!(!p1 and !p2 and !p3))
        for (int64_t i = 0; i < selectivities.count(); i ++) {
          selectivities.at(i) = 1 - selectivities.at(i);
        }
        selectivity = ctx.get_correlation_model().combine_filters_selectivity(selectivities);
        selectivity = 1- selectivity;
      }
    }
  } else if (T_OP_AND == qual.get_expr_type()) {
    if (OB_FAIL(ObOptSelectivity::calculate_selectivity(
            table_metas, ctx, child_estimators_, selectivity, all_predicate_sel))) {
      LOG_WARN("failed to calculate selectivity", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", KPC(this));
  }
  return ret;
}

int ObRangeSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                          const OptSelectivityCtx &ctx,
                                          const ObRawExpr &expr,
                                          ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  bool is_valid = true;
  estimator = NULL;
  ObArray<ObRawExpr*> column_exprs;
  ObRangeSelEstimator *range_estimator = NULL;
  if (OB_FAIL(ObOptEstUtils::is_range_expr(&expr, is_valid))) {
    LOG_WARN("judge range expr failed", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(&expr, column_exprs))) {
    LOG_WARN("extract_column_exprs error in clause_selectivity", K(ret));
  } else if (column_exprs.count() != 1) {
    is_valid = false;
  } else if (OB_FAIL(factory.create_estimator_inner(range_estimator))) {
    LOG_WARN("failed to create estimator ", K(ret));
  } else {
    range_estimator->column_expr_ = static_cast<ObColumnRefRawExpr *>(column_exprs.at(0));
    range_estimator->eigen_expr_ = range_estimator->column_expr_;
    if (OB_FAIL(range_estimator->range_exprs_.push_back(const_cast<ObRawExpr *>(&expr)))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      estimator = range_estimator;
    }
  }
  return ret;
}

int ObRangeSelEstimator::merge(const ObSelEstimator &other, bool &is_success)
{
  int ret = OB_SUCCESS;
  is_success = false;
  if (get_type() == other.get_type()) {
    const ObRangeSelEstimator &est_other = static_cast<const ObRangeSelEstimator &>(other);
    if (column_expr_ == est_other.column_expr_) {
      is_success = true;
      if (OB_FAIL(append(range_exprs_, est_other.range_exprs_))) {
        LOG_WARN("failed to append", K(ret));
      }
    }
  }
  return ret;
}

int ObRangeSelEstimator::get_sel(const OptTableMetas &table_metas,
                                 const OptSelectivityCtx &ctx,
                                 double &selectivity,
                                 ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_expr_) || OB_UNLIKELY(range_exprs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", KPC(this));
  } else if (OB_FAIL(ObOptSelectivity::get_column_range_sel(
      table_metas, ctx, *column_expr_, range_exprs_, true, selectivity))) {
    LOG_WARN("failed to calc qual selectivity", KPC(column_expr_), K(range_exprs_), K(ret));
  } else {
    selectivity = ObOptSelectivity::revise_between_0_1(selectivity);
  }
  return ret;
}

int ObSimpleJoinSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                               const OptSelectivityCtx &ctx,
                                               const ObRawExpr &expr,
                                               ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  estimator = NULL;
  ObSimpleJoinSelEstimator *simple_join_estimator = NULL;
  bool is_valid = false;
  const ObRelIds *left_rel_ids = ctx.get_left_rel_ids();
  const ObRelIds *right_rel_ids = ctx.get_right_rel_ids();
  if (OB_FAIL(is_simple_join_condition(expr, ctx.get_left_rel_ids(), ctx.get_right_rel_ids(), is_valid))) {
    LOG_WARN("failed to check is simple join", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(factory.create_estimator_inner(simple_join_estimator))) {
    LOG_WARN("failed to create estimator ", K(ret));
  } else if (OB_FAIL(simple_join_estimator->join_conditions_.push_back(const_cast<ObRawExpr *>(&expr)))) {
    LOG_WARN("failed to push back", K(ret));
  } else {
    simple_join_estimator->left_rel_ids_ = left_rel_ids;
    simple_join_estimator->right_rel_ids_ = right_rel_ids;
    simple_join_estimator->join_rel_ids_ = &expr.get_relation_ids();
    estimator = simple_join_estimator;
  }
  return ret;
}

/**
 * check if qual is a simple join condition.
 * This recommend each side of `=` belong to different subtree.
 */
int ObSimpleJoinSelEstimator::is_simple_join_condition(const ObRawExpr &qual,
                                                       const ObRelIds *left_rel_ids,
                                                       const ObRelIds *right_rel_ids,
                                                       bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (NULL == left_rel_ids || NULL == right_rel_ids) {
    // do nothing
  } else if (T_OP_EQ == qual.get_expr_type() || T_OP_NSEQ == qual.get_expr_type()) {
    const ObRawExpr *expr0 = qual.get_param_expr(0);
    const ObRawExpr *expr1 = qual.get_param_expr(1);
    if (OB_ISNULL(expr0) || OB_ISNULL(expr1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null exprs", K(ret), K(expr0), K(expr1));
    } else if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(expr0)) ||
               OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(expr1))) {
      LOG_WARN("failed to remove ignorable function", K(ret));
    } else if (!expr0->is_column_ref_expr() || !expr1->is_column_ref_expr()) {
      // do nothing
    } else if ((left_rel_ids->is_superset(expr0->get_relation_ids()) &&
                right_rel_ids->is_superset(expr1->get_relation_ids())) ||
               (left_rel_ids->is_superset(expr1->get_relation_ids()) &&
                right_rel_ids->is_superset(expr0->get_relation_ids()))) {
      is_valid = true;
    }
  }
  return ret;
}

int ObSimpleJoinSelEstimator::merge(const ObSelEstimator &other, bool &is_success)
{
  int ret = OB_SUCCESS;
  is_success = false;
  if (get_type() == other.get_type()) {
    const ObSimpleJoinSelEstimator &est_other = static_cast<const ObSimpleJoinSelEstimator &>(other);
    if (OB_ISNULL(left_rel_ids_) || OB_ISNULL(right_rel_ids_) ||
        OB_ISNULL(est_other.left_rel_ids_) || OB_ISNULL(est_other.right_rel_ids_) ||
        OB_ISNULL(join_rel_ids_) || OB_ISNULL(est_other.join_rel_ids_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", KPC(this), K(est_other));
    } else if (*left_rel_ids_ == *est_other.left_rel_ids_ &&
               *right_rel_ids_ == *est_other.right_rel_ids_ &&
               *join_rel_ids_ == *est_other.join_rel_ids_
               ) {
      is_success = true;
      if (OB_FAIL(append(join_conditions_, est_other.join_conditions_))) {
        LOG_WARN("failed to append", K(ret));
      }
    }
  }
  return ret;
}

int ObSimpleJoinSelEstimator::get_sel(const OptTableMetas &table_metas,
                                      const OptSelectivityCtx &ctx,
                                      double &selectivity,
                                      ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  selectivity = 1.0;
  if (OB_UNLIKELY(join_conditions_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty join condition", KPC(this));
  } else if (1 == join_conditions_.count()) {
    // only one join condition, calculate selectivity directly
    if (OB_ISNULL(join_conditions_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObEqualSelEstimator::get_equal_sel(table_metas, ctx, *join_conditions_.at(0), selectivity))) {
      LOG_WARN("Failed to get equal selectivity", K(ret));
    } else {
      LOG_PRINT_EXPR(TRACE, "get single equal expr selectivity", *join_conditions_.at(0), K(selectivity));
    }
  } else if (join_conditions_.count() > 1) {
    // 存在多个连接条件，检查是否涉及联合主键
    if (OB_FAIL(get_multi_equal_sel(table_metas, ctx, join_conditions_, selectivity))) {
      LOG_WARN("failed to get equal sel");
    } else {
      selectivity = ObOptSelectivity::revise_between_0_1(selectivity);
      LOG_TRACE("get multi equal expr selectivity", KPC(this), K(selectivity));
    }
  }
  return ret;
}

int ObSimpleJoinSelEstimator::get_multi_equal_sel(const OptTableMetas &table_metas,
                                                  const OptSelectivityCtx &ctx,
                                                  ObIArray<ObRawExpr *> &quals,
                                                  double &selectivity)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> left_exprs;
  ObSEArray<ObRawExpr *, 4> right_exprs;
  ObSEArray<bool, 4> null_safes;
  bool is_valid;
  if (OB_ISNULL(ctx.get_left_rel_ids()) || OB_ISNULL(ctx.get_right_rel_ids())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed get unexpected null", K(ret), K(ctx));
  } else if (OB_FAIL(is_valid_multi_join(quals, is_valid))) {
    LOG_WARN("failed to check is valid multi join", K(ret));
  } else if (OB_UNLIKELY(!is_valid)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(extract_join_exprs(quals, *ctx.get_left_rel_ids(), *ctx.get_right_rel_ids(),
                                        left_exprs, right_exprs, null_safes))) {
    LOG_WARN("failed to extract join exprs", K(ret));
  } else if (OB_FAIL(get_cntcols_eq_cntcols_sel(table_metas, ctx, left_exprs, right_exprs,
                                                null_safes, selectivity))) {
    LOG_WARN("Failed to get equal sel", K(ret));
  } else { /* do nothing */ }
  return ret;
}

/**
 *  check if multi join condition only related to two table
 */
int ObSimpleJoinSelEstimator::is_valid_multi_join(ObIArray<ObRawExpr *> &quals,
                                                  bool &is_valid)
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
    const ObRelIds &rel_ids = quals.at(0)->get_relation_ids();
    is_valid = rel_ids.num_members() == 2;
    for (int64_t i = 1; OB_SUCC(ret) && is_valid && i < quals.count(); ++i) {
      ObRawExpr *cur_expr = quals.at(i);
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

int ObSimpleJoinSelEstimator::extract_join_exprs(ObIArray<ObRawExpr *> &quals,
                                                 const ObRelIds &left_rel_ids,
                                                 const ObRelIds &right_rel_ids,
                                                 ObIArray<ObRawExpr *> &left_exprs,
                                                 ObIArray<ObRawExpr *> &right_exprs,
                                                 ObIArray<bool> &null_safes)
{
  int ret = OB_SUCCESS;
  ObRawExpr *left_expr = NULL;
  ObRawExpr *right_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObRawExpr *cur_expr = quals.at(i);
    if (OB_ISNULL(cur_expr) ||
        OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
        OB_ISNULL(right_expr = cur_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_expr), K(left_expr), K(right_expr));
    } else if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(left_expr)) ||
               OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(right_expr))) {
      LOG_WARN("failed to remove ignorable function", K(ret));
    } else if (OB_UNLIKELY(!left_expr->is_column_ref_expr() || !right_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all expr should be column ref", K(ret), K(*cur_expr));
    } else if (left_rel_ids.is_superset(left_expr->get_relation_ids()) &&
               right_rel_ids.is_superset(right_expr->get_relation_ids())) {
      // do nothing
    } else if (left_rel_ids.is_superset(right_expr->get_relation_ids()) &&
               right_rel_ids.is_superset(left_expr->get_relation_ids())) {
      std::swap(left_expr, right_expr);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(left_expr), K(right_expr));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(left_exprs.push_back(left_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(right_exprs.push_back(right_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(null_safes.push_back(T_OP_NSEQ == cur_expr->get_expr_type()))) {
        LOG_WARN("failed to push back null safe", K(ret));
      }
    }
  }
  return ret;
}

int ObSimpleJoinSelEstimator::get_cntcols_eq_cntcols_sel(const OptTableMetas &table_metas,
                                                         const OptSelectivityCtx &ctx,
                                                         const ObIArray<ObRawExpr *> &left_exprs,
                                                         const ObIArray<ObRawExpr *> &right_exprs,
                                                         const ObIArray<bool> &null_safes,
                                                         double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_EQ_SEL;
  ObSEArray<double, 4> left_ndvs;
  ObSEArray<double, 4> right_ndvs;
  ObSEArray<double, 4> left_not_null_sels;
  ObSEArray<double, 4> right_not_null_sels;
  double left_ndv = 1.0;
  double right_ndv = 1.0;
  double left_nns = 1.0;
  double right_nns = 1.0;
  double left_rows = 1.0;
  double right_rows = 1.0;
  double left_origin_rows = 1.0;
  double right_origin_rows = 1.0;
  bool left_contain_pk = false;
  bool right_contain_pk = false;
  bool is_union_pk = false;
  bool refine_right_ndv = false;
  bool refine_left_ndv = false;
  double left_base_ndv = -1.0;
  double right_base_ndv = -1.0;

  if (OB_ISNULL(ctx.get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::is_columns_contain_pkey(table_metas, left_exprs,
                                                               left_contain_pk, is_union_pk))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  } else if (OB_FALSE_IT(refine_right_ndv = left_contain_pk && is_union_pk)) {
  } else if (OB_FAIL(ObOptSelectivity::is_columns_contain_pkey(table_metas, right_exprs,
                                                               right_contain_pk, is_union_pk))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  } else if (OB_FALSE_IT(refine_left_ndv = right_contain_pk && is_union_pk)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_exprs.count(); ++i) {
      if (OB_ISNULL(left_exprs.at(i)) || OB_ISNULL(right_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::get_column_ndv_and_nns(table_metas, ctx, *left_exprs.at(i),
                                                                  &left_ndv, &left_nns))) {
        LOG_WARN("failed to get left ndv and nns", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::get_column_ndv_and_nns(table_metas, ctx, *right_exprs.at(i),
                                                                  &right_ndv, &right_nns))) {
        LOG_WARN("failed to get left ndv and nns", K(ret));
      } else if (OB_FAIL(left_not_null_sels.push_back(left_nns))) {
        LOG_WARN("failed to push back not null sel", K(ret));
      } else if (OB_FAIL(right_not_null_sels.push_back(right_nns))) {
        LOG_WARN("failed to push back not null sel", K(ret));
      } else if (OB_FAIL(left_ndvs.push_back(left_ndv))) {
        LOG_WARN("failed to push back ndv", K(ret));
      } else if (OB_FAIL(right_ndvs.push_back(right_ndv))) {
        LOG_WARN("failed to push back ndv", K(ret));
      } else if (0 == i) {
        if (OB_FAIL(ObOptSelectivity::get_column_basic_info(table_metas, ctx, *left_exprs.at(i),
                                                            NULL, NULL, NULL, &left_rows))) {
          LOG_WARN("failed to get column basic info", K(ret));
        } else if (OB_FAIL(ObOptSelectivity::get_column_basic_info(table_metas, ctx, *right_exprs.at(i),
                                                                   NULL, NULL, NULL, &right_rows))) {
          LOG_WARN("failed to get column basic info", K(ret));
        } else if (refine_right_ndv &&
                  OB_FAIL(ObOptSelectivity::get_column_basic_info(ctx.get_plan()->get_basic_table_metas(),
                                                                  ctx, *left_exprs.at(i),
                                                                  NULL, NULL, NULL, &left_origin_rows))) {
          LOG_WARN("failed to get column basic info", K(ret));
        } else if (refine_left_ndv &&
                  OB_FAIL(ObOptSelectivity::get_column_basic_info(ctx.get_plan()->get_basic_table_metas(),
                                                                  ctx, *right_exprs.at(i),
                                                                  NULL, NULL, NULL, &right_origin_rows))) {
          LOG_WARN("failed to get column basic info", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    left_nns = 1.0;
    right_nns = 1.0;
    for (int64_t i = 0; i < left_not_null_sels.count(); ++i) {
      left_nns *= left_not_null_sels.at(i);
    }
    for (int64_t i = 0; i < right_not_null_sels.count(); ++i) {
      right_nns *= right_not_null_sels.at(i);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(table_metas, ctx, left_exprs, left_rows, left_ndv))) {
    LOG_WARN("Failed to calculate distinct", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(table_metas, ctx, right_exprs, right_rows, right_ndv))) {
    LOG_WARN("Failed to calculate distinct", K(ret));
  } else if (IS_SEMI_ANTI_JOIN(ctx.get_assumption_type())) {
    // do nothing
  } else if (left_contain_pk == right_contain_pk) {
    // 两侧都不是主键或都是主键, 不做修正
  } else if (refine_right_ndv) {
    // 一侧有主键时, 认为是主外键连接, 外键上最大的ndv为即为主键的原始ndv
    right_ndv = std::min(right_ndv, left_origin_rows);
  } else if (refine_left_ndv) {
    left_ndv = std::min(left_ndv, right_origin_rows);
  }
  if (OB_SUCC(ret) && IS_ANTI_JOIN(ctx.get_join_type())) {
    if (OB_FAIL(ObOptSelectivity::calculate_distinct(table_metas, ctx, left_exprs, -1.0, left_base_ndv, false, DistinctEstType::BASE))) {
      LOG_WARN("Failed to calculate distinct", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(table_metas, ctx, right_exprs, -1.0, right_base_ndv, false, DistinctEstType::BASE))) {
      LOG_WARN("Failed to calculate distinct", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    selectivity = ObOptSelectivity::calc_equal_join_sel(ctx, T_OP_EQ, left_ndv, right_ndv, left_nns, right_nns, left_base_ndv, right_base_ndv);
    // 处理 null safe，这里假设多列上同时为null即小概率事件，只考虑特定列上为null的情况
    for (int64_t i = 0; i < null_safes.count(); ++i) {
      if (null_safes.at(i)) {
        double factor = 1.0;
        for (int64_t j = 0; j < null_safes.count(); ++j) {
          if (i == j) {
            if (IS_LEFT_SEMI_ANTI_JOIN(ctx.get_join_type())) {
              factor *= (1 - left_not_null_sels.at(j));
            } else if (IS_RIGHT_SEMI_ANTI_JOIN(ctx.get_join_type())) {
              factor *= (1 - right_not_null_sels.at(j));
            } else {
              factor *= (1 - left_not_null_sels.at(j)) * (1 - right_not_null_sels.at(j));
            }
          } else {
            if (IS_LEFT_SEMI_ANTI_JOIN(ctx.get_join_type())) {
              factor *= left_not_null_sels.at(j) * std::min(left_ndvs.at(j), right_ndvs.at(j)) / left_ndvs.at(j);
            } else if (IS_RIGHT_SEMI_ANTI_JOIN(ctx.get_join_type())) {
              factor *= right_not_null_sels.at(j) * std::min(left_ndvs.at(j), right_ndvs.at(j)) / right_ndvs.at(j);
            } else {
              factor *= left_not_null_sels.at(j) * right_not_null_sels.at(j) / std::max(left_ndvs.at(j), right_ndvs.at(j));
            }
          }
        }
        selectivity += factor;
      }
    }
  }
  LOG_TRACE("selectivity of `col_ref1 =|<=> col_ref1 and col_ref2 =|<=> col_ref2`", K(selectivity));
  return ret;
}

// extract expr like '(-) col1 +(-) col2 + offset'
int ObInequalJoinSelEstimator::extract_column_offset(const OptSelectivityCtx &ctx,
                                                     const ObRawExpr *expr,
                                                     bool is_minus,
                                                     bool &is_valid,
                                                     ObInequalJoinSelEstimator::Term &term,
                                                     double &offset)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", KPC(expr));
  } else if (!ob_is_numeric_type(expr->get_data_type())) {
    is_valid = false;
  } else if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(expr))) {
    LOG_WARN("failed to remove ignorable expr", KPC(expr));
  } else if (!ob_is_numeric_type(expr->get_data_type())) {
    is_valid = false;
  } else if (T_OP_ADD == expr->get_expr_type() || T_OP_MINUS == expr->get_expr_type()) {
    bool child_is_minus = (T_OP_MINUS == expr->get_expr_type()) ? !is_minus : is_minus;
    if (OB_UNLIKELY(expr->get_param_count() != 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param", KPC(expr));
    } else if (OB_FAIL(SMART_CALL(extract_column_offset(ctx, expr->get_param_expr(0), is_minus, is_valid, term, offset)))) {
      LOG_WARN("failed to extract col offset", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(SMART_CALL(extract_column_offset(ctx, expr->get_param_expr(1), child_is_minus, is_valid, term, offset)))) {
      LOG_WARN("failed to extract col offset", K(ret));
    }
  } else if (T_OP_NEG == expr->get_expr_type() || T_OP_POS == expr->get_expr_type()) {
    bool child_is_minus = (T_OP_NEG == expr->get_expr_type()) ? !is_minus : is_minus;
    if (OB_UNLIKELY(expr->get_param_count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param", KPC(expr));
    } else if (OB_FAIL(SMART_CALL(extract_column_offset(ctx, expr->get_param_expr(0), child_is_minus, is_valid, term, offset)))) {
      LOG_WARN("failed to extract col offset", K(ret));
    }
  } else if (expr->is_column_ref_expr()) {
    if (term.col1_ == NULL) {
      term.col1_ = static_cast<const ObColumnRefRawExpr *>(expr);
      term.coefficient1_ = is_minus ? -1.0 : 1.0;
    } else if (term.col2_ == NULL) {
      term.col2_ = static_cast<const ObColumnRefRawExpr *>(expr);
      term.coefficient2_ = is_minus ? -1.0 : 1.0;
    } else {
      is_valid = false;
    }
  } else if (expr->is_static_scalar_const_expr()) {
    ObObj const_value;
    ObObj scalar_value;
    bool got_result = false;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                          expr,
                                                          const_value,
                                                          got_result,
                                                          ctx.get_allocator()))) {
      LOG_WARN("failed to calc const or calculable expr", K(ret));
    } else if (!got_result || !const_value.is_numeric_type() || const_value.is_null()) {
      is_valid = false;
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_scalar_obj(&const_value, &scalar_value))) {
      LOG_WARN("failed to convert obj to scalar", K(const_value));
    } else {
      if (is_minus) {
        offset -= scalar_value.get_double();
      } else {
        offset += scalar_value.get_double();
      }
    }
  } else {
    is_valid = false;
  }
  return ret;
}

int ObInequalJoinSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                                const OptSelectivityCtx &ctx,
                                                const ObRawExpr &expr,
                                                ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  ObInequalJoinSelEstimator *ineq_join_estimator = NULL;
  bool is_valid = true;
  if (IS_RANGE_CMP_OP(expr.get_expr_type())) {
    Term term;
    double offset = 0.0;
    if (2 != expr.get_param_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should have 2 param", K(ret), K(expr));
    } else if (OB_FAIL(extract_column_offset(ctx, expr.get_param_expr(0), false, is_valid, term, offset))) {
      LOG_WARN("failed to extract column diff", KPC(expr.get_param_expr(0)));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(extract_column_offset(ctx, expr.get_param_expr(1), true, is_valid, term, offset))) {
      LOG_WARN("failed to extract column diff", KPC(expr.get_param_expr(1)));
    } else if (!is_valid || !term.is_valid()) {
      is_valid = false;
    } else if (OB_FAIL(factory.create_estimator_inner(ineq_join_estimator))) {
      LOG_WARN("failed to create estimator ", K(ret));
    } else  {
      ineq_join_estimator->term_ = term;
      ineq_join_estimator->range_.set_bound(expr.get_expr_type(), -offset);
    }
  } else if (T_OP_BTW == expr.get_expr_type()) {
    Term term1;
    Term term2;
    double offset1 = 0.0;
    double offset2 = 0.0;
    bool is_same = false;
    bool need_reverse = false;
    if (3 != expr.get_param_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("between expr should have 3 param", K(ret), K(expr));
    } else if (OB_FAIL(extract_column_offset(ctx, expr.get_param_expr(0), false, is_valid, term1, offset1))) {
      LOG_WARN("failed to extract column diff", KPC(expr.get_param_expr(0)));
    } else if (!is_valid) {
      // do nothing
    } else if (FALSE_IT(offset2 = offset1) || FALSE_IT(term2 = term1)) {
    } else if (OB_FAIL(extract_column_offset(ctx, expr.get_param_expr(1), true, is_valid, term1, offset1))) {
      LOG_WARN("failed to extract column diff", KPC(expr.get_param_expr(1)));
    } else if (OB_FAIL(extract_column_offset(ctx, expr.get_param_expr(2), true, is_valid, term2, offset2))) {
      LOG_WARN("failed to extract column diff", KPC(expr.get_param_expr(2)));
    } else if (!is_valid || !term1.is_valid() || !term2.is_valid()) {
      is_valid = false;
    } else if (FALSE_IT(cmp_term(term1, term2, is_same, need_reverse))) {
    } else if (!is_same || need_reverse) {
      is_valid = false;
    } else if (OB_FAIL(factory.create_estimator_inner(ineq_join_estimator))) {
      LOG_WARN("failed to create estimator ", K(ret));
    } else {
      ineq_join_estimator->term_ = term1;
      ineq_join_estimator->range_.set_bound(T_OP_GE, -offset1);
      ineq_join_estimator->range_.set_bound(T_OP_LE, -offset2);
    }
  }
  estimator = ineq_join_estimator;
  return ret;
}

void ObInequalJoinSelEstimator::cmp_term(const ObInequalJoinSelEstimator::Term &t1,
                                         const ObInequalJoinSelEstimator::Term &t2,
                                         bool &is_equal, bool &need_reverse)
{
  is_equal = false;
  need_reverse = false;
  if (t1.col1_ == t2.col1_ && t1.col2_ == t2.col2_) {
    if (t1.coefficient1_ == t2.coefficient1_ && t1.coefficient2_ == t2.coefficient2_) {
      is_equal = true;
    } else if (t1.coefficient1_ == -t2.coefficient1_ && t1.coefficient2_ == -t2.coefficient2_) {
      is_equal = true;
      need_reverse = true;
    }
  } else if (t1.col1_ == t2.col2_ && t1.col2_ == t2.col1_) {
    if (t1.coefficient1_ == t2.coefficient2_ && t1.coefficient2_ == t2.coefficient1_) {
      is_equal = true;
    } else if (t1.coefficient1_ == -t2.coefficient2_ && t1.coefficient2_ == -t2.coefficient1_) {
      is_equal = true;
      need_reverse = true;
    }
  }
}

int ObInequalJoinSelEstimator::merge(const ObSelEstimator &other_estmator, bool &is_success)
{
  int ret = OB_SUCCESS;
  is_success = false;
  if (get_type() == other_estmator.get_type()) {
    const ObInequalJoinSelEstimator &other = static_cast<const ObInequalJoinSelEstimator &>(other_estmator);
    bool need_reverse = false;
    cmp_term(term_, other.term_, is_success, need_reverse);
    if (is_success) {
      if (need_reverse) {
        term_.coefficient1_ = -term_.coefficient1_;
        term_.coefficient2_ = -term_.coefficient2_;
        range_.multiply_double(-1.0);
      }
      range_.intersect(other.range_);
    }
  }
  return ret;
}

double ObInequalJoinSelEstimator::get_gt_sel(double min1,
                                             double max1,
                                             double min2,
                                             double max2,
                                             double offset)
{
  double selectivity = 0.0;
  double total = (max2 - min2) * (max1 - min1);

  if (offset < min1 + min2) {
    selectivity = 1.0;
  } else if (offset < max1 + min2 && offset < min1 + max2 && total > OB_DOUBLE_EPSINON) {
    selectivity = 1 - (offset - min1  - min2) * (offset - min1  - min2) / (2 * total);
  } else if (offset >= max1 + min2 && offset < min1 + max2 && max2 - min2 > OB_DOUBLE_EPSINON) {
    selectivity = (2 * max2 + min1 + max1 - 2 * offset) / (2 * (max2 - min2));
  } else if (offset >= min1 + max2 && offset < max1 + min2 && max1 - min1 > OB_DOUBLE_EPSINON) {
    selectivity = (min2 + max2 + 2 * max1 - 2 * offset) / (2 * (max1 - min1));
  } else if (offset < max1 + max2 && total > OB_DOUBLE_EPSINON) {
    selectivity = (max1 + max2 - offset) * (max1 + max2 - offset) / (2 * total);
  } else {
    selectivity = 0.0;
  }
  return selectivity;
}

double ObInequalJoinSelEstimator::get_any_gt_sel(double min1,
                                                 double max1,
                                                 double min2,
                                                 double max2,
                                                 double offset)
{
  double selectivity = 0.0;
  if (offset < min1 + max2) {
    selectivity = 1.0;
  } else if (offset < max1 + max2 && max1 - min1 > OB_DOUBLE_EPSINON) {
    selectivity = (max1 + max2 - offset) / (max1 - min1);
  } else {
    selectivity = 0.0;
  }
  return selectivity;
}

double ObInequalJoinSelEstimator::get_all_gt_sel(double min1,
                                                 double max1,
                                                 double min2,
                                                 double max2,
                                                 double offset)
{
  double selectivity = 0.0;
  if (offset < min1 + min2) {
    selectivity = 1.0;
  } else if (offset < max1 + min2 && max1 - min1 > OB_DOUBLE_EPSINON) {
    selectivity = (max1 + min2 - offset) / (max1 - min1);
  } else {
    selectivity = 0.0;
  }
  return selectivity;
}

double ObInequalJoinSelEstimator::get_equal_sel(double min1,
                                                double max1,
                                                double ndv1,
                                                double min2,
                                                double max2,
                                                double ndv2,
                                                double offset,
                                                bool is_semi)
{
  double selectivity = 0.0;
  double overlap = 0.0;
  double overlap_ndv1 = 1.0, overlap_ndv2 = 1.0;
  if (offset < min1 + min2) {
    overlap = 0.0;
  } else if (offset < max1 + min2 && offset < min1 + max2) {
    overlap = offset - min1 - min2;
  } else if (offset >= max1 + min2 && offset < min1 + max2) {
    overlap = max1 - min1;
  } else if (offset >= min1 + max2 && offset < max1 + min2) {
    overlap = max2 - min2;
  } else if (offset < max1 + max2) {
    overlap = max1 + max2 - offset;
  } else {
    overlap = 0.0;
  }
  if (max1 - min1 > OB_DOUBLE_EPSINON) {
    overlap_ndv1 = revise_ndv(ndv1 * overlap / (max1 - min1)) ;
  } else {
    overlap_ndv1 = 1;
  }
  if (max2 - min2 > OB_DOUBLE_EPSINON) {
    overlap_ndv2 = revise_ndv(ndv2 * overlap / (max2 - min2)) ;
  } else {
    overlap_ndv2 = 1;
  }
  if (is_semi) {
    selectivity = overlap_ndv1 / ndv1;
  } else {
    selectivity = 1 / max(overlap_ndv1, overlap_ndv2) * (overlap_ndv1 / ndv1) * (overlap_ndv2 / ndv2);
  }
  return selectivity;
}

int ObInequalJoinSelEstimator::get_sel(const OptTableMetas &table_metas,
                                       const OptSelectivityCtx &ctx,
                                       double &selectivity,
                                       ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  ObObj obj_min, obj_max, tmp_obj;
  selectivity = 1.0;
  double nns1, nns2, ndv1, ndv2;
  double min1, min2, max1, max2;
  double lower_bound = range_.start_.get_double();
  bool has_lower_bound = !range_.start_.is_min_value();
  double upper_bound = range_.end_.get_double();
  bool has_upper_bound = !range_.end_.is_max_value();
  bool is_valid = range_.is_valid_range();
  bool is_eq = range_.inclusive_start_ && range_.inclusive_end_ &&
               !range_.start_.is_min_value() && !range_.end_.is_max_value() &&
               fabs(range_.end_.get_double() - range_.start_.get_double()) <= OB_DOUBLE_EPSINON;
  if (OB_ISNULL(term_.col1_) ||
      OB_ISNULL(term_.col2_) ||
      OB_UNLIKELY(fabs(term_.coefficient1_) != 1.0) ||
      OB_UNLIKELY(fabs(term_.coefficient2_) != 1.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", KPC(this));
  } else if (OB_FAIL(ObOptSelectivity::get_column_ndv_and_nns(table_metas, ctx, *term_.col1_, &ndv1, &nns1))) {
    LOG_WARN("failed to get nns");
  } else if (OB_FAIL(ObOptSelectivity::get_column_ndv_and_nns(table_metas, ctx, *term_.col2_, &ndv2, &nns2))) {
    LOG_WARN("failed to get nns");
  } else if (!range_.is_valid_range()) {
    // always false
    // e.g.  1 < c1 + c2 < 0
    selectivity = 0.0;
  } else if (term_.col1_->get_table_id() == term_.col2_->get_table_id() &&
             term_.col1_->get_column_id() == term_.col2_->get_column_id()) {
    // same column
    if (fabs(term_.coefficient1_ + term_.coefficient2_) <= OB_DOUBLE_EPSINON) {
      // e.g. : c1 - c1 < 1
      //        c1 - c1 > 1
      selectivity = get_sel_for_point(0.0) * nns1;
    } else {
      // TODO : c1 + c1 < 1
      selectivity = DEFAULT_INEQ_JOIN_SEL;
      use_default_ = true;
    }
  } else if (OB_FAIL(ObOptSelectivity::get_column_min_max(table_metas, ctx, *term_.col1_, obj_min, obj_max))) {
    LOG_WARN("failed to get column min max", K(ret), KPC(term_.col1_));
  } else if (obj_min.is_min_value() || obj_min.is_max_value() ||
             obj_max.is_max_value() || obj_max.is_min_value()) {
    selectivity = DEFAULT_INEQ_JOIN_SEL;
    use_default_ = true;
  } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_scalar_obj(&obj_min, &tmp_obj))) {
    LOG_WARN("failed to convert obj", K(obj_min));
  } else if (FALSE_IT(min1 = tmp_obj.get_double() * term_.coefficient1_)) {
  } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_scalar_obj(&obj_max, &tmp_obj))) {
    LOG_WARN("failed to convert obj", K(obj_max));
  } else if (FALSE_IT(max1 = tmp_obj.get_double() * term_.coefficient1_)) {
  } else if (OB_FAIL(ObOptSelectivity::get_column_min_max(table_metas, ctx, *term_.col2_, obj_min, obj_max))) {
    LOG_WARN("failed to get column min max", K(ret), KPC(term_.col2_));
  } else if (obj_min.is_min_value() || obj_min.is_max_value() ||
             obj_max.is_max_value() || obj_max.is_min_value()) {
    selectivity = DEFAULT_INEQ_JOIN_SEL;
    use_default_ = true;
  } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_scalar_obj(&obj_min, &tmp_obj))) {
    LOG_WARN("failed to convert obj", K(obj_min));
  } else if (FALSE_IT(min2 = tmp_obj.get_double() * term_.coefficient2_)) {
  } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_scalar_obj(&obj_max, &tmp_obj))) {
    LOG_WARN("failed to convert obj", K(obj_max));
  } else if (FALSE_IT(max2 = tmp_obj.get_double() * term_.coefficient2_)) {
  } else {
    if (term_.coefficient1_ < 0) {
      std::swap(min1, max1);
    }
    if (term_.coefficient2_ < 0) {
      std::swap(min2, max2);
    }
    bool is_semi = IS_SEMI_ANTI_JOIN(ctx.get_join_type());
    if (is_semi) {
      if (OB_ISNULL(ctx.get_left_rel_ids()) || OB_ISNULL(ctx.get_right_rel_ids())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ctx.get_left_rel_ids()), K(ctx.get_right_rel_ids()));
      } else if (IS_LEFT_SEMI_ANTI_JOIN(ctx.get_join_type()) &&
                (term_.col1_->get_relation_ids().overlap(*ctx.get_right_rel_ids()) ||
                 term_.col2_->get_relation_ids().overlap(*ctx.get_left_rel_ids()))) {
        std::swap(min1, min2);
        std::swap(max1, max2);
        std::swap(ndv1, ndv2);
        std::swap(nns1, nns2);
      } else if (IS_RIGHT_SEMI_ANTI_JOIN(ctx.get_join_type()) &&
                 (term_.col1_->get_relation_ids().overlap(*ctx.get_left_rel_ids()) ||
                  term_.col2_->get_relation_ids().overlap(*ctx.get_right_rel_ids()))) {
        std::swap(min1, min2);
        std::swap(max1, max2);
        std::swap(ndv1, ndv2);
        std::swap(nns1, nns2);
      }
    }
    ndv1 = std::max(ndv1, 1.0);
    ndv2 = std::max(ndv2, 1.0);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(min1 > max1) ||
        OB_UNLIKELY(min2 > max2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected min max", K(min1), K(max1), K(min2), K(max2), KPC(this));
    } else if (fabs(max1 - min1) <= OB_DOUBLE_EPSINON && fabs(max2 - min2) <= OB_DOUBLE_EPSINON) {
      // Both c1 and c2 have only one value
      // e.g. c1 in [1,1] and c2 in [2,2]
      selectivity = get_sel_for_point(min1 + min2);
    } else if (is_eq) {
      // lower bound is the same as the upper bound
      // e.g : 1 <= c1 + c2 <= 1;
      selectivity = ObInequalJoinSelEstimator::get_equal_sel(min1, max1, ndv1, min2, max2, ndv2, lower_bound, is_semi);
    } else if (is_semi) {
      // calculate selectivity for semi join
      // e.g. : 0 <= c1 + c2 < 1
      double sel1 = has_lower_bound ? ObInequalJoinSelEstimator::get_any_gt_sel(min1, max1, min2, max2, lower_bound) : 1.0;
      double sel2 = has_upper_bound ? ObInequalJoinSelEstimator::get_all_gt_sel(min1, max1, min2, max2, upper_bound) : 0.0;
      // the sel of `any c2 satisfy 'a < c1 + c2 < b'` =
      // the sel of `any c2 satisfy 'c1 + c2 > a'` minus the sel of `all c2 satisfy 'c1 + c2 > b'`
      selectivity = sel1 - sel2;
      if (range_.inclusive_start_ && ndv1 > 1) {
        selectivity += 1 / ndv1;
      }
      if (range_.inclusive_end_ && ndv1 > 1) {
        selectivity += 1 / ndv1;
      }
    } else {
      // calculate selectivity for inner join
      // e.g. : 0 <= c1 + c2 < 1
      double sel1 = has_lower_bound ? ObInequalJoinSelEstimator::get_gt_sel(min1, max1, min2, max2, lower_bound) : 1.0;
      double sel2 = has_upper_bound ? ObInequalJoinSelEstimator::get_gt_sel(min1, max1, min2, max2, upper_bound) : 0.0;
      // the sel of 'a < c1 + c2 < b' =
      // the sel of 'c1 + c2 > a' minus the sel of 'c1 + c2 > b'
      selectivity = sel1 - sel2;
      if (range_.inclusive_start_) {
        selectivity += ObInequalJoinSelEstimator::get_equal_sel(min1, max1, ndv1, min2, max2, ndv2, lower_bound, is_semi);
      }
      if (range_.inclusive_end_) {
        selectivity += ObInequalJoinSelEstimator::get_equal_sel(min1, max1, ndv1, min2, max2, ndv2, upper_bound, is_semi);
      }
    }
    selectivity = ObOptSelectivity::revise_between_0_1(selectivity);

    // process not null sel
    if (is_semi) {
      selectivity = std::max(selectivity, 1 / ndv1);
      selectivity *= nns1;
    } else {
      selectivity = std::max(selectivity, 1 / (ndv1 * ndv2));
      selectivity *= nns1 * nns2;
    }
  }
  return ret;
}

double ObInequalJoinSelEstimator::get_sel_for_point(double point)
{
  SimpleRange point_range;
  point_range.set_bound(T_OP_EQ, point);
  return range_.is_superset(point_range) ? 1.0 : 0.0;
}

int ObSelEstimatorFactory::create_estimator(const OptSelectivityCtx &ctx,
                                            const ObRawExpr *expr,
                                            ObSelEstimator *&new_estimator)
{
  int ret = OB_SUCCESS;
  new_estimator = NULL;
  /*
  * The ordering to create the estimator is important
  */
  static const CreateEstimatorFunc create_estimator_funcs[] =
  {
    ObSimpleJoinSelEstimator::create_estimator,
    ObRangeSelEstimator::create_estimator,
    ObInequalJoinSelEstimator::create_estimator,
    ObAggSelEstimator::create_estimator,
    ObConstSelEstimator::create_estimator,
    ObColumnSelEstimator::create_estimator,
    ObEqualSelEstimator::create_estimator,
    ObLikeSelEstimator::create_estimator,
    ObBoolOpSelEstimator::create_estimator,
    ObInSelEstimator::create_estimator,
    ObIsSelEstimator::create_estimator,
    ObUniformRangeSelEstimator::create_estimator,
    ObCmpSelEstimator::create_estimator,
    ObBtwSelEstimator::create_estimator,
    ObDefaultSelEstimator::create_estimator,
  };
  static const int64_t func_cnt = sizeof(create_estimator_funcs)/sizeof(CreateEstimatorFunc);
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", KPC(expr));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(expr, expr))) {
    LOG_WARN("failed to get lossless cast expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && NULL == new_estimator && i < func_cnt; i ++) {
    if (OB_FAIL(create_estimator_funcs[i](*this, ctx, *expr, new_estimator))) {
      LOG_WARN("failed to create estimator", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(new_estimator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create estimator", KPC(new_estimator), KPC(expr));
  }
  LOG_DEBUG("succeed to create estimator", KPC(new_estimator));
  return ret;
}

int ObSelEstimatorFactory::create_estimators(const OptSelectivityCtx &ctx,
                                             ObIArray<ObRawExpr *> &exprs,
                                             ObIArray<ObSelEstimator *> &estimators)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i ++) {
    ObSelEstimator *estimator = NULL;
    if (OB_FAIL(create_estimator(ctx, exprs.at(i), estimator))) {
      LOG_WARN("failed to create estimator", K(ret));
    } else if (OB_FAIL(ObSelEstimator::append_estimators(estimators, estimator))) {
      LOG_WARN("failed to append estimators", K(ret));
    }
  }
  return ret;
}

int ObEqualSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                          const OptSelectivityCtx &ctx,
                                          const ObRawExpr &expr,
                                          ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_simple_estimator<ObEqualSelEstimator>(factory, ctx, expr, estimator))) {
    LOG_WARN("failed to create simple estimator", K(ret));
  } else if (OB_ISNULL(estimator)) {
    //do nothing
  } else if (OB_UNLIKELY(expr.get_param_count() != 2) ||
             OB_ISNULL(expr.get_param_expr(0)) ||
             OB_ISNULL(expr.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(expr));
  } else if (OB_FAIL(check_can_calc_sel(*expr.get_param_expr(0),
                                        *expr.get_param_expr(1),
                                        static_cast<ObEqualSelEstimator*>(estimator)->can_calc_sel_))) {
    LOG_WARN("failed to check can calc sel", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObEqualSelEstimator::check_can_calc_sel(const ObRawExpr &l_expr,
                                            const ObRawExpr &r_expr,
                                            bool &can_calc_sel)
{
  int ret = OB_SUCCESS;
  can_calc_sel = true;
  if (T_OP_ROW == l_expr.get_expr_type() && T_OP_ROW == r_expr.get_expr_type()) {
    //row compare row
    const ObRawExpr *l_param = NULL;
    const ObRawExpr *r_param = NULL;
    const ObRawExpr *l_row = &l_expr;
    const ObRawExpr *r_row = &r_expr;
    if (l_expr.get_param_count() == 1 && OB_NOT_NULL(l_expr.get_param_expr(0)) &&
        T_OP_ROW == l_expr.get_param_expr(0)->get_expr_type()) {
      l_row = l_expr.get_param_expr(0);
    }
    if (r_expr.get_param_count() == 1 && OB_NOT_NULL(r_expr.get_param_expr(0)) &&
        T_OP_ROW == r_expr.get_param_expr(0)->get_expr_type()) {
      r_row = r_expr.get_param_expr(0);
    }
    if (OB_UNLIKELY(l_row->get_param_count() != r_row->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", KPC(l_row), KPC(r_row), K(ret));
    } else {
      int64_t num = l_row->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && can_calc_sel && i < num; ++i) {
        if (OB_ISNULL(l_param = l_row->get_param_expr(i)) ||
            OB_ISNULL(r_param = r_row->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null expr", K(ret), K(l_row), K(r_row), K(i));
        } else if (OB_FAIL(SMART_CALL(check_can_calc_sel(*l_param, *r_param, can_calc_sel)))) {
          LOG_WARN("failed to check can calc sel", K(ret));
        }
      }
    }
  } else if ((l_expr.has_flag(CNT_COLUMN) && !r_expr.has_flag(CNT_COLUMN)) ||
              (!l_expr.has_flag(CNT_COLUMN) && r_expr.has_flag(CNT_COLUMN))) {
    //column compare const
    const ObRawExpr *cnt_col_expr = l_expr.has_flag(CNT_COLUMN) ? &l_expr : &r_expr;
    ObSEArray<const ObColumnRefRawExpr *, 2> column_exprs;
    bool only_monotonic_op = true;
    if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(cnt_col_expr))) {
      LOG_WARN("failed to remove ignorable function", K(ret));
    } else if (cnt_col_expr->is_column_ref_expr()) {
      //do nothing
    } else if (OB_FAIL(ObOptEstUtils::extract_column_exprs_with_op_check(cnt_col_expr,
                                                                          column_exprs,
                                                                          only_monotonic_op))) {
      LOG_WARN("failed to extract column exprs with op check", K(ret));
    } else if (!only_monotonic_op || column_exprs.count() > 1) {
      can_calc_sel= false;
    } else {/*do nothing*/}
  } else if (l_expr.has_flag(CNT_COLUMN) && r_expr.has_flag(CNT_COLUMN)) {
    //column compare column
    const ObRawExpr* left_expr = &l_expr;
    const ObRawExpr* right_expr = &r_expr;
    if (left_expr->get_relation_ids() != right_expr->get_relation_ids()) {
      //do noting, not same table, dynamic sampling not support join.
    } else if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(left_expr)) ||
               OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(right_expr))) {
      LOG_WARN("failed to remove ignorable function", K(ret));
    } else if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(left_expr), K(right_expr));
    } else if (left_expr->is_column_ref_expr() && right_expr->is_column_ref_expr()) {
      //do nothing
    } else {// func(col) = func(col) or col = func(col)
      can_calc_sel = false;
    }
  } else {
    //const compare const
    //do nothing
  }
  return ret;
}

int ObIsSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                       const OptSelectivityCtx &ctx,
                                       const ObRawExpr &expr,
                                       ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_simple_estimator<ObIsSelEstimator>(factory, ctx, expr, estimator))) {
    LOG_WARN("failed to create simple estimator", K(ret));
  } else if (OB_ISNULL(estimator)) {
    //do nothing
  } else if (OB_UNLIKELY(expr.get_param_count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(expr));
  } else {
    const ParamStore *params = ctx.get_params();
    const ObDMLStmt *stmt = ctx.get_stmt();
    const ObRawExpr *left_expr = expr.get_param_expr(0);
    const ObRawExpr *right_expr = expr.get_param_expr(1);
    bool got_result = false;
    if (OB_ISNULL(params) || OB_ISNULL(stmt) || OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpect null", K(ret), K(params), K(stmt), K(left_expr), K(right_expr));
    } else if (OB_UNLIKELY(!ObOptEstUtils::is_calculable_expr(*right_expr, params->count()))) {
      //do nothing
    } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                                 right_expr,
                                                                 static_cast<ObIsSelEstimator*>(estimator)->right_const_obj_,
                                                                 got_result,
                                                                 ctx.get_allocator()))) {
      LOG_WARN("failed to calculate const or calculable expr", K(ret));
    } else if (!got_result) {
      // do nothing
    } else if (OB_FAIL(ObOptSelectivity::remove_ignorable_func_for_est_sel(left_expr))) {
      LOG_WARN("failed to remove ignorable func", KPC(left_expr));
    } else if (left_expr->is_column_ref_expr()) {
      if (OB_FAIL(ObOptSelectivity::check_column_in_current_level_stmt(stmt, *left_expr))) {
        LOG_WARN("Failed to check column whether is in current stmt", K(ret));
      } else if (static_cast<ObIsSelEstimator*>(estimator)->right_const_obj_.is_null() ||
                 (static_cast<ObIsSelEstimator*>(estimator)->right_const_obj_.is_tinyint() &&
                  !ob_is_string_or_lob_type(left_expr->get_data_type()))) {
        static_cast<ObIsSelEstimator*>(estimator)->can_calc_sel_ = true;
        static_cast<ObIsSelEstimator*>(estimator)->left_expr_ = left_expr;
      }
    }
  }
  return ret;
}

int ObCmpSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                        const OptSelectivityCtx &ctx,
                                        const ObRawExpr &expr,
                                        ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_simple_estimator<ObCmpSelEstimator>(factory, ctx, expr, estimator))) {
    LOG_WARN("failed to create simple estimator", K(ret));
  } else if (OB_ISNULL(estimator)) {
    //do nothing
  } else if (OB_UNLIKELY(expr.get_param_count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(expr));
  } else {
    const ObRawExpr *left_expr = expr.get_param_expr(0);
    const ObRawExpr *right_expr = expr.get_param_expr(1);
    ObCmpSelEstimator *cmp_estimator = static_cast<ObCmpSelEstimator*>(estimator);
    if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret), K(left_expr), K(right_expr));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(left_expr, left_expr)) ||
               OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(right_expr, right_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if ((left_expr->is_column_ref_expr() && right_expr->is_const_expr()) ||
               (left_expr->is_const_expr() && right_expr->is_column_ref_expr())) {
      cmp_estimator->can_calc_sel_ = true;
      cmp_estimator->col_expr_ = left_expr->is_column_ref_expr() ? static_cast<const ObColumnRefRawExpr*>(left_expr) :
                                                                   static_cast<const ObColumnRefRawExpr*>(right_expr);
    } else if (T_OP_ROW == left_expr->get_expr_type() && T_OP_ROW == right_expr->get_expr_type()) {
      //only deal (col1, xx, xx) CMP (const, xx, xx)
      if (left_expr->get_param_count() == 1 && OB_NOT_NULL(left_expr->get_param_expr(0)) &&
          T_OP_ROW == left_expr->get_param_expr(0)->get_expr_type()) {
        left_expr = left_expr->get_param_expr(0);
      }
      if (right_expr->get_param_count() == 1 && OB_NOT_NULL(right_expr->get_param_expr(0)) &&
          T_OP_ROW == right_expr->get_param_expr(0)->get_expr_type()) {
        right_expr = right_expr->get_param_expr(0);
      }
      if (left_expr->get_param_count() != right_expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param count should be equal",
                    K(left_expr->get_param_count()), K(right_expr->get_param_count()));
      } else if (left_expr->get_param_count() <= 1) {
        // do nothing
      } else if (OB_ISNULL(left_expr = left_expr->get_param_expr(0)) ||
                OB_ISNULL(right_expr = right_expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(left_expr), K(right_expr));
      } else if ((left_expr->is_column_ref_expr() && right_expr->is_const_expr()) ||
                 (left_expr->is_const_expr() && right_expr->is_column_ref_expr())) {
        cmp_estimator->can_calc_sel_ = true;
        cmp_estimator->col_expr_ = left_expr->is_column_ref_expr() ? static_cast<const ObColumnRefRawExpr*>(left_expr) :
                                                                     static_cast<const ObColumnRefRawExpr*>(right_expr);
      } else { /* no dothing */ }
    }
    if (OB_SUCC(ret) && NULL != cmp_estimator->col_expr_) {
      estimator->set_eigen_expr(cmp_estimator->col_expr_);
    }
  }
  return ret;
}

int ObBtwSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                        const OptSelectivityCtx &ctx,
                                        const ObRawExpr &expr,
                                        ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_simple_estimator<ObBtwSelEstimator>(factory, ctx, expr, estimator))) {
    LOG_WARN("failed to create simple estimator", K(ret));
  } else if (OB_ISNULL(estimator)) {
    //do nothing
  } else {
    const ObRawExpr *cmp_expr = NULL;
    const ObRawExpr *l_expr = NULL;
    const ObRawExpr *r_expr = NULL;
    const ObRawExpr *col_expr = NULL;
    const ParamStore *params = ctx.get_params();
    if (3 != expr.get_param_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("between expr should have 3 param", K(ret), K(expr));
    } else if (OB_ISNULL(params) ||
              OB_ISNULL(cmp_expr = expr.get_param_expr(0)) ||
              OB_ISNULL(l_expr = expr.get_param_expr(1)) ||
              OB_ISNULL(r_expr = expr.get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null params", K(ret), K(params), K(cmp_expr), K(l_expr), K(r_expr));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(cmp_expr, cmp_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (cmp_expr->is_column_ref_expr() &&
               ObOptEstUtils::is_calculable_expr(*l_expr, params->count()) &&
               ObOptEstUtils::is_calculable_expr(*r_expr, params->count())) {
      static_cast<ObBtwSelEstimator*>(estimator)->can_calc_sel_ = true;
      static_cast<ObBtwSelEstimator*>(estimator)->col_expr_ = static_cast<const ObColumnRefRawExpr*>(cmp_expr);
    } else if (ObOptEstUtils::is_calculable_expr(*cmp_expr, params->count()) &&
               l_expr->is_column_ref_expr() &&
               ObOptEstUtils::is_calculable_expr(*r_expr, params->count())) {
      static_cast<ObBtwSelEstimator*>(estimator)->can_calc_sel_ = true;
      static_cast<ObBtwSelEstimator*>(estimator)->col_expr_ = static_cast<const ObColumnRefRawExpr*>(l_expr);
    } else if (ObOptEstUtils::is_calculable_expr(*cmp_expr, params->count()) &&
               ObOptEstUtils::is_calculable_expr(*l_expr, params->count()) &&
               r_expr->is_column_ref_expr()) {
      static_cast<ObBtwSelEstimator*>(estimator)->can_calc_sel_ = true;
      static_cast<ObBtwSelEstimator*>(estimator)->col_expr_ = static_cast<const ObColumnRefRawExpr*>(r_expr);
    }
  }
  return ret;
}

int ObNormalRangeSelEstimator::get_expr_range(const OptSelectivityCtx &ctx,
                                              const ObRawExpr &qual,
                                              const ObRawExpr *&expr,
                                              SimpleRange &range,
                                              bool &is_not_op,
                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  is_not_op = false;
  expr = NULL;
  const ObRawExpr *const_expr1 = NULL;
  const ObRawExpr *const_expr2 = NULL;
  ObObj const_value1;
  ObObj const_value2;
  bool got_result = false;
  range.set_whole_range();
  ObItemType type = qual.get_expr_type();
  if (OB_FAIL(ObOptEstUtils::extract_var_op_const(&qual,
                                                  expr,
                                                  const_expr1,
                                                  const_expr2,
                                                  type,
                                                  is_valid))) {
    LOG_WARN("failed to extract var and const", K(ret), K(qual));
  } else if (!is_valid) {
    // do nothing
  } else if (NULL == const_expr1 || !const_expr1->is_static_scalar_const_expr()) {
    is_valid = false;
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                               const_expr1,
                                                               const_value1,
                                                               got_result,
                                                               ctx.get_allocator()))) {
    LOG_WARN("failed to calc const value", K(expr), K(ret));
  } else if (!got_result) {
    is_valid = false;
  } else if (NULL == const_expr2 || !const_expr2->is_static_scalar_const_expr()) {
    // do nothing
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                               const_expr2,
                                                               const_value2,
                                                               got_result,
                                                               ctx.get_allocator()))) {
    LOG_WARN("failed to calc const value", K(expr), K(ret));
  } else if (!got_result) {
    is_valid = false;
  }
  if (OB_SUCC(ret) && is_valid) {
    if (IS_RANGE_CMP_OP(type) || T_OP_EQ == type || T_OP_NSEQ == type) {
      range.set_bound(type, const_value1);
    } else if (T_OP_NE == type) {
      range.set_bound(T_OP_EQ, const_value1);
      is_not_op = true;
    } else if (T_OP_IS_NOT == type || T_OP_IS == type) {
      if (const_value1.is_null()) {
        range.set_bound(type, const_value1);
      } else {
        is_valid = false;
      }
    } else if (T_OP_BTW == type || T_OP_NOT_BTW == type) {
      range.set_bound(T_OP_GE, const_value1);
      range.set_bound(T_OP_LE, const_value2);
      is_not_op = (T_OP_NOT_BTW == type);
    }
  }
  return ret;
}

int ObNormalRangeSelEstimator::merge(const ObSelEstimator &other_estmator, bool &is_success)
{
  int ret = OB_SUCCESS;
  is_success = false;
  if (get_type() == other_estmator.get_type() && !is_not_op_) {
    const ObNormalRangeSelEstimator &other = static_cast<const ObNormalRangeSelEstimator &>(other_estmator);
    if (!other.is_not_op_ && expr_ == other.expr_ && range_.intersect(other.range_)) {
      is_success = true;
    }
  }
  return ret;
}

int ObUniformRangeSelEstimator::create_estimator(ObSelEstimatorFactory &factory,
                                                const OptSelectivityCtx &ctx,
                                                const ObRawExpr &expr,
                                                ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  estimator = NULL;
  ObUniformRangeSelEstimator *range_estimator = NULL;
  bool is_valid = false;
  const ObRawExpr *param_expr = NULL;
  SimpleRange range;
  bool is_not_op = false;
  if (!ctx.check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                    COMPAT_VERSION_4_3_3)) {
    // do nothing
  } else if (OB_FAIL(get_expr_range(ctx, expr, param_expr, range, is_not_op, is_valid))) {
    LOG_WARN("failed to get the range form", K(ret), K(expr));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(factory.create_estimator_inner(range_estimator))) {
    LOG_WARN("failed to create estimator", K(ret));
  } else {
    estimator = range_estimator;
    range_estimator->expr_ = param_expr;
    range_estimator->range_ = range;
    range_estimator->is_not_op_ = is_not_op;
    range_estimator->eigen_expr_ = param_expr;
  }
  return ret;
}

int ObUniformRangeSelEstimator::get_sel(const OptTableMetas &table_metas,
                                        const OptSelectivityCtx &ctx,
                                        double &selectivity,
                                        ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_INEQ_SEL;
  ObObj expr_min;
  ObObj expr_max;
  ObObj min_scalar;
  ObObj max_scalar;
  ObObj start_scalar;
  ObObj end_scalar;
  expr_min.set_min_value();
  expr_max.set_max_value();
  double ndv = 1.0;
  double not_null_sel = 1.0; // todo
  bool dummy = false;
  bool discrete = (expr_->get_type_class() != ObFloatTC) && (expr_->get_type_class() != ObDoubleTC);
  ObBorderFlag border_flag;
  if (range_.inclusive_start_){
    border_flag.set_inclusive_start();
  }
  if (range_.inclusive_end_) {
    border_flag.set_inclusive_end();
  }
  if (!range_.is_valid_range()) {
    selectivity = 0.0;
  } else if (OB_FAIL(ObOptSelectivity::calc_expr_min_max(table_metas, ctx, expr_,
                                                         expr_min, expr_max))) {
    LOG_WARN("failed to get min max", K(ret));
  } else if (expr_min.is_min_value() || expr_min.is_max_value() || expr_min.is_null() ||
             expr_max.is_min_value() || expr_max.is_max_value() || expr_max.is_null()) {
    // do nothing
  } else if (OB_UNLIKELY(!expr_min.can_compare(expr_max)) ||
             OB_UNLIKELY(!expr_min.can_compare(range_.start_)) ||
             OB_UNLIKELY(!expr_min.can_compare(range_.end_))) {
    // ignore
  } else if (OB_FAIL(ObOptEstObjToScalar::convert_objs_to_scalars(&expr_min, &expr_max,
                                                                  &range_.start_, &range_.end_,
                                                                  &min_scalar, &max_scalar,
                                                                  &start_scalar, &end_scalar))) {
    LOG_WARN("failed to convert obj to scalars", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(table_metas, ctx, *expr_, ctx.get_current_rows(), ndv))) {
    LOG_WARN("failed to calculate distinct", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::do_calc_range_selectivity(min_scalar.get_double(),
                                                                 max_scalar.get_double(),
                                                                 start_scalar,
                                                                 end_scalar,
                                                                 ndv,
                                                                 discrete,
                                                                 border_flag,
                                                                 dummy,
                                                                 selectivity))) {
    LOG_WARN("failed to do calc range selectivity", K(ret));
  } else if (!is_not_op_ &&
             OB_FAIL(refine_out_of_bounds_sel(table_metas,
                                              ctx,
                                              expr_min,
                                              expr_max,
                                              min_scalar.get_double(),
                                              max_scalar.get_double(),
                                              start_scalar.get_double(),
                                              end_scalar.get_double(),
                                              selectivity))) {
    LOG_WARN("failed to refine out of bounds sel", K(ret));
  } else {
    if (is_not_op_) {
      selectivity = 1 - selectivity;
    }
    selectivity = std::max(selectivity, 1.0 / ndv);
    selectivity *= not_null_sel;
  }
  LOG_DEBUG("succeed to calculate uniform range sel",
      K(selectivity), K(discrete), K(expr_min), K(expr_max), K(range_), K(not_null_sel), K(ndv), KPC(expr_));
  return ret;
}

int ObUniformRangeSelEstimator::refine_out_of_bounds_sel(const OptTableMetas &table_metas,
                                                         const OptSelectivityCtx &ctx,
                                                         const ObObj &min_val,
                                                         const ObObj &max_val,
                                                         const double min_scalar,
                                                         const double max_scalar,
                                                         const double start_scalar,
                                                         const double end_scalar,
                                                         double &selectivity)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 1> column_exprs;
  const OptTableMeta *table_meta = NULL;
  double increase_rows_ratio = 0.0;
  bool is_half = range_.start_.is_min_value() || range_.end_.is_max_value();
  double out_of_bounds_sel = 0.0;
  bool need_calc = true;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KPC(this));
  } else if (expr_->get_relation_ids().num_members() != 1 ||
             min_val.is_min_value() || max_val.is_min_value() ||
             min_val.is_max_value() || max_val.is_max_value() ||
             (max_scalar - min_scalar < OB_DOUBLE_EPSINON) ||
             fabs(selectivity - 1.0) < OB_DOUBLE_EPSINON ||
             !range_.is_valid_range()) {
    need_calc = false;
  } else if (is_half) {
    need_calc = true;
  } else if (start_scalar >= min_scalar && end_scalar <= max_scalar) {
    need_calc = false;
  } else if (start_scalar <= min_scalar && end_scalar >= max_scalar) {
    need_calc = false;
    selectivity = 1.0;
  } else if (start_scalar < min_scalar) {
    need_calc = true;
    out_of_bounds_sel = (std::min(min_scalar, end_scalar) - start_scalar) / (max_scalar - min_scalar);
  } else if (end_scalar > max_scalar) {
    need_calc = true;
    out_of_bounds_sel = (end_scalar - std::max(max_scalar, start_scalar)) / (max_scalar - min_scalar);
  }
  if (OB_FAIL(ret) || !need_calc) {
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr_, column_exprs))) {
    LOG_WARN("extract_column_exprs error in clause_selectivity", K(ret));
  } else if (OB_UNLIKELY(column_exprs.count() < 1) || OB_ISNULL(column_exprs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count", KPC(expr_), K(column_exprs));
  } else if (FALSE_IT(table_meta = table_metas.get_table_meta_by_table_id(
      static_cast<ObColumnRefRawExpr *>(column_exprs.at(0))->get_table_id()))) {
  } else if (NULL == table_meta) {
    // do nothing
  } else if (OB_FAIL(table_meta->get_increase_rows_ratio(ctx.get_opt_ctx(), increase_rows_ratio))) {
    LOG_WARN("failed to get extra rows", K(ret));
  } else if (is_half) {
    selectivity = std::max(selectivity, DEFAULT_OUT_OF_BOUNDS_SEL * increase_rows_ratio);
  } else {
    selectivity += std::min(out_of_bounds_sel, increase_rows_ratio);
  }
  selectivity = ObOptSelectivity::revise_between_0_1(selectivity);
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
