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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_standard_group_checker.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_any_value_checker.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
int ObStandardGroupChecker::add_group_by_expr(const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (OB_HASH_EXIST == (ret = settled_columns_.exist_refactored(reinterpret_cast<int64_t>(expr)))) {
    //ignore
    ret = OB_SUCCESS;
  } else if (OB_HASH_NOT_EXIST == ret) {
    //add to columns
    if (OB_FAIL(settled_columns_.set_refactored(reinterpret_cast<int64_t>(expr)))) {
      LOG_WARN("set expr to settled columns failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && !expr->is_column_ref_expr()) {
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < settled_exprs_.count(); ++i) {
      if (settled_exprs_.at(i) == expr) {
        is_found = true;
        break;
      }
    }
    if (!is_found && OB_FAIL(settled_exprs_.push_back(expr))) {
      LOG_WARN("add expr to settled exprs failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    //this stmt has group
    set_has_group(true);
  }
  return ret;
}

int ObStandardGroupChecker::add_unsettled_column(const ObRawExpr *column_ref)
{
  int ret = OB_SUCCESS;
  if (column_ref->has_generalized_column()) {
    if (OB_FAIL(dependent_columns_.push_back(column_ref))) {
      LOG_WARN("add column to dependent columns failed", K(ret));
    }
  }
  return ret;
}

int ObStandardGroupChecker::add_unsettled_expr(const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (expr->has_generalized_column()) {
    ObUnsettledExprItem unsettled_expr_item;
    unsettled_expr_item.expr_ = expr;
    if (expr->has_flag(CNT_AGG)) {
      set_has_group(true);
    }
    if (unsettled_exprs_.count() <= 0) {
      unsettled_expr_item.start_idx_ = 0;
      unsettled_expr_item.dependent_column_cnt_ = dependent_columns_.count();
    } else {
      ObUnsettledExprItem &last_unsettled_expr = unsettled_exprs_.at(unsettled_exprs_.count() - 1);
      unsettled_expr_item.start_idx_ = last_unsettled_expr.start_idx_ + last_unsettled_expr.dependent_column_cnt_;
      unsettled_expr_item.dependent_column_cnt_ = dependent_columns_.count() - unsettled_expr_item.start_idx_;
    }
    if (OB_FAIL(unsettled_exprs_.push_back(unsettled_expr_item))) {
      LOG_WARN("add unsettled exprs failed", K(ret));
    }
  }
  return ret;
}

int ObStandardGroupChecker::check_only_full_group_by()
{
  int ret = OB_SUCCESS;
  const ObColumnRefRawExpr *undefined_column = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && has_group_ && i < unsettled_exprs_.count(); ++i) {
    //not check aggregate function or column in aggregate function, such as select count(c1) from t1 group by c2;
    //but if expr has column outside of aggregate function, we only check the column not in aggregate function
    //such as: select c1+count(c2) from t1 group by c1;
    const ObUnsettledExprItem &unsettled_expr = unsettled_exprs_.at(i);
    common::ObArray<const ObColumnRefRawExpr*> undefined_columns;
    for (int64_t j = unsettled_expr.start_idx_;
        OB_SUCC(ret) && j < unsettled_expr.start_idx_ + unsettled_expr.dependent_column_cnt_; ++j) {
      const ObRawExpr *unsettled_column = NULL;
      if (j < 0 && j > dependent_columns_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsettled column index is invalid", K(j), K(dependent_columns_.count()));
      } else if (OB_ISNULL(unsettled_column = dependent_columns_.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsettled column is null");
      } else if (OB_FAIL(check_unsettled_column(unsettled_column, undefined_column))) {
        LOG_WARN("check unsettled column failed", K(ret));
      } else if (undefined_column != NULL) {
        //当前stmt中存在undefined的列，根据only full group by语义，如果一个表达式中所有列都是defined，那么该表达式是defined
        //如果所有列不能保证defined，需要看看表达式本身是否是defined
        //例如：select c1+c2 from t1 group by c1, c1+c2
        //c1是defined列，c2不是，c1+c2整体是defined，所以这条语句满足only full group by
        undefined_columns.push_back(undefined_column);
        undefined_column = NULL;
      }
    }
   if (OB_SUCC(ret) && 0 != undefined_columns.size()) {
      //has undefined column, must check expr whether defined in group by or in any_value
      for (int64_t i = 0; OB_SUCC(ret) && i < undefined_columns.size(); ++i) {
        if (OB_FAIL(check_unsettled_expr(unsettled_expr.expr_, *(undefined_columns.at(i))))) {
          LOG_WARN("check unsettled expr failed", K(ret));
          break;
        }
      }
    }
    if (OB_SUCC(ret) && !unsettled_expr.expr_->has_flag(CNT_AGG)) {
      //当前的表达式满足only full group by语义，所以将其加入到settled columns中，作为下次检查的参考列
      if (OB_FAIL(settled_columns_.set_refactored(reinterpret_cast<int64_t>(unsettled_expr.expr_)))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("add settled expr to settled columns failed", K(ret));
        }
      }
    }
    undefined_column = NULL;
  }
  //本次检查结束
  //无论 被检查的表达式是否满足only full group by语义，
  //都需要清空待检查表达式和待检查列的缓存，为下次检查做准备
  unsettled_exprs_.reset();
  dependent_columns_.reset();
  return ret;
}

int ObStandardGroupChecker::check_unsettled_column(const ObRawExpr *unsettled_column,
                                                   const ObColumnRefRawExpr *&undefined_column)
{
  int ret = OB_SUCCESS;
  undefined_column = NULL;
  if (OB_HASH_EXIST == (ret = settled_columns_.exist_refactored(reinterpret_cast<int64_t>(unsettled_column)))) {
    ret = OB_SUCCESS;
    //continue to check next column
  } else if (OB_HASH_NOT_EXIST == ret) {
    if (!unsettled_column->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsettled column isn't column reference expr", K(*unsettled_column));
    } else {
      //mark undefined column index in unsettled expr after group by
      undefined_column = static_cast<const ObColumnRefRawExpr*>(unsettled_column);
      ret = OB_SUCCESS;
    }
  } else {
    LOG_WARN("check column in settled columns failed", K(ret));
  }
  return ret;
}

int ObStandardGroupChecker::check_unsettled_expr(const ObRawExpr *unsettled_expr, const ObColumnRefRawExpr &undefined_column)
{
  int ret = OB_SUCCESS;
  bool scalar_aggr = settled_columns_.size() <= 0 && has_group_;
  if (OB_ISNULL(unsettled_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsettled expr is null");
  } else if (scalar_aggr) {
    //当前语句包含聚集但是却没有group by column，所以是scalar aggregate
    //对于scalar group by, mysql不允许聚集函数和单列混用
    //such as select c1+min(c1) from t1
    //select c2+max(c1) from t1
    ret = OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS;
  } else if (unsettled_expr->has_flag(CNT_AGG) || unsettled_expr == &undefined_column) {
    //not check aggregate function or column in aggregate function, such as select count(c1) from t1 group by c2;
    //but if expr has column outside of aggregate function, we only check the column not in aggregate function
    //such as: select c1+count(c2) from t1 group by c1;
    ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
    LOG_DEBUG("column not in group by", K(*unsettled_expr), K(undefined_column));
  } else if (OB_HASH_EXIST == (ret = settled_columns_.exist_refactored(reinterpret_cast<int64_t>(unsettled_expr)))) {
    //this expr satisfy the only full group by semantic constraints
    ret = OB_SUCCESS;
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    bool is_defined = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < settled_exprs_.count(); ++i) {
      if (OB_ISNULL(settled_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("settled expr is null");
      } else if (unsettled_expr->same_as(*settled_exprs_.at(i))) {
        is_defined = true;
        break;
      }
    }
    if (OB_SUCC(ret) && !is_defined) {
      ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
    }
  } else {
    LOG_WARN("check unsettled expr failed", K(ret));
  }

  if (OB_ERR_WRONG_FIELD_WITH_GROUP == ret || OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS == ret) {
    int tmp_ret = ret;
    ObAnyValueChecker any_value_checker;
    if (OB_FAIL(any_value_checker.check_any_value(unsettled_expr, &undefined_column))) {
      LOG_WARN("check any value expr fail", K(ret));
    } else {
      if (any_value_checker.is_pass_after_check()) {
        ret = OB_SUCCESS;
      } else {
        ret = tmp_ret;
        if (OB_ERR_WRONG_FIELD_WITH_GROUP == ret) {
          ObString column_name = concat_qualified_name(undefined_column.get_database_name(),
                                                    undefined_column.get_table_name(),
                                                    undefined_column.get_column_name());
          LOG_USER_ERROR(OB_ERR_WRONG_FIELD_WITH_GROUP, column_name.length(), column_name.ptr());
        }
      }
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
