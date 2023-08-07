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

#define USING_LOG_PREFIX  SQL_ENG

//#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/ob_sql_utils.h"
#include "lib/utility/utility.h"
#include "sql/engine/expr/ob_expr_interval.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprInterval::ObExprInterval(common::ObIAllocator &alloc) :
    ObExprOperator(alloc, T_FUN_SYS_INTERVAL, "interval", MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
    use_binary_search_(true)
{
}

OB_SERIALIZE_MEMBER((ObExprInterval, ObExprOperator), use_binary_search_);

int ObExprInterval::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprInterval *tmp_other = static_cast<const ObExprInterval *>(&other);
  if (other.get_type() != T_FUN_SYS_INTERVAL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (this != tmp_other) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(ret));
    } else {
      this->use_binary_search_ = tmp_other->use_binary_search_;
    }
  }
  return ret;
}

int ObExprInterval::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (!is_mysql_mode()) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("interval expr only exists in mysql mode", K(ret));
  } else if (OB_ISNULL(types) || param_num < 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(types), K(param_num), K(ret));
  } else if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_int();
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    //set calc type
    common::ObObjType calc_type = types[0].get_type();

    if (calc_type != ObNumberType && calc_type != ObUNumberType)
      calc_type = ObDoubleType;

    for (int64_t i = 0; i < param_num; ++i) {
      types[i].set_calc_type(calc_type);
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }

  return ret;
}

int ObExprInterval::calc_interval_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg0 = NULL;
  if (OB_UNLIKELY(2 > expr.arg_cnt_ || 1 != expr.inner_func_cnt_) ||
      OB_ISNULL(expr.inner_functions_) || OB_ISNULL(expr.inner_functions_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg0))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (arg0->is_null()) {
    res.set_int(-1);
  } else {
    ObDatumCmpFuncType cmp_func = reinterpret_cast<ObDatumCmpFuncType>(
                                    expr.inner_functions_[0]);
    bool use_binary_search = static_cast<bool>(expr.extra_);
    int cmp_ret = 0;
    if (!use_binary_search) {
      int64_t i = 1;
      for (; i < expr.arg_cnt_ && OB_SUCC(ret); ++i) {
        const ObDatum &arg_i = expr.locate_param_datum(ctx, static_cast<int>(i));
        if (arg_i.is_null()) {
          continue;
        } else if (OB_FAIL(cmp_func(arg_i, *arg0, cmp_ret))) {
          LOG_WARN("faile to compare", K(ret));
        } else if (cmp_ret > 0) {
          // if arg_i > *arg0 break
          break;
        }
      }
      if (OB_SUCC(ret)) {
        res.set_int(i - 1);
      }
    } else {
      int64_t high = expr.arg_cnt_ - 1;
      int64_t low = 1;
      while (low <= high && OB_SUCC(ret)) {
        int64_t mid = (low + high) / 2;
        const ObDatum &arg_i = expr.locate_param_datum(ctx, static_cast<int>(mid));
        if (OB_FAIL(cmp_func(arg_i, *arg0, cmp_ret))) {
          LOG_WARN("faile to compare", K(ret));
        } else if (cmp_ret <= 0) {
          low = mid + 1;
        } else if (low == high) {
          break;
        } else {
          high = mid - 1;
        }
      } // while
      if (OB_SUCC(ret)) {
        res.set_int(low - 1);
      }
    }
  }
  return ret;
}

int ObExprInterval::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_UNLIKELY(2 > rt_expr.arg_cnt_) || OB_ISNULL(expr_cg_ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt or allocator is NULL", K(ret), K(rt_expr.arg_cnt_),
                                                        KP(expr_cg_ctx.allocator_));
  } else if (OB_ISNULL(rt_expr.inner_functions_ =
        reinterpret_cast<void**>(expr_cg_ctx.allocator_->alloc(sizeof(void*))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem for inner func failed", K(ret));
  } else {
    // make sure all arg type is same
    for (int64_t i = 0; OB_SUCC(ret) && i < rt_expr.arg_cnt_; ++i) {
      const ObObjType &arg_type = rt_expr.args_[i]->datum_meta_.type_;
      if (!(ObNumberType == arg_type || ObUNumberType == arg_type ||
            ObDoubleType == arg_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("all type must be number type or double type", K(ret), K(arg_type), K(i));
      }
    }
    // checking input parameters:
    // if the number of input parameters is:
    //  1. more then 8 and
    //  2. all the parameters are const and
    //  3. not null,
    // we will do a binary search during calc
    // otherwise, use sequential search
    static const int64_t MYSQL_BINARY_SEARCH_BOUND = 8;
    bool use_binary_search = true;
    const ObSysFunRawExpr *sys_fun_expr = dynamic_cast<const ObSysFunRawExpr*>(&raw_expr);
    CK(OB_NOT_NULL(sys_fun_expr));
    if (OB_SUCC(ret) && rt_expr.arg_cnt_ > MYSQL_BINARY_SEARCH_BOUND) {
      for (int64_t i = 0; OB_SUCC(ret) && i < rt_expr.arg_cnt_; ++i) {
        if (OB_ISNULL(sys_fun_expr->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is NULL", K(ret), K(i), K(*sys_fun_expr));
        } else if (!sys_fun_expr->get_param_expr(i)->is_const_expr() ||
                   !sys_fun_expr->get_param_expr(i)->is_not_null_for_read()) {
          use_binary_search = false;
          break;
        }
      }
    } else {
      use_binary_search = false;
    }

    if (OB_SUCC(ret)) {
      const ObObjType &arg_type = rt_expr.args_[0]->datum_meta_.type_;
      // 数字比较不用考虑collation,
      // do not care NULL_FIRST or NULL_LAST, will ignore null in calc_interval_expr()
      rt_expr.inner_functions_[0] = reinterpret_cast<void*>(
          ObDatumFuncs::get_nullsafe_cmp_func(arg_type, arg_type, default_null_pos(),
                                              CS_TYPE_BINARY, rt_expr.args_[0]->datum_meta_.scale_, false,
                                              rt_expr.args_[0]->obj_meta_.has_lob_header()));
      if (OB_ISNULL(rt_expr.inner_functions_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cmp_func is NULL", K(ret), K(arg_type));
      } else {
        rt_expr.inner_func_cnt_ = 1;
        rt_expr.eval_func_ = calc_interval_expr;
        rt_expr.extra_ = static_cast<uint64_t>(use_binary_search);
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

