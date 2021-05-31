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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_or.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprOr::ObExprOr(ObIAllocator& alloc)
    : ObLogicalExprOperator(alloc, T_OP_OR, N_OR, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION)
{
  param_lazy_eval_ = true;
}

int ObExprOr::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  UNUSED(param_num);
  int ret = OB_SUCCESS;
  // just keep enumset as origin
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  return ret;
}

// expect obj1 and obj2 already evaluated
int ObExprOr::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  bool obj1_is_true = false;
  EXPR_SET_CAST_CTX_MODE(expr_ctx);
  if (OB_FAIL(ObLogicalExprOperator::is_true(obj1, expr_ctx.cast_mode_ | CM_NO_RANGE_CHECK, obj1_is_true))) {
    LOG_WARN("fail to evaluate obj1", K(obj1), K(ret));
  } else if (obj1_is_true) {
    result.set_int32(static_cast<int32_t>(true));
  } else {
    if (OB_UNLIKELY(obj1.is_null() && obj2.is_null())) {
      result.set_null();
    } else if (OB_UNLIKELY(obj1.is_null())) {
      ret = cacl_res_with_one_param_null(result, obj1, obj2, expr_ctx);
    } else if (OB_UNLIKELY(obj2.is_null())) {
      ret = cacl_res_with_one_param_null(result, obj2, obj1, expr_ctx);
    } else {
      // obj1 must be false here.
      bool bool_v2 = false;
      if (OB_FAIL(ObLogicalExprOperator::is_true(obj2, expr_ctx.cast_mode_ | CM_NO_RANGE_CHECK, bool_v2))) {
        LOG_WARN("fail to evaluate obj2", K(obj2), K(ret));
      } else {
        result.set_int32(static_cast<int32_t>(bool_v2));
      }
    }
  }
  return ret;
}

int ObExprOr::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(objs) || OB_UNLIKELY(1 >= param_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(objs), K(param_num));
  } else if (OB_FAIL(param_eval(expr_ctx, objs[0], 0))) {
    LOG_WARN("failed: eval objs[0]", K(ret));
  } else {
    result = objs[0];
    if (result.is_true()) {
      result.set_int32(static_cast<int32_t>(true));
    }
    for (int64_t idx = 1; idx < param_num && OB_SUCCESS == ret && !result.is_true(); ++idx) {
      if (OB_FAIL(param_eval(expr_ctx, objs[idx], idx))) {
        LOG_WARN("failed: eval objs[idx]", K(idx), K(ret));
      } else {
        ret = calc_result2(result, result, objs[idx], expr_ctx);
      }
    }
  }
  return ret;
}

int ObExprOr::cacl_res_with_one_param_null(
    common::ObObj& res, const common::ObObj& left, const common::ObObj& right, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((!left.is_null()) || right.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(left), K(right));
  } else {
    // left is null while right is not null. evaluate the right obj
    bool value = false;

    EXPR_SET_CAST_CTX_MODE(expr_ctx);
    if (OB_FAIL(ObObjEvaluator::is_true(right, expr_ctx.cast_mode_ | CM_NO_RANGE_CHECK, value))) {
      LOG_WARN("fail to evaluate obj1", K(right), K(ret));
      // ugly. but compatible with mysql.
      if (OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == ret) {
        ret = OB_SUCCESS;
        res.set_null();
      }
      // By design, compatible with mysql and oracle:
      // null or false == NULL. null or true == true.
      // see "NULL and the three-valued logic"
      // https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
    } else if (value) {
      res.set_int32(static_cast<int32_t>(true));
    } else {
      res.set_null();
    }
  }
  return ret;
}

// create table t1 as (select null or ''); -> NULL;
// create table t1 as (select 1 or ''); -> 1;
// create table t1 as (select 0 or ''); -> 0;
static int calc_with_one_empty_str(const ObDatum& in_datum, ObDatum& out_datum)
{
  int ret = OB_SUCCESS;
  if (in_datum.is_null()) {
    out_datum.set_null();
  } else {
    out_datum.set_bool(in_datum.get_bool());
  }
  return ret;
}

static int calc_or_expr2(const ObDatum& left, const ObDatum& right, const bool left_empty_str_cast_failed,
    const bool right_empty_str_cast_failed, ObDatum& res)
{
  int ret = OB_SUCCESS;
  if (left_empty_str_cast_failed) {
    calc_with_one_empty_str(right, res);
  } else if (right_empty_str_cast_failed) {
    calc_with_one_empty_str(left, res);
  } else {
    if (left.is_true() || right.is_true()) {
      res.set_true();
    } else if (left.is_null() || right.is_null()) {
      res.set_null();
    } else {
      res.set_false();
    }
  }
  return ret;
}

// Special treatment:
// 1. select null or''; -> or the result is 0
// Reason: For the select statement, cast_mode will automatically add WARN_ON_FAIL,
// so when the empty string is converted to number,
// The result is 0, the error code is overwritten, the above statement will return 0
// 2. create table t1 as (select null or''); -> the result of or is NULL
// Reason: For non-select/explain statements, cast_mode does not have WARN_ON_FAIL,
// so when the above empty string is converted to number,
// Report OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD, but in order to be compatible with MySQL,
// special processing will be carried out here,
// Overwrite the error code, and the result of or is NULL instead of 0
//
// The special processing for empty strings mentioned above is for MySQL,
// because the child nodes on both sides of or must be in Oracle mode
// An expression with Boolean semantics will not be an empty string directly
int calc_or_exprN(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* tmp_res = NULL;
  ObDatum* child_res = NULL;
  bool left_empty_str_cast_failed = false;
  bool right_empty_str_cast_failed = false;
  if (OB_FAIL(expr.args_[0]->eval(ctx, tmp_res))) {
    // An empty string will fail to cast to number, which requires special handling
    if (OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == ret) {
      ret = OB_SUCCESS;
      left_empty_str_cast_failed = true;
    } else {
      LOG_WARN("eval arg 0 failed", K(ret));
    }
  } else if (tmp_res->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_bool(tmp_res->get_bool());
  }
  if (OB_SUCC(ret) && !res_datum.is_true()) {
    for (int64_t i = 1; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (OB_FAIL(expr.args_[i]->eval(ctx, child_res))) {
        if (OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == ret) {
          ret = OB_SUCCESS;
          right_empty_str_cast_failed = true;
        } else {
          LOG_WARN("eval arg failed", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(calc_or_expr2(
                res_datum, *child_res, left_empty_str_cast_failed, right_empty_str_cast_failed, res_datum))) {
          LOG_WARN("calc_or_expr2 failed", K(ret), K(i));
        } else if (res_datum.is_true()) {
          break;
        } else {
          left_empty_str_cast_failed = false;
          right_empty_str_cast_failed = false;
        }
      }
    }
  }
  return ret;
}

int ObExprOr::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (OB_ISNULL(rt_expr.args_) || OB_UNLIKELY(2 > rt_expr.arg_cnt_) ||
      OB_UNLIKELY(rt_expr.arg_cnt_ != raw_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("args_ is NULL or arg_cnt_ is invalid or raw_expr is invalid", K(ret), K(rt_expr), K(raw_expr));
  } else {
    rt_expr.eval_func_ = calc_or_exprN;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
