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
#include "sql/engine/expr/ob_expr_xor.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "share/datum/ob_datum.h"
#include "share/config/ob_server_config.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {

const double ObExprXor::FLOAT_BOUND = 0.5;

ObExprXor::ObExprXor(ObIAllocator& alloc)
    : ObLogicalExprOperator(alloc, T_OP_XOR, N_XOR, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION)
{
  // not sure about the op name here
}

int ObExprXor::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(types_stack);
  UNUSED(param_num);
  int ret = OB_SUCCESS;
  // just keep enumset as origin
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  const ObSQLSessionInfo* session = dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null session", K(type_ctx.get_session()));
  } else if (session->use_static_typing_engine()) {
    for (int i = 0; i < param_num; i++) {
      types_stack[i].set_calc_type(ObDoubleType);
    }
  }
  return ret;
}

int ObExprXor::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  return calc(result, obj1, obj2, expr_ctx);
}

int ObExprXor::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(objs) || OB_UNLIKELY(1 >= param_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(objs), K(param_num));
  } else {
    ret = calc(result, objs[0], objs[1], expr_ctx);
    if (OB_SUCC(ret)) {
      for (int64_t idx = 2; idx < param_num && OB_SUCCESS == ret && !result.is_true(); ++idx) {
        ret = calc(result, objs[idx], result, expr_ctx);
      }
    }
  }
  return ret;
}

int ObExprXor::calc(ObObj& res, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.is_null())) {
    if (OB_UNLIKELY(right.is_null())) {
      res.set_null();
    } else {
      ret = cacl_res_with_one_param_null(res, left, right, expr_ctx);
    }
  } else {
    if (OB_UNLIKELY(right.is_null())) {
      ret = cacl_res_with_one_param_null(res, right, left, expr_ctx);
    } else {
      double lvalue = 0;
      double rvalue = 0;
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_DOUBLE_V2(left, lvalue);
      EXPR_GET_DOUBLE_V2(right, rvalue);

      // in MySQL, values in (-0.5, 0.5) are treated as zero,
      // other values are treated as non-zero
      bool bool_v1 = (fabs(lvalue) >= FLOAT_BOUND);
      bool bool_v2 = (fabs(rvalue) >= FLOAT_BOUND);
      res.set_bool(bool_v1 ^ bool_v2);
      //      if (OB_FAIL(ObLogicalExprOperator::is_true(left, expr_ctx.cast_mode_ | CM_NO_RANGE_CHECK, bool_v1))) {
      //        LOG_WARN("fail to evaluate left", K(left), K(ret));
      //      } else if (OB_FAIL(ObLogicalExprOperator::is_true(right, expr_ctx.cast_mode_ | CM_NO_RANGE_CHECK,
      //      bool_v2))) {
      //        LOG_WARN("fail to evaluate right", K(right), K(ret));
      //      } else {
      //        res.set_bool(bool_v1 ^ bool_v2);
      //      }
    }
  }
  return ret;
}

int ObExprXor::cacl_res_with_one_param_null(
    common::ObObj& res, const common::ObObj& left, const common::ObObj& right, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  if (OB_UNLIKELY((!left.is_null()) || right.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(left), K(right));
  } else {
    // xor returns null
    res.set_null();
  }
  return ret;
}

int ObExprXor::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);

  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 2 || rt_expr.type_ != T_OP_XOR)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    rt_expr.eval_func_ = &eval_xor;
  }
  return ret;
}

int ObExprXor::eval_xor(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr.args_) || OB_UNLIKELY(expr.arg_cnt_ < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    // xor supports short-circuit calculation, if there is null, return null directly
    ObDatum* param = NULL;
    bool found_null = false;
    bool cur_bool_v = false;
    if (OB_ISNULL(expr.args_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(expr.args_[0]));
    } else if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
      LOG_WARN("failed to eval", K(ret));
    } else if (OB_ISNULL(param)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param", K(ret), K(param));
    } else if (param->is_null()) {
      expr_datum.set_null();
      found_null = true;
    } else {
      // (-0.5, 0.5) are treated as zero
      cur_bool_v = (fabs(param->get_double()) >= FLOAT_BOUND);
    }

    for (int i = 1; OB_SUCC(ret) && !found_null && i < expr.arg_cnt_; i++) {
      if (OB_ISNULL(expr.args_[i])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(i));
      } else if (OB_FAIL(expr.args_[i]->eval(ctx, param))) {
        LOG_WARN("failed to eval param", K(ret));
      } else if (OB_ISNULL(param)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid param", K(ret), K(param));
      } else if (param->is_null()) {
        expr_datum.set_null();
        found_null = true;
      } else {
        cur_bool_v = cur_bool_v ^ ((fabs(param->get_double()) >= FLOAT_BOUND));
      }
    }
    if (OB_SUCC(ret) && !found_null) {
      expr_datum.set_int(cur_bool_v);
    }
  }
  return ret;
}

// int ObExprXor::eval_xor()

}  // namespace sql
}  // namespace oceanbase
