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

#include "sql/engine/expr/ob_expr_bool.h"

namespace oceanbase {
namespace sql {

using namespace oceanbase::common;

ObExprBool::ObExprBool(ObIAllocator& alloc) : ObLogicalExprOperator(alloc, T_OP_BOOL, N_BOOL, 1, NOT_ROW_DIMENSION)
{}

ObExprBool::~ObExprBool()
{}

int ObExprBool::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (!lib::is_mysql_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bool expr is only for mysql mode", K(ret));
  } else if (ob_is_numeric_type(type1.get_type())) {
    type1.set_calc_meta(type1.get_obj_meta());
    type1.set_calc_accuracy(type1.get_accuracy());
  } else {
    const ObObjType& calc_type = ObDoubleType;
    type1.set_calc_type(calc_type);
    const ObAccuracy& calc_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][calc_type];
    type1.set_calc_accuracy(calc_acc);
  }
  const ObAccuracy& res_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObInt32Type];
  type.set_type(ObInt32Type);
  type.set_accuracy(res_acc);
  ObExprOperator::calc_result_flag1(type, type1);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NO_RANGE_CHECK);
  return ret;
}

#define CHECK_IS_TRUE_FUNC_NAME(type) \
  int calc_bool_expr_for_##type(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)

#define EVAL_ARG()                                      \
  int ret = OB_SUCCESS;                                 \
  ObDatum* child_datum = NULL;                          \
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_datum))) { \
    LOG_WARN("eval arg 0 failed", K(ret));              \
  } else if (child_datum->is_null()) {                  \
    res_datum.set_null();                               \
  } else

CHECK_IS_TRUE_FUNC_NAME(integer_type)
{
  EVAL_ARG()
  {
    int32_t res = (0 == child_datum->get_int()) ? 0 : 1;
    res_datum.set_int32(res);
  }
  return ret;
}

CHECK_IS_TRUE_FUNC_NAME(float_type)
{
  EVAL_ARG()
  {
    int32_t res = (0 == child_datum->get_float()) ? 0 : 1;
    res_datum.set_int32(res);
  }
  return ret;
}

CHECK_IS_TRUE_FUNC_NAME(double_type)
{
  EVAL_ARG()
  {
    int32_t res = (0 == child_datum->get_double()) ? 0 : 1;
    res_datum.set_int32(res);
  }
  return ret;
}

CHECK_IS_TRUE_FUNC_NAME(other_type)
{
  EVAL_ARG()
  {
    int32_t res = child_datum->get_number().is_zero() ? 0 : 1;
    res_datum.set_int32(res);
  }
  return ret;
}

int ObExprBool::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(false == lib::is_oracle_mode());
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg cnt is invalid or args_ is NULL", K(ret), K(rt_expr));
  } else {
    const ObDatumMeta child_res_meta = rt_expr.args_[0]->datum_meta_;
    switch (child_res_meta.type_) {
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType:
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type:
      case ObBitType: {
        rt_expr.eval_func_ = calc_bool_expr_for_integer_type;
        break;
      }
      case ObFloatType:
      case ObUFloatType: {
        rt_expr.eval_func_ = calc_bool_expr_for_float_type;
        break;
      }
      case ObDoubleType:
      case ObUDoubleType: {
        rt_expr.eval_func_ = calc_bool_expr_for_double_type;
        break;
      }
      case ObMaxType: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bool expr got unexpected type", K(ret), K(child_res_meta));
        break;
      }
      default: {
        rt_expr.eval_func_ = calc_bool_expr_for_other_type;
        break;
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
