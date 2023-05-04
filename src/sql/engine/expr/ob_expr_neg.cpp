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
#include "sql/engine/expr/ob_expr_neg.h"
#include "share/object/ob_obj_cast.h"
#include "share/datum/ob_datum_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_base.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

template<ObObjTypeClass>
static int eval_neg_func(const ObExpr &expr,
                         ObEvalCtx &eval_ctx,
                         ObDatum &expr_datum)
{
  UNUSED(expr);
  UNUSED(eval_ctx);
  UNUSED(expr_datum);
  int ret = OB_ERR_INVALID_TYPE_FOR_OP;
  return ret;
}

static int check_expr_and_eval_param(const ObExpr &expr,
                                     ObEvalCtx &eval_ctx,
                                     ObDatum *&param_datum,
                                     bool &found_null)
{
  int ret = OB_SUCCESS;
  param_datum = NULL;
  found_null = false;
  if (OB_UNLIKELY(expr.type_ != T_OP_NEG) ||
      OB_ISNULL(expr.args_) ||
      OB_UNLIKELY(expr.arg_cnt_ != 1) ||
      OB_ISNULL(expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(eval_ctx, param_datum))) {
    LOG_WARN("failed to eval", K(ret));
  } else if (param_datum->is_null()) {
    found_null = true;
  } else {
    LOG_DEBUG("succeed to check expr and eval param", K(ret));
  }
  return ret;
}

#define DEF_EVAL_NEG_FUNC(obj_tc)                                           \
  template <>                                                               \
  int eval_neg_func<obj_tc>(const ObExpr &expr, ObEvalCtx &eval_ctx, \
                                   ObDatum &expr_datum)

DEF_EVAL_NEG_FUNC(ObNullTC) {
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(eval_ctx);
  expr_datum.set_null();
  return ret;
}

DEF_EVAL_NEG_FUNC(ObIntTC) {
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval_param(expr, eval_ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else if (param->get_int() == INT64_MIN) {
    // following is compatiable with mysql:
    // 1. select -c1 from t1; will give error if c1 is INT64_MIN
    // 2. select --9223372036854775808 from dual; will return 9223372036854775808;
    //    will be handled in eval func of ObNumberTC. see ObExprNeg::calc_param_type(),
    //    type of -9223372036854775808 will be decimal.
    ret = OB_OPERATE_OVERFLOW;
  } else {
    expr_datum.set_int(-param->get_int());
  }
  return ret;
}

DEF_EVAL_NEG_FUNC(ObUIntTC) {
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval_param(expr, eval_ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    if (param->get_uint64() > (1UL + INT64_MAX)) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      expr_datum.set_int(-param->get_uint64());
    }
  }
  return ret;
}

DEF_EVAL_NEG_FUNC(ObFloatTC) {
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval_param(expr, eval_ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_float(-param->get_float());
  }
  return ret;
}

DEF_EVAL_NEG_FUNC(ObDoubleTC) {
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval_param(expr, eval_ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_double(-param->get_double());
  }
  return ret;
}

DEF_EVAL_NEG_FUNC(ObNumberTC) {
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval_param(expr, eval_ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    number::ObNumber param_nmb(param->get_number());
    expr_datum.set_number(param_nmb.negate());
  }
  return ret;
}

static ObExpr::EvalFunc eval_neg_funcs[ObMaxTC];

template<int N>
struct ObEvalNegFuncIniter
{
  static int init_array()
  {
    eval_neg_funcs[N] = &eval_neg_func<static_cast<ObObjTypeClass>(N)>;
    return 0;
  }
};

static bool init_ret = ObArrayConstIniter<ObMaxTC, ObEvalNegFuncIniter>::init();

static_assert(ObMaxTC == sizeof(eval_neg_funcs) / sizeof(void *), "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_NEG_EVAL, eval_neg_funcs, ARRAYSIZEOF(eval_neg_funcs));


ObExprNeg::ObExprNeg(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_OP_NEG, N_NEG, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {};
int ObExprNeg::calc_result_type1(ObExprResType &type, ObExprResType &type1, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  static const int16_t NEG_PRECISION_OFFSET = 1;
  const ObSQLSessionInfo *session = dynamic_cast<const ObSQLSessionInfo *>(type_ctx.get_session());
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    // result type
    ObObjType itype = ObMaxType;
    if (OB_SUCC(ObExprResultTypeUtil::get_neg_result_type(itype, type1.get_type()))) {
      if (ObMaxType == itype) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("unsupported type for neg", K(ret), K(type1), K(itype));
      } else {
        type.set_type(itype);
      }
    }
    if (OB_SUCC(ret)) {
      // collation
      // 结果不可能为字符类型，无需专门设置collation

      // null flag
      ObExprOperator::calc_result_flag1(type, type1);

      if (lib::is_oracle_mode()) {
        type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].
            get_scale());
        type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].
            get_precision());
      } else {
        // scale
        type.set_scale(type1.get_scale());
        if (OB_LIKELY(0 < type1.get_precision())) {
          if (type1.get_type() == ObUNumberType) {
            type.set_precision(static_cast<int16_t>(type1.get_precision()));
          } else {
            type.set_precision(static_cast<int16_t>(
              MIN(type1.get_precision() + NEG_PRECISION_OFFSET, OB_MAX_INTEGER_DISPLAY_WIDTH)));
          }
        }
      }
      
      ObObjType res_param_type = ObMaxType;
      ObObjType result_type = ObMaxType;
      if (type1.get_type() == ObJsonType) {
          type.set_type(ObDoubleType);
          type1.set_calc_type(ObDoubleType);
      } else if (OB_FAIL(calc_param_type(type1, res_param_type, result_type))) {
        LOG_WARN("failed to calc param type", K(ret));
      } else {
        LOG_DEBUG("calc reuslt type", K(res_param_type), K(result_type));
        type.set_type(result_type);
        type1.set_calc_type(res_param_type);
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObExprNeg::cg_expr(ObExprCGCtx &op_cg_ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(T_OP_NEG != rt_expr.type_) ||
      OB_ISNULL(rt_expr.args_) ||
      OB_UNLIKELY(rt_expr.arg_cnt_ != 1) ||
      OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObObjTypeClass param_tc = OBJ_TYPE_TO_CLASS[rt_expr.args_[0]->datum_meta_.type_];
    rt_expr.eval_func_ = eval_neg_funcs[param_tc];
  }
  return ret;
}

int ObExprNeg::calc_param_type(const ObExprResType &param_type,
                               ObObjType &calc_type,
                               ObObjType &result_type)
{
  int ret = OB_SUCCESS;
  switch (OBJ_TYPE_TO_CLASS[param_type.get_type()]) {
  case ObUIntTC:
  case ObBitTC:
  case ObEnumSetTC: {
    calc_type = ObUInt64Type;
    result_type = ObIntType;
    if (ObUInt64Type == param_type.get_type() && param_type.is_literal()) {
      result_type = calc_type = ObNumberType;
    }
    break;
  }
  case ObYearTC:
  case ObIntTC: {
    if (ObIntType == param_type.get_type() && param_type.is_literal()) {
      const ObObj &obj = param_type.get_param();
      if (OB_UNLIKELY(!obj.is_int())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected obj type", K(ret), K(obj), K(param_type));
      } else if (INT64_MIN == obj.get_int()) {
        // select --9223372036854775808;
        // -9223372036854775808 is a decimal. otherwise neg will overflow.
        result_type = calc_type = ObNumberType;
      } else {
        result_type = calc_type = ObIntType;
      }
    } else {
      result_type = calc_type = ObIntType;
    }
    break;
  }
  case ObFloatTC: {
    result_type = calc_type = is_oracle_mode() ? ObFloatType : ObDoubleType;
    break;
  }
  case ObDoubleTC: {
    result_type = calc_type = ObDoubleType;
    break;
  }
  case ObNumberTC: {
    result_type = calc_type = ObNumberType;
    break;
  }
  case ObDateTC:
  case ObDateTimeTC:
  case ObTimeTC:
  case ObTextTC:
  case ObStringTC: {
    if (lib::is_oracle_mode()) {
      result_type = calc_type = ObNumberType;
    } else {
      result_type = calc_type = ObDoubleType;
    }
    break;
  }
  case ObExtendTC:
  case ObRawTC: {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    break;
  }
  case ObJsonTC: {
    result_type = ObDoubleType;
    calc_type = ObDoubleType;
    break;
  }
  default: {
    result_type = calc_type = ObNullType;
    break;
  }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
