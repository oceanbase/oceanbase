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
#include "sql/engine/expr/ob_expr_to_number.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_number_format_models.h"
#include "lib/number/ob_number_v2.h"
#include "lib/worker.h"
#include <ctype.h>

namespace oceanbase
{
using namespace common;

namespace sql
{

ObExprToNumberBase::ObExprToNumberBase(common::ObIAllocator &alloc,
                                       const ObExprOperatorType type,
                                       const char *name)
    : ObFuncExprOperator(alloc, type, name, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprToNumberBase::~ObExprToNumberBase()
{
}

int ObExprToNumberBase::calc_result_typeN(ObExprResType &type,
                      ObExprResType *types,
                      int64_t param_num,
                      ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lib::is_oracle_mode())) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("to_char only support on oracle mode", K(type), K(ret));
  } else if (OB_UNLIKELY(NOT_ROW_DIMENSION != row_dimension_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid row_dimension_", K(row_dimension_), K(ret));
  } else if (OB_UNLIKELY(param_num < 1 || param_num > 3 || OB_ISNULL(types))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num) , K(types));
  } else if (param_num == 3 && !types[2].is_varchar_or_char()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("Invalid argument type", K(ret), K(param_num) , K(types));
  } else {
    ObObjOType o_type = types[0].get_oracle_type();
    if (types[0].is_varchar_or_char()) {
      types[0].set_calc_collation_utf8();
    } else if (param_num >= 2 && (ObOBinFloatType == o_type || ObOBinDoubleType == o_type)) {
      ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
      LOG_WARN("binarydouble or binaryfloat only support one argument", K(ret));
    } else if (param_num >= 2
        || (!types[0].is_number() && !types[0].is_number_float() && !types[0].is_decimal_int())) {
      types[0].set_calc_type_default_varchar();
    }
    if (OB_SUCC(ret)) {
      if (param_num == 2) {
        if (types[1].is_varchar_or_char()) {
          types[1].set_calc_collation_utf8();
        } else {
          types[1].set_calc_type_default_varchar();
        }
      }
      if (param_num == 3) {
        types[2].set_calc_type_default_varchar();
      }
      type.set_type(ObNumberType);
      type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
      type.set_precision(PRECISION_UNKNOWN_YET);
    }
  }
  return ret;
}

ObExprToNumber::ObExprToNumber(ObIAllocator &alloc)
    : ObExprToNumberBase(alloc, T_FUN_SYS_TO_NUMBER, N_TO_NUMBER)
{
}

ObExprToNumber::~ObExprToNumber()
{
}

int ObExprToNumber::calc_tonumber_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // to_number(ori, fmt)
  ObDatum *ori = NULL;
  ObDatum *fmt = NULL;
  ObDatum *nlsparam_datum = NULL;
  ObString nlsparam;
  if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, ori, fmt, nlsparam_datum))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ == 2 && fmt->get_string().length() >= MAX_FMT_STR_LEN)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
  } else if (ori->is_null()) {
    // bug here:
    res_datum.set_null();
  } else {
    const ObObjType &ori_type = expr.args_[0]->datum_meta_.type_;
    const ObCollationType &ori_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    number::ObNumber res_nmb;
    if (1 == expr.arg_cnt_) {
      if (ob_is_varchar_or_char(ori_type, ori_cs_type)) {
        const ObString &ori_str = ori->get_string();
        ObNumStackOnceAlloc tmp_alloc;
        ObPrecision res_precision = -1;
        ObScale res_scale = -1;
        if (OB_FAIL(res_nmb.from_sci_opt(ori_str.ptr(), ori_str.length(), tmp_alloc,
                                         &res_precision, &res_scale))) {
          LOG_WARN("fail to calc function to_number", K(ret), K(ori_str));
        } else {
          res_datum.set_number(res_nmb);
        }
      } else if (ObNumberTC == ob_obj_type_class(ori_type)) {
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(res_nmb.from(ori->get_number(), tmp_alloc))) {
          LOG_WARN("get nmb failed", K(ret));
        } else {
          res_datum.set_number(res_nmb);
        }
      } else if (ObDecimalIntTC == ob_obj_type_class(ori_type)) {
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(wide::to_number(ori->get_decimal_int(), ori->get_int_bytes(),
                                    expr.args_[0]->datum_meta_.scale_, tmp_alloc, res_nmb))) {
          LOG_WARN("to_number failed", K(ret));
        } else {
          res_datum.set_number(res_nmb);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg type", K(ret), K(ori_type));
      }
    } else {
      if (OB_ISNULL(fmt)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("fmt datum is NULL", K(ret));
      } else if (expr.arg_cnt_ > 2 && OB_ISNULL(nlsparam_datum)) {
        ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
        LOG_WARN("nls datum is NULL", K(ret));
      } else {
        if (OB_NOT_NULL(nlsparam_datum)) {
          nlsparam = nlsparam_datum->get_string();
        }
        if (!nlsparam.empty() && OB_UNLIKELY(!ObExprOperator::is_valid_nls_param(nlsparam))) {
          ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
          LOG_WARN("date format is invalid", K(ret), K(nlsparam));
        } else {
          const ObString &ori_str = ori->get_string();
          const ObString &fmt_str = fmt->get_string();
          ObNFMToNumber nfm;
          if (OB_FAIL(nfm.convert_char_to_num(ori_str, fmt_str, calc_alloc, ctx, res_nmb))) {
            LOG_WARN("calc to number failed", K(ret), K(ori_str), K(fmt_str));
          } else {
            res_datum.set_number(res_nmb);
          }
        }
      }
    }
  }
  return ret;
}

// for static engine batch
int ObExprToNumber::calc_tonumber_expr_batch(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval to_number in batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObDatum *fmt_datum = NULL;
  ObDatum *nlsparam_datum = NULL; // no use now, just made grammar supported.

  // Do some pre check, mainly for checking if args are valid
  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else {
    const ObObjType &ori_type = expr.args_[0]->datum_meta_.type_;
    const ObCollationType &ori_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (1 == expr.arg_cnt_) {
      if (!ob_is_varchar_or_char(ori_type, ori_cs_type)
          && (ObNumberTC != ob_obj_type_class(ori_type))
          && (ObDecimalIntTC != ob_obj_type_class(ori_type))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg type", K(ret), K(ori_type));
      }
    } else if (2 == expr.arg_cnt_ || 3 == expr.arg_cnt_) {
      if (OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
        LOG_WARN("eval fmt failed", K(ret));
      } else if (OB_ISNULL(fmt_datum)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("fmt datum is NULL", K(ret));
      } else if (fmt_datum->get_string().length() >= MAX_FMT_STR_LEN) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("invalid number format model", K(ret), K(expr.arg_cnt_));
      } else if (3 == expr.arg_cnt_) {
        if (OB_FAIL(expr.args_[2]->eval(ctx, nlsparam_datum))) {
          LOG_WARN("eval fmt failed", K(ret));
        } else if (OB_ISNULL(nlsparam_datum)) {
          ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
          LOG_WARN("nlsparam datum is NULL", K(ret));
        } else {
          ObString nlsparam = nlsparam_datum->get_string();
          if (!nlsparam.empty() && OB_UNLIKELY(!ObExprOperator::is_valid_nls_param(nlsparam))) {
            ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
            LOG_WARN("date format is invalid", K(ret), K(nlsparam));
          }
        }
      }
    }
    // Calculate for batch
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("failed to eval batch result args0", K(ret));
    } else {
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
      ObDatum *datum_array = expr.args_[0]->locate_batch_datums(ctx);
      number::ObNumber res_nmb;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      for (int64_t j = 0; OB_SUCC(ret) && (j < batch_size); ++j) {
        if (skip.at(j) || eval_flags.at(j)) {
          continue;
        } else if (datum_array[j].is_null()) {
          results[j].set_null();
          eval_flags.set(j);
        } else if (1 == expr.arg_cnt_) {
          ObNumStackOnceAlloc tmp_alloc;
          ObPrecision res_precision = -1;
          ObScale res_scale = -1;
          if (ob_is_varchar_or_char(ori_type, ori_cs_type)
              && OB_FAIL(res_nmb.from_sci_opt(
                         datum_array[j].get_string().ptr(), datum_array[j].get_string().length(),
                         tmp_alloc, &res_precision, &res_scale))) {
            LOG_WARN("fail to calc function to_number", K(ret), K(datum_array[j].get_string()));
          } else if (ObNumberTC == ob_obj_type_class(ori_type)
                     && OB_FAIL(res_nmb.from(datum_array[j].get_number(), tmp_alloc))) {
              LOG_WARN("get nmb failed", K(ret));
          } else if (ObDecimalIntTC == ob_obj_type_class(ori_type)
                     && OB_FAIL(wide::to_number(
                         datum_array[j].get_decimal_int(), datum_array[j].get_int_bytes(),
                         expr.args_[0]->datum_meta_.scale_, tmp_alloc, res_nmb))) {
            LOG_WARN("to_number failed", K(ret));
          } else {
            results[j].set_number(res_nmb);
            eval_flags.set(j);
          }
        } else {
          ObNFMToNumber nfm;
          if (OB_FAIL(nfm.convert_char_to_num(
                      datum_array[j].get_string(), fmt_datum->get_string(),
                      calc_alloc, ctx, res_nmb))) {
            LOG_WARN("calc to number failed",
                     K(ret), K(datum_array[j].get_string()), K(fmt_datum->get_string()));
          } else {
            results[j].set_number(res_nmb);
            eval_flags.set(j);
          }
        }
      }
    }
  }

  return ret;
}


int ObExprToNumber::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_tonumber_expr;

  // for static engine batch
  if ((1 == rt_expr.arg_cnt_ && rt_expr.args_[0]->is_batch_result())
      || (2 == rt_expr.arg_cnt_
          && rt_expr.args_[0]->is_batch_result()
          && !rt_expr.args_[1]->is_batch_result())
      || (3 == rt_expr.arg_cnt_
          && rt_expr.args_[0]->is_batch_result()
          && !rt_expr.args_[1]->is_batch_result()
          && !rt_expr.args_[2]->is_batch_result())) {
    rt_expr.eval_batch_func_ = calc_tonumber_expr_batch;
  }

  return ret;
}

ObExprToBinaryFloat::ObExprToBinaryFloat(ObIAllocator &alloc)
    : ObExprToNumberBase(alloc, T_FUN_SYS_TO_BINARY_FLOAT, N_TO_BINARY_FLOAT)
{
}

ObExprToBinaryFloat::~ObExprToBinaryFloat()
{
}

int ObExprToBinaryFloat::calc_result_typeN(ObExprResType &type,
                      ObExprResType *types,
                      int64_t param_num,
                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_UNLIKELY(param_num != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num) , K(types));
  } else if (ObTinyIntType == types[0].get_type()) {
    // oracle compatible
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, 15, "TO_BINARY_FLOAT");
    LOG_WARN("wrong number or types of arguments in function", K(ret), K(types[0].get_type()));
  } else if (ObExtendType == types[0].get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent datatypes", K(ret),
             "expected_type_class", ob_obj_type_class(ObFloatType),
             "got", ob_obj_type_class(types[0].get_type()));
  } else if (OB_FAIL(ObExprToNumberBase::calc_result_typeN(type, types, param_num, type_ctx))) {
    LOG_WARN("fail to calc_result_typeN", K(ret));
  } else if (OB_FAIL(ObObjCaster::can_cast_in_oracle_mode(ObFloatType, CS_TYPE_BINARY,
                                            types[0].get_type(), types[0].get_collation_type()))) {
    LOG_WARN("input type can not cast to binary_float", K(types[0].get_type()), K(ret));
  } else {
    types[0].set_calc_type(ObFloatType);
    type.set_type(ObFloatType);
  }
  return ret;
}

int ObExprToBinaryFloat::calc_to_binaryfloat_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                                  ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // to_number(ori, fmt)
  ObDatum *ori = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, ori))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (ori->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_float(ori->get_float());
  }
  return ret;
}

int ObExprToBinaryFloat::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_to_binaryfloat_expr;
  return ret;
}

ObExprToBinaryDouble::ObExprToBinaryDouble(ObIAllocator &alloc)
    : ObExprToNumberBase(alloc, T_FUN_SYS_TO_BINARY_DOUBLE, N_TO_BINARY_DOUBLE)
{
}

ObExprToBinaryDouble::~ObExprToBinaryDouble()
{
}

int ObExprToBinaryDouble::calc_result_typeN(ObExprResType &type,
                      ObExprResType *types,
                      int64_t param_num,
                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_UNLIKELY(param_num != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num) , K(types));
  } else if (ObTinyIntType == types[0].get_type()) {
    // oracle compatible
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, 16, "TO_BINARY_DOUBLE");
    LOG_WARN("wrong number or types of arguments in function", K(ret), K(types[0].get_type()));
  } else if (ObExtendType == types[0].get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent datatypes", K(ret),
             "expected_type_class", ob_obj_type_class(ObDoubleType),
             "got", ob_obj_type_class(types[0].get_type()));
  } else if (OB_FAIL(ObExprToNumberBase::calc_result_typeN(type, types, param_num, type_ctx))) {
    LOG_WARN("fail to calc_result_typeN", K(ret));
  } else if (OB_FAIL(ObObjCaster::can_cast_in_oracle_mode(ObDoubleType, CS_TYPE_BINARY,
                                            types[0].get_type(), types[0].get_collation_type()))) {
    LOG_WARN("input type can not cast to binary_double", K(types[0].get_type()), K(ret));
  } else {
    types[0].set_calc_type(ObDoubleType);
    type.set_type(ObDoubleType);
  }
  return ret;
}

int ObExprToBinaryDouble::calc_to_binarydouble_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                                    ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // to_number(ori, fmt)
  ObDatum *ori = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, ori))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (ori->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_double(ori->get_double());
  }
  return ret;
}

int ObExprToBinaryDouble::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_to_binarydouble_expr;
  return ret;
}

}/* namespace sql */
}/* namespace oceanbase */
