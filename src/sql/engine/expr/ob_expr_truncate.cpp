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

#include "sql/engine/expr/ob_expr_truncate.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "objit/common/ob_item_type.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"

#define GET_SCALE_FOR_CALC(scale) (scale < 0 ? max((-1) * OB_MAX_DECIMAL_PRECISION, scale) : \
                                   min(OB_MAX_DECIMAL_SCALE, scale))

#define GET_SCALE_FOR_CALC_ORACLE(scale) (scale < 0 ? max((-1) * OB_MAX_NUMBER_PRECISION, scale) : \
                                   min(OB_MAX_NUMBER_SCALE, scale))

#define GET_SCALE_FOR_DEDUCE(scale) (scale < 0 ? 0 : min(OB_MAX_DECIMAL_SCALE, scale))
#define GET_SCALE_FOR_DEDUCE_ORACLE(scale) (scale < 0 ? 0 : min(OB_MAX_NUMBER_SCALE, scale))

namespace oceanbase
{
namespace sql
{
using namespace common;

ObExprTruncate::ObExprTruncate(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc,
                       T_FUN_SYS_TRUNCATE,
                       N_TRUNCATE,
                       2,
                       VALID_FOR_GENERATED_COL,
                       NOT_ROW_DIMENSION) {}

int ObExprTruncate::calc_result_type2(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprResType &type2,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session =
    dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast basic session to sql session failed", K(ret));
  } else if (type1.is_null() || type2.is_null()) {
    type.set_null();
  } else {
    bool not_int_flag = true;
    if (type2.is_literal()) {
      ObObjTypeClass target_tc = type1.get_type_class();
      switch(target_tc) {
      case ObIntTC: {
        type.set_int();
        type.set_scale(type1.get_scale());
        not_int_flag = false;
        break;
      }
      case ObUIntTC:
      case ObBitTC:
      case ObYearTC: {
        type.set_uint64();
        type.set_scale(type1.get_scale());
        not_int_flag = false;
        break;
      }
      case ObNumberTC: {
        if (type1.is_unumber()) {
          type.set_unumber();
        } else {
          type.set_number();
        }
        break;
      }
      default: {
        if (type1.is_ufloat() || type1.is_udouble()) {
          type.set_udouble();
        } else {
          type.set_double();
        }
        break;
      }
      }

      const ObObj& obj2 = type2.get_param();
      ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC);
      ObCastMode cast_mode = CM_NONE;
      ObCollationType cast_coll_type = type_ctx.get_coll_type();
      const ObDataTypeCastParams dtc_params =
            ObBasicSessionInfo::create_dtc_params(session);
      if (FAILEDx(ObSQLUtils::get_default_cast_mode(session, cast_mode))) {
        LOG_WARN("failed to get default cast mode", K(ret));
      }
      ObCastCtx cast_ctx(&oballocator,
                         &dtc_params,
                         0,
                         cast_mode,
                         cast_coll_type);
      int64_t scale_val = 0;
      EXPR_GET_INT64_V2(obj2, scale_val);

      if (0 <= scale_val) {
        if (OB_SUCC(ret)) {
          if (not_int_flag) {
            if (OB_UNLIKELY(lib::is_oracle_mode())) {
              type.set_scale(static_cast<int16_t>(GET_SCALE_FOR_DEDUCE_ORACLE(scale_val)));
            } else {
              type.set_scale(static_cast<int16_t>(GET_SCALE_FOR_DEDUCE(scale_val)));
            }
            ObPrecision precision = static_cast<ObPrecision>
              (type1.get_precision() - type1.get_scale() + scale_val + 1);
            //防御可能出现的没有设置precision的decimal 或者scale_val 为负数
            precision = std::max(static_cast<int16_t>(-1), precision);
            type.set_precision(precision);
          } else { /* do nothing */}
        } else if (ret == OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD ||
            ret == OB_ERR_DATA_TRUNCATED){
          // cast obj2 to int failed, just set scale to SCALE_UNKNOWN_YET
          type.set_scale(SCALE_UNKNOWN_YET);
          type.set_precision(PRECISION_UNKNOWN_YET);
          ret = OB_SUCCESS;
        } else {
          // do nothing
        }
      } else {
        type.set_scale(SCALE_UNKNOWN_YET);
        type.set_precision(PRECISION_UNKNOWN_YET);
        ret = OB_SUCCESS;
      }
    } else {
      ObObjType expr1_ty = type1.get_type();
      if (ObNumberType == expr1_ty || ObUNumberType == expr1_ty) {
        type.set_type(expr1_ty);
      } else if ((expr1_ty >= ObUTinyIntType && expr1_ty <= ObUInt64Type) ||
          (expr1_ty >= ObUFloatType && expr1_ty <= ObUDoubleType) ||
          (expr1_ty == ObYearType || expr1_ty == ObBitType)) {
        type.set_udouble();
      } else {
        type.set_double();
      }
      if (type1.is_numeric_type() || ob_is_temporal_type(type1.get_type())) {
        type.set_scale(type1.get_scale());
        type.set_precision(type1.get_precision());
      } else {
        type.set_scale(SCALE_UNKNOWN_YET);
      }
    }
    type1.set_calc_type(type.get_type());
    type2.set_calc_type(ObIntType);

    ObExprOperator::calc_result_flag2(type, type1, type2);
  }
  return ret;
}

int ObExprTruncate::set_trunc_val(common::ObObj &result,
                                  common::number::ObNumber &nmb,
                                  ObExprCtx &expr_ctx,
                                  ObObjType res_type)
{
  int ret = OB_SUCCESS;
  ObObj tmp_obj;
  tmp_obj.set_number(nmb);
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE)
  switch(res_type) {
  case ObIntType: {
    int64_t ival = 0;
    EXPR_GET_INT64_V2(tmp_obj, ival);
    if (OB_SUCC(ret)) {
      result.set_int(ival);
    }
    break;
  }
  case ObUInt64Type: {
    uint64_t uval = 0;
    EXPR_GET_UINT64_V2(tmp_obj, uval);
    if (OB_SUCC(ret)) {
      result.set_uint64(uval);
    }
    break;
  }
  case ObFloatType: {
    float fval = 0.0;
    EXPR_GET_FLOAT_V2(tmp_obj, fval);
    if (OB_SUCC(ret)) {
      result.set_float(fval);
    }
    break;
  }
  case ObUFloatType: {
    float fval = 0.0;
    EXPR_GET_FLOAT_V2(tmp_obj, fval);
    if (OB_SUCC(ret)) {
      result.set_ufloat(fval);
    }
    break;
  }
  case ObDoubleType: {
    double dval = 0.0;
    EXPR_GET_DOUBLE_V2(tmp_obj, dval);
    if (OB_SUCC(ret)) {
      result.set_double(dval);
    }
    break;
  }
  case ObUDoubleType: {
    double dval = 0.0;
    EXPR_GET_DOUBLE_V2(tmp_obj, dval);
    if (OB_SUCC(ret)) {
      result.set_udouble(dval);
    }
    break;
  }
  case ObNumberType: {
    result.set_number(nmb);
    break;
  }
  case ObUNumberType: {
    result.set_unumber(nmb);
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected result type", K(ret), K(res_type));
    break;
  }
  }
  return ret;
}

int calc_truncate_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const ObObjType res_type = expr.datum_meta_.type_;
  const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
  // truncate(x, d)
  ObDatum *x_datum = NULL;
  ObDatum *d_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, d_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (x_datum->is_null() || d_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_UNLIKELY(res_type != arg_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected res_type or arg_type", K(ret), K(res_type), K(arg_type), K(expr));
  } else {
    const int64_t scale = d_datum->get_int();
    switch (arg_type) {
      case ObIntType: {
        res_datum.set_int(ObExprUtil::trunc_integer(x_datum->get_int(), scale));
        break;
      }
      case ObUInt64Type: {
        res_datum.set_uint(ObExprUtil::trunc_integer(x_datum->get_uint(), scale));
        break;
      }
      case ObUDoubleType:
      case ObDoubleType: {
        res_datum.set_double(ObExprUtil::trunc_double(x_datum->get_double(), scale));
        break;
      }
      case ObUNumberType:
      case ObNumberType: {
        ObNumStackOnceAlloc tmp_alloc;
        number::ObNumber arg_nmb(x_datum->get_number());
        number::ObNumber res_nmb;
        if (OB_FAIL(res_nmb.from(arg_nmb, tmp_alloc))) {
          LOG_WARN("get nmb from arg failed", K(ret), K(arg_nmb));
        } else if (OB_FAIL(res_nmb.trunc(lib::is_oracle_mode()
                                ? GET_SCALE_FOR_CALC_ORACLE(scale)
                                : GET_SCALE_FOR_CALC(scale)))) {
          LOG_WARN("trunc number failed", K(ret), K(res_nmb), K(scale));
        } else {
          res_datum.set_number(res_nmb);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg_type for trunc expr", K(ret), K(arg_type));
        break;
      }
    }
  }
  return ret;
}

int ObExprTruncate::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_truncate_expr;
  return ret;
}
} // namespace sql
} // namespace oceanbase

#undef GET_SCALE_FOR_CALC
#undef GET_SCALE_FOR_CALC_ORACLE
#undef GET_SCALE_FOR_DEDUCE
#undef GET_SCALE_FOR_DEDUCE_ORACLE
