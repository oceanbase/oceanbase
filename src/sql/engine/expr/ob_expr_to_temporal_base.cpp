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

#include "sql/engine/expr/ob_expr_to_temporal_base.h"

#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

template <ObObjType type>
int set_datum_from_ob_time(
    ObEvalCtx &ctx,
    ObTime &ob_time,
    ObDatum &result)
{
  UNUSED(ctx);
  UNUSED(ob_time);
  UNUSED(result);
  return OB_NOT_SUPPORTED;
}

template <>
int set_datum_from_ob_time<ObDateTimeType>(
    ObEvalCtx &ctx,
    ObTime &ob_time,
    ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObTimeConvertCtx time_cvrt_ctx(TZ_INFO(ctx.exec_ctx_.get_my_session()), false);
  ObDateTime result_value;
  OZ (ObTimeConverter::ob_time_to_datetime(ob_time, time_cvrt_ctx, result_value));
  OX (result.set_datetime(result_value));
  return ret;
}

template <>
int set_datum_from_ob_time<ObTimestampNanoType>(
    ObEvalCtx &ctx,
    ObTime &ob_time,
    ObDatum &result)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObOTimestampData result_value;
  OZ (ObTimeConverter::ob_time_to_otimestamp(ob_time, result_value));
  OX (result.set_otimestamp_tiny(result_value));
  return ret;
}

template <>
int set_datum_from_ob_time<ObTimestampTZType>(
    ObEvalCtx &ctx,
    ObTime &ob_time,
    ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObTimeConvertCtx time_cvrt_ctx(TZ_INFO(ctx.exec_ctx_.get_my_session()), false);
  ObOTimestampData result_value;
  OZ (ObTimeConverter::ob_time_to_utc(ObTimestampTZType, time_cvrt_ctx, ob_time));
  OZ (ObTimeConverter::ob_time_to_otimestamp(ob_time, result_value));
  OX (result.set_otimestamp_tz(result_value));
  return ret;
}

int calc_to_temporal_expr(const ObExpr &expr,
                     ObEvalCtx &ctx,
                     ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  ObDatum *input_char = nullptr;
  ObDatum *fmt = nullptr;
  ObDatum *nls_param = nullptr;

  ObObjType target_type = expr.datum_meta_.type_;

  CK (OB_NOT_NULL(session = ctx.exec_ctx_.get_my_session()));

  OZ (expr.args_[0]->eval(ctx, input_char));

  if (expr.arg_cnt_ > 1) {
    OZ (expr.args_[1]->eval(ctx, fmt));
  }

  if (expr.arg_cnt_ > 2) {
    OZ (expr.args_[2]->eval(ctx, nls_param));
  }

  if (input_char->is_null()
      || (OB_NOT_NULL(fmt) && fmt->is_null())
      || (OB_NOT_NULL(nls_param) && nls_param->is_null())) {
    res_datum.set_null();
  } else {

    ObString format_str;
    if (OB_NOT_NULL(fmt)) {
      format_str = fmt->get_string();
    } else {
      OZ (session->get_local_nls_format(target_type, format_str));
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(nls_param)) {
      if (!ObArithExprOperator::is_valid_nls_param(nls_param->get_string())) {
        ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
        LOG_WARN("nls param is invalid", K(ret), "nls_param", nls_param->get_string());
      }
    }

    ObTime ob_time;
    ObTimeConvertCtx time_cvrt_ctx(TZ_INFO(session), format_str, false);
    ObScale scale = 0; //not used

    if (OB_SUCC(ret)) {
      if (expr.arg_cnt_ > 1 && !!(expr.args_[1]->is_static_const_)) {
        auto rt_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
        ObExprDFMConvertCtx *dfm_convert_ctx = NULL;
        if (NULL == (dfm_convert_ctx = static_cast<ObExprDFMConvertCtx *>
                     (ctx.exec_ctx_.get_expr_op_ctx(rt_ctx_id)))) {
          if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(rt_ctx_id, dfm_convert_ctx))) {
            LOG_WARN("failed to create operator ctx", K(ret));
          } else if (OB_FAIL(dfm_convert_ctx->parse_format(format_str, target_type,
                                                           true, ctx.exec_ctx_.get_allocator()))) {
            LOG_WARN("fail to parse format", K(ret), K(format_str));
          }
          LOG_DEBUG("new dfm convert ctx", K(ret), KPC(dfm_convert_ctx));
        }
        OZ (ObTimeConverter::str_to_ob_time_by_dfm_elems(input_char->get_string(),
                                                         dfm_convert_ctx->get_dfm_elems(),
                                                         dfm_convert_ctx->get_elem_flags(),
                                                         time_cvrt_ctx,
                                                         target_type,
                                                         ob_time,
                                                         scale));
      } else {
        OZ (ObTimeConverter::str_to_ob_time_oracle_dfm(input_char->get_string(),
                                                       time_cvrt_ctx,
                                                       target_type,
                                                       ob_time,
                                                       scale));
      }
    }
    if (OB_SUCC(ret)) {
      switch (target_type) {
      case ObDateTimeType:
        OZ (set_datum_from_ob_time<ObDateTimeType>(ctx, ob_time, res_datum));
        break;
      case ObTimestampNanoType:
        OZ (set_datum_from_ob_time<ObTimestampNanoType>(ctx, ob_time, res_datum));
        break;
      case ObTimestampTZType:
        OZ (set_datum_from_ob_time<ObTimestampTZType>(ctx, ob_time, res_datum));
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected target_type", K(target_type), K(ret));
        break;
      }
    }
  }
  return ret;
}

ObExprToTemporalBase::ObExprToTemporalBase(ObIAllocator &alloc,
                                             ObExprOperatorType type,
                                             const char *name)
    : ObFuncExprOperator(alloc, type, name, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprToTemporalBase::calc_result_typeN(ObExprResType &type,
                                             ObExprResType *types_array,
                                             int64_t param_num,
                                             ObExprTypeCtx &type_ctx) const
{
  //https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions193.htm
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 1) || OB_UNLIKELY(param_num > 3)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("not enough params for function to_date", K(ret), K(param_num));
  } else if (OB_ISNULL(types_array) || OB_ISNULL(type_ctx.get_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(types_array), K(type_ctx.get_session()));
  } else {
    ObExprResType &input_char = types_array[0];
    ObCollationType nls_collation = type_ctx.get_session()->get_nls_collation();
    if (OB_SUCC(ret)) {
      //to_date 可以接受任何可以转换成varchar的input
      bool accept_input_type = ob_is_datetime(get_my_target_obj_type())
          || input_char.is_null()
          || ob_is_string_tc(input_char.get_type())
          || input_char.is_datetime()
          || input_char.is_otimestamp_type();
      if (OB_UNLIKELY(!accept_input_type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent type", K(ret), K(input_char));
      } else {
        input_char.set_calc_type(ObVarcharType);
        input_char.set_calc_collation_type(nls_collation);
      }
    }

    ObExprResType &fmt = types_array[1];
    if (OB_SUCC(ret) && param_num > 1) {
      bool accept_fmt_type = fmt.is_null() || fmt.is_string_type();
      if (OB_UNLIKELY(!accept_fmt_type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent type", K(ret), K(fmt));
      } else {
        fmt.set_calc_type(ObVarcharType);
        fmt.set_calc_collation_type(nls_collation);
      }
    }

    ObExprResType &nlsparam = types_array[2];
    if (OB_SUCC(ret) && param_num > 2) {
      bool accept_nlsparam_type = nlsparam.is_null() || nlsparam.is_string_type();
      if (OB_UNLIKELY(!accept_nlsparam_type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent type", K(ret), K(nlsparam));
      } else {
        nlsparam.set_calc_type(ObVarcharType);
        nlsparam.set_calc_collation_type(nls_collation);
      }
    }

    //result type
    if (OB_SUCC(ret)) {
      type.set_type(get_my_target_obj_type());
    }
    //result scale
    if (OB_SUCC(ret)) {
      ObScale max_scale =
          ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][get_my_target_obj_type()].get_scale();
      ObScale result_scale = 0;
      if (input_char.is_null()) {
        //do nothing
      } else if (input_char.is_string_type()) {
        result_scale = max_scale;
      } else if (input_char.is_datetime() || input_char.is_otimestamp_type()) {
        result_scale = std::min(input_char.get_scale(), max_scale);
      } else if (ob_is_datetime(get_my_target_obj_type())) {
        result_scale = max_scale;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(ret), K(input_char));
      }
      type.set_scale(result_scale);
    }
  }
  return ret;
}

int ObExprToTemporalBase::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int64_t param_cnt = raw_expr.get_param_count();
  CK (param_cnt >= 1 && param_cnt <= 3);

  rt_expr.eval_func_ = calc_to_temporal_expr;

  return ret;
}

} //namespace sql
} //namespace oceanbase
