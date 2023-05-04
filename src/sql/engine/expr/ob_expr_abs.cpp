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

#include "sql/engine/expr/ob_expr_abs.h"

#include "share/object/ob_obj_cast.h"
#include "share/config/ob_server_config.h"
#include "share/datum/ob_datum_util.h"

#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace oceanbase::common;

namespace sql
{

#define DEF_EVAL_ABS_FUNC(type)                                \
  template <>                                                  \
  int eval_datum_abs<type>(const ObExpr &expr, ObEvalCtx &ctx, \
                           ObDatum &expr_datum)

static int check_expr_and_eval(const ObExpr &expr, ObEvalCtx &ctx,
                               ObDatum *&param_datum, bool &found_null)
{
  int ret = OB_SUCCESS;
  found_null = false;
  if (OB_UNLIKELY(expr.type_ != T_OP_ABS)
      || OB_UNLIKELY(expr.arg_cnt_ != 1) || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("failed to eval", K(ret));
  } else if (param_datum->is_null()) {
    found_null = true;
  } else {
    // do nothing
  }
  return ret;
}

template<ObObjType obj_type>
int eval_datum_abs(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          ObDatum &expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(expr_datum);
  return OB_NOT_SUPPORTED;
}

DEF_EVAL_ABS_FUNC(ObNullType)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(expr);
  expr_datum.set_null();
  return ret;
}

DEF_EVAL_ABS_FUNC(ObNumberType)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param_datum, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    number::ObNumber param_nmb(param_datum->get_number());
    number::ObNumber res_num = param_nmb;
    if (param_nmb.is_negative()) {
      res_num = param_nmb.negate();
    }
    expr_datum.set_number(res_num);
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObUNumberType)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param_datum, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    number::ObNumber param_nmb(param_datum->get_number());
    number::ObNumber res_num = param_nmb;
    if (param_nmb.is_negative()) {
      res_num = param_nmb.negate();
    }
    expr_datum.set_number(res_num);
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObFloatType)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_float(param->get_float() >= 0.0
                         ? param->get_float() : -param->get_float());
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObDoubleType)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_double(param->get_double() >= 0
                          ? param->get_double() : -param->get_double());
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObUDoubleType)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_double(param->get_udouble());
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObIntType)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    int64_t param_int = param->get_int();
    // 只有mysql模式会调到这个函数，如果发现是INT64_MIN，需要报out of range
    if (INT64_MIN == param_int) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("value out of range", K(ret));
    } else {
      expr_datum.set_int(param_int >= 0 ? param_int : -param_int);
    }
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObUInt64Type)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_uint(param->get_uint64());
  }
  return ret;
}

ObExpr::EvalFunc abs_funcs[ObMaxType];

template<int IDX>
struct AbsFuncIniter
{
  static bool init_array()
  {
    abs_funcs[IDX] = &eval_datum_abs<static_cast<ObObjType>(IDX)>;
    return true;
  }
};

static bool abs_eval_func_init_ret = ObArrayConstIniter<ObMaxType, AbsFuncIniter>::init();

static_assert(ObMaxType == sizeof(abs_funcs) / sizeof(void *), "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_ABS_EVAL, abs_funcs, ARRAYSIZEOF(abs_funcs));


ObExprAbs::ObExprAbs(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_OP_ABS, N_ABS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
      func_(NULL) {}

int ObExprAbs::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  func_ = NULL;
  ret = ObExprOperator::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    if (input_types_.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error. invalid input types",
                K(ret),
                K(input_types_.count()));
    } else {
      ObObjType param_type = input_types_.at(0).get_calc_type();
      if (OB_FAIL(set_func(param_type))) {
        LOG_WARN("set func failed", K(ret), K(param_type));
        func_ = NULL;//defensive code
      }
    }
  }
  return ret;
}

int ObExprAbs::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprAbs *tmp_other = dynamic_cast<const ObExprAbs *>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(ret));
    } else {
      this->func_ = tmp_other->func_;
    }
  }
  return ret;
}

int ObExprAbs::calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                 ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (NOT_ROW_DIMENSION == row_dimension_) {
    // result type
    ObObjType itype;
    if (OB_SUCC(ObExprResultTypeUtil::get_abs_result_type(itype, type1.get_type()))) {
      if (lib::is_oracle_mode() && ob_is_json(type1.get_type())) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
      } else if (ObMaxType == itype) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
      } else {
        type.set_type(itype);
      }
    }

    // collation
    // 结果不可能为字符类型，无需专门设置collation
    if (lib::is_oracle_mode() && (type1.is_varchar_or_char() || type1.is_number_float())) {
      type.set_precision(PRECISION_UNKNOWN_YET);
      type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
    } else if (lib::is_mysql_mode() && type.is_double() && type1.get_scale() != SCALE_UNKNOWN_YET) {
      type.set_scale(type1.get_scale());
      type.set_precision(static_cast<ObPrecision>(ObMySQLUtil::float_length(type1.get_scale())));
    } else {
      type.set_accuracy(type1.get_accuracy());
    }

    // null flag
    ObExprOperator::calc_result_flag1(type, type1);

    if (OB_SUCC(ret)) {
      // set calc type for param
      ObObjType param_calc_type = calc_param_type(type1.get_type(),
                                                  lib::is_oracle_mode());
      if (OB_UNLIKELY(ObMaxType == param_calc_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param calc type", K(ret), K(type1.get_type()), K(param_calc_type));
      } else {
        type1.set_calc_type(param_calc_type);
        if (type1.get_type() == ObJsonType) {
          type1.set_calc_type(ObDoubleType);
          type.set_type(ObDoubleType);
        }
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObExprAbs::set_func(ObObjType param_type)
{
  int ret = OB_SUCCESS;
  if (ObMaxType == param_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param_type is invalid", K(ret), K(param_type));
  } else {
    if (lib::is_oracle_mode()) {
      if (OB_FAIL(set_func_oracle(param_type))) {
        LOG_WARN("set_func_oracle failed", K(ret));
      }
    } else {
      if (OB_FAIL(set_func_mysql(param_type))) {
        LOG_WARN("set_func_mysql failed", K(ret));
      }
    }
  }
  return ret;
}
int ObExprAbs::set_func_oracle(ObObjType param_type)
{
  int ret = OB_SUCCESS;
  func_ = NULL;
  switch (param_type) {
    case ObNullType:
      func_ = abs_null;
      break;
    case ObFloatType:
      func_ = abs_float;
      break;
    case ObDoubleType:
      func_ = abs_double;
      break;
    case ObNumberFloatType:
    case ObNumberType:
      func_ = abs_number;
      break;
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType:
    case ObTinyIntType:
    case ObSmallIntType:
    case ObInt32Type:
    case ObIntType:
    case ObCharType:
    case ObVarcharType:
    case ObIntervalDSType:
    case ObIntervalYMType:
    case ObNVarchar2Type:
    case ObNCharType:
    case ObURowIDType:
      func_ = abs_others_number;
      break;
    default:
      LOG_ERROR("unexpected param type", K(param_type));
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}
int ObExprAbs::set_func_mysql(ObObjType param_type)
{
  int ret = OB_SUCCESS;
  func_ = NULL;
  switch (param_type) {
    case ObTinyIntType:
      func_ = abs_int;
      break;
    case ObSmallIntType:
      func_ = abs_int;
      break;
    case ObMediumIntType:
      func_ = abs_int;
      break;
    case ObInt32Type:
      func_ = abs_int;
      break;
    case ObIntType:
      func_ = abs_int64;
      break;
    case ObUTinyIntType:
      func_ = abs_uint;
      break;
    case ObUSmallIntType:
      func_ = abs_uint;
      break;
    case ObUMediumIntType:
      func_ = abs_uint;
      break;
    case ObUInt32Type:
      func_ = abs_uint32_uint64;
      break;
    //in mysql, abs(uint32/uint64/year) returns uint64
    case ObUInt64Type:
      func_ = abs_uint32_uint64;
      break;
    case ObFloatType:
      func_ = abs_float_double;
      break;
    case ObDoubleType:
      func_ = abs_double;
      break;
    case ObUFloatType:
      func_ = abs_float_double;
      break;
    case ObUDoubleType:
      func_ = abs_udouble;
      break;
    case ObNumberFloatType:
    case ObNumberType:
      func_ = abs_number;
      break;
    case ObUNumberType:
      func_ = abs_unumber;
      break;
    case ObNullType:
      func_ = abs_null;
      break;
    case ObYearType:
      func_ = abs_year;
      break;
    case ObDateTimeType:
    case ObTimestampType:
    case ObDateType:
    case ObTimeType:
    case ObVarcharType:
    case ObCharType:
    // case ObExtendType:
    case ObUnknownType:
    // TODO@hanhui text share with varchar temporarily
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
      func_ = abs_others_double;
      break;
    case ObHexStringType:
      func_ = abs_hexstring;
      break;
    case ObBitType:
      func_ = abs_bit;
      break;
    case ObEnumType:
    case ObSetType:
      func_ = abs_enum_set;
      break;
    case ObJsonType:
      func_ = abs_double;
      break;
    default: {
      LOG_ERROR("unexpected param type", K(param_type));
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}

//tinyint, mediumint, smallint, int32
int ObExprAbs::abs_int(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  res.set_int(param.get_int() >= 0 ? param.get_int() : -param.get_int());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//int64
int ObExprAbs::abs_int64(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  if (INT64_MIN == param.get_int()) {
    ret = OB_OPERATE_OVERFLOW; //INT64_MIN时，mysql会返回一个out of range的错误
    LOG_WARN("value out of range", K(ret), K(INT64_MIN), K(param));
  } else {
    res.set_int(param.get_int() >= 0LL ? param.get_int() : -param.get_int());
  }
  UNUSED(expr_ctx);
  return ret;
}
//utiniyint, umediumint, usmallint
int ObExprAbs::abs_uint(ObObj &res,
                    const ObObj &param,
                    ObExprCtx &expr_ctx)
{
  res.set_uint64(static_cast<uint64_t>(param.get_uint32()));
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//uint32 uint64
int ObExprAbs::abs_uint32_uint64(ObObj &res,
                             const ObObj &param,
                             ObExprCtx &expr_ctx)
{
  res.set_uint64(param.get_uint64());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//float
int ObExprAbs::abs_float(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  res.set_float(param.get_float() >= 0.0f ? param.get_float() : -param.get_float());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
int ObExprAbs::abs_float_double(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  res.set_double(static_cast<double>(param.get_float() >= 0.0f ?
                                        param.get_float() :
                                        -param.get_float()));
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//double
int ObExprAbs::abs_double(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  res.set_double(param.get_double() >= 0.0 ? param.get_double() : -param.get_double());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//ufloat
int ObExprAbs::abs_ufloat_udouble(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  res.set_udouble(static_cast<double>(param.get_ufloat()));
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//udouble
int ObExprAbs::abs_udouble(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  res.set_udouble(param.get_udouble());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//number
int ObExprAbs::abs_number(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_number() && !param.is_number_float())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(param), K(ret));
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    LOG_WARN("allocator should not be null");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    number::ObNumber param_nmb = param.get_number();
    number::ObNumber res_nmb;
    if (param_nmb.is_negative()) {
      ret = param_nmb.negate(res_nmb, *expr_ctx.calc_buf_);
    } else {
      ret = res_nmb.from(param_nmb, *expr_ctx.calc_buf_);
    }
    if (OB_SUCC(ret)) {
      res.set_number(res_nmb);
    }
  }
  return ret;
}
//unumber
int ObExprAbs::abs_unumber(ObObj &res,
                      const ObObj &param,
                      ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  TYPE_CHECK(param, ObUNumberType);
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    LOG_WARN("allocator should not be null");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    number::ObNumber param_nmb = param.get_unumber();
    number::ObNumber res_nmb;
    if (OB_FAIL(res_nmb.from(param_nmb, *expr_ctx.calc_buf_))) {
      LOG_WARN("deep copy number failed", K(ret), K(param_nmb));
    } else {
      res.set_unumber(res_nmb);
    }
  }
  return ret;
}

//null
int ObExprAbs::abs_null(ObObj &res,
                      const ObObj &param,
                      ObExprCtx &expr_ctx)
{
  res.set_null();
  UNUSED(param);
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}

//others. (datetime time varchar, etc)
int ObExprAbs::abs_others_double(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  double value = 0.0;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_GET_DOUBLE_V2(param, value);
  if (OB_SUCC(ret)) {
    res.set_double(value >= 0.0 ? value : -value);
  }
  return ret;
}

//others for oracle. (datetime time varchar, etc)
int ObExprAbs::abs_others_number(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  number::ObNumber value;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_GET_NUMBER_V2(param, value);
  if (OB_SUCC(ret)) {
    res.set_number(value.is_negative() ? value.negate() : value);
  }
  return ret;
}

int ObExprAbs::abs_hexstring(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  double value = 0.0;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_GET_DOUBLE_V2(param, value);
  if (OB_SUCC(ret)) {
    //udouble, not double. compatible with mysql.
    res.set_udouble(value >= 0.0 ? value : -value);
  }
  return ret;
}

int ObExprAbs::abs_year(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0.0;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_GET_UINT64_V2(param, value);
  if (OB_SUCC(ret)) {
    res.set_uint64(value);//abs(year) returns uint64. compatible with mysql.
  }
  return ret;
}

//bit
int ObExprAbs::abs_bit(ObObj &res,
                       const ObObj &param,
                       ObExprCtx &expr_ctx)
{
  res.set_uint64(param.get_bit());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//enum_set
int ObExprAbs::abs_enum_set(ObObj &res,
                            const ObObj &param,
                            ObExprCtx &expr_ctx)
{
  res.set_uint64(param.get_uint64());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}

int ObExprAbs::cg_expr(ObExprCGCtx &ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(T_OP_ABS != rt_expr.type_)
      || OB_ISNULL(rt_expr.args_)
      || OB_UNLIKELY(rt_expr.arg_cnt_ !=  1)
      || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(rt_expr.args_[0]->datum_meta_.type_ >= ObMaxType)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg type for abs", K(ret));
  } else {
    rt_expr.eval_func_ = abs_funcs[rt_expr.args_[0]->datum_meta_.type_];
  }
  return ret;
}

ObObjType ObExprAbs::calc_param_type(const ObObjType orig_param_type,
                                     const bool is_oracle_mode)
{
  ObObjType calc_type = ObMaxType;
  if (is_oracle_mode) {
    switch (orig_param_type)
    {
     case ObNullType: {
       calc_type = ObNullType;
        break;
     }
     case ObFloatType: {
       calc_type = ObFloatType;
       break;
     }
     case ObDoubleType: {
       calc_type = ObDoubleType;
       break;
     }
     case ObNumberType:
     case ObTinyIntType:
     case ObSmallIntType:
     case ObInt32Type:
     case ObIntType:
     case ObNumberFloatType:
     case ObTimestampTZType:
     case ObTimestampLTZType:
     case ObTimestampNanoType:
     case ObCharType:
     case ObVarcharType:
     case ObIntervalDSType:
     case ObIntervalYMType:
     case ObNVarchar2Type:
     case ObNCharType:
     case ObURowIDType: {
       calc_type = ObNumberType;
       break;
     }
     default: {
       // do nothing
       break;
     }
    }
  } else {
    switch (orig_param_type)
    {
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      calc_type = ObIntType;
      break;
    }
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
      calc_type = ObUInt64Type;
      break;
    }
    case ObFloatType:
    case ObDoubleType: {
      calc_type = ObDoubleType;
      break;
    }
    case ObUFloatType:
    case ObUDoubleType: {
      calc_type = ObUDoubleType;
      break;
    }
    case ObNumberType: {
      calc_type = ObNumberType;
      break;
    }
    case ObUNumberType: {
      calc_type = ObUNumberType;
      break;
    }
    case ObNullType: {
      calc_type = ObNullType;
      break;
    }
    case ObYearType: {
      calc_type = ObUInt64Type;
      break;
    }
    case ObDateTimeType:
    case ObTimestampType:
    case ObDateType:
    case ObTimeType:
    case ObVarcharType:
    case ObCharType:
    case ObUnknownType:
    case ObHexStringType:
    case ObTextType:
    case ObTinyTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      calc_type = ObDoubleType;
      break;
    }
    case ObBitType: {
      calc_type = ObUInt64Type;
      break;
    }
    case ObEnumType:
    case ObSetType:
    case ObJsonType: {
      calc_type = ObDoubleType;
      break;
    }
    default: {
      break;
    }
    }
  }
  return calc_type;
}

} // namespace sql
} // namespace oceanbase
