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
#include "sql/engine/expr/ob_expr_nvl.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_is.h"
#include <math.h>

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObExprNvlUtil::calc_result_type(ObExprResType &type,

                                     ObExprResType &type1,
                                     ObExprResType &type2,
                                     ObExprTypeCtx &type_ctx)
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprPromotionUtil::get_nvl_type(type, type1, type2))) {
    LOG_WARN("get nvl type failed", K(ret), K(type1), K(type2));
  } else if (OB_UNLIKELY(type.is_invalid())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else if (ob_is_string_type(type.get_type())) {
    ObCollationLevel res_cs_level = CS_LEVEL_INVALID;
    ObCollationType res_cs_type = CS_TYPE_INVALID;
    if (lib::is_oracle_mode()) {
      res_cs_level = CS_LEVEL_IMPLICIT;
      if (type1.is_string_type()) {
        res_cs_type = type1.get_collation_type();
      } else {
        res_cs_type = type_ctx.get_session()->get_dtc_params().nls_collation_;
      }
    } else {
      if (OB_FAIL(ObCharset::aggregate_collation(type1.get_collation_level(), type1.get_collation_type(),
                                            type2.get_collation_level(), type2.get_collation_type(),
                                            res_cs_level, res_cs_type))) {
        LOG_WARN("aggregate collation failed", K(ret), K(type1), K(type2));
      }
    }
    if (OB_SUCC(ret)) {
      type.set_collation_level(res_cs_level);
      type.set_collation_type(res_cs_type);
    }
  } else if (ob_is_raw(type.get_type())) {
    type.set_collation_level(CS_LEVEL_NUMERIC);
    type.set_collation_type(CS_TYPE_BINARY);
  } else if (ob_is_json(type.get_type())) {
    type.set_collation_level(CS_LEVEL_IMPLICIT);
  } else if (ob_is_geometry(type.get_type())) {
    type.set_geometry();
  }
  if (OB_SUCC(ret)) {
    type.set_length(MAX(type1.get_length(), type2.get_length()));
    // flag. if both type1 and type2 are NULL, type is null
    if (type1.has_result_flag(NOT_NULL_FLAG) || type2.has_result_flag(NOT_NULL_FLAG)) {
      type.set_result_flag(NOT_NULL_FLAG);
    }
  }

  if (OB_SUCC(ret) && ob_is_user_defined_sql_type(type.get_type())) {
    bool is_one_type_null = ob_is_null(type1.get_type()) || ob_is_null(type2.get_type());
    ObExprResType &udt_type = !type1.is_null() ? type1 : type2;
    if (type1.is_xml_sql_type() || type2.is_xml_sql_type()) {
      type.set_subschema_id(ObXMLSqlType);
    } else if ((is_one_type_null || type1.get_udt_id() == type2.get_udt_id())
                && udt_type.get_udt_id() != OB_INVALID_ID) {
      type.set_subschema_id(udt_type.get_subschema_id());
      type.set_udt_id(udt_type.get_udt_id());
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported udt for nvl", K(ret), K(type1), K(type2));
    }
  }
  return ret;
}

ObExprNvl::ObExprNvl(ObIAllocator &alloc)
 : ObFuncExprOperator(alloc, T_FUN_SYS_NVL, N_NVL, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprNvl::~ObExprNvl()
{}

void set_calc_type(const ObExprResType &in_type, ObExprResType &out_type)
{
  out_type.set_calc_meta(in_type.get_obj_meta());
  out_type.set_calc_accuracy(in_type.get_accuracy());
  out_type.set_calc_collation_type(in_type.get_collation_type());
  out_type.set_calc_collation_level(in_type.get_collation_level());
}

int ObExprNvl::calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session =
    dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  if (OB_FAIL(ObExprNvlUtil::calc_result_type(type, type1, type2, type_ctx))) {
    LOG_WARN("calc_result_type failed", K(ret), K(type1), K(type2));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast basic session to sql session failed", K(ret));
  } else {
    // accuracy.
    if (type.get_type() == type1.get_type()) {
      type.set_accuracy(type1.get_accuracy());
    } else {
      type.set_accuracy(type2.get_accuracy());
    }
    ObScale scale1 = type1.is_null() ? 0 : type1.get_scale();
    ObScale scale2 = type2.is_null() ? 0 : type2.get_scale();
    if (-1 != scale1 && -1 != scale2) {
      type.set_scale(static_cast<ObScale>(max(scale1, scale2)));
    } else {
      type.set_scale(-1);
    }
    if (lib::is_mysql_mode() && SCALE_UNKNOWN_YET != type.get_scale()) {
      if (ob_is_real_type(type.get_type())) {
        type.set_precision(static_cast<ObPrecision>(ObMySQLUtil::float_length(type.get_scale())));
      } else if (ob_is_number_or_decimal_int_tc(type.get_type())) {
        const int16_t intd1 = type1.get_precision() - type1.get_scale();
        const int16_t intd2 = type2.get_precision() - type2.get_scale();
        const int16_t prec = MIN(OB_MAX_DECIMAL_POSSIBLE_PRECISION, MAX(type.get_precision(), MAX(intd1, intd2) + type.get_scale()));
        type.set_precision(static_cast<ObPrecision>(prec));
      }
    }
    type.set_length(MAX(type1.get_length(), type2.get_length()));
    //对于 int 和uint64的混合类型，需要提升类型至decimal
    if (lib::is_mysql_mode()
        && (ObUInt64Type == type1.get_type() || ObUInt64Type == type2.get_type())
        && ObIntType == type.get_type()) {
      bool enable_decimalint = false;
      if (OB_FAIL(ObSQLUtils::check_enable_decimalint(session, enable_decimalint))) {
        LOG_WARN("fail to check_enable_decimalint",
            K(ret), K(session->get_effective_tenant_id()));
      } else if (enable_decimalint) {
        type.set_type(ObDecimalIntType);
      } else {
        type.set_type(ObNumberType);
      }
      type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].get_accuracy());
    }
    if (OB_SUCC(ret)) {
      if (ObDecimalIntType == type.get_type()) {
        type.set_scale(static_cast<ObScale>(max(scale1, scale2)));
        type.set_precision(max(type1.get_precision() - type1.get_scale(),
                               type2.get_precision() - type2.get_scale())
                         + max(type1.get_scale(), type2.get_scale()));
        type1.set_calc_accuracy(type.get_accuracy());
        type2.set_calc_accuracy(type.get_accuracy());
      }
      // enumset.
      const bool type1_is_enumset = ob_is_enumset_tc(type1.get_type());
      const bool type2_is_enumset = ob_is_enumset_tc(type2.get_type());
      if (type1_is_enumset || type2_is_enumset) {
        ObObjType calc_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[type.get_type()]];
        if (OB_UNLIKELY(ObMaxType == calc_type)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "invalid type of parameter ", K(type1), K(type2), K(ret));
        } else if (ObVarcharType == calc_type) {
          if (type1_is_enumset) {
            type1.set_calc_type(calc_type);
            type1.set_calc_collation_type(ObCharset::get_system_collation());
          } else {
            set_calc_type(type, type1);
          }
          if (type2_is_enumset) {
            type2.set_calc_type(calc_type);
          } else {
            set_calc_type(type, type2);
          }
        } else {
          set_calc_type(type, type1);
          set_calc_type(type, type2);
        }
      } else {
        set_calc_type(type, type1);
        set_calc_type(type, type2);
      }
    }
  }
  return ret;
}


ObExprOracleNvl::ObExprOracleNvl(ObIAllocator &alloc)
 : ObFuncExprOperator(alloc, T_FUN_SYS_NVL, N_NVL, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprOracleNvl::~ObExprOracleNvl()
{}

int ObExprOracleNvl::calc_result_type2(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprResType &type2,
                                       ObExprTypeCtx &type_ctx) const
{
  return calc_nvl_oralce_result_type(type, type1, type2, type_ctx);
}

int ObExprOracleNvl::calc_nvl_oralce_result_type(ObExprResType &type,
                                                 ObExprResType &type1,
                                                 ObExprResType &type2,
                                                 ObExprTypeCtx &type_ctx)
{
 /*
  create table t1 (col_num1 decimal(10, 2), col_num2 decimal(10, 8),
                   col_str1 varchar(20 char), col_str2 varchar2(60 byte),
                   col_tz timestamp(3) with time zone,
                   col_ltz timestamp(6) with local time zone);
  create view tmp as
  select nvl(col_str1, col_num1) s1_n1,
         nvl(col_str1, col_num2) s1_n2,
         nvl(col_str2, col_num1) s2_n1,
         nvl(col_str2, col_num2) s2_n2,
         nvl(col_str1, col_str2) s1_s2,
         nvl(col_tz, col_ltz) tz_ltz,
         nvl(NULL, col_ltz) null_ltz
    from t1;
  desc tmp;

  SQL> desc tmp;
   Name            Null?    Type
   --------------- -------- ----------------------------
   S1_N1                    VARCHAR2(40 CHAR)
   S1_N2                    VARCHAR2(40 CHAR)
   S2_N1                    VARCHAR2(60)
   S2_N2                    VARCHAR2(60)
   S1_S2                    VARCHAR2(80)
   TZ_LTZ                   TIMESTAMP(3) WITH TIME ZONE
   NULL_LTZ                 TIMESTAMP(6) WITH LOCAL TIME ZONE
  */
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprNvlUtil::calc_result_type(type, type1, type2, type_ctx))) {
    LOG_WARN("calc_result_type2 failed", K(ret), K(type1), K(type2));
  } else {
    if (type.is_oracle_decimal()) {
      // unknow precision and scale
      type.set_precision(PRECISION_UNKNOWN_YET);
      type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
    } else if (type.is_otimestamp_type()
               || type.is_interval_ym()
               || type.is_interval_ds()) {
      /*
       * for otimestamp:
       * if type is otimestamp, it must be one of these situations:
       * 1. type1 is otimestamp, or
       * 2. type1 is const NULL, and type2 is otimestamp.
       * but the index of NVL_TYPE_PROMOTION_ORACLE is type class, so it can't tell us which
       * otimestamp type should be used (timestamp / timestamp with time zone / timestamp with
       * loacl time zone), we must adjust here.
       * see the sql results above.
       *
       * the same case for interval type class
       */
      ObExprResType &res_type = !type1.is_null() ? type1 : type2;
      type.set_type(res_type.get_type());
      type.set_accuracy(res_type.get_accuracy());
      type2.set_calc_accuracy(res_type.get_accuracy());
    } else if (type.is_character_type()) {
      ObSEArray<ObExprResType*, 2, ObNullAllocator> params;
      // the result type and collation is only determined by type1 unless type1 is const NULL.
      if (!type1.is_null()) {
        OZ (params.push_back(&type1));
        OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, type,
                                                    PREFER_VAR_LEN_CHAR));
        OZ (params.push_back(&type2));
      } else {
        OZ (params.push_back(&type2));
        OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, type,
                                                    PREFER_VAR_LEN_CHAR));
        OZ (params.push_back(&type1));
      }
      // the result length semantics is determined by all params
      OZ (aggregate_length_semantics_oracle(*type_ctx.get_session(), params, type));
      OZ (deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, params));
      OX (type.set_length(MAX(type1.get_calc_length(), type2.get_calc_length())));
    } else if (lib::is_oracle_mode() && type.is_ext()) {
      CK (type1.get_udt_id() == type2.get_udt_id());
      OX (type.set_udt_id(type1.get_udt_id()));
    } else if (type.is_temporal_type()) {
      type.set_scale(0);
    } else if (type.is_user_defined_sql_type()) {
      // only two situations:
      // 1. both type1 and type2 are UDT, and have same udt_id
      // 2. one of type1 and type2 is null, need to set accuracy for udt_id_
      ObExprResType &null_type = type1.is_null() ? type1 : type2;
      if (null_type.is_null()) {
        null_type.set_calc_accuracy(type.get_accuracy());
      }
    }
    /*
     *
     * select nvl(0, 'hello') from dual;
     * the sql above will return error: ORA-01722: invalid number.
     * we must execute necessary cast operation before we determine which obj should be returned,
     * so we call set_calc_meta() here to tell ObExprOperator::call() "cast all parameters for me".
     */
    type1.set_calc_meta(type);
    type2.set_calc_meta(type);
  }
  return ret;
}

ObExprNaNvl::ObExprNaNvl(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_NANVL, T_NANVL, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprNaNvl::~ObExprNaNvl()
{}

int ObExprNaNvl::calc_result_type2(ObExprResType &type,
                                   ObExprResType &type1,
                                   ObExprResType &type2,
                                   ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret =  OB_SUCCESS;
  if (ob_is_lob_tc(type1.get_type()) || ob_is_oracle_datetime_tc(type1.get_type())
     || ob_is_rowid_tc(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "NUMBER",
                  ob_obj_type_str(type1.get_type()));
    LOG_WARN("invalid type of parameter", K(ret), K(type1));
  } else if (ob_is_lob_tc(type2.get_type()) || ob_is_oracle_datetime_tc(type2.get_type())
            || ob_is_rowid_tc(type2.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "NUMBER",
                  ob_obj_type_str(type2.get_type()));
    LOG_WARN("invalid type of parameter", K(ret), K(type2));
  } else if (ObTinyIntType == type1.get_type() || ObTinyIntType == type2.get_type()) {
    ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("PLS-00306: wrong number or types of arguments in call", K(ret));
    LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, static_cast<int>(strlen(get_name())), get_name());
  }
  if (OB_SUCC(ret)) {
    type.set_type(ObNumberType);
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
    if (ob_is_double_type(type1.get_type()) || ob_is_double_type(type2.get_type())) {
      type.set_type(ObDoubleType);
    } else if (ob_is_float_type(type1.get_type()) || ob_is_float_type(type2.get_type())) {
      type.set_type(ObFloatType);
    }
    type1.set_calc_type(type.get_type());
    type2.set_calc_type(type.get_type());
  }
  return ret;
}

int ObExprNvlUtil::calc_nvl_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // nvl(arg0, arg1)
  ObDatum *arg0 = NULL;
  ObDatum *arg1 = NULL;

  if (OB_FAIL(expr.eval_param_value(ctx, arg0, arg1))) {
    LOG_WARN("eval args failed", K(ret));
  } else if (!(arg0->is_null())) {
    res_datum.set_datum(*arg0);
  } else {
    res_datum.set_datum(*arg1);
  }
  return ret;
}

int ObExprNvlUtil::calc_nvl_expr_batch(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const int64_t batch_size) {
  LOG_DEBUG("eval nvl batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum* results = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatumVector args0;
  ObDatumVector args1;
  if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, batch_size, args0,
                                          args1))) {
    LOG_WARN("eval batch args failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      eval_flags.set(i);
      ObDatum *arg0 = args0.at(i);
      ObDatum *arg1 = args1.at(i);
      if (!(arg0->is_null())) {
        results[i].set_datum(*arg0);
      } else {
        results[i].set_datum(*arg1);
      }
    }
  }

  return ret;
}

int ObExprNvlUtil::calc_nvl_expr2(const ObExpr &expr, ObEvalCtx &ctx,
                                  ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // nvl(arg0, arg1, arg2)
  ObDatum *arg0 = NULL;
  ObDatum *arg1 = NULL;
  ObDatum *arg2 = NULL;

  if (OB_FAIL(expr.eval_param_value(ctx, arg0, arg1, arg2))) {
    LOG_WARN("eval args failed", K(ret));
  } else if (!(arg0->is_null())) {
    res_datum.set_datum(*arg1);
  } else {
    res_datum.set_datum(*arg2);
  }
  return ret;
}

int ObExprNvl::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprNvlUtil::calc_nvl_expr;
  rt_expr.eval_batch_func_ = ObExprNvlUtil::calc_nvl_expr_batch;
  return ret;
}

int ObExprOracleNvl::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprNvlUtil::calc_nvl_expr;
  rt_expr.eval_batch_func_ = ObExprNvlUtil::calc_nvl_expr_batch;
  return ret;
}

int ObExprNaNvl::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  if (rt_expr.arg_cnt_ != 2 || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of children is not 2 or children is null", K(ret), K(rt_expr.arg_cnt_),
                                                              K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
  } else {
    rt_expr.eval_func_ = eval_nanvl;
    rt_expr.eval_batch_func_ = eval_nanvl_batch;
  }
  return ret;
}
// nanvl computational abstraction.
int ObExprNaNvl::eval_nanvl_util(const ObExpr &expr, ObDatum &expr_datum, ObDatum *param1, ObDatum *param2, bool &ret_bool)
{
  int ret = OB_SUCCESS;
  if (expr.args_[0]->datum_meta_.type_ != ObFloatType
             && expr.args_[0]->datum_meta_.type_ != ObDoubleType) {
    expr_datum.set_datum(*param1);
  } else {
    if (OB_FAIL(ObExprIs::is_infinite_nan(expr.args_[0]->datum_meta_.type_, param1,
                                ret_bool, ObExprIs::Ieee754::NAN_VALUE))) {
      LOG_WARN("calc_is_infinite unexpect error", K(ret));
    } else {
      if (ret_bool) {
        expr_datum.set_datum(*param2);
      } else {
        expr_datum.set_datum(*param1);
      }
    }
  }
  return ret;
}

int ObExprNaNvl::eval_nanvl(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param1 = NULL;
  ObDatum *param2 = NULL;
  bool ret_bool = false;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("eval first param failed", K(ret));
  } else if (param1->is_null() && ObOBinDoubleType != ob_obj_type_to_oracle_type(expr.args_[0]->datum_meta_.get_type())) {
    expr_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param2))) {
    LOG_WARN("eval second param failed", K(ret));
  } else if (param1->is_null() || (param2->is_null() && ObDoubleType != expr.args_[1]->datum_meta_.get_type())) {
    expr_datum.set_null();
  } else {
    if (OB_FAIL(eval_nanvl_util(expr, expr_datum, param1, param2, ret_bool))){
      LOG_WARN("eval_nanvl unexpect error", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObExprNaNvl::eval_nanvl_batch(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObBitVector &skip,
                                  const int64_t batch_size) {
  LOG_DEBUG("eval nanvl batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum* results = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[0] failed", K(ret));
  } else {
    ObBitVector &my_skip = expr.get_pvt_skip(ctx);
    my_skip.deep_copy(skip, batch_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (my_skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      ObDatum *param1 = NULL;
      param1 = &expr.args_[0]->locate_expr_datum(ctx, i);
      if (param1->is_null() && ObOBinDoubleType != ob_obj_type_to_oracle_type(expr.args_[0]->datum_meta_.get_type())) {
        results[i].set_null();
        eval_flags.set(i);
        my_skip.set(i);
      }
    }
    if (OB_FAIL(expr.args_[1]->eval_batch(ctx, my_skip, batch_size))) {
      LOG_WARN("eval args_[1] failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        if (my_skip.at(i) || eval_flags.at(i)) {
          continue;
        }
        ObDatum *param1 = NULL;
        ObDatum *param2 = NULL;
        bool ret_bool = false;
        param1 = &expr.args_[0]->locate_expr_datum(ctx, i);
        param2 = &expr.args_[1]->locate_expr_datum(ctx, i);
        eval_flags.set(i);
        if (param1->is_null() || (param2->is_null() && ObDoubleType != expr.args_[1]->datum_meta_.get_type())) {
          results[i].set_null();
        } else if (OB_FAIL(eval_nanvl_util(expr, results[i], param1, param2, ret_bool))){
          LOG_WARN("eval_nanvl unexpect error", K(ret));
        } else {
          // do nothing
        }
      }
    }
  }

  return ret;
}

}//namespace sql
}//namespace oceanbase
