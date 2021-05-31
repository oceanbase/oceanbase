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

namespace oceanbase {
using namespace common;
namespace sql {

int ObExprNvlUtil::calc_result_type(ObExprResType& type,

    ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx)
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprPromotionUtil::get_nvl_type(type, type1, type2))) {
    LOG_WARN("get nvl type failed", K(ret), K(type1), K(type2));
  } else if (OB_UNLIKELY(type.is_invalid())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  if (OB_SUCC(ret) && ob_is_string_type(type.get_type())) {
    ObCollationLevel res_cs_level = CS_LEVEL_INVALID;
    ObCollationType res_cs_type = CS_TYPE_INVALID;
    if (share::is_oracle_mode()) {
      res_cs_level = CS_LEVEL_IMPLICIT;
      if (type1.is_string_type()) {
        res_cs_type = type1.get_collation_type();
      } else {
        res_cs_type = type_ctx.get_session()->get_dtc_params().nls_collation_;
      }
    } else {
      if (OB_FAIL(ObCharset::aggregate_collation(type1.get_collation_level(),
              type1.get_collation_type(),
              type2.get_collation_level(),
              type2.get_collation_type(),
              res_cs_level,
              res_cs_type))) {
        LOG_WARN("aggregate collation failed", K(ret), K(type1), K(type2));
      }
    }
    if (OB_SUCC(ret)) {
      type.set_collation_level(res_cs_level);
      type.set_collation_type(res_cs_type);
    }
  } else if (OB_SUCC(ret) && ob_is_raw(type.get_type())) {
    type.set_collation_level(CS_LEVEL_NUMERIC);
    type.set_collation_type(CS_TYPE_BINARY);
  }
  if (OB_SUCC(ret)) {
    type.set_length(MAX(type1.get_length(), type2.get_length()));
    // flag. if both type1 and type2 are NULL, type is null
    if (type1.has_result_flag(OB_MYSQL_NOT_NULL_FLAG) || type2.has_result_flag(OB_MYSQL_NOT_NULL_FLAG)) {
      type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
    }
  }
  return ret;
}

ObExprNvl::ObExprNvl(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_NVL, N_NVL, 2, NOT_ROW_DIMENSION)
{}

ObExprNvl::~ObExprNvl()
{}

void set_calc_type(const ObExprResType& in_type, ObExprResType& out_type)
{
  out_type.set_calc_meta(in_type.get_obj_meta());
  out_type.set_calc_accuracy(in_type.get_accuracy());
  out_type.set_calc_collation_type(in_type.get_collation_type());
  out_type.set_calc_collation_level(in_type.get_collation_level());
}

int ObExprNvl::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprNvlUtil::calc_result_type(type, type1, type2, type_ctx))) {
    LOG_WARN("calc_result_type failed", K(ret), K(type1), K(type2));
  } else {
    // accuracy.
    if (type.get_type() == type1.get_type()) {
      type.set_accuracy(type1.get_accuracy());
    } else {
      type.set_accuracy(type2.get_accuracy());
    }
    ObScale scale1 = type1.get_scale();
    ObScale scale2 = type2.get_scale();
    if (-1 != scale1 && -1 != scale2) {
      type.set_scale(static_cast<ObScale>(max(scale1, scale2)));
    } else {
      type.set_scale(-1);
    }
    // For the mixed type of int and uint64, the type needs to be promoted to decimal
    if (share::is_mysql_mode() && (ObUInt64Type == type1.get_type() || ObUInt64Type == type2.get_type()) &&
        ObIntType == type.get_type()) {
      type.set_type(ObNumberType);
      type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].get_accuracy());
    }
    // enumset.
    const bool type1_is_enumset = ob_is_enumset_tc(type1.get_type());
    const bool type2_is_enumset = ob_is_enumset_tc(type2.get_type());
    const ObSQLSessionInfo* session = dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast basic session to sql session failed", K(ret));
    } else if (type1_is_enumset || type2_is_enumset) {
      ObObjType calc_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[type.get_type()]];
      if (OB_UNLIKELY(ObMaxType == calc_type)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "invalid type of parameter ", K(type1), K(type2), K(ret));
      } else if (ObVarcharType == calc_type) {
        if (type1_is_enumset) {
          type1.set_calc_type(calc_type);
          type1.set_calc_collation_type(ObCharset::get_system_collation());
        } else if (session->use_static_typing_engine()) {
          set_calc_type(type, type1);
        }
        if (type2_is_enumset) {
          type2.set_calc_type(calc_type);
        } else if (session->use_static_typing_engine()) {
          set_calc_type(type, type2);
        }
      } else if (session->use_static_typing_engine()) {
        set_calc_type(type, type1);
        set_calc_type(type, type2);
      }
    } else if (session->use_static_typing_engine()) {
      set_calc_type(type, type1);
      set_calc_type(type, type2);
    }
  }
  return ret;
}

int ObExprNvl::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  const ObObj* p_res = NULL;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument. allocator or session is NULL", K(ret), K(expr_ctx.calc_buf_), K(expr_ctx.my_session_));
  } else {
    p_res = obj1.is_null() ? &obj2 : &obj1;
    // no necessary to check p_res is NULL or not
    if (!p_res->is_null() && p_res->get_type() != get_result_type().get_type()) {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      cast_ctx.dest_collation_ = get_result_type().get_collation_type();
      if (OB_FAIL(ObObjCaster::to_type(get_result_type().get_type(), cast_ctx, *p_res, result))) {
        LOG_WARN("cast failed", K(ret), K(get_result_type().get_type()), K(*p_res));
      }
    } else {
      result = *p_res;
    }
  }
  return ret;
}

ObExprOracleNvl::ObExprOracleNvl(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_NVL, N_NVL, 2, NOT_ROW_DIMENSION)
{}

ObExprOracleNvl::~ObExprOracleNvl()
{}

int ObExprOracleNvl::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  return calc_nvl_oralce_result_type(type, type1, type2, type_ctx);
}

int ObExprOracleNvl::calc_nvl_oralce_result_type(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx)
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
    } else if (type.is_otimestamp_type() || type.is_interval_ym() || type.is_interval_ds()) {
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
      ObExprResType& res_type = !type1.is_null() ? type1 : type2;
      type.set_type(res_type.get_type());
      type.set_accuracy(res_type.get_accuracy());
      type2.set_calc_accuracy(res_type.get_accuracy());
    } else if (type.is_varchar()) {
      ObLengthSemantics length_semantics = type1.get_length_semantics();
      if (type2.is_number()) {
        /*
         * when type is varchar and type2 is number, type length is determined by type1 length
         * and the DEFAULT length of ObNumberType (which is 40 in oracle), not type2 length.
         * see the sql results above:
         * the length value of col_num1 and col_num2 are both 11, but s1_n1 length is 40,
         * which is max(col_str1 length, ObNumberType DEFAULT length).
         * s1_n2 / s2_n1 / s2_n2 are similar.
         */
        ObLength default_number_length = 40;
        type.set_length(MAX(type1.get_length(), default_number_length));
      } else if (type2.is_string_type()) {
        /*
         * when type1 and type2 have different length semantics, type semantics should be byte,
         * and the length is of type1 length and type2 length in byte.
         * we need not adjust the length in other condition.
         */
        if (type1.is_null()) {
          length_semantics = type2.get_length_semantics();
        } else if (type1.get_length_semantics() != type2.get_length_semantics()) {
          ObLength length1_byte = 0;
          ObLength length2_byte = 0;
          if (OB_FAIL(type1.get_length_for_meta_in_bytes(length1_byte)) ||
              OB_FAIL(type2.get_length_for_meta_in_bytes(length2_byte))) {
            LOG_WARN("get length in bytes failed", K(ret), K(type1), K(type2));
          } else {
            length_semantics = LS_BYTE;
            type.set_length(MAX(length1_byte, length2_byte));
          }
        }
      }
      type.set_length_semantics(length_semantics);
    }
    /*
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

int ObExprOracleNvl::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  result = obj1.is_null() ? obj2 : obj1;
  return OB_SUCCESS;
}

ObExprNaNvl::ObExprNaNvl(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_NANVL, T_NANVL, 2, NOT_ROW_DIMENSION)
{}

ObExprNaNvl::~ObExprNaNvl()
{}

int ObExprNaNvl::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjType result_type = ObMaxType;
  if (OB_FAIL(ObExprResultTypeUtil::get_nanvl_result_type(result_type, type1.get_type(), type2.get_type()))) {
    LOG_WARN("fail to get_round_result_type", K(ret), K(type1), K(type2));
  } else {
    type.set_type(result_type);
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
    type1.set_calc_type(result_type);
    type1.set_calc_type(result_type);
  }
  return ret;
}

int ObExprNaNvl::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  // TODO::
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  if (obj1.is_null() || obj2.is_null()) {
    result.set_null();
  } else {
    result = obj1;
  }
  return OB_SUCCESS;
}

int ObExprNvlUtil::calc_nvl_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // nvl(arg0, arg1)
  ObDatum* arg0 = NULL;
  ObDatum* arg1 = NULL;

  if (OB_FAIL(expr.eval_param_value(ctx, arg0, arg1))) {
    LOG_WARN("eval args failed", K(ret));
  } else if (!(arg0->is_null())) {
    res_datum.set_datum(*arg0);
  } else {
    res_datum.set_datum(*arg1);
  }
  return ret;
}

int ObExprNvlUtil::calc_nvl_expr2(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // nvl(arg0, arg1, arg2)
  ObDatum* arg0 = NULL;
  ObDatum* arg1 = NULL;
  ObDatum* arg2 = NULL;

  if (OB_FAIL(expr.eval_param_value(ctx, arg0, arg1, arg2))) {
    LOG_WARN("eval args failed", K(ret));
  } else if (!(arg0->is_null())) {
    res_datum.set_datum(*arg1);
  } else {
    res_datum.set_datum(*arg2);
  }
  return ret;
}

int ObExprNvl::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprNvlUtil::calc_nvl_expr;
  return ret;
}

int ObExprOracleNvl::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprNvlUtil::calc_nvl_expr;
  return ret;
}

int ObExprNaNvl::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprNvlUtil::calc_nvl_expr;
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
