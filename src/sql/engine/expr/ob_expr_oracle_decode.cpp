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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/expr/ob_expr_oracle_decode.h"
#include "sql/engine/expr/ob_expr_case.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_arg_case.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprOracleDecode::ObExprOracleDecode(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_ORA_DECODE, N_ORA_DECODE, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
      param_flags_(0)
{
}

ObExprOracleDecode::~ObExprOracleDecode()
{
}

int ObExprOracleDecode::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types_stack,
                                          int64_t param_num,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const int64_t LEAST_PARAM_NUMS = 3;
  const int64_t RESULT_TYPE_INDEX = 2;//In oracle, the result type will only depend on the third param.
  const int64_t CALC_TYPE_INDEX = 1;//In oracle, the calc type will only depend on the second param.
  ObCollationType collation_connection = type_ctx.get_coll_type();
  bool has_default = (param_num % 2 == 0);
  if (OB_ISNULL(types_stack)) {
    LOG_WARN("null type stack", K(types_stack));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(param_num < LEAST_PARAM_NUMS
                      || CALC_TYPE_INDEX < 0 || CALC_TYPE_INDEX >= param_num
                      || RESULT_TYPE_INDEX >= param_num || RESULT_TYPE_INDEX < 0)) {
    LOG_WARN("invalid params", K(param_num), K(RESULT_TYPE_INDEX), K(LEAST_PARAM_NUMS), K(CALC_TYPE_INDEX));
    ret = OB_INVALID_ARGUMENT;
  } else {
    //除了返回值， 其他参数不能为lob类型
    if (types_stack[0].is_lob()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-",
                  ob_obj_type_str(types_stack[0].get_type()));
      LOG_WARN("invalid type of parameter", K(ret), K(types_stack[0]));
    } else if (lib::is_oracle_mode() && types_stack[CALC_TYPE_INDEX].get_type() == ObUserDefinedSQLType) {
      ret = OB_ERR_NO_ORDER_MAP_SQL;
      LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
    }
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i += 2) {
      if (has_default && i == param_num - 1) {
        // ignore default expr when calc calc_type
      } else if (types_stack[i].is_lob()) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-",
                    ob_obj_type_str(types_stack[i].get_type()));
        LOG_WARN("invalid type of parameter", K(ret), K(types_stack[i]));
      }
    }
  }
  if (OB_SUCC(ret)) {
    //deduce result type
    if (OB_UNLIKELY(types_stack[RESULT_TYPE_INDEX].is_null())) {
      type.set_varchar();
      const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
      type.set_length_semantics(default_length_semantics);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_collation_type(collation_connection);
    } else if (ob_is_string_type(types_stack[RESULT_TYPE_INDEX].get_type())) {
      const common::ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session())
              ? type_ctx.get_session()->get_actual_nls_length_semantics()
              : common::LS_BYTE);
      if (lib::is_oracle_mode() && ObCharType == types_stack[RESULT_TYPE_INDEX].get_type()) {
        // 兼容Oracle行为, 如果结果是Char类型, 需要转为Varchar
        type.set_varchar();
        type.set_length_semantics(types_stack[RESULT_TYPE_INDEX].is_varchar_or_char() ?  types_stack[RESULT_TYPE_INDEX].get_length_semantics() : default_length_semantics);
      } else if (lib::is_oracle_mode() && ob_is_nstring(types_stack[RESULT_TYPE_INDEX].get_type())) {
        type.set_nvarchar2();
        type.set_length_semantics(LS_CHAR);
      } else {
        // 保留原mysql下的行为
        type.set_type(types_stack[RESULT_TYPE_INDEX].get_type());
      }
      if (lib::is_oracle_mode() && types_stack[RESULT_TYPE_INDEX].is_varchar()) {
        type.set_length_semantics(types_stack[RESULT_TYPE_INDEX].is_varchar_or_char() ?  types_stack[RESULT_TYPE_INDEX].get_length_semantics() : default_length_semantics);
      }
      type.set_collation_level(types_stack[RESULT_TYPE_INDEX].get_collation_level());
      type.set_collation_type(types_stack[RESULT_TYPE_INDEX].get_collation_type());
    } else if (types_stack[RESULT_TYPE_INDEX].is_enum_or_set()) {
      type.set_type(ObVarcharType);
      type.set_collation_level(types_stack[RESULT_TYPE_INDEX].get_collation_level());
      type.set_collation_type(types_stack[RESULT_TYPE_INDEX].get_collation_type());
    } else if (types_stack[RESULT_TYPE_INDEX].is_raw()) {
      type.set_type(ObRawType);
      type.set_collation_level(CS_LEVEL_NUMERIC);
      type.set_collation_type(CS_TYPE_BINARY);
    } else if (lib::is_mysql_mode() && types_stack[RESULT_TYPE_INDEX].is_integer_type()) {
      bool has_number = false;
      for (int64_t i = RESULT_TYPE_INDEX; i < param_num && !has_number; i += 2 /*skip conditions */) {
        if (ob_is_number_tc(types_stack[i].get_type())) {
          has_number = true;
        }
      }
      if (has_default && !has_number) {
        has_number = ob_is_number_tc(types_stack[param_num - 1].get_type());
      }
      if (has_number) {
        type.set_number();
      } else {
        type.set_type(types_stack[RESULT_TYPE_INDEX].get_type());
      }
    } else {
      type.set_type(types_stack[RESULT_TYPE_INDEX].get_type());
    }
    //deduce calc type
    ObExprResType &calc_type = types_stack[CALC_TYPE_INDEX];
    if (OB_UNLIKELY(calc_type.is_null())) {
      type.set_calc_type(ObVarcharType);
      type.set_calc_collation_level(CS_LEVEL_IMPLICIT);
      type.set_calc_collation_type(collation_connection);
    } else if (calc_type.is_enum_or_set()) {
      type.set_calc_type(ObVarcharType);
      type.set_calc_collation_level(calc_type.get_collation_level());
      type.set_calc_collation_type(calc_type.get_collation_type());
    } else {
      if (lib::is_oracle_mode() && ObCharType == calc_type.get_type()) {
        // 兼容oracle, 入参如果是Char类型, 将统一转为varchar进行计算
        type.set_calc_type(ObVarcharType);
      } else if (lib::is_oracle_mode() && ob_is_nchar(calc_type.get_type())) {
        type.set_calc_type(ObNVarchar2Type);
      } else {
        // 保留原mysql下的行为
        type.set_calc_type(calc_type.get_type());
      }
      type.set_calc_collation_level(calc_type.get_collation_level());
      type.set_calc_collation_type(calc_type.get_collation_type());
      if (ob_is_integer_type(type.get_calc_type())) {
        //除非expr和search都是整型, 否则类型设置为numberic
        if (!ob_is_integer_type(types_stack[0].get_type())) {
          type.set_calc_type(ObNumberType);
        } else {
          for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i += 2) {
            if (has_default && i == param_num - 1) {
              // ignore default expr when calc calc_type
            } else if (!ob_is_integer_type(types_stack[i].get_type())) {
              type.set_calc_type(ObNumberType);
              break;
            }
          }
        }
      }
    }
    /*If the first search-result pair are numeric, then Oracle compares all search-result
    *expressions and the first expr to determine the argument with the highest numeric
    *precedence, implicitly converts the remaining arguments to that datatype, and returns
    *that datatype.*/
    if (lib::is_oracle_mode() && ob_is_oracle_numeric_type(type.get_calc_type())) {
      for (int64_t i = 0; OB_SUCC(ret)
                          && ObOBinDoubleType != ob_obj_type_to_oracle_type(type.get_calc_type())
                          && i < param_num;
           i += (i == 0 ? 1 : 2)) {
        if (has_default && i == param_num - 1) {
          // ignore default expr when calc calc_type
        } else if (ob_is_numeric_type(types_stack[i].get_type())) {
          if (ObOBinDoubleType == ob_obj_type_to_oracle_type(types_stack[i].get_type())) {
            type.set_calc_type(ObDoubleType);
          } else if (ObOBinFloatType == ob_obj_type_to_oracle_type(types_stack[i].get_type())) {
            type.set_calc_type(ObFloatType);
          } else if (ObNumberType == types_stack[i].get_type()
                     && !ob_is_float_type(type.get_calc_type())) {
            type.set_calc_type(ObNumberType);
          } else {
            //integer type is ignored
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (type.is_number()) {
      if (lib::is_oracle_mode()) {
        type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
      } else {
        ObExprResTypes res_types;
        for (int64_t i = RESULT_TYPE_INDEX; OB_SUCC(ret) && i < param_num; i += 2) {
          if (OB_FAIL(res_types.push_back(types_stack[i]))) {
            LOG_WARN("fail to push back res type", K(ret));
          }
        }
        if (has_default) {
          if (OB_FAIL(res_types.push_back(types_stack[param_num - 1]))) {
            LOG_WARN("fail to push back res type", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (res_types.count() == 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result type count", K(ret));
        } else if (OB_FAIL(aggregate_numeric_accuracy_for_merge(type, &res_types.at(0),
                                                                res_types.count(), false))) {
          LOG_WARN("fail to merge numeric accuracy", K(ret));
        }
      }
    } else {
      type.set_scale(SCALE_UNKNOWN_YET);//the scale of res in decode should be calced dynamically during runtime
    }
  }
  if (OB_SUCC(ret)) {
    ObObjType result_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[type.get_type()]];
    if (OB_UNLIKELY(ObMaxType == result_type)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "invalid type of parameter ", K(type), K(ret));
    } else if (ObVarcharType == result_type) {
      for (int64_t i = 2; i < param_num; i += 2 /*skip conditions */) {
        if (types_stack[i].is_enum_or_set()) {
          types_stack[i].set_calc_type(ObVarcharType);
        }
      }
      if (has_default) {
        if (types_stack[param_num - 1].is_enum_or_set()) {
          types_stack[param_num - 1].set_calc_type(ObVarcharType);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObObjType calc_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[type.get_calc_type()]];
    if (OB_UNLIKELY(ObMaxType == calc_type)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "invalid type of parameter ", K(type), K(ret));
    } else if (ObVarcharType == calc_type) {
      if (types_stack[0].is_enum_or_set()) {
        types_stack[0].set_calc_type(ObVarcharType);
      }
      for (int64_t i = 1; i < param_num; i += 2 /*skip conditions */) {
        //here to let enumset wrapper knows
        if (types_stack[i].is_enum_or_set()) {
          types_stack[i].set_calc_type(ObVarcharType);
        }
      }
    } else {/*do nothing*/}
  }
  // 兼容Oracle行为, 预判断隐式转换, 如果不能转, 提前报错
  // decode的隐式转换除了enumset类型可能会通过框架转为string外，其他类型的隐式转换均发生在calc_resultN
  // oracle下没有enumset类型, 因此这里不特别处理enumset
  bool all_literal = true; // 记录下是否所有的入参都是literal
  if (lib::is_oracle_mode()) {
    ObObjType calc_type = type.get_calc_type();
    ObObjType result_type = type.get_type();
    ObObjOType o_calc_type = ob_obj_type_to_oracle_type(calc_type);
    ObObjOType o_result_type = ob_obj_type_to_oracle_type(result_type);
    ImplicitCastDirection dir = ImplicitCastDirection::IC_NOT_SUPPORT;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
      all_literal = all_literal & types_stack[i].is_literal();
      ObObjOType o_type = ob_obj_type_to_oracle_type(types_stack[i].get_calc_type());
      if (o_type >= ObOMaxType || o_result_type >= ObOMaxType || o_calc_type >= ObOMaxType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid oracle obj type", K(o_type), K(o_result_type), K(o_calc_type));
      } else {
        if (0 == i || ((i % 2) == 1 && i != param_num - 1)) { // expr and search
          // 通过隐式转换矩阵判断两个类型间是否可以进行转换
          dir = OB_OBJ_IMPLICIT_CAST_DIRECTION_FOR_ORACLE[o_type][o_calc_type];
        } else { // result and default
          dir = OB_OBJ_IMPLICIT_CAST_DIRECTION_FOR_ORACLE[o_type][o_result_type];
        }
        if (ImplicitCastDirection::IC_NOT_SUPPORT == dir) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("invalid oracle type implict cast",
                   K(ret), K(o_calc_type), K(o_result_type), K(o_type), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode() &&type.get_type() == ObUserDefinedSQLType) {
      type.set_subschema_id(ObXMLSqlType);
    }
  }
  if (OB_SUCC(ret)) {
    if (ob_is_otimestamp_type(types_stack[RESULT_TYPE_INDEX].get_type())) {
      type.set_accuracy(types_stack[RESULT_TYPE_INDEX].get_accuracy());
    }
    //deduce string length
    if (ob_is_string_type(type.get_type()) || ob_is_raw(type.get_type())) {
      ObLength len = -1;
      if (is_oracle_mode() && (ob_is_string_tc(type.get_type()) || ob_is_raw(type.get_type()))) {
        ObLength deduced_len = -1;
        if (OB_ISNULL(type_ctx.get_session())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session is NULL", K(ret));
        }
        for (int64_t i = 2; OB_SUCC(ret) && i < param_num; i += 2 /*skip conditions */) {
          if (OB_FAIL(ObExprResultTypeUtil::deduce_max_string_length_oracle(type_ctx.get_session()->get_dtc_params(),
                                  types_stack[i], type, deduced_len, type.get_length_semantics()))) {
            LOG_WARN("fail to deduce max string length", K(ret));
          } else if (deduced_len > len) {
            len = deduced_len;
          }
        }
        if (OB_SUCC(ret) && has_default) {
          if (OB_FAIL(ObExprResultTypeUtil::deduce_max_string_length_oracle(type_ctx.get_session()->get_dtc_params(),
                                  types_stack[param_num - 1], type, deduced_len, type.get_length_semantics()))) {
            LOG_WARN("fail to deduce max string length", K(ret));
          } else if (deduced_len > len) {
            len = deduced_len;
          }
        }
      } else {
        for (int64_t i = 2; i < param_num; i += 2 /*skip conditions */) {
          if (types_stack[i].get_length() > len) {
            len = types_stack[i].get_length();
          }
        }
        if (has_default) {
          len = static_cast<ObLength>(MAX(types_stack[param_num - 1].get_length(), len));
        }
      }
      if (all_literal && lib::is_oracle_mode()) {
        if (OB_FAIL(calc_result_type_for_literal(type, types_stack, param_num, type_ctx))) {
          LOG_WARN("failed to calc result for literal", K(ret));
        }
      } else {
        type.set_length(len);
      }
    }
  }
  if (OB_SUCC(ret)) {
    types_stack[0].set_calc_meta(type.get_calc_meta());
    types_stack[0].set_calc_accuracy(types_stack[0].get_accuracy());
    for (int64_t i = 1; i < param_num; i += 2) {
      types_stack[i].set_calc_meta(type.get_calc_meta());
      // 参数的calc_acc不能使用type.get_calc_accuracy(),类型推导没有推导calc accuracy信息
      // 参数的calc_acc保持原来的accuracy
      types_stack[i].set_calc_accuracy(types_stack[i].get_accuracy());
    }
    for (int64_t i = 2; i < param_num; i += 2) {
      types_stack[i].set_calc_meta(type.get_obj_meta());
      types_stack[i].set_calc_accuracy(type.get_accuracy());
    }
    if (has_default) {
      types_stack[param_num-1].set_calc_meta(type.get_obj_meta());
      types_stack[param_num-1].set_calc_accuracy(type.get_accuracy());
    }
  }
  return ret;
}

int ObExprOracleDecode::calc_result_type_for_literal(ObExprResType &type,
                                                     ObExprResType *types_stack,
                                                     int64_t param_num,
                                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObj result;
  ObObj *obj_stack = NULL;
  ObArenaAllocator allocator;
  const ObSQLSessionInfo *session = NULL;
  const ObTimeZoneInfo *tz_info = NULL;
  int64_t tz_offset = 0;
  ObExprCtx expr_ctx;
  if (OB_ISNULL(session = static_cast<const ObSQLSessionInfo*>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_ISNULL(tz_info = get_timezone_info(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tz info pointer is null", K(ret));
  } else if (OB_FAIL(get_tz_offset(tz_info, tz_offset))) {
    LOG_WARN("get tz offset failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session, expr_ctx.cast_mode_))) {
    LOG_WARN("failed to get default cast mode", K(ret));
  } else if (OB_ISNULL(obj_stack = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * param_num)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc obj stack", K(ret));
  } else {
    expr_ctx.my_session_ = const_cast<ObSQLSessionInfo *>(session);
    expr_ctx.calc_buf_ = &allocator;
    expr_ctx.tz_offset_ = tz_offset;
    expr_ctx.cast_mode_ &= ~(CM_WARN_ON_FAIL);
    EXPR_DEFINE_CMP_CTX(type.get_calc_meta(), true, expr_ctx);
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    for (int64_t  i = 0; i < param_num; ++i) {
      obj_stack[i] = types_stack[i].get_param();
    }
    if (OB_FAIL(ObExprArgCase::calc_with_cast(result, obj_stack, param_num,
                                              cmp_ctx, cast_ctx, type,
                                              ObExprOracleDecode::get_cmp_type))) {
      // 兼容ORACLE: 忽略掉calc_type阶段的计算错误, 在实际计算时再报
      ret = OB_SUCCESS;
    } else if (result.is_null()) {
      type.set_length(0);
    } else if (!result.is_string_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decode calc literal result is not string", K(ret));
    } else {
      type.set_length(result.get_string().length());
    }
  }
  return ret;
}

int ObExprOracleDecode::no_cast_calc(ObObj &result,
                                     const ObObj *objs_stack,
                                     int64_t param_num) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(objs_stack) || OB_UNLIKELY(param_num < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null, or param num is too small", K(ret), K(objs_stack), K(param_num));
  } else {
    int i = 1;
    for (; OB_SUCC(ret) && i < param_num - 1; i += 2) {
      if (objs_stack[0] == objs_stack[i]) {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (i < param_num - 1) {
        result = objs_stack[i + 1];
      } else if (param_num % 2 == 0) {
        result = objs_stack[param_num - 1]; // match else (default value).
      } else {
        result.set_null();  // no else (null).
      }
    }
  }
  return ret;
}

inline int ObExprOracleDecode::get_cmp_type(ObObjType &type,
                                            const ObObjType &type1,
                                            const ObObjType &type2,
                                            const ObObjType &type3)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 < ObNullType || type2 < ObNullType || type3 < ObNullType
               || type1 >= ObMaxType || type2 >= ObMaxType || type3 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. wrong type", K(type1), K(type2), K(type3));
  } else {
    type = type3;
  }
  return ret;
}

// decode(expr0, search0, res0,
//               search1, res1,
//               def)
int ObExprOracleDecode::eval_decode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  const bool has_def = (expr.arg_cnt_ % 2 == 0) ? true : false;
  bool has_res = false;
  ObDatum *e_0 = NULL;
  ObDatum *search_i = NULL;
  ObDatum *res_i = NULL;
  int64_t res_idx = OB_INVALID_ID;
  ObDatumCmpFuncType cmp_func = NULL;
  if (OB_UNLIKELY(1 != expr.inner_func_cnt_) || OB_ISNULL(expr.inner_functions_) ||
      OB_ISNULL(expr.inner_functions_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, e_0))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else {
    cmp_func = reinterpret_cast<ObDatumCmpFuncType>(expr.inner_functions_[0]);
  }
  if (OB_SUCC(ret) && OB_ISNULL(cmp_func)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cmp_func is NULL", K(ret), K(expr));
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < expr.arg_cnt_ - 1; i += 2) {
    int cmp_ret = 0;
    if (OB_FAIL(expr.args_[i]->eval(ctx, search_i))) {
      LOG_WARN("eval search expr failed", K(ret), K(i));
    } else if (OB_FAIL(cmp_func(*e_0, *search_i, cmp_ret))) {
      LOG_WARN("do cmp failed", K(ret), K(i));
    } else if (cmp_ret == 0) {
      has_res = true;
      res_idx = i + 1;
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (!has_res && has_def) {
      res_idx = expr.arg_cnt_ - 1;
      has_res = true;
    }
    if (has_res) {
      if (OB_UNLIKELY(0 >= res_idx || expr.arg_cnt_ <= res_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected result idx", K(ret), K(expr.arg_cnt_), K(res_idx));
      } else if (OB_FAIL(expr.args_[res_idx]->eval(ctx, res_i))) {
        LOG_WARN("eval result expr failed", K(ret));
      } else {
        res.set_datum(*res_i);
      }
    } else {
      res.set_null();
    }
  }
  return ret;
}

bool ObExprOracleDecode::can_compare_directly(ObObjType type1, ObObjType type2)
{
  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  return (tc1 == tc2 && !(ObDateTimeTC == tc1 && type1 != type2) && !ob_is_enumset_tc(type1));
}

int ObExprOracleDecode::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  // check all type is ok
  bool has_def = (rt_expr.arg_cnt_ % 2 == 0);
  const ObDatumMeta &cmp_meta = rt_expr.args_[0]->datum_meta_;
  const ObDatumMeta &res_meta = rt_expr.datum_meta_;
  CK(3 <= rt_expr.arg_cnt_);
  for (int64_t i = 1; OB_SUCC(ret) && i < rt_expr.arg_cnt_ - 1; i += 2) {
    CK(cmp_meta.type_ == rt_expr.args_[i]->datum_meta_.type_);
    CK(cmp_meta.cs_type_ == rt_expr.args_[i]->datum_meta_.cs_type_);
  }
  for (int64_t i = 2; OB_SUCC(ret) && i < rt_expr.arg_cnt_; i += 2) {
    CK(res_meta.type_ == rt_expr.args_[i]->datum_meta_.type_);
    CK(res_meta.cs_type_ == rt_expr.args_[i]->datum_meta_.cs_type_);
  }
  if (OB_SUCC(ret) && has_def) {
    CK(res_meta.type_ == rt_expr.args_[rt_expr.arg_cnt_-1]->datum_meta_.type_);
    CK(res_meta.cs_type_ == rt_expr.args_[rt_expr.arg_cnt_-1]->datum_meta_.cs_type_);
  }

  OV(OB_NOT_NULL(rt_expr.inner_functions_ =
        reinterpret_cast<void**>(expr_cg_ctx.allocator_->alloc(sizeof(void*)))),
      OB_ALLOCATE_MEMORY_FAILED);
  if (OB_SUCC(ret)) {
    rt_expr.inner_func_cnt_ = 1;
    rt_expr.eval_func_ = ObExprOracleDecode::eval_decode;
    rt_expr.inner_functions_[0] = reinterpret_cast<void*>(
        ObDatumFuncs::get_nullsafe_cmp_func(cmp_meta.type_, cmp_meta.type_,
                                            default_null_pos(), cmp_meta.cs_type_,
                                            cmp_meta.scale_, lib::is_oracle_mode(),
                                            rt_expr.args_[0]->obj_meta_.has_lob_header()));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObExprOracleDecode,
                    row_dimension_,
                    real_param_num_,
                    result_type_,
                    input_types_,
                    id_,
                    param_flags_);

} // namespace sql
} // namespace oceanbase
