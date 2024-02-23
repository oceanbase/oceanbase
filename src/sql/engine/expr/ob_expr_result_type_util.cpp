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

#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_res_type_map.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;

//Since ob use c++11, so we can not use loop or constains multiple return in constexpr function(which is supported in c++14)
//So we use recursion and conditional assignment statement instead
template <typename T, std::size_t row, std::size_t col>
constexpr bool is_array_fully_initialized(const T (&arr)[row][col], int row_depth)
{
  return row_depth == 0 ?
           true :
           arr[row_depth - 1][col - 1] == T() ? false :
                                                is_array_fully_initialized(arr, row_depth - 1);
}

template <typename T, std::size_t col>
constexpr bool is_array_fully_initialized(const T (&arr)[col])
{
  return arr[col - 1] != T();
}

// following .map file depends on ns oceanbase::common;
#include "sql/engine/expr/ob_expr_merge_result_type_oracle.map"
#include "sql/engine/expr/ob_expr_relational_result_type.map"
#include "sql/engine/expr/ob_expr_abs_result_type.map"
#include "sql/engine/expr/ob_expr_neg_result_type.map"
#include "sql/engine/expr/ob_expr_round_result_type.map"
#include "sql/engine/expr/ob_expr_div_result_type.map"
#include "sql/engine/expr/ob_expr_int_div_result_type.map"
#include "sql/engine/expr/ob_expr_mod_result_type.map"
#include "sql/engine/expr/ob_expr_arithmetic_result_type.map"
#include "sql/engine/expr/ob_expr_relational_cmp_type.map"
#include "sql/engine/expr/ob_expr_relational_equal_type.map"
#include "sql/engine/expr/ob_expr_sum_result_type.map"


using namespace share;
namespace sql
{

const int64_t MAX_NUMBER_BUFFER_SIZE_IN_TYPE_UTIL = 40;

using ArithOp = ObArithResultTypeMap::OP;
int ObExprResultTypeUtil::get_relational_cmp_type(ObExprResType &res_type,
                                                  const ObExprResType &res_type1,
                                                  const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType type1 = res_type1.get_calc_type();
  ObObjType type2 = res_type2.get_calc_type();
  if (OB_FAIL(get_relational_cmp_type(type, type1, type2))) {
    LOG_WARN("fail to get_relational_cmp_type", K(type1), K(type2),K(ret));
  } else {
    res_type.set_calc_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_relational_cmp_type(ObObjType &type,
                                                  const ObObjType &type1,
                                                  const ObObjType &type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else {
    type = (lib::is_oracle_mode()
            ? ORACLE_RELATIONAL_CMP_TYPE[type1][type2]
            : RELATIONAL_CMP_TYPE[type1][type2]);
  }
  return ret;
}

int ObExprResultTypeUtil::get_relational_result_type(ObObjType &type,
                                                     const ObObjType &type1,
                                                     const ObObjType &type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else {
    type = RELATIONAL_RESULT_TYPE[type1][type2];
  }

  return ret;
}

int ObExprResultTypeUtil::get_relational_equal_type(ObObjType &type,
                                                    const ObObjType &type1,
                                                    const ObObjType &type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type1 < ObNullType || type2 < ObNullType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    // we donot have ORACLE_RELATIONAL_EQUAL_TYPE, just use ORACLE_RELATIONAL_CMP_TYPE instead.
    type = (lib::is_oracle_mode()
            ? ORACLE_RELATIONAL_CMP_TYPE[type1][type2]
            : RELATIONAL_EQUAL_TYPE[type1][type2]);
  }
  return ret;
}

int ObExprResultTypeUtil::get_merge_result_type(ObExprResType &res_type,
                                                const ObExprResType &res_type1,
                                                const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type1 = res_type1.get_type();
  ObObjType type2 = res_type2.get_type();
  ObObjType type = ObMaxType;
  ret = get_merge_result_type(type, type1, type2);
  res_type.set_type(type);
  return ret;
}

int ObExprResultTypeUtil::get_merge_result_type(ObObjType &type,
                                                const ObObjType &type1,
                                                const ObObjType &type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else {
    type = lib::is_oracle_mode()
           ? MERGE_RESULT_TYPE_ORACLE[type1][type2]
           : MERGE_RESULT_TYPE[type1][type2];
  }

  return ret;
}

int ObExprResultTypeUtil::get_abs_result_type(ObObjType &type,
                                              const ObObjType &type1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(ret));
  } else {
    if (is_oracle_mode()) {
      type = ABS_RESULT_TYPE_ORACLE[type1];
    } else {
      type = ABS_RESULT_TYPE[type1];
    }
  }

  return ret;
}

int ObExprResultTypeUtil::get_neg_result_type(ObObjType &type,
                                              const ObObjType &type1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(ret));
  } else if (is_oracle_mode()) {
    type = NEG_RESULT_TYPE_ORACLE[type1];
  } else {
    type = NEG_RESULT_TYPE[type1];
  }

  return ret;
}

int ObExprResultTypeUtil::get_round_result_type(ObObjType &type,
                                                const ObObjType &type1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type1, ArithOp::ROUND);
    type = rule.result_type;

    if (OB_UNLIKELY(!ob_is_valid_obj_type(type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for round", K(ret), K(type), K(type1), K(lbt()));
    }
  } else {
    type = ROUND_RESULT_TYPE[type1];
  }
  return ret;
}

int ObExprResultTypeUtil::get_nanvl_result_type(ObObjType &type,
                                                const ObObjType type1,
                                                const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else {
    const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::NANVL);
    type = rule.result_type;

    if (OB_UNLIKELY(!ob_is_valid_obj_type(type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for round", K(ret), K(type1), K(type2));
    }
  }
  return ret;
}

int ObExprResultTypeUtil::get_div_result_type(ObObjType &result_type,
                                              ObObjType &result_ob1_type,
                                              ObObjType &result_ob2_type,
                                              const ObObjType type1,
                                              const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::DIV);
    result_type = rule.result_type;
    result_ob1_type = (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : result_type) : rule.param1_calc_type);
    result_ob1_type = (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : result_type) : rule.param2_calc_type);

    if (OB_UNLIKELY(!ob_is_valid_obj_type(result_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for div", K(ret), K(type1), K(type2), K(lbt()));
    }
  } else {
    result_type = DIV_RESULT_TYPE[type1][type2];
    // FIXME: @zuojiao.hzj : remove this after we can keep high division calc scale
    // using decimal int
    bool all_decint_args = (ob_is_decimal_int(type1) && ob_is_decimal_int(type2));
    if (ob_is_decimal_int(result_type)) {
      result_type = ObNumberType;
    }
    if (ob_is_decimal_int(result_type) && !all_decint_args) {
      result_ob1_type = ObNumberType;
      result_ob2_type = ObNumberType;
    } else {
      result_ob1_type = result_type;
      result_ob2_type = result_type;
    }
  }
  return ret;
}

int ObExprResultTypeUtil::get_div_result_type(ObExprResType &res_type,
                                              const ObExprResType &res_type1,
                                              const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(get_div_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_int_div_result_type(ObObjType &result_type,
                                                  ObObjType &result_ob1_type,
                                                  ObObjType &result_ob2_type,
                                                  const ObObjType type1,
                                                  const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else {
    result_type = INT_DIV_RESULT_TYPE[type1][type2];
    result_ob1_type = result_type;
    result_ob2_type = result_type;
  }
  return ret;
}

int ObExprResultTypeUtil::get_int_div_result_type(ObExprResType &res_type,
                                                  const ObExprResType &res_type1,
                                                  const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(get_int_div_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_int_div_calc_type(ObObjType &calc_type,
                                                ObObjType &calc_ob1_type,
                                                ObObjType &calc_ob2_type,
                                                const ObObjType type1,
                                                const ObObjType type2)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  if ((ObIntTC != tc1 && ObUIntTC != tc1 && ObEnumSetTC != tc1)
      || (ObIntTC != tc2 && ObUIntTC != tc2 && ObEnumSetTC != tc2)) {
    calc_type = ObNumberType;
    calc_ob1_type = ob_is_unsigned_type(type1) ? ObUNumberType : ObNumberType;
    calc_ob2_type = ob_is_unsigned_type(type2) ? ObUNumberType : ObNumberType;
  } else if (OB_FAIL(get_int_div_result_type(calc_type, calc_ob1_type, calc_ob2_type, type1, type2))) {
  }
  return ret;
}

int ObExprResultTypeUtil::get_mod_result_type(ObObjType &result_type,
                                              ObObjType &result_ob1_type,
                                              ObObjType &result_ob2_type,
                                              const ObObjType type1,
                                              const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::MOD);
    result_type = rule.result_type;
    result_ob1_type = (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : result_type) : rule.param1_calc_type);
    result_ob2_type = (rule.param2_calc_type == ObMaxType
                       ? (ObNullType == type2 ? ObNullType : result_type)
                       : rule.param2_calc_type);

    if (OB_UNLIKELY(!ob_is_valid_obj_type(result_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for div", K(ret), K(type1), K(type2), K(lbt()));
    }
  } else {
    result_type = MOD_RESULT_TYPE[type1][type2];
    result_ob1_type = result_type;
    result_ob2_type = result_type;
  }
  return ret;
}

int ObExprResultTypeUtil::get_mod_result_type(ObExprResType &res_type,
                                              const ObExprResType &res_type1,
                                              const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(get_mod_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(),
                                  res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_remainder_result_type(ObObjType &result_type,
                                              ObObjType &result_ob1_type,
                                              ObObjType &result_ob2_type,
                                              const ObObjType type1,
                                              const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else {
    const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::MOD);
    result_type = rule.result_type;
    result_ob1_type = (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ?
                                                ObNullType : result_type) : rule.param1_calc_type);
    result_ob2_type = (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ?
                                                ObNullType : result_type) : rule.param2_calc_type);
    if (OB_UNLIKELY(!ob_is_valid_obj_type(result_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for div", K(ret), K(type1), K(type2), K(lbt()));
    }
  }
  return ret;
}
int ObExprResultTypeUtil::get_remainder_result_type(ObExprResType &res_type,
                                              const ObExprResType &res_type1,
                                              const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(get_remainder_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}
int ObExprResultTypeUtil::get_remainder_calc_type(ObObjType &calc_type,
                                            ObObjType &calc_ob1_type,
                                            ObObjType &calc_ob2_type,
                                            const ObObjType type1,
                                            const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remainder_result_type(calc_type, calc_ob1_type, calc_ob2_type, type1, type2))) {
  } else {
    // result type: UNumber, calc type: Number.
    //              UDouble             Double.
    calc_type = ob_obj_default_type(ob_obj_type_class(calc_type));
    calc_ob1_type = calc_type;
    calc_ob2_type = calc_type;
  }
  return ret;
}

int ObExprResultTypeUtil::get_arith_result_type(ObObjType &result_type,
                                                ObObjType &result_ob1_type,
                                                ObObjType &result_ob2_type,
                                                const ObObjType type1,
                                                const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else if (is_oracle_mode()) {
    //only mul still use this on oralce mode
    //TODO binary_double/binary_float/interval
    //Update: create a get_mul_result_type function, oracle mode never goes here
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("oracle mode never goes here", K(ret));
  } else {
    result_type = ARITH_RESULT_TYPE[type1][type2];
  }
  result_ob1_type = result_type;
  result_ob2_type = result_type;

  return ret;
}

int ObExprResultTypeUtil::get_mul_result_type(ObObjType &result_type,
                                              ObObjType &result_ob1_type,
                                              ObObjType &result_ob2_type,
                                              const ObObjType type1,
                                              const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::MUL);
    result_type = rule.result_type;
    result_ob1_type = (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : result_type) : rule.param1_calc_type);
    result_ob1_type = (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : result_type) : rule.param2_calc_type);

    if (OB_UNLIKELY(!ob_is_valid_obj_type(result_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for arith", K(ret), K(type1), K(type2), K(lbt()));
    }
  } else {
    result_type = ARITH_RESULT_TYPE[type1][type2];
    result_ob1_type = result_type;
    result_ob2_type = result_type;
  }
  return ret;
}

int ObExprResultTypeUtil::get_add_result_type(ObObjType &result_type,
                                              ObObjType &result_ob1_type,
                                              ObObjType &result_ob2_type,
                                              const ObObjType type1,
                                              const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
    } else {
      const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::ADD);
      result_type = rule.result_type;
      result_ob1_type = (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : result_type) : rule.param1_calc_type);
      result_ob2_type = (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : result_type) : rule.param2_calc_type);

      if (OB_UNLIKELY(!ob_is_valid_obj_type(result_type))) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("unsupported type for add", K(ret), K(type1), K(type2), K(lbt()));
      }
      LOG_DEBUG("get_add_result_type", K(result_type), K(type1), K(type2), K(rule));
    }
  } else {
    ret = get_arith_result_type(result_type, result_ob1_type, result_ob2_type, type1, type2);
  }
  return ret;
}


int ObExprResultTypeUtil::get_minus_result_type(ObObjType &type,
                                                ObObjType &result_ob1_type,
                                                ObObjType &result_ob2_type,
                                                const ObObjType type1,
                                                const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("the wrong type", K(type1),K(type2),K(ret));
    } else {
      const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::SUB);
      type = rule.result_type;
      result_ob1_type = (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : type) : rule.param1_calc_type);
      result_ob2_type = (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : type) : rule.param2_calc_type);

      if (OB_UNLIKELY(!ob_is_valid_obj_type(type))) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("unsupported type for minus", K(ret), K(type1),K(type2), K(lbt()));
      }
      LOG_DEBUG("get_minus_result_type", K(type), K(type1), K(type2), K(lbt()));
    }
  } else {
    ret = get_arith_result_type(type, result_ob1_type, result_ob2_type, type1, type2);
  }
  return ret;
}

int ObExprResultTypeUtil::get_arith_result_type(ObExprResType &res_type,
                                                const ObExprResType &res_type1,
                                                const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if(OB_FAIL(get_arith_result_type(type,
                                   result_ob1_type,
                                   result_ob2_type,
                                   res_type1.get_type(),
                                   res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_mul_result_type(ObExprResType &res_type,
                                              const ObExprResType &res_type1,
                                              const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if(OB_FAIL(get_mul_result_type(type,
                                 result_ob1_type,
                                 result_ob2_type,
                                 res_type1.get_type(),
                                 res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}


int ObExprResultTypeUtil::get_add_result_type(ObExprResType &res_type,
    const ObExprResType &res_type1, const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if(OB_FAIL(get_add_result_type(type,
                                 result_ob1_type,
                                 result_ob2_type,
                                 res_type1.get_type(),
                                 res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_minus_result_type(ObExprResType &res_type,
    const ObExprResType &res_type1, const ObExprResType &res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if(OB_FAIL(get_minus_result_type(type,
                                   result_ob1_type,
                                   result_ob2_type,
                                   res_type1.get_type(),
                                   res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_sum_result_type(ObObjType &type,
                                              const ObObjType &type1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1),K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type1, ArithOp::SUM);
    type = rule.result_type;

    if (OB_UNLIKELY(!ob_is_valid_obj_type(type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for round", K(ret), K(type1), K(lbt()));
    }
    LOG_DEBUG("succ to get_sum_result_type", K(ret), K(type1), K(type), K(lbt()));
  } else {
    type = SUM_RESULT_TYPE[type1];
  }

  return ret;
}

int ObExprResultTypeUtil::get_arith_calc_type(ObObjType &calc_type,
                                              ObObjType &calc_ob1_type,
                                              ObObjType &calc_ob2_type,
                                              const ObObjType type1,
                                              const ObObjType type2,
                                              const ArithOp oper)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    const ObArithRule &rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, oper);
    calc_type = rule.result_type;
    calc_ob1_type = (rule.param1_calc_type == ObMaxType
                     ? (ObNullType == type1 ? ObNullType : calc_type)
                     : rule.param1_calc_type);
    calc_ob2_type = (rule.param2_calc_type == ObMaxType
                     ? (ObNullType == type2 ? ObNullType : calc_type)
                     : rule.param2_calc_type);

    if (OB_UNLIKELY(!ob_is_valid_obj_type(calc_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for add", K(ret), K(type1), K(type2), K(lbt()));
    }
    LOG_DEBUG("get_arith_calc_type", K(type1), K(type2), K(calc_type), K(calc_ob1_type),
              K(calc_ob2_type), K(rule), K(oper), K(ret));
  } else {
    ret = get_arith_result_type(calc_type, calc_ob1_type, calc_ob2_type, type1, type2);
  }
  return ret;
}

int CHECK_STRING_RES_TYPE_ORACLE(const ObExprResType &type)
{
  int ret = OB_SUCCESS;
  if (!type.is_string_or_lob_locator_type() && !type.is_raw()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("incorrect type of target type", K(ret), K(type));
  } else if (type.is_blob() || type.is_blob_locator()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "blob cast to other type");
    LOG_WARN("not support blob cast to other type", K(ret));
  } else if (!ObCharset::is_valid_collation(type.get_collation_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("incorrect charset of target type", K(ret), K(type));
  } else if (!type.is_clob() && !type.is_clob_locator() && !type.is_raw() &&
             LS_CHAR != type.get_length_semantics() && LS_BYTE != type.get_length_semantics()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("incorrect length_semantics of target type", K(ret), K(type));
  }
  return ret;
}

/**
 * @brief Oracle 模式专用
 * 在表达式类型推导时，如果参数需要被隐式转化为字符串类型，那么使用该函数可以获得参数转化为字符串之后的最大长度。
 * 比如参数为 date，那么根据nls_date_format的不同，可以推导出不同的长度。
 * TODO: 常量的长度推导还需要优化，目前推导结果是过长的
 * @param dtc_params 类型转换信息，通过 type_ctx.get_session()->get_dtc_params() 获取
 * @param orig_type 被转化的参数类型
 * @param target_type 目标类型
 * @param length 推导出的最大长度，语义和 target_type 中的长度语义匹配
 * @param calc_ls 当参数的长度语义需要和结果不同时，可以显示指定，例如replace、translate表达式
 * @return ret
 */
int ObExprResultTypeUtil::deduce_max_string_length_oracle(const ObDataTypeCastParams &dtc_params,
                                                          const ObExprResType &orig_type,
                                                          const ObExprResType &target_type,
                                                          ObLength &length,
                                                          const int16_t calc_ls)
{
  int ret = OB_SUCCESS;
  ObLengthSemantics length_semantics = target_type.get_length_semantics();
  if (target_type.is_varchar_or_char() && (LS_BYTE == calc_ls || LS_CHAR == calc_ls)) {
    length_semantics = calc_ls;
  } else if (target_type.is_raw()) {
    length_semantics = LS_BYTE;
  }
  if (OB_FAIL(CHECK_STRING_RES_TYPE_ORACLE(target_type))) {
    LOG_WARN("invalid target_type", K(ret));
  } else {
    if (orig_type.is_literal()) {
      ObArenaAllocator oballocator(ObModIds::OB_SQL_RES_TYPE);
      ObCastMode cast_mode = CM_WARN_ON_FAIL;
      const ObObj &orig_obj = orig_type.get_param();
      ObAccuracy res_accuracy;
      ObCastCtx cast_ctx(&oballocator,
                      &dtc_params,
                      cast_mode,
                      target_type.get_collation_type(),
                      &res_accuracy);
      ObObj out;
      if (OB_FAIL(ObObjCaster::to_type(target_type.get_type(), cast_ctx, orig_obj, out))) {
        LOG_WARN("failed to cast obj", K(ret), K(orig_obj), K(target_type.get_type()));
      } else {
        if (LS_BYTE == length_semantics) {
          length = out.get_string_len();
        } else if (LS_CHAR == length_semantics) {
          length = static_cast<ObLength>(ObCharset::strlen_char(out.get_collation_type(),
                                                                out.get_string_ptr(),
                                                                out.get_string_len()));
          if (!ObCharset::is_valid_collation(out.get_collation_type())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected const value cast result", K(ret), K(out), K(orig_obj), K(target_type), K(cast_mode));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid length_semantics", K(length_semantics), K(ret));
        }
      }
    } else if (orig_type.is_string_or_lob_locator_type()) {
      if (OB_FAIL(CHECK_STRING_RES_TYPE_ORACLE(orig_type))) {
        LOG_WARN("invalid orig_type", K(ret), K(orig_type));
      } else if (orig_type.is_clob() || orig_type.is_clob_locator()) {
        if (target_type.is_clob() || target_type.is_clob_locator()) {
          length = orig_type.get_length();
        } else if (LS_CHAR == length_semantics) {
          // clob to LS_CHAR
          int64_t mbminlen = ObCharset::get_charset(target_type.get_collation_type())->mbminlen;
          length = OB_MAX_ORACLE_VARCHAR_LENGTH / mbminlen;
        } else if (target_type.is_raw()) {
          //cast not support, return max length
          length = OB_MAX_ORACLE_RAW_PL_VAR_LENGTH;
        } else {
          // clob to LS_BYTE
          length = OB_MAX_ORACLE_VARCHAR_LENGTH;
        }
      } else {
        length = orig_type.get_length();
        if (orig_type.is_varchar_or_char()
            && (target_type.is_nchar() || target_type.is_nvarchar2())) {
          // handle charset convert to nchar
          if (LS_CHAR == orig_type.get_length_semantics()) {
            length *= ObCharset::get_charset(orig_type.get_collation_type())->mbmaxlen;
          }
          length *= ObCharset::MAX_MB_LEN;
          length /= ObCharset::get_charset(target_type.get_collation_type())->mbminlen;
        } else if ((orig_type.is_varchar_or_char() || orig_type.is_nstring())
                  && target_type.is_raw()) {
          if (LS_CHAR == orig_type.get_length_semantics()) {
            length *= ObCharset::get_charset(orig_type.get_collation_type())->mbmaxlen;
          }
          length = (length + 1) / 2;
        } else if (LS_CHAR == orig_type.get_length_semantics()
            && LS_BYTE == length_semantics) {
          // LS_CHAR to LS_BYTE
          length *= ObCharset::get_charset(target_type.get_collation_type())->mbmaxlen;
        } else if (LS_BYTE == orig_type.get_length_semantics()
                   && LS_CHAR == length_semantics) {
          // LS_BYTE to LS_CHAR
          length /= ObCharset::get_charset(target_type.get_collation_type())->mbminlen;
        }
      }
    } else if (orig_type.is_user_defined_sql_type() || orig_type.is_ext()) {
      // udt types like xml can cast to string, the accuracy in pl extend is used for udt id
      if (LS_CHAR == length_semantics) {
        int64_t mbminlen = ObCharset::get_charset(target_type.get_collation_type())->mbminlen;
        length = OB_MAX_VARCHAR_LENGTH_KEY / mbminlen;
      } else {
        length = OB_MAX_VARCHAR_LENGTH_KEY; // issue 49536718: CREATE INDEX index ON table (UPPER(c1));
      }
    } else {
      int64_t ascii_bytes = 0;
      if (orig_type.is_null()) {
        // do nothing
      } else if (orig_type.is_numeric_type()) {
        ascii_bytes = MAX_NUMBER_BUFFER_SIZE_IN_TYPE_UTIL;
      } else if (orig_type.is_datetime()
                 || orig_type.is_otimestamp_type()) {
        // deduce by format
        if (OB_FAIL(ObTimeConverter::deduce_max_len_from_oracle_dfm(
                      dtc_params.get_nls_format(orig_type.get_type()), ascii_bytes))) {
          LOG_WARN("fail to deduce max len from dfm format", K(ret));
        }
      } else if (orig_type.is_interval_type()) {
        ascii_bytes = orig_type.get_length() + 1;
      } else if (orig_type.is_raw()) {
        ascii_bytes = orig_type.get_length() * 2;
      } else {
        // TODO: support rowid and urowid
        ascii_bytes = orig_type.get_length();
      }
      if (OB_SUCC(ret)) {
        if (target_type.is_raw()) {
          if (orig_type.is_raw() || orig_type.is_json()) {
            length = orig_type.get_length();
          } else {
            //cast not support, return max length
            length = OB_MAX_ORACLE_RAW_PL_VAR_LENGTH;
          }
        } else if (LS_BYTE == length_semantics &&
            ObCharset::is_cs_nonascii(target_type.get_collation_type())) {
          length = (ObLength)(ascii_bytes
                              * ObCharset::get_charset(target_type.get_collation_type())->mbminlen);
        } else {
          length = (ObLength)ascii_bytes;
        }
      }
    }
  }

  return ret;
}

} /* sql */
} /* oceanbase */
