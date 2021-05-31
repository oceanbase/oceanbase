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

namespace oceanbase {
using namespace common;
// following .map file depends on ns oceanbase::common;
#include "sql/engine/expr/ob_expr_merge_result_type.map"
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
namespace sql {
using ArithOp = ObArithResultTypeMap::OP;
int ObExprResultTypeUtil::get_relational_cmp_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType type1 = res_type1.get_calc_type();
  ObObjType type2 = res_type2.get_calc_type();
  if (OB_FAIL(get_relational_cmp_type(type, type1, type2))) {
    LOG_WARN("fail to get_relational_cmp_type", K(type1), K(type2), K(ret));
  } else {
    res_type.set_calc_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_relational_cmp_type(ObObjType& type, const ObObjType& type1, const ObObjType& type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    type = (lib::is_oracle_mode() ? ORACLE_RELATIONAL_CMP_TYPE[type1][type2] : RELATIONAL_CMP_TYPE[type1][type2]);
  }
  return ret;
}

int ObExprResultTypeUtil::get_relational_result_type(ObObjType& type, const ObObjType& type1, const ObObjType& type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    type = RELATIONAL_RESULT_TYPE[type1][type2];
  }

  return ret;
}

int ObExprResultTypeUtil::get_relational_equal_type(ObObjType& type, const ObObjType& type1, const ObObjType& type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type1 < ObNullType || type2 < ObNullType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    // we donot have ORACLE_RELATIONAL_EQUAL_TYPE, just use ORACLE_RELATIONAL_CMP_TYPE instead.
    type = (lib::is_oracle_mode() ? ORACLE_RELATIONAL_CMP_TYPE[type1][type2] : RELATIONAL_EQUAL_TYPE[type1][type2]);
  }
  return ret;
}

int ObExprResultTypeUtil::get_merge_result_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type1 = res_type1.get_type();
  ObObjType type2 = res_type2.get_type();
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    res_type.set_type(MERGE_RESULT_TYPE[type1][type2]);
  }

  return ret;
}

int ObExprResultTypeUtil::get_merge_result_type(ObObjType& type, const ObObjType& type1, const ObObjType& type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    type = MERGE_RESULT_TYPE[type1][type2];
  }

  return ret;
}

int ObExprResultTypeUtil::get_abs_result_type(ObObjType& type, const ObObjType& type1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(ret));
  } else {
    if (is_oracle_mode()) {
      type = ABS_RESULT_TYPE_ORACLE[type1];
    } else {
      type = ABS_RESULT_TYPE[type1];
    }
  }

  return ret;
}

int ObExprResultTypeUtil::get_neg_result_type(ObObjType& type, const ObObjType& type1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(ret));
  } else if (is_oracle_mode()) {
    type = NEG_RESULT_TYPE_ORACLE[type1];
  } else {
    type = NEG_RESULT_TYPE[type1];
  }

  return ret;
}

int ObExprResultTypeUtil::get_round_result_type(ObObjType& type, const ObObjType& type1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type1, ArithOp::ROUND);
    type = rule.result_type;

    if (OB_UNLIKELY(!ob_is_valid_obj_type(type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for round", K(ret), K(type1), K(lbt()));
    }
  } else {
    type = ROUND_RESULT_TYPE[type1];
  }
  return ret;
}

int ObExprResultTypeUtil::get_nanvl_result_type(ObObjType& type, const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::NANVL);
    type = rule.result_type;

    if (OB_UNLIKELY(!ob_is_valid_obj_type(type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for round", K(ret), K(type1), K(type2));
    }
  }
  return ret;
}

int ObExprResultTypeUtil::get_div_result_type(ObObjType& result_type, ObObjType& result_ob1_type,
    ObObjType& result_ob2_type, const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::DIV);
    result_type = rule.result_type;
    result_ob1_type =
        (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : result_type) : rule.param1_calc_type);
    result_ob1_type =
        (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : result_type) : rule.param2_calc_type);

    if (OB_UNLIKELY(!ob_is_valid_obj_type(result_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for div", K(ret), K(type1), K(type2), K(lbt()));
    }
  } else {
    result_type = DIV_RESULT_TYPE[type1][type2];
    result_ob1_type = result_type;
    result_ob2_type = result_type;
  }
  return ret;
}

int ObExprResultTypeUtil::get_div_result_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(
          get_div_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_int_div_result_type(ObObjType& result_type, ObObjType& result_ob1_type,
    ObObjType& result_ob2_type, const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    result_type = INT_DIV_RESULT_TYPE[type1][type2];
    result_ob1_type = result_type;
    result_ob2_type = result_type;
  }
  return ret;
}

int ObExprResultTypeUtil::get_int_div_result_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(get_int_div_result_type(
          type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_int_div_calc_type(ObObjType& calc_type, ObObjType& calc_ob1_type,
    ObObjType& calc_ob2_type, const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  if ((ObIntTC != tc1 && ObUIntTC != tc1 && ObEnumSetTC != tc1) ||
      (ObIntTC != tc2 && ObUIntTC != tc2 && ObEnumSetTC != tc2)) {
    calc_type = ObNumberType;
    calc_ob1_type = ob_is_unsigned_type(type1) ? ObUNumberType : ObNumberType;
    calc_ob2_type = ob_is_unsigned_type(type2) ? ObUNumberType : ObNumberType;
  } else if (OB_FAIL(get_int_div_result_type(calc_type, calc_ob1_type, calc_ob2_type, type1, type2))) {
  }
  return ret;
}

int ObExprResultTypeUtil::get_mod_result_type(ObObjType& result_type, ObObjType& result_ob1_type,
    ObObjType& result_ob2_type, const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::MOD);
    result_type = rule.result_type;
    result_ob1_type =
        (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : result_type) : rule.param1_calc_type);
    result_ob2_type =
        (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : result_type) : rule.param2_calc_type);

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

int ObExprResultTypeUtil::get_mod_result_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(
          get_mod_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_remainder_result_type(ObObjType& result_type, ObObjType& result_ob1_type,
    ObObjType& result_ob2_type, const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::MOD);
    result_type = rule.result_type;
    result_ob1_type =
        (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : result_type) : rule.param1_calc_type);
    result_ob2_type =
        (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : result_type) : rule.param2_calc_type);
    if (OB_UNLIKELY(!ob_is_valid_obj_type(result_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for div", K(ret), K(type1), K(type2), K(lbt()));
    }
  }
  return ret;
}
int ObExprResultTypeUtil::get_remainder_result_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(get_remainder_result_type(
          type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}
int ObExprResultTypeUtil::get_remainder_calc_type(ObObjType& calc_type, ObObjType& calc_ob1_type,
    ObObjType& calc_ob2_type, const ObObjType type1, const ObObjType type2)
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

int ObExprResultTypeUtil::get_arith_result_type(ObObjType& result_type, ObObjType& result_ob1_type,
    ObObjType& result_ob2_type, const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else if (is_oracle_mode()) {
    // only mul still use this on oralce mode
    // TODO binary_double/binary_float/interval
    // Update: create a get_mul_result_type function, oracle mode never goes here
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("oracle mode never goes here", K(ret));
  } else {
    result_type = ARITH_RESULT_TYPE[type1][type2];
  }
  result_ob1_type = result_type;
  result_ob2_type = result_type;

  return ret;
}

int ObExprResultTypeUtil::get_mul_result_type(ObObjType& result_type, ObObjType& result_ob1_type,
    ObObjType& result_ob2_type, const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::MUL);
    result_type = rule.result_type;
    result_ob1_type =
        (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : result_type) : rule.param1_calc_type);
    result_ob1_type =
        (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : result_type) : rule.param2_calc_type);

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

int ObExprResultTypeUtil::get_add_result_type(ObObjType& result_type, ObObjType& result_ob1_type,
    ObObjType& result_ob2_type, const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (share::is_oracle_mode()) {
    if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
    } else {
      const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::ADD);
      result_type = rule.result_type;
      result_ob1_type = (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : result_type)
                                                            : rule.param1_calc_type);
      result_ob2_type = (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : result_type)
                                                            : rule.param2_calc_type);

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

int ObExprResultTypeUtil::get_minus_result_type(ObObjType& type, ObObjType& result_ob1_type, ObObjType& result_ob2_type,
    const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  if (share::is_oracle_mode()) {
    if (OB_UNLIKELY(type1 >= ObMaxType || type2 >= ObMaxType)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
    } else {
      const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, ArithOp::SUB);
      type = rule.result_type;
      result_ob1_type =
          (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : type) : rule.param1_calc_type);
      result_ob2_type =
          (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : type) : rule.param2_calc_type);

      if (OB_UNLIKELY(!ob_is_valid_obj_type(type))) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("unsupported type for minus", K(ret), K(type1), K(type2), K(lbt()));
      }
      LOG_DEBUG("get_minus_result_type", K(type), K(type1), K(type2), K(lbt()));
    }
  } else {
    ret = get_arith_result_type(type, result_ob1_type, result_ob2_type, type1, type2);
  }
  return ret;
}

int ObExprResultTypeUtil::get_arith_result_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(
          get_arith_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_mul_result_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(
          get_mul_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_add_result_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(
          get_add_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_minus_result_type(
    ObExprResType& res_type, const ObExprResType& res_type1, const ObExprResType& res_type2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  ObObjType result_ob1_type = ObMaxType;
  ObObjType result_ob2_type = ObMaxType;
  if (OB_FAIL(
          get_minus_result_type(type, result_ob1_type, result_ob2_type, res_type1.get_type(), res_type2.get_type()))) {
  } else {
    res_type.set_type(type);
  }
  return ret;
}

int ObExprResultTypeUtil::get_sum_result_type(ObObjType& type, const ObObjType& type1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1 >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(ret));
  } else if (is_oracle_mode()) {
    const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type1, ArithOp::SUM);
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

int ObExprResultTypeUtil::get_arith_calc_type(ObObjType& calc_type, ObObjType& calc_ob1_type, ObObjType& calc_ob2_type,
    const ObObjType type1, const ObObjType type2, const ArithOp oper)
{
  int ret = OB_SUCCESS;
  if (share::is_oracle_mode()) {
    const ObArithRule& rule = ARITH_RESULT_TYPE_ORACLE.get_rule(type1, type2, oper);
    calc_type = rule.result_type;
    calc_ob1_type =
        (rule.param1_calc_type == ObMaxType ? (ObNullType == type1 ? ObNullType : calc_type) : rule.param1_calc_type);
    calc_ob2_type =
        (rule.param2_calc_type == ObMaxType ? (ObNullType == type2 ? ObNullType : calc_type) : rule.param2_calc_type);

    if (OB_UNLIKELY(!ob_is_valid_obj_type(calc_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unsupported type for add", K(ret), K(type1), K(type2), K(lbt()));
    }
    LOG_DEBUG("get_arith_calc_type",
        K(type1),
        K(type2),
        K(calc_type),
        K(calc_ob1_type),
        K(calc_ob2_type),
        K(rule),
        K(oper),
        K(ret));
  } else {
    ret = get_arith_result_type(calc_type, calc_ob1_type, calc_ob2_type, type1, type2);
  }
  return ret;
}

/**
 * @brief For Oracle Mode
 * In the expression type derivation, if the parameter needs to be converted to a string type,
 * then use this function to get the maximum length after the parameter is converted to a string
 * For example, if the parameter is date,
 * then different lengths can be derived according to the difference of nls_date_format
 * @param session: session info
 * @param orig_type:The parameter type being converted
 * @param target_ls: Target semantics,LS_CHAR or LS_BYTE
 * @param length: Deduced maximum length
 * @return ret
 */
int ObExprResultTypeUtil::deduce_max_string_length_oracle(const ObSQLSessionInfo& session,
    const ObExprResType& orig_type, const ObLengthSemantics target_ls, ObLength& length)
{
  int ret = OB_SUCCESS;
  int64_t mbmaxlen = 1;
  int64_t mbminlen = 1;
  const ObLengthSemantics orig_ls = orig_type.get_length_semantics();
  length = orig_type.get_length();
  CK(LS_CHAR == target_ls || LS_BYTE == target_ls);
  OZ(ObCharset::get_mbmaxlen_by_coll(orig_type.get_collation_type(), mbmaxlen));
  OZ(ObCharset::get_mbminlen_by_coll(orig_type.get_collation_type(), mbminlen));

  if (OB_FAIL(ret)) {
  } else if (orig_type.is_character_type()) {
    CK(LS_BYTE == orig_ls || LS_CHAR == orig_ls);
    if (OB_FAIL(ret)) {
    } else if (LS_BYTE == target_ls && LS_CHAR == orig_ls) {
      length *= mbmaxlen;
    } else if (LS_CHAR == target_ls && LS_BYTE == orig_ls) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid language semantics for orig_type", K(ret), K(orig_type));
    } else {
    }
  } else if (orig_type.is_numeric_type()) {
    length = OB_MAX_NUMBER_PRECISION_INNER;
    if (LS_BYTE == target_ls) {
      length *= mbminlen;
    }
  } else if (orig_type.is_datetime()) {
    int64_t max_char_len = 0;
    OZ(ObTimeConverter::deduce_max_len_from_oracle_dfm(session.get_local_nls_date_format(), max_char_len));
    OX(length = static_cast<ObLength>(max_char_len));
    if (OB_SUCC(ret) && LS_BYTE == target_ls) {
      length *= mbminlen;
    }
  } else if (orig_type.is_timestamp_nano() || orig_type.is_timestamp_ltz() || orig_type.is_timestamp_tz()) {
    int64_t max_char_len = 0;
    ObString format_str;
    const ObDataTypeCastParams& dtc_params = session.get_dtc_params();
    format_str = dtc_params.get_nls_format(orig_type.get_type());
    OZ(ObTimeConverter::deduce_max_len_from_oracle_dfm(format_str, max_char_len), K(format_str));
    OX(length = static_cast<ObLength>(max_char_len));
    if (OB_SUCC(ret) && LS_BYTE == target_ls) {
      length *= mbminlen;
    }
  } else if (orig_type.is_interval_type()) {
    length += 1;
    if (LS_BYTE == target_ls) {
      length *= mbminlen;
    }
  } else if (orig_type.is_raw()) {
    length *= 2;
    if (LS_BYTE == target_ls) {
      length *= mbminlen;
    }
  } else if (orig_type.is_clob()) {
    length = OB_MAX_ORACLE_VARCHAR_LENGTH;
  } else {
    OZ(orig_type.get_length_for_meta_in_bytes(length));
  }
  OX(length = MIN(OB_MAX_VARCHAR_LENGTH, length));

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
