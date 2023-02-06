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

#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

#include "sql/engine/expr/ob_expr_div_promotion.map"
#include "sql/engine/expr/ob_expr_nvl_promotion.map"

struct MyIntegrityChecker3
{
  MyIntegrityChecker3()
  {
    for (int tc1 = ObNullTC; tc1 < ObMaxTC; tc1++) {
      for (int tc2 = ObNullTC; tc2 < tc1; tc2++) {
        if (DIV_TYPE_PROMOTION[tc1][tc2] != DIV_TYPE_PROMOTION[tc2][tc1]) {
          LOG_ERROR_RET(OB_ERR_UNEXPECTED, "DIV_TYPE_PROMOTION is wrong", "ret", OB_ERR_UNEXPECTED,
                    K(tc1), K(tc2),
                    "tc1 => tc2", DIV_TYPE_PROMOTION[tc1][tc2],
                    "tc2 => tc1", DIV_TYPE_PROMOTION[tc2][tc1]);
        }
      }
    }
  }
} MY_DIV_TYPE_PROMOTION_CHECKER;


int ObExprPromotionUtil::get_nvl_type(ObExprResType &type, const ObExprResType &type1,
                                      const ObExprResType &type2)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    // process boolean parameter in pl/sql
    if (type1.get_type() == ObTinyIntType || type2.get_type() == ObTinyIntType) {
      if (ObNullType == type1.get_type()
          || ObNullType == type2.get_type()
          || (ObTinyIntType == type1.get_type() && ObTinyIntType == type2.get_type())) {
        type.set_type(ObTinyIntType);
      } else {
        if (OB_FAIL(get_calc_type(type, type1, type2, NVL_TYPE_PROMOTION_ORACLE))) {
          LOG_WARN("failed to get calc type", K(ret), K(type1), K(type2), K(type));
        }
      }
    } else {
      if (OB_FAIL(get_calc_type(type, type1, type2, NVL_TYPE_PROMOTION_ORACLE))) {
        LOG_WARN("failed to get calc type", K(ret), K(type1), K(type2), K(type));
      }
    }
  } else {
    if (OB_FAIL(get_calc_type(type, type1, type2, NVL_TYPE_PROMOTION))) {
      LOG_WARN("failed to get calc type", K(ret), K(type1), K(type2), K(type));
    }
  }
  return ret;
}

int ObExprPromotionUtil::get_calc_type(ObExprResType &type,
                                       const ObExprResType &type1,
                                       const ObExprResType &type2,
                                       const ObObjType map[ObMaxTC][ObMaxTC])
{
  int ret = OB_SUCCESS;
  ObObjTypeClass l_tc = type1.get_type_class();
  ObObjTypeClass r_tc = type2.get_type_class();
  if (OB_UNLIKELY(l_tc < ObNullTC || l_tc >= ObMaxTC || r_tc < ObNullTC || r_tc >= ObMaxTC
                  || OB_ISNULL(map))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left / right tc out of range, or map array is null", K(ret), K(l_tc), K(r_tc));
  } else {
    type.set_type(map[l_tc][r_tc]);
    if (ObMaxType != type.get_type() && ob_is_nstring(type1.get_type())) {
      type.set_type(ObNVarchar2Type);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
