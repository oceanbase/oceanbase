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
#include "sql/engine/expr/ob_expr_equal.h"
#include <math.h>
#include "common/object/ob_obj_compare.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
using namespace common;

namespace sql
{

ObExprEqual::ObExprEqual(ObIAllocator &alloc)
    : ObRelationalExprOperator(alloc, T_OP_EQ, N_EQUAL, 2)
{
}

int ObExprEqual::calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (is_called_in_sql() && lib::is_oracle_mode() && (type1.is_lob()
                                || type1.is_lob_locator())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(type1.get_type()),
                    ob_obj_type_str(type2.get_type()));
  } else if (is_called_in_sql() && lib::is_oracle_mode() && (type2.is_lob()
                                || type2.is_lob_locator())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(type1.get_type()),
                    ob_obj_type_str(type2.get_type()));
  } else if (is_called_in_sql()
              && lib::is_oracle_mode()
              && (type1.is_json() || type2.is_json())) {
    ret = OB_ERR_INVALID_CMP_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_CMP_OP);
  } else {
    ret = ObRelationalExprOperator::calc_result_type2(type, type1, type2, type_ctx);
  }
  return ret;
}

int ObExprEqual::calc(ObObj &result, const ObObj &obj1, const ObObj &obj2,
                      const ObCompareCtx &cmp_ctx, ObCastCtx &cast_ctx)
{
  return ObRelationalExprOperator::compare(result, obj1, obj2, cmp_ctx, cast_ctx, CO_EQ);
}

int ObExprEqual::calc_cast(ObObj &result, const ObObj &obj1, const ObObj &obj2,
                           const ObCompareCtx &cmp_ctx, ObCastCtx &cast_ctx)
{
  return ObRelationalExprOperator::compare_cast(result, obj1, obj2, cmp_ctx, cast_ctx, CO_EQ);
}

int ObExprEqual::calc_without_cast(ObObj &result, const ObObj &obj1, const ObObj &obj2,
                           const ObCompareCtx &cmp_ctx, bool &need_cast)
{
  return ObRelationalExprOperator::compare_nocast(result, obj1, obj2, cmp_ctx, CO_EQ, need_cast);
}

}
}
