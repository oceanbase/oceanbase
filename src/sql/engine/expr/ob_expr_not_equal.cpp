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
#include "sql/engine/expr/ob_expr_not_equal.h"
#include <math.h>
#include "common/object/ob_obj_compare.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_equal.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprNotEqual::ObExprNotEqual(ObIAllocator &alloc)
    : ObRelationalExprOperator(alloc, T_OP_NE, N_NOT_EQUAL, 2)
{
}

int ObExprNotEqual::calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const
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
  } else {
    ret = ObRelationalExprOperator::calc_result_type2(type, type1, type2, type_ctx);
  }
  return ret;
}

}
}
