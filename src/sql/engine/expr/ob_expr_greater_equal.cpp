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
#include "sql/engine/expr/ob_expr_greater_equal.h"
#include <math.h>
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "common/object/ob_obj_compare.h"

namespace oceanbase {
using namespace common;

namespace sql {

ObExprGreaterEqual::ObExprGreaterEqual(ObIAllocator& alloc)
    : ObRelationalExprOperator(alloc, T_OP_GE, N_GREATER_EQUAL, 2)
{}

int ObExprGreaterEqual::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  return ObRelationalExprOperator::calc_result2(result, obj1, obj2, expr_ctx, false, CO_GE);
}

int ObExprGreaterEqual::calc_resultN(
    ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  return ObRelationalExprOperator::calc_resultN(result, objs_array, param_num, expr_ctx, false, CO_GE);
}

}  // namespace sql
}  // namespace oceanbase
