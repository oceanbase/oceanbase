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
#include "sql/engine/expr/ob_expr_greater_than.h"
#include <math.h>
#include "common/object/ob_obj_compare.h"
#include "sql/engine/expr/ob_expr_greater_equal.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprGreaterThan::ObExprGreaterThan(ObIAllocator& alloc) : ObRelationalExprOperator(alloc, T_OP_GT, N_GREATER_THAN, 2)
{}

int ObExprGreaterThan::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  return ObRelationalExprOperator::calc_result2(result, obj1, obj2, expr_ctx, false, CO_GT);
}

int ObExprGreaterThan::calc_resultN(
    ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  return ObRelationalExprOperator::calc_resultN(result, objs_array, param_num, expr_ctx, false, CO_GT);
}

}  // namespace sql
}  // namespace oceanbase
