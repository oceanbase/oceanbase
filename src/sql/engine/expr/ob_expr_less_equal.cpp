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
#include "sql/engine/expr/ob_expr_less_equal.h"
#include <math.h>
#include "lib/timezone/ob_time_convert.h"
#include "common/object/ob_obj_compare.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"

namespace oceanbase {
using namespace common;

namespace sql {

ObExprLessEqual::ObExprLessEqual(ObIAllocator& alloc) : ObRelationalExprOperator(alloc, T_OP_LE, N_LESS_EQUAL, 2)
{}

int ObExprLessEqual::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, ObExprCtx& expr_ctx) const
{
  return ObRelationalExprOperator::calc_result2(result, obj1, obj2, expr_ctx, false, CO_LE);
}

int ObExprLessEqual::calc_resultN(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  return ObRelationalExprOperator::calc_resultN(result, objs_array, param_num, expr_ctx, false, CO_LE);
}

int ObExprLessEqual::calc(
    ObObj& result, const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx, ObCastCtx& cast_ctx)
{
  return ObRelationalExprOperator::compare(result, obj1, obj2, cmp_ctx, cast_ctx, CO_LE);
}

int ObExprLessEqual::calc_nocast(ObObj& result, const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx)
{
  int ret = OB_SUCCESS;
  bool need_cast = false;
  ret = ObRelationalExprOperator::compare_nocast(result, obj1, obj2, cmp_ctx, CO_LE, need_cast);
  if (OB_FAIL(ret)) {
    LOG_WARN("compare objs failed", K(ret), K(obj1), K(obj2), K(cmp_ctx.cmp_type_));
  } else if (OB_UNLIKELY(need_cast)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("compare objs failed", K(ret), K(obj1), K(obj2), K(cmp_ctx.cmp_type_));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
