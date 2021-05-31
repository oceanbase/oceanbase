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
namespace oceanbase {
using namespace common;

namespace sql {

ObExprEqual::ObExprEqual(ObIAllocator& alloc) : ObRelationalExprOperator(alloc, T_OP_EQ, N_EQUAL, 2)
{}

int ObExprEqual::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && type1.is_ext() && type2.is_ext()) {
    type.set_int32();  // not tinyint, compatible with MySQL
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_calc_type(type1.get_calc_type());
    type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
  } else {
    ret = ObRelationalExprOperator::calc_result_type2(type, type1, type2, type_ctx);
  }
  return ret;
}

int ObExprEqual::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && obj1.get_meta().is_ext() && obj1.get_meta().is_ext()) {
    ret = OB_NOT_SUPPORTED;
  } else {
    ret = ObRelationalExprOperator::calc_result2(result, obj1, obj2, expr_ctx, false, CO_EQ);
  }
  return ret;
}

int ObExprEqual::calc_resultN(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  return ObRelationalExprOperator::calc_resultN(result, objs_array, param_num, expr_ctx, false, CO_EQ);
}

int ObExprEqual::calc(
    ObObj& result, const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx, ObCastCtx& cast_ctx)
{
  return ObRelationalExprOperator::compare(result, obj1, obj2, cmp_ctx, cast_ctx, CO_EQ);
}

int ObExprEqual::calc_cast(
    ObObj& result, const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx, ObCastCtx& cast_ctx)
{
  return ObRelationalExprOperator::compare_cast(result, obj1, obj2, cmp_ctx, cast_ctx, CO_EQ);
}

int ObExprEqual::calc_without_cast(
    ObObj& result, const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx, bool& need_cast)
{
  return ObRelationalExprOperator::compare_nocast(result, obj1, obj2, cmp_ctx, CO_EQ, need_cast);
}

}  // namespace sql
}  // namespace oceanbase
