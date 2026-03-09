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

#define USING_LOG_PREFIX STORAGE_FTS
#include "ob_expr_pos_list.h"

namespace oceanbase
{
namespace sql
{

ObExprPosList::ObExprPosList(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_POS_LIST, N_POS_LIST, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {
  need_charset_convert_ = false;
}

int ObExprPosList::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 1) || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for fulltext expr", K(ret), K(param_num), KP(types));
  } else {
    type.set_varchar();
    type.set_length(OB_MAX_VARCHAR_LENGTH); // TODO: need to be optimized
    type.set_collation_type(CS_TYPE_BINARY);
  }
  return ret;
}

int ObExprPosList::calc_resultN(ObObj &result,
                                const ObObj *objs_array,
                                int64_t param_num,
                                common::ObExprCtx &expr_ctx) const
{
  return OB_NOT_SUPPORTED;
}

int ObExprPosList::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 1) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(rt_expr.arg_cnt_), KP(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = generate_pos_list;
  }
  return ret;
}

int ObExprPosList::generate_pos_list(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(res);
  res.set_string(ObString::make_string(""));
  return ret;
}

} // namespace oceanbase
} // namespace sql
