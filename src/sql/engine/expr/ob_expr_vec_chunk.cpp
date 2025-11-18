/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

 #define USING_LOG_PREFIX COMMON

 #include "sql/engine/expr/ob_expr_vec_chunk.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprVecChunk::ObExprVecChunk(ObIAllocator &allocator)
   : ObFuncExprOperator(allocator, T_FUN_SYS_HYBRID_VEC_CHUNK, N_VEC_CHUNK, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
   need_charset_convert_ = false;
}

int ObExprVecChunk::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for semantic vector index expr", K(ret), K(param_num), KP(types));
  } else {
    type.set_type(types->get_type());
    type.set_length(types->get_length());
    type.set_binary();
  }
  return ret;
}

int ObExprVecChunk::calc_resultN(ObObj &result,
                                  const ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
   return OB_NOT_SUPPORTED;
}

int ObExprVecChunk::cg_expr(
    ObExprCGCtx &expr_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 1) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(rt_expr.arg_cnt_), KP(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = generate_vec_chunk;
  }
  return ret;
}

int ObExprVecChunk::generate_vec_chunk(
      const ObExpr &raw_ctx,
      ObEvalCtx &eval_ctx,
      ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  if (OB_FAIL(raw_ctx.args_[0]->eval(eval_ctx, datum))) {
    LOG_WARN("fail to eval arg expr", K(ret), KPC(raw_ctx.args_[0]));
  } else if (OB_ISNULL(datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null datum", K(ret), KPC(raw_ctx.args_[0]));
  } else if (datum->is_null()) {
    expr_datum.set_null();
  } else {
    expr_datum.set_string(datum->get_string());
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
