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

#include "sql/engine/expr/ob_expr_vec_vector.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprVecVector::ObExprVecVector(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_VEC_VECTOR, N_VEC_VECTOR, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprVecVector::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObDataType elem_type;
  elem_type.meta_.set_float();
  uint16_t subschema_id;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (param_num != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param num", K(ret), K(param_num));
  } else if (!types[0].is_null() && !types[0].is_collection_sql_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input types", K(ret), K(types[0]));
  } else if (types[0].is_null()) {
    type.is_null();
  } else {
    type.set_collection(types[0].get_subschema_id());
  }
  return ret;
}

int ObExprVecVector::calc_resultN(ObObj &result,
                                  const ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  return OB_NOT_SUPPORTED;
}

int ObExprVecVector::cg_expr(
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
    rt_expr.eval_func_ = generate_vec_vector;
  }
  return ret;
}

/*static*/ int ObExprVecVector::generate_vec_vector(
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
  } else if (datum->get_string().length() == 0) {
    expr_datum.set_string(datum->get_string());
  } else {
    // transform outrow vector into inrow vector
    ObString vector = datum->get_string();
    ObLobLocatorV2 lob(vector, raw_ctx.args_[0]->obj_meta_.has_lob_header());
    if (lob.has_inrow_data()) {
      // inrow lob do not need to build new result
      expr_datum.set_string(datum->get_string());
    } else {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
      common::ObArenaAllocator &ctx_allocator = tmp_alloc_g.get_allocator();
      ObLobAccessParam param;
      ObLobManager* lob_mngr = MTL(ObLobManager*);
      int64_t timeout = 0;
      int64_t query_st = eval_ctx.exec_ctx_.get_my_session()->get_query_start_time();
      if (OB_ISNULL(lob_mngr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get lob manager handle null.", K(ret));
      } else if (OB_FAIL(eval_ctx.exec_ctx_.get_my_session()->get_query_timeout(timeout))) {
        LOG_WARN("failed to get session query timeout", K(ret));
      } else {
        timeout += query_st;
        param.tx_desc_ = eval_ctx.exec_ctx_.get_my_session()->get_tx_desc();
        int64_t lob_len = 0;
        ObString vector_buff;
        char *vec_buff_ptr = nullptr;
        int64_t buff_len = 0;
        ObTextStringDatumResult text_result(ObLongTextType, &raw_ctx, &eval_ctx, &expr_datum);
        if (OB_FAIL(lob.get_lob_data_byte_len(lob_len))) {
          LOG_WARN("fail to get vector byte len", K(ret), K(lob));
        } else if (OB_FAIL(text_result.init(lob_len, nullptr))) {
          LOG_WARN("init lob result failed");
        } else if (OB_FAIL(text_result.get_reserved_buffer(vec_buff_ptr, buff_len))) {
          LOG_WARN("fail to get reserved buffer", K(ret));
        } else if (FALSE_IT(vector_buff.assign_buffer(vec_buff_ptr, buff_len))) {
        } else if (OB_FAIL(lob_mngr->build_lob_param(param, ctx_allocator, CS_TYPE_BINARY, 0, UINT64_MAX, timeout, lob))) {
          LOG_WARN("fail to build lob param", K(ret));
        } else if (OB_FAIL(lob_mngr->query(param, vector_buff))) {
          LOG_WARN("fail to do query vector", K(ret));
        } else if (OB_FAIL(text_result.lseek(vector_buff.length(), 0))) {
          LOG_WARN("result lseek failed", K(ret));
        } else {
          ObString res_str;
          text_result.get_result_buffer(res_str);
          expr_datum.set_string(res_str);
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
