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

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_server_struct.h"
#include "lib/time/ob_time_utility.h"
#include "sql/engine/expr/ob_expr_embedded_vec.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObExprEmbeddedVec::ObExprEmbeddedVec(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_EMBEDDED_VEC, N_EMBEDDED_VEC, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprEmbeddedVec::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *types,
                                         int64_t param_num,
                                         ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(param_num);
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObDataType elem_type;
  elem_type.meta_.set_float();
  uint16_t subschema_id;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (!types[0].is_null() && !types[0].is_string_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input types for vec vector expr", K(ret), K(types[0]));
  } else if (types[0].is_null()) {
    type.is_null();
  } else {
    // If input is string/lob type (VARCHAR, TEXT, etc.), convert to vector type
    if (OB_FAIL(exec_ctx->get_subschema_id_by_collection_elem_type(ObNestedType::OB_VECTOR_TYPE,
                                                                   elem_type, subschema_id))) {
      LOG_WARN("failed to get vector subschema id", K(ret), K(types[0]));
    } else {
      LOG_WARN("exec ctx is null", K(ret));
      type.set_collection(subschema_id);
    }
  }
  return ret;
}

int ObExprEmbeddedVec::calc_resultN(ObObj &result,
                                  const ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  return OB_NOT_SUPPORTED;
}

int ObExprEmbeddedVec::cg_expr(
    ObExprCGCtx &expr_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 6 && rt_expr.arg_cnt_ != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(rt_expr.arg_cnt_), KP(rt_expr.args_), K(rt_expr.type_));
  } else if (OB_UNLIKELY(rt_expr.arg_cnt_ == 6 && OB_ISNULL(rt_expr.args_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, rt_expr.args_ is nullptr", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = generate_embedded_vec;
  }
  return ret;
}

/*static*/ int ObExprEmbeddedVec::generate_embedded_vec(
    const ObExpr &raw_ctx,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *data_datum = nullptr;
  ObDatum *model_datum = NULL;
  ObDatum *url_datum = NULL;
  ObDatum *user_key_datum = NULL;
  ObDatum *sync_mode_datum = NULL;
  ObDatum *dim_datum = NULL;
  if (raw_ctx.arg_cnt_ == 1) {
    if (OB_FAIL(raw_ctx.args_[0]->eval(eval_ctx, data_datum))) {
      LOG_WARN("failed to eval data parameter", K(ret));
    } else {
      expr_datum.set_string(data_datum->get_string());
    }
  } else if (raw_ctx.arg_cnt_ == 6) {
    if (OB_FAIL(raw_ctx.args_[0]->eval(eval_ctx, data_datum))) {
      LOG_WARN("failed to eval data parameter", K(ret));
    } else if (OB_FAIL(raw_ctx.args_[1]->eval(eval_ctx, model_datum))) {
      LOG_WARN("failed to eval model parameter", K(ret));
    } else if (OB_FAIL(raw_ctx.args_[2]->eval(eval_ctx, url_datum))) {
      LOG_WARN("failed to eval url parameter", K(ret));
    } else if (OB_FAIL(raw_ctx.args_[3]->eval(eval_ctx, user_key_datum))) {
      LOG_WARN("failed to eval user_key parameter", K(ret));
    } else if (OB_FAIL(raw_ctx.args_[4]->eval(eval_ctx, sync_mode_datum))) {
      LOG_WARN("failed to eval sync_mode parameter", K(ret));
    } else if (OB_FAIL(raw_ctx.args_[5]->eval(eval_ctx, dim_datum))) {
      LOG_WARN("failed to eval dim parameter", K(ret));
    } else if (OB_ISNULL(data_datum) || OB_ISNULL(model_datum) || OB_ISNULL(url_datum) || OB_ISNULL(user_key_datum) || OB_ISNULL(sync_mode_datum) || OB_ISNULL(dim_datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null datum", K(ret));
    } else if (data_datum->is_null() || model_datum->is_null() || url_datum->is_null() || user_key_datum->is_null() || sync_mode_datum->is_null() || dim_datum->is_null()) {
      // If any parameter is null, return null
      expr_datum.set_null();
    } else {
      expr_datum.set_string(data_datum->get_string());
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument count", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
