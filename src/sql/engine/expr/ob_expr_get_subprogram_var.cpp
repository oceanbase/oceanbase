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

#define USING_LOG_PREFIX  SQL_ENG

#include "ob_expr_get_subprogram_var.h"
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
using namespace pl;
namespace sql
{
int ObExprGetSubprogramVar::calc_result_typeN(ObExprResType &type,
                                              ObExprResType *types,
                                              int64_t param_num,
                                              ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  CK (OB_NOT_NULL(types));
  CK (OB_LIKELY(4 == param_num));
  if (OB_SUCC(ret)) {
    ObExprResType *result_type = reinterpret_cast<ObExprResType *>(types[3].get_param().get_int());
    type.set_type(result_type->get_type());
    type.set_udt_id(result_type->get_udt_id());
    type.set_extend_type(result_type->get_extend_type());
    if (ob_is_string_tc(result_type->get_type())) {
      type.set_length(result_type->get_length());
      type.set_length_semantics(result_type->get_length_semantics());
      type.set_collation_type(result_type->get_collation_type());
      type.set_collation_level(result_type->get_collation_level());
    } else if (ob_is_number_tc(result_type->get_type())) {
      type.set_precision(result_type->get_precision());
      type.set_scale(result_type->get_scale());
    } else if (ob_is_text_tc(result_type->get_type())) {
      type.set_length(result_type->get_length());
      type.set_collation_type(result_type->get_collation_type());
      type.set_collation_level(result_type->get_collation_level());
      type.set_scale(result_type->get_scale());
    } else if (ob_is_lob_tc(result_type->get_type())) {
      type.set_collation_type(result_type->get_collation_type());
      type.set_collation_level(result_type->get_collation_level());
    }
  }
  return ret;
}

int ObExprGetSubprogramVar::cg_expr(ObExprCGCtx &op_cg_ctx,
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  CK (OB_LIKELY(4 == rt_expr.arg_cnt_));
  OX (rt_expr.eval_func_ = &calc_get_subprogram_var);
  return ret;
}


int ObExprGetSubprogramVar::calc_get_subprogram_var(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int64_t ret = OB_SUCCESS;
  ObPLContext *pl_context = NULL;
  ObObjParam res_var;
  ObSQLSessionInfo *session_info = ctx.exec_ctx_.get_my_session();
  ObDatum *pkg_id_datum = NULL;
  ObDatum *subprogram_id_datum = NULL;
  ObDatum *var_idx_datum = NULL;
  CK (OB_NOT_NULL(session_info));
  CK (OB_LIKELY(4 == expr.arg_cnt_));
  CK (OB_NOT_NULL(pl_context = session_info->get_pl_context()));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, pkg_id_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, subprogram_id_datum)) ||
      OB_FAIL(expr.args_[2]->eval(ctx, var_idx_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else {
    uint64_t package_id = pkg_id_datum->get_uint64();
    uint64_t subprogram_id = subprogram_id_datum->get_uint64();
    int64_t var_idx = var_idx_datum->get_int();
    OZ (pl_context->get_subprogram_var_from_local(
    *session_info, package_id, subprogram_id, var_idx, res_var));
    OZ (expr_datum.from_obj(res_var));
    if (is_lob_storage(res_var.get_type())) {
      OZ (ob_adjust_lob_datum(res_var, expr.obj_meta_, ctx.exec_ctx_.get_allocator(), expr_datum));
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
