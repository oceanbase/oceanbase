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
#include "sql/engine/expr/ob_expr_xor.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "share/datum/ob_datum.h"
#include "share/config/ob_server_config.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{


ObExprXor::ObExprXor(ObIAllocator &alloc)
    : ObLogicalExprOperator(alloc, T_OP_XOR, N_XOR, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION)
{
  //not sure about the op name here
}

int ObExprXor::calc_result_typeN(ObExprResType &type,
                                 ObExprResType *types_stack,
                                 int64_t param_num,
                                 ObExprTypeCtx &type_ctx) const
{
  UNUSED(types_stack);
  UNUSED(param_num);
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  //just keep enumset as origin
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  //for expr and/or/xor, we depend on expr bool to get correct params, 
  //no need to set calc type
  return ret;
}

int ObExprXor::cg_expr(ObExprCGCtx &op_cg_ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);

  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 2 || rt_expr.type_ != T_OP_XOR)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    rt_expr.eval_func_ = &eval_xor;
  }
  return ret;
}

int ObExprXor::eval_xor(const ObExpr &expr,
                        ObEvalCtx &ctx,
                        ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr.args_)
      || OB_UNLIKELY(expr.arg_cnt_ < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    // xor支持短路，如果有null，直接返回null
    ObDatum *param = NULL;
    bool found_null = false;
    bool cur_bool_v = false;
    if (OB_ISNULL(expr.args_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(expr.args_[0]));
    } else if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
      LOG_WARN("failed to eval", K(ret));
    } else if (OB_ISNULL(param)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param", K(ret), K(param));
    } else if (param->is_null()) {
      expr_datum.set_null();
      found_null = true;
    } else {
      cur_bool_v = (param->get_int() != 0);
    }

    for (int i = 1; OB_SUCC(ret) && !found_null && i < expr.arg_cnt_; i++) {
      if (OB_ISNULL(expr.args_[i])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(i));
      } else if (OB_FAIL(expr.args_[i]->eval(ctx, param))) {
        LOG_WARN("failed to eval param", K(ret));
      } else if (OB_ISNULL(param)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid param", K(ret), K(param));
      } else if (param->is_null()) {
        expr_datum.set_null();
        found_null = true;
      } else {
        cur_bool_v = cur_bool_v ^ ((param->get_int() != 0));
      }
    }
    if (OB_SUCC(ret) && !found_null) {
      expr_datum.set_int(cur_bool_v);
    }
  }
  return ret;
}

// int ObExprXor::eval_xor()


}
}

