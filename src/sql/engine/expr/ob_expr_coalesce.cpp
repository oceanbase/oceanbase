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
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_coalesce.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {
ObExprCoalesce::ObExprCoalesce(ObIAllocator& alloc)
    : ObExprOperator(alloc, T_FUN_SYS_COALESCE, N_COALESCE, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{}
ObExprCoalesce::~ObExprCoalesce()
{}

int ObExprCoalesce::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObLengthSemantics default_length_semantics =
      (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
  if (OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types", K(ret));
  } else if (OB_UNLIKELY(param_num < 1 || (lib::is_oracle_mode() && param_num <= 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no enough param", K(ret), K(param_num));
  } else if (OB_FAIL(aggregate_result_type_for_case(type,
                 types,
                 param_num,
                 type_ctx.get_coll_type(),
                 lib::is_oracle_mode(),
                 default_length_semantics,
                 type_ctx.get_session(),
                 true,
                 true))) {
    LOG_WARN("failed to agg resul type", K(ret));
  } else {
    const ObSQLSessionInfo* session = dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast basic session to sql session info failed", K(ret));
    } else {
      ObExprOperator::calc_result_flagN(type, types, param_num);
      ObObjType calc_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[type.get_type()]];
      bool is_expr_integer_type = (ob_is_int_tc(type.get_type()) || ob_is_uint_tc(type.get_type()));
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
        if (ob_is_enumset_tc(types[i].get_type())) {
          if (OB_UNLIKELY(ObMaxType == calc_type)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "invalid type of parameter ", K(i), K(ret));
          } else {
            types[i].set_calc_type(calc_type);
          }
        } else if (session->use_static_typing_engine()) {
          bool is_arg_integer_type = (ob_is_int_tc(types[i].get_type()) || ob_is_uint_tc(types[i].get_type()));
          if ((is_arg_integer_type && is_expr_integer_type) || ObNullType == types[i].get_type()) {
            // When the parameter is int/uint tc, the calc type cannot be set to the type of the result
            // eg: select coalesce(null, col_tinyint, col_mediumint) from t1;
            // Due to the processing of null by merge type,
            // the result type will change when the type is deduced multiple times
            types[i].set_calc_meta(types[i].get_obj_meta());
            types[i].set_calc_accuracy(types[i].get_accuracy());
          } else {
            types[i].set_calc_meta(type.get_obj_meta());
            types[i].set_calc_accuracy(type.get_accuracy());
          }
        }
      }
    }
  }
  return ret;
}

int ObExprCoalesce::calc_resultN(ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    LOG_WARN("null calc_buf_", K(expr_ctx.calc_buf_));
    ret = OB_NOT_INIT;
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    ret = calc(result, objs_stack, param_num, result_type_, cast_ctx);
  }
  return ret;
}

int ObExprCoalesce::calc(ObObj& result, const ObObj* objs_stack, int64_t param_num, const ObExprResType& expected_type,
    common::ObCastCtx& cast_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(objs_stack) || param_num < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for coalesce", K(objs_stack), K(param_num));
  } else if (OB_UNLIKELY(expected_type.is_invalid())) {
    LOG_WARN("invalid expect type", K(expected_type));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t result_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (ObNullType != objs_stack[i].get_type()) {
        result_idx = i;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      const ObObj* res_point = NULL;
      const ObObj& obj_tmp = objs_stack[result_idx];
      cast_ctx.dest_collation_ = expected_type.get_collation_type();
      EXPR_CAST_OBJ_V2(expected_type.get_type(), obj_tmp, res_point);
      if (OB_FAIL(ret) || OB_ISNULL(res_point)) {
        LOG_WARN("fail cast obj", K(expected_type.get_type()), K(ret));
      } else {
        result = *res_point;
        if (!result.is_null() && ob_is_string_type(expected_type.get_type())) {
          result.set_collation(expected_type);
        }
      }
    }
  }
  return ret;
}

int calc_coalesce_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  res_datum.set_null();
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObDatum* child_res = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, child_res))) {
      LOG_WARN("eval arg failed", K(ret), K(i));
    } else if (!(child_res->is_null())) {
      res_datum.set_datum(*child_res);
      break;
    }
  }
  return ret;
}

int ObExprCoalesce::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_coalesce_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
