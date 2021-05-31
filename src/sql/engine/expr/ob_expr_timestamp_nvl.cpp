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

#include "sql/engine/expr/ob_expr_timestamp_nvl.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprTimestampNvl::ObExprTimestampNvl(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_TIMESTAMP_NVL, N_TIMESTAMP_NVL, 2)
{}

ObExprTimestampNvl::~ObExprTimestampNvl()
{}

int ObExprTimestampNvl::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(type2);
  if (is_oracle_mode()) {
    type.set_type(ObTimestampNanoType);
  } else {
    type.set_type(ObTimestampType);
  }
  type.set_accuracy(type1.get_accuracy());
  type.set_collation_level(type1.get_collation_level());
  type.set_collation_type(type1.get_collation_type());

  const ObSQLSessionInfo* session = static_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  if (OB_ISNULL(session)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret));
  } else if (session->use_static_typing_engine()) {
    type1.set_calc_meta(type.get_obj_meta());
    type2.set_calc_meta(type.get_obj_meta());
  }
  return ret;
}

int ObExprTimestampNvl::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (obj1.is_null()) {
    result = obj2;
  } else {
    result = obj1;
  }

  if (false == result.is_null()) {
    ObObjType res_type = get_result_type().get_type();
    EXPR_DEFINE_CAST_CTX(expr_ctx, expr_ctx.cast_mode_);
    if (OB_FAIL(ObObjCaster::to_type(res_type, cast_ctx, result, result))) {
      LOG_WARN("convert result to timestamp failed", K(res_type), K(result), K(ret));
    }
  }
  return ret;
}

int ObExprTimestampNvl::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("timestampnvl expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of timestampnvl expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprTimestampNvl::calc_timestampnvl;
  }
  return ret;
}

int ObExprTimestampNvl::calc_timestampnvl(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* param_datum1 = NULL;
  ObDatum* param_datum2 = NULL;
  bool oracle_mode = is_oracle_mode();
  if (OB_FAIL(expr.eval_param_value(ctx, param_datum1, param_datum2))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (param_datum1->is_null()) {
    if (param_datum2->is_null()) {
      expr_datum.set_null();
    } else if (oracle_mode) {
      expr_datum.set_otimestamp_tiny(param_datum2->get_otimestamp_tiny());
    } else {
      expr_datum.set_timestamp(param_datum2->get_timestamp());
    }
  } else if (oracle_mode) {
    expr_datum.set_otimestamp_tiny(param_datum1->get_otimestamp_tiny());
  } else {
    expr_datum.set_timestamp(param_datum1->get_timestamp());
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
