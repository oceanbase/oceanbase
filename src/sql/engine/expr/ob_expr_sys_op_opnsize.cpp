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
#include "sql/engine/expr/ob_expr_sys_op_opnsize.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

int ObExprSysOpOpnsize::calc_result_type1(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_int();
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObExprSysOpOpnsize::calc_sys_op_opnsize_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                                 ObDatum &res)
{
	int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval param failed", K(ret));
  } else {
    int64_t size = 0;
    if (OB_FAIL(calc_sys_op_opnsize(expr.args_[0], arg, size))) {
      LOG_WARN("fail to cal sys_op_opnsize", K(ret));
    } else {
      res.set_int(size);
    }
  }
	return ret;
}

int ObExprSysOpOpnsize::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_sys_op_opnsize_expr;
  return ret;
}

int ObExprSysOpOpnsize::calc_sys_op_opnsize(const ObExpr *expr,const ObDatum *arg, int64_t &size) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else if (!is_lob_storage(expr->datum_meta_.type_) || arg->is_null()) {
    size = sizeof(*arg) + (arg->is_null() ? 0 : arg->len_);
  } else { // (texts except tiny, json, gis)
    ObLobLocatorV2 locator(arg->get_string(), expr->obj_meta_.has_lob_header());
    int64_t lob_data_byte_len = 0;
    if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
      LOG_WARN("get lob data byte length failed", K(ret), K(locator));
    } else {
      size = sizeof(*arg) + static_cast<int64_t>(lob_data_byte_len);
    }
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
