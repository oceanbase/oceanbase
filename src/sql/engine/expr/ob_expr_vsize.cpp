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
#include "sql/engine/expr/ob_expr_vsize.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "objit/common/ob_item_type.h"
#include "share/object/ob_obj_cast.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
}
}

ObExprVsize::ObExprVsize(ObIAllocator &alloc)
  :ObFuncExprOperator(alloc, T_FUN_SYS_VSIZE, N_VSIZE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprVsize::~ObExprVsize()
{
}

bool ObExprVsize::is_blob_type(const ObObj &input) const
{
  return input.meta_.is_clob()
            || input.meta_.is_blob()
            || input.meta_.is_lob();
}

int ObExprVsize::calc_result_type1(ObExprResType &type,
                                              ObExprResType &type1,
                                              common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type1);
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type1.is_lob())){
    // consistent to oracle error code;
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent datatypes.", K(ret));
  } else {
    type.set_number();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].precision_);
    type.set_calc_type(common::ObNumberType);
  }
  return ret;
}

int ObExprVsize::calc_vsize_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res.set_null();
  } else {
    number::ObNumber res_nmb;
    ObNumStackOnceAlloc res_alloc;
    ret = res_nmb.from(static_cast<uint64_t>(arg->len_), res_alloc);
    if (OB_FAIL(ret)) {
      LOG_WARN("generator vsize result failed.", K(ret));
    } else {
      res.set_number(res_nmb);
    }
  }
  return ret;
}

int ObExprVsize::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_vsize_expr;
  return ret;
}
