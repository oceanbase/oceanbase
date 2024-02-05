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

#include "ob_expr_extract_cert_expired_time.h"
#include "lib/utility/utility.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprExtractExpiredTime::ObExprExtractExpiredTime(common::ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_EXTRACT_CERT_EXPIRED_TIME,
                         N_EXTRACT_CERT_EXPIRED_TIME,
                         1,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}

ObExprExtractExpiredTime::~ObExprExtractExpiredTime() {}

int ObExprExtractExpiredTime::calc_result_type1(ObExprResType &type,
                              ObExprResType &text,
                              common::ObExprTypeCtx &type_ctx)  const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    type.set_null();
  } else {
    if (!is_type_valid(text.get_type())) {
      ret = OB_INVALID_ARGUMENT_NUM;
      LOG_WARN("the param is not castable", K(text), K(ret));
    } else {
      type.set_timestamp();
      type.set_scale(common::MAX_SCALE_FOR_TEMPORAL);
    }
  }
  return ret;
}

int ObExprExtractExpiredTime::eval_extract_cert_expired_time(const ObExpr &expr, ObEvalCtx &ctx,
                                                              ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.arg_cnt_ != 1)
      || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObDatum *arg = NULL;
    int64_t expired_time = 0;
    ObObjTypeClass in_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
    if (!ob_is_castable_type_class(in_tc)) {
      res.set_null();
    } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
      LOG_WARN("eval arg failed", K(ret));
    } else if (arg->is_null()) {
      res.set_null();
    } else if (in_tc != ObTextTC) {
      const ObString &arg_str = arg->get_string();
      if (OB_FAIL(extract_cert_expired_time(arg_str.ptr(), arg_str.length(), expired_time))) {
        LOG_WARN("failed to extract expired time", K(ret), K(*arg), K(in_tc));
      } else {
        res.set_timestamp(expired_time);
      }
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
      ObString text_str;
      if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_alloc,
                  0, arg, text_str))) {
        LOG_WARN("failed to read realdata", K(ret));
      } else if (OB_FAIL(extract_cert_expired_time(text_str.ptr(), text_str.length(), expired_time))) {
        LOG_WARN("failed to extract expired time", K(ret), K(*arg), K(in_tc));
      } else {
        res.set_timestamp(expired_time);
      }
    }
  }
  return ret;
}

int ObExprExtractExpiredTime::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_extract_cert_expired_time;
  return ret;
}

}
}
