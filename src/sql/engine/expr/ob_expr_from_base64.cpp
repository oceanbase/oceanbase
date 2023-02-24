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

#include "ob_expr_from_base64.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/encode/ob_base64_encode.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObExprFromBase64::ObExprFromBase64(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_FROM_BASE64, N_FROM_BASE64, 1)
{}

ObExprFromBase64::~ObExprFromBase64()
{}

int ObExprFromBase64::calc(ObObj &result, const ObObj &obj, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null allocator", K(ret), K(allocator));
  } else {
    const ObString &in_raw = obj.get_string();
    ObLength in_raw_len = in_raw.length();
    if (OB_UNLIKELY(in_raw_len == 0)) {
      result.set_string(obj.get_type(), nullptr, 0);
    } else {
      const char *buf = in_raw.ptr();
      int64_t buf_len = base64_needed_decoded_length(in_raw_len);
      int64_t pos = 0;
      char *output_buf = static_cast<char *>(allocator->alloc(buf_len));
      if (OB_FAIL(ObBase64Encoder::decode(buf, in_raw_len, reinterpret_cast<uint8_t *>(output_buf), buf_len, pos, true))) {
        if (OB_UNLIKELY(ret == OB_INVALID_ARGUMENT)) {
          ret = OB_SUCCESS;
          result.set_null();
        } else {
          LOG_WARN("failed to decode base64", K(ret));
        }
      } else {
        result.set_string(obj.get_type(), output_buf, pos);
        result.set_collation_level(obj.get_collation_level());
        result.set_collation_type(obj.get_collation_type());
      }
    }
  }
  return ret;
}

int ObExprFromBase64::calc_result1(ObObj &result, const ObObj &obj, ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(obj.is_null())) {
    result.set_null();
  } else if (OB_FAIL(calc(result, obj, expr_ctx.calc_buf_))) {
    LOG_WARN("fail to calc function from_base64", K(ret), K(obj));
  } else {
    result.set_collation_level(obj.get_collation_level());
    result.set_collation_type(get_result_type().get_collation_type());
  }

  return ret;
}

int ObExprFromBase64::calc_result_type1(ObExprResType &type, ObExprResType &str, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  str.set_calc_type(ObVarcharType);
  str.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);

  int64_t mbmaxlen = 0;
  int64_t max_result_length = 0;
  if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(str.get_collation_type(), mbmaxlen))) {
    LOG_WARN("fail to get mbmaxlen", K(type.get_collation_type()), K(ret));
  } else {
    max_result_length = base64_needed_decoded_length(str.get_length()) * mbmaxlen;
    if (max_result_length > MAX_BLOB_WIDTH) {
      max_result_length = MAX_BLOB_WIDTH;
    }
    int64_t max_l = max_result_length / mbmaxlen;
    int64_t max_deduce_length = max_l * mbmaxlen;
    if (max_deduce_length < OB_MAX_MONITOR_INFO_LENGTH) {
      type.set_varbinary();
      type.set_length(max_deduce_length);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
    } else {
      type.set_blob();
      // TODO : Fixme the blob type do not need to set_length.
      // Maybe need wait ObDDLResolver::check_text_length fix the judge of length.
      type.set_length(max_deduce_length);
    }
  }

  return ret;
}

int ObExprFromBase64::eval_from_base64(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = nullptr;
  ObExecContext* exec_ctx = &ctx.exec_ctx_;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (OB_UNLIKELY(arg->is_null())) {
    res.set_null();
  } else {
    const ObString &in_raw = arg->get_string();
    ObLength in_raw_len = in_raw.length();
    const char *buf = in_raw.ptr();
    if (NULL == buf) {
      res.set_string(nullptr, 0);
    } else {
      char *output_buf = NULL;
      int64_t buf_len = base64_needed_decoded_length(in_raw_len);
      int64_t pos = 0;
      output_buf = static_cast<char *>(exec_ctx->get_allocator().alloc(buf_len));
      if (OB_FAIL(ObBase64Encoder::decode(buf, in_raw_len, reinterpret_cast<uint8_t *>(output_buf), buf_len, pos, true))) {
        if (OB_UNLIKELY(ret == OB_INVALID_ARGUMENT)) {
          ret = OB_SUCCESS;
          res.set_null();
        } else {
          LOG_WARN("failed to decode base64", K(ret));
        }
      } else {
        res.set_string(output_buf, pos);
        if (OB_FAIL(ObExprUtil::set_expr_ascii_result(expr, ctx, res, ObString(pos, output_buf)))) {
          LOG_WARN("set ASCII result failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprFromBase64::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = ObExprFromBase64::eval_from_base64;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
