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

#include "ob_expr_to_base64.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/encode/ob_base64_encode.h"
#include "lib/oblog/ob_log.h"
#include "objit/common/ob_item_type.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprToBase64::ObExprToBase64(ObIAllocator &alloc)
    :ObStringExprOperator(alloc, T_FUN_SYS_TO_BASE64, N_TO_BASE64, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprToBase64::~ObExprToBase64()
{
}

int ObExprToBase64::calc(ObObj &result, const ObObj &obj, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null allocator", K(ret), K(allocator));
  } else {
    const ObString & in_raw = obj.get_string();
    ObLength in_raw_len = in_raw.length();
    const char *buf = in_raw.ptr();
    int64_t buf_len = base64_needed_encoded_length(in_raw_len);
    if (OB_UNLIKELY(buf_len == 0)) {
      result.set_string(obj.get_type(), nullptr, 0);
    } else {
      int64_t pos = 0;
      char *output_buf = static_cast<char*>(allocator->alloc(buf_len));
      if (OB_ISNULL(output_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(buf_len), K(in_raw.length()));
      } else if (OB_FAIL(ObBase64Encoder::encode(reinterpret_cast<const uint8_t*>(buf),
                                                 in_raw_len, output_buf, buf_len,
                                                 pos, NUM_OF_CHAR_PER_LINE_QUOTED_PRINTABLE))) {
        LOG_WARN("failed to encode base64", K(ret));
      } else {
        result.set_string(obj.get_type(), output_buf, pos);
      }
    }
  }
  return ret;
}

int ObExprToBase64::calc_result_type1(ObExprResType &type,
                                      ObExprResType &str,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  str.set_calc_type(ObVarcharType);
  str.set_calc_collation_type(
    str.is_string_type() ? str.get_collation_type() : CS_TYPE_UTF8MB4_BIN);

  int64_t mbmaxlen = 0;
  int64_t max_result_length = 0;
  if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(str.get_collation_type(), mbmaxlen))) {
    LOG_WARN("fail to get mbmaxlen", K(str.get_collation_type()), K(ret));
  } else {
    max_result_length = (base64_needed_encoded_length(str.get_length()) - 1) * mbmaxlen;
    if (max_result_length > OB_MAX_BLOB_WIDTH) {
      max_result_length = OB_MAX_BLOB_WIDTH;
    }
    int64_t max_l = max_result_length / mbmaxlen;
    int64_t max_deduce_length = max_l * mbmaxlen;
    if (max_deduce_length < OB_MAX_MYSQL_VARCHAR_LENGTH) {
      type.set_varchar();
      type.set_length(max_deduce_length);
      type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));
    } else {
      type.set_blob();
      // TODO : Fixme the blob type do not need to set_length.
      // Maybe need wait ObDDLResolver::check_text_length fix the judge of length.
      type.set_length(max_deduce_length);
      type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));
    }
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }

  return ret;
}

int ObExprToBase64::eval_to_base64(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   ObDatum &res)
{
    int ret = OB_SUCCESS;
    ObDatum *arg = nullptr;

    if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
      LOG_WARN("eval arg failed", K(ret));
    } else if (OB_UNLIKELY(arg->is_null())) {
      res.set_null();
    } else {
      const ObString & in_raw = arg->get_string();
      ObLength in_raw_len = in_raw.length();
      const char *buf = in_raw.ptr();
      char *output_buf = nullptr;
      int64_t buf_len = base64_needed_encoded_length(in_raw_len);
      if (OB_UNLIKELY(buf_len == 0)) {
        res.set_string(nullptr, 0);
      } else {
        int64_t pos = 0;
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        output_buf = static_cast<char*>(alloc_guard.get_allocator().alloc(buf_len));
        if (OB_ISNULL(output_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(buf_len), K(in_raw.length()));
        } else if (OB_FAIL(ObBase64Encoder::encode(reinterpret_cast<const uint8_t*>(buf),
                                                   in_raw_len, output_buf, buf_len,
                                                   pos, NUM_OF_CHAR_PER_LINE_QUOTED_PRINTABLE))) {
          LOG_WARN("failed to encode base64", K(ret));
        } else {
          res.set_string(output_buf, pos);
          if (OB_FAIL(ObExprUtil::set_expr_ascii_result(
            expr, ctx, res, ObString(pos, output_buf)))) {
            LOG_WARN("set ASCII result failed", K(ret));
          }
        }
      }
    }

    return ret;
}

int ObExprToBase64::eval_to_base64_batch(const ObExpr &expr,
                         ObEvalCtx &ctx,
                         const ObBitVector &skip,
                         const int64_t batch_size) {
  int ret = OB_SUCCESS;
  ObDatum *res = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObDatumVector args = expr.args_[0]->locate_expr_datumvector(ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(batch_size);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      batch_info_guard.set_batch_idx(j);
      ObDatum *arg = args.at(j);
      const ObString &in_raw = arg->get_string();
      ObLength in_raw_len = in_raw.length();
      const char *buf = in_raw.ptr();
      char *output_buf = nullptr;
      int64_t buf_len = 0;
      if (arg->is_null()) {
        res[j].set_null();
      } else if (OB_UNLIKELY((buf_len = base64_needed_encoded_length(in_raw_len)) == 0)) {
        res[j].set_string(nullptr, 0);
      } else {
        int64_t pos = 0;
        output_buf = static_cast<char *>(alloc_guard.get_allocator().alloc(buf_len));
        if (OB_ISNULL(output_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(buf_len), K(in_raw.length()));
        } else if (OB_FAIL(ObBase64Encoder::encode(reinterpret_cast<const uint8_t *>(buf),
                                                   in_raw_len, output_buf, buf_len,
                                                   pos, NUM_OF_CHAR_PER_LINE_QUOTED_PRINTABLE))) {
          LOG_WARN("failed to encode base64", K(ret));
        } else {
          res[j].set_string(output_buf, pos);
          if (OB_FAIL(ObExprUtil::set_expr_ascii_result(
            expr, ctx, res[j], ObString(pos, output_buf)))) {
            LOG_WARN("set ASCII result failed", K(ret));
          }
        }
      }
      eval_flags.set(j);
    } // end for batch
  }
  return ret;
}

int ObExprToBase64::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
    UNUSED(expr_cg_ctx);
    UNUSED(raw_expr);
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprToBase64::eval_to_base64;
    rt_expr.eval_batch_func_ = ObExprToBase64::eval_to_base64_batch;
    return ret;
}

}//namespace sql
}//namespace oceanbase
